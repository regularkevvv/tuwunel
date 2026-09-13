mod adopt;
pub mod association;

use std::{
	iter::once,
	sync::Arc,
	time::{Duration, SystemTime},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as b64};
use derive_more::Debug;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use ruma::{OwnedDeviceId, OwnedUserId, UserId};
use serde::{Deserialize, Serialize};
use tuwunel_core::{
	Err, Result, at, err, implement, redacted_debug,
	utils::{
		MutexMap,
		hash::sha256,
		stream::{IterStream, ReadyExt, TryExpect},
		timepoint_from_now,
	},
	warn,
};
use tuwunel_database::{Cbor, Database, Deserialized, Handle, Map};
use url::Url;

pub use self::adopt::Counts;
use super::{
	Provider, Providers, TokenResponse, UserInfo,
	seal::{Keys, Material, Sealed},
	unique_id as session_unique_id,
};
use crate::SelfServices;

pub struct Sessions {
	services: SelfServices,

	/// Serializes probes and writes for each unique identity.
	///
	/// Transaction batches cannot conditionally claim an index key. Each
	/// identity therefore has an independent critical section. Updates made
	/// through [`Sessions::update`] run inside it as well — including the
	/// upstream refresh exchange — so two devices of one identity cannot race
	/// on a rotating upstream refresh token, and a re-check that read a grant
	/// cannot restore it after a logout cleared it.
	write_locks: MutexMap<String, ()>,

	providers: Arc<Providers>,
	keys: Keys,
	db: Data,
}

struct Data {
	oauthid_session: Arc<Map>,
	oauthidpuserid_pendingclaims: Arc<Map>,
	oauthuniqid_oauthid: Arc<Map>,
	userid_oauthid: Arc<Map>,
	database: Arc<Database>,
}

/// Persistent state for one upstream OAuth authorization.
///
/// The record carries provider, redirect, PKCE, nonce, and token data across
/// the authorization flow. Once linked, it also associates the provider
/// identity with a Matrix user, and once its login token is redeemed, it is
/// bound to the Matrix device that sign-in created.
///
/// Its `Debug` redacts every credential it holds. The record is recorded into
/// tracing spans at `info` level, and no access token, refresh token, ID
/// token, PKCE verifier or nonce may reach a log (plan.md, non-negotiable
/// invariant 7).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Session {
	/// Identity Provider ID (the `client_id` in the configuration) associated
	/// with this session.
	pub idp_id: Option<String>,

	/// Session ID used as the index key for this session itself.
	pub sess_id: Option<SessionId>,

	/// Token type (bearer, mac, etc).
	pub token_type: Option<String>,

	/// Access token to the provider.
	#[debug("{}", redacted_debug!(access_token))]
	pub access_token: Option<String>,

	/// OIDC ID token returned by the provider.
	#[debug("{}", redacted_debug!(id_token))]
	pub id_token: Option<String>,

	/// Duration in seconds the access_token is valid for.
	pub expires_in: Option<u64>,

	/// Point in time that the access_token expires.
	pub expires_at: Option<SystemTime>,

	/// Token used to refresh the access_token.
	#[debug("{}", redacted_debug!(refresh_token))]
	pub refresh_token: Option<String>,

	/// Duration in seconds the refresh_token is valid for
	pub refresh_token_expires_in: Option<u64>,

	/// Point in time that the refresh_token expires.
	pub refresh_token_expires_at: Option<SystemTime>,

	/// Access scope actually granted (if supported).
	pub scope: Option<String>,

	/// Redirect URL
	pub redirect_url: Option<Url>,

	/// Challenge preimage
	#[debug("{}", redacted_debug!(code_verifier))]
	pub code_verifier: Option<String>,

	/// Random string passed exclusively in the grant session cookie.
	#[debug("{}", redacted_debug!(cookie_nonce))]
	pub cookie_nonce: Option<String>,

	/// Random single-use string passed in the provider redirect.
	#[debug("{}", redacted_debug!(query_nonce))]
	pub query_nonce: Option<String>,

	/// Point in time the authorization grant session expires.
	pub authorize_expires_at: Option<SystemTime>,

	/// Associated User Id registration.
	pub user_id: Option<OwnedUserId>,

	/// Last userinfo response persisted here.
	pub user_info: Option<UserInfo>,

	/// Matrix device this grant authorizes.
	///
	/// Set when the login token the grant's callback issued is redeemed. A
	/// grant obtained before grants were bound to devices has none, and backs
	/// every device of its user that holds no grant of its own.
	pub device_id: Option<OwnedDeviceId>,

	/// Hash of the login token the grant's callback issued, until that token
	/// is redeemed and the grant is bound to its device.
	#[debug("{}", redacted_debug!(login_token_hash))]
	pub login_token_hash: Option<String>,

	/// When the login token was issued, or the grant bound to its device.
	pub bound_at: Option<SystemTime>,

	/// The token material sealed at rest. Only the persisted form carries it:
	/// reading a record opens it into the token fields above.
	#[debug("{}", redacted_debug!(sealed))]
	pub sealed: Option<Sealed>,

	/// Identifier of the key the material was sealed with when this record was
	/// read, `None` when it was stored unsealed. Never persisted.
	#[serde(skip)]
	pub sealed_with: Option<String>,

	/// The record carries sealed material no configured key opens. Never
	/// persisted: it describes this process's keys, not the record.
	#[serde(skip)]
	pub grant_unreadable: bool,
}

impl Session {
	/// Fold a provider token response into this session.
	///
	/// Fields the response omits keep their stored value. RFC 6749 §5.1 lets a
	/// refresh response leave out `scope` (identical to the request) and
	/// `refresh_token` (the presented one stays valid), so overwriting them
	/// unconditionally would discard the grant on the first refresh.
	pub fn apply_token_response(self, token: TokenResponse) -> Result<Self> {
		let expires_at = token
			.expires_in
			.map(Duration::from_secs)
			.map(timepoint_from_now)
			.transpose()?
			.or(self.expires_at);

		let refresh_token_expires_at = token
			.refresh_token_expires_in
			.map(Duration::from_secs)
			.map(timepoint_from_now)
			.transpose()?
			.or(self.refresh_token_expires_at);

		Ok(Self {
			scope: token.scope.or(self.scope),
			token_type: token.token_type.or(self.token_type),
			access_token: token.access_token.or(self.access_token),
			id_token: token.id_token.or(self.id_token),
			refresh_token: token.refresh_token.or(self.refresh_token),
			expires_in: token.expires_in.or(self.expires_in),
			refresh_token_expires_in: token
				.refresh_token_expires_in
				.or(self.refresh_token_expires_in),
			expires_at,
			refresh_token_expires_at,
			..self
		})
	}

	/// Whether this record holds upstream token material, readable or not.
	#[must_use]
	pub fn has_grant(&self) -> bool {
		self.grant_unreadable
			|| self.access_token.is_some()
			|| self.refresh_token.is_some()
			|| self.id_token.is_some()
	}

	/// This record without its upstream grant: token material, one-time
	/// authorization state and device binding are dropped; the provider,
	/// identity and user association are kept.
	#[must_use]
	pub fn cleared(self) -> Self {
		Self {
			access_token: None,
			refresh_token: None,
			id_token: None,
			expires_at: None,
			expires_in: None,
			refresh_token_expires_at: None,
			refresh_token_expires_in: None,
			token_type: None,
			code_verifier: None,
			query_nonce: None,
			cookie_nonce: None,
			authorize_expires_at: None,
			device_id: None,
			login_token_hash: None,
			bound_at: None,
			sealed: None,
			sealed_with: None,
			grant_unreadable: false,
			..self
		}
	}

	fn material(&self) -> Material {
		Material {
			access_token: self.access_token.clone(),
			refresh_token: self.refresh_token.clone(),
			id_token: self.id_token.clone(),
		}
	}
}

/// Session Identifier type.
pub type SessionId = String;

/// Number of characters generated for our code_verifier. The code_verifier is a
/// random string which must be between 43 and 128 characters.
pub const CODE_VERIFIER_LENGTH: usize = 64;

/// Number of characters we will generate for the Session ID.
pub const SESSION_ID_LENGTH: usize = 32;

/// Stored form of a login token bound to a grant: its SHA-256, so the record
/// never holds a redeemable token.
#[must_use]
pub fn login_token_hash(token: &str) -> String { b64.encode(sha256::hash(token.as_bytes())) }

#[implement(Sessions)]
pub(super) fn build(args: &crate::Args<'_>, providers: Arc<Providers>) -> Result<Self> {
	let config = &args.server.config;
	let keys = Keys::new(config.oauth_grant_key.as_deref(), &config.oauth_grant_previous_keys)?;

	if keys.current_kid().is_none() && !config.identity_provider.is_empty() {
		warn!(
			"oauth_grant_key is not set, so upstream OAuth grants are stored unsealed. Set it \
			 wherever an identity provider is used in a deployment."
		);
	}

	Ok(Self {
		services: args.services.clone(),
		write_locks: MutexMap::new(),
		providers,
		keys,
		db: Data {
			oauthid_session: args.db["oauthid_session"].clone(),
			oauthidpuserid_pendingclaims: args.db["oauthidpuserid_pendingclaims"].clone(),
			oauthuniqid_oauthid: args.db["oauthuniqid_oauthid"].clone(),
			userid_oauthid: args.db["userid_oauthid"].clone(),
			database: args.db.clone(),
		},
	})
}

/// Identifier of the key grants are sealed with now, if one is configured.
#[implement(Sessions)]
#[must_use]
pub fn current_kid(&self) -> Option<&str> { self.keys.current_kid() }

/// Delete database state for the session.
///
/// The canonical session and every association index that still refers to it
/// commit together. A failed write is logged, never a panic.
#[implement(Sessions)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn delete(&self, sess_id: &str) {
	let (session, unique_id, _write_guard) = loop {
		let Ok(snapshot) = self.get(sess_id).await else {
			return;
		};

		let unique_id = self.identity_of(&snapshot).await;
		let write_guard = match unique_id.as_deref() {
			| Some(unique_id) => Some(self.write_locks.lock(unique_id).await),
			| None => None,
		};

		let Ok(session) = self.get(sess_id).await else {
			return;
		};

		if session.idp_id.as_deref() != snapshot.idp_id.as_deref() {
			continue;
		}

		if self.identity_of(&session).await == unique_id {
			break (session, unique_id, write_guard);
		}
	};

	// Preserve a unique identity association updated to a newer session.
	let unique_id = async {
		let unique_id = unique_id.as_deref()?;
		let assoc_id = self
			.get_sess_id_by_unique_id(unique_id)
			.map(Result::ok)
			.await?;

		(assoc_id == sess_id).then_some(unique_id)
	}
	.await;

	let user_sessions = async {
		let user_id = session.user_id.as_deref()?;
		let sess_ids: Vec<_> = self
			.get_sess_id_by_user(user_id)
			.ready_filter_map(Result::ok)
			.ready_filter(|assoc_id| assoc_id != sess_id)
			.collect()
			.await;

		Some((user_id, sess_ids))
	}
	.await;

	let mut txn = self.db.database.txn();

	if let Some((user_id, sess_ids)) = user_sessions {
		if !sess_ids.is_empty() {
			txn.raw_put(&self.db.userid_oauthid, user_id, sess_ids);
		} else {
			txn.del_raw(&self.db.userid_oauthid, user_id);
		}
	}

	if let Some(unique_id) = unique_id {
		txn.del_raw(&self.db.oauthuniqid_oauthid, unique_id);
	}

	txn.del_raw(&self.db.oauthid_session, sess_id);

	if let Err(error) = txn.execute().await {
		warn!(%sess_id, "Upstream grant record could not be deleted: {error}");
	}
}

/// Create or overwrite database state for the session.
///
/// The canonical session and its available identity and user indexes commit
/// together.
#[implement(Sessions)]
#[tracing::instrument(level = "info", skip(self))]
pub async fn put(&self, session: &Session) -> Result {
	let unique_id = self.identity_of(session).await;

	let _write_guard = match unique_id.as_deref() {
		| Some(unique_id) => Some(self.write_locks.lock(unique_id).await),
		| None => None,
	};

	self.put_locked(session, unique_id.as_deref())
		.await
}

/// Build and commit a session while exclusively claiming its identity key.
///
/// The callback's identity lookup and user selection remain ordered with bulk
/// adoption until the canonical session and indexes have committed.
#[implement(Sessions)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn commit_identity_session<T, F, Fut>(
	&self,
	unique_id: &str,
	build: F,
) -> Result<(Session, T, Option<SessionId>)>
where
	T: Send,
	F: FnOnce(Option<OwnedUserId>) -> Fut + Send,
	Fut: Future<Output = Result<(Session, T)>> + Send,
{
	let write_guard = self.write_locks.lock(unique_id).await;
	let existing = match self.get_by_unique_id(unique_id).await {
		| Ok(session) => Some(session),
		| Err(e) if e.is_not_found() => None,
		| Err(e) => return Err(e),
	};

	let old_sess_id = existing
		.as_ref()
		.and_then(|session| session.sess_id.clone());

	let old_user_id = existing.and_then(|session| session.user_id);
	let (session, value) = build(old_user_id).await?;

	self.put_locked(&session, Some(unique_id)).await?;
	drop(write_guard);

	Ok((session, value, old_sess_id))
}

/// Apply `update` to the current record of `sess_id` inside its identity's
/// critical section, and persist the record it returns.
///
/// The record is read again once the lock is held, so `update` always acts on
/// the latest committed state, and it may make network requests. The identity
/// index is left untouched: an update never makes a record its identity's
/// current one. Returns `None` when the record does not exist.
#[implement(Sessions)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn update<T, F, Fut>(&self, sess_id: &str, update: F) -> Result<Option<T>>
where
	T: Send,
	F: FnOnce(Session) -> Fut + Send,
	Fut: Future<Output = Result<(Option<Session>, T)>> + Send,
{
	let (session, _write_guard) = loop {
		let snapshot = match self.get(sess_id).await {
			| Ok(snapshot) => snapshot,
			| Err(e) if e.is_not_found() => return Ok(None),
			| Err(e) => return Err(e),
		};

		let unique_id = self.identity_of(&snapshot).await;
		let write_guard = match unique_id.as_deref() {
			| Some(unique_id) => Some(self.write_locks.lock(unique_id).await),
			| None => None,
		};

		let session = match self.get(sess_id).await {
			| Ok(session) => session,
			| Err(e) if e.is_not_found() => return Ok(None),
			| Err(e) => return Err(e),
		};

		if self.identity_of(&session).await == unique_id {
			break (session, write_guard);
		}
	};

	let (updated, value) = update(session).await?;

	if let Some(updated) = updated {
		self.put_locked(&updated, None).await?;
	}

	Ok(Some(value))
}

/// Record the login token a grant's callback issued, so its redemption can
/// bind the grant to the device it creates.
#[implement(Sessions)]
pub async fn bind_login_token(&self, sess_id: &str, token: &str) -> Result {
	let hash = login_token_hash(token);

	self.update(sess_id, async move |session| {
		let session = Session {
			login_token_hash: Some(hash),
			bound_at: Some(SystemTime::now()),
			..session
		};

		Ok((Some(session), ()))
	})
	.await?
	.ok_or_else(|| err!(Request(NotFound("The sign-in's grant record no longer exists."))))
}

/// The record of `user_id` whose unredeemed login token is `token`, if any.
#[implement(Sessions)]
pub async fn find_login_token(&self, user_id: &UserId, token: &str) -> Result<Option<Session>> {
	let hash = login_token_hash(token);
	let sessions: Vec<Session> = self.get_by_user(user_id).try_collect().await?;

	Ok(sessions
		.into_iter()
		.find(|session| session.login_token_hash.as_deref() == Some(hash.as_str())))
}

/// Whether this record is the one its identity's association index names.
///
/// That record is what maps `(iss, sub)` to the account; deleting it would
/// make the next sign-in provision a second account. An unreadable index is
/// answered `true`, so cleanup never deletes on doubt.
#[implement(Sessions)]
pub async fn is_identity_record(&self, session: &Session) -> bool {
	let (Some(unique_id), Some(sess_id)) =
		(self.identity_of(session).await, session.sess_id.as_deref())
	else {
		return false;
	};

	match self.get_sess_id_by_unique_id(&unique_id).await {
		| Ok(current) => current == sess_id,
		| Err(e) if e.is_not_found() => false,
		| Err(_) => true,
	}
}

#[implement(Sessions)]
async fn identity_of(&self, session: &Session) -> Option<String> {
	let idp_id = session.idp_id.as_deref()?;
	let provider = self.providers.get(idp_id).map(Result::ok).await?;

	session_unique_id((&provider, session)).ok()
}

#[implement(Sessions)]
async fn put_locked(&self, session: &Session, unique_id: Option<&str>) -> Result {
	let Some(sess_id) = session.sess_id.as_deref() else {
		return Err!(Request(InvalidParam("A grant record needs a session id to be stored.")));
	};

	let stored = self.sealed_form(sess_id, session)?;

	let user_sessions = async {
		let user_id = session.user_id.as_deref()?;
		let sess_ids = self
			.get_sess_id_by_user(user_id)
			.ready_filter_map(Result::ok)
			.chain(once(sess_id.to_owned()).stream())
			.collect::<Vec<_>>()
			.map(|mut ids| {
				ids.sort_unstable();
				ids.dedup();
				ids
			})
			.await;

		Some((user_id, sess_ids))
	}
	.await;

	let mut txn = self.db.database.txn();

	txn.raw_put(&self.db.oauthid_session, sess_id, Cbor(&stored));

	if let Some(unique_id) = unique_id {
		txn.insert_raw(&self.db.oauthuniqid_oauthid, unique_id, sess_id);
	}

	if let Some((user_id, sess_ids)) = user_sessions {
		txn.raw_put(&self.db.userid_oauthid, user_id, sess_ids);
	}

	txn.execute().await?;

	Ok(())
}

/// The persisted form of `session`: token material sealed under the current
/// key when one is configured, or unchanged when none is.
///
/// A record whose sealed material this process could not open keeps that
/// material as it was rather than having it overwritten with nothing, so a
/// missing key never destroys a grant that the right key would still open.
#[implement(Sessions)]
fn sealed_form(&self, sess_id: &str, session: &Session) -> Result<Session> {
	let material = session.material();

	if material.is_empty() {
		let sealed = session
			.grant_unreadable
			.then(|| session.sealed.clone())
			.flatten();

		return Ok(Session { sealed, ..session.clone() });
	}

	Ok(match self.keys.seal(sess_id, &material)? {
		| None => Session { sealed: None, ..session.clone() },
		| Some(sealed) => Session {
			access_token: None,
			refresh_token: None,
			id_token: None,
			sealed: Some(sealed),
			..session.clone()
		},
	})
}

/// Open a stored record: sealed material becomes the token fields again. A
/// record no configured key opens is marked unreadable and keeps its sealed
/// material.
#[implement(Sessions)]
fn opened(&self, sess_id: &str, mut session: Session) -> Session {
	let Some(sealed) = session.sealed.take() else {
		return session;
	};

	match self.keys.open(sess_id, &sealed) {
		| Ok(material) => Session {
			access_token: material.access_token,
			refresh_token: material.refresh_token,
			id_token: material.id_token,
			sealed_with: Some(sealed.kid),
			grant_unreadable: false,
			..session
		},
		| Err(error) => {
			warn!(%sess_id, kid = %sealed.kid, "Stored upstream grant could not be opened: {error}");

			Session {
				sealed: Some(sealed),
				grant_unreadable: true,
				..session
			}
		},
	}
}

/// Fetch database state for a session from its associated `(iss,sub)`, in case
/// `sess_id` is not known.
#[implement(Sessions)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "debug"))]
pub async fn get_by_unique_id(&self, unique_id: &str) -> Result<Session> {
	self.get_sess_id_by_unique_id(unique_id)
		.and_then(async |sess_id| self.get(&sess_id).await)
		.await
}

/// Fetch database state for one or more sessions from its associated `user_id`,
/// in case `sess_id` is not known.
#[implement(Sessions)]
#[tracing::instrument(level = "debug", skip(self))]
pub fn get_by_user(&self, user_id: &UserId) -> impl Stream<Item = Result<Session>> + Send {
	self.get_sess_id_by_user(user_id)
		.and_then(async |sess_id| self.get(&sess_id).await)
}

/// Fetch database state for a session from its `sess_id`.
#[implement(Sessions)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "debug"))]
pub async fn get(&self, sess_id: &str) -> Result<Session> {
	self.db
		.oauthid_session
		.get(sess_id)
		.await
		.deserialized::<Cbor<_>>()
		.map(at!(0))
		.map(|session| self.opened(sess_id, session))
}

/// Resolve the `sess_id` associations with a `user_id`.
#[implement(Sessions)]
#[tracing::instrument(level = "debug", skip(self))]
pub fn get_sess_id_by_user(&self, user_id: &UserId) -> impl Stream<Item = Result<String>> + Send {
	self.db
		.userid_oauthid
		.get(user_id)
		.map(association_ids)
		.map_ok(Vec::into_iter)
		.map_ok(IterStream::try_stream)
		.try_flatten_stream()
}

/// Only a missing user association index means there is no upstream grant.
/// Missing referenced sessions/providers and undecodable or unreadable index
/// values are failures, not evidence that the user is exempt from rechecking.
pub(super) fn association_ids(result: Result<Handle<'_>>) -> Result<Vec<String>> {
	match result {
		| Err(error) if error.is_not_found() => Ok(Vec::new()),
		| result => result.deserialized(),
	}
}

/// Resolve the `sess_id` from an associated provider issuer and subject hash.
#[implement(Sessions)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "debug"))]
pub async fn get_sess_id_by_unique_id(&self, unique_id: &str) -> Result<String> {
	self.db
		.oauthuniqid_oauthid
		.get(unique_id)
		.await
		.deserialized()
}

#[implement(Sessions)]
pub fn users(&self) -> impl Stream<Item = OwnedUserId> + Send {
	self.db
		.userid_oauthid
		.keys()
		.expect_ok()
		.map(UserId::to_owned)
}

#[implement(Sessions)]
pub fn stream(&self) -> impl Stream<Item = Session> + Send {
	self.db
		.oauthid_session
		.stream()
		.expect_ok()
		.map(|(sess_id, session): (&str, Cbor<Session>)| self.opened(sess_id, session.0))
}

#[implement(Sessions)]
pub async fn provider(&self, session: &Session) -> Result<Provider> {
	let Some(idp_id) = session.idp_id.as_deref() else {
		return Err!(Request(NotFound("No provider for this session")));
	};

	self.providers.get(idp_id).await
}
