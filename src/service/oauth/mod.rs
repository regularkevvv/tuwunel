pub mod id_token;
pub mod providers;
pub mod seal;
pub mod server;
pub mod sessions;
#[cfg(test)]
mod tests;
pub mod token_response;
pub mod user_info;

use std::{
	collections::HashMap,
	net::IpAddr,
	sync::{Arc, Mutex},
	time::{Duration, Instant, SystemTime},
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as b64encode};
use futures::{Stream, StreamExt, TryStreamExt};
use http::StatusCode;
use reqwest::{
	Method,
	header::{ACCEPT, CONTENT_TYPE},
};
use ruma::{
	DeviceId, OwnedDeviceId, UserId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use serde::Serialize;
use serde_json::Value as JsonValue;
use tuwunel_core::{
	Err, Error, Result, debug_warn, err, implement, info,
	itertools::Itertools,
	utils::{hash::sha256, result::LogErr, stream::ReadyExt, time::timepoint_has_passed},
	warn,
};
use url::Url;

pub use self::{
	id_token::{IdTokenClaims, verify as verify_id_token_claims},
	providers::{Provider, ProviderId},
	server::Server,
	sessions::{CODE_VERIFIER_LENGTH, SESSION_ID_LENGTH, Session, SessionId, login_token_hash},
	token_response::TokenResponse,
	user_info::UserInfo,
};
use self::{providers::Providers, sessions::Sessions};
use crate::{SelfServices, client::read_response_capped};

/// Per-client-IP token-bucket table: last-refill instant and remaining tokens.
type Ratelimiter = Mutex<HashMap<IpAddr, (Instant, f64)>>;

pub struct Service {
	services: SelfServices,
	pub providers: Arc<Providers>,
	pub sessions: Arc<Sessions>,
	pub server: Option<Arc<Server>>,
	ratelimiter: Ratelimiter,
	device_ratelimiter: Ratelimiter,
}

/// How often stored grants are maintained (resealed, and cleaned up).
const MAINTENANCE_INTERVAL: Duration = Duration::from_hours(1);

/// Most records one maintenance pass examines, bounding its storage reads.
const MAINTENANCE_BATCH: usize = 4096;

/// How long past the login-token lifetime an unredeemed grant is kept.
const UNREDEEMED_GRACE: Duration = Duration::from_mins(10);

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		let providers = Arc::new(Providers::build(args));
		let sessions = Arc::new(Sessions::build(args, providers.clone())?);
		let server = Server::build(args)?.map(Arc::new);

		Ok(Arc::new(Self {
			services: args.services.clone(),
			sessions,
			providers,
			server,
			ratelimiter: Mutex::new(HashMap::new()),
			device_ratelimiter: Mutex::new(HashMap::new()),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		if self.services.globals.is_read_only() {
			return Ok(());
		}

		loop {
			self.maintain().await;

			let shutdown = self.services.server.until_shutdown();
			if tokio::time::timeout(MAINTENANCE_INTERVAL, shutdown)
				.await
				.is_ok()
			{
				return Ok(());
			}
		}
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
#[inline]
pub fn get_server(&self) -> Result<&Server> {
	self.server
		.as_deref()
		.ok_or_else(|| err!(Request(Unrecognized("OIDC server not configured"))))
}

/// Cap on the rate-limit table; fully refilled buckets are pruned past it.
const RATELIMIT_MAP_CAP: usize = 1 << 16;

/// Always-on throttle for the RFC 8628 device user-code endpoints. The
/// `user_code` is low-entropy by design (§6.1), so §5.1 requires bounding
/// guesses regardless of the optional `oidc_rc_*` knobs; the burst stays
/// generous for the one code a real user enters.
const DEVICE_RC_PER_SECOND: f64 = 1.0;
const DEVICE_RC_BURST: f64 = 60.0;

/// Shared per-client-IP token-bucket throttle for the OIDC endpoints. A no-op
/// unless both `oidc_rc_per_second` and `oidc_rc_burst_count` are configured.
#[implement(Service)]
pub fn check_rate_limit(&self, client: IpAddr) -> Result {
	let config = &self.services.config;
	let rate = f64::from(config.oidc_rc_per_second);
	let burst = f64::from(config.oidc_rc_burst_count);

	if rate <= 0.0 || burst <= 0.0 {
		return Ok(());
	}

	check_bucket(&self.ratelimiter, client, rate, burst)
}

/// Always-on anti-brute-force throttle for the device user-code endpoints
/// (RFC 8628 §5.1), independent of the optional `oidc_rc_*` knobs.
#[implement(Service)]
pub fn check_device_rate_limit(&self, client: IpAddr) -> Result {
	check_bucket(&self.device_ratelimiter, client, DEVICE_RC_PER_SECOND, DEVICE_RC_BURST)
}

fn check_bucket(table: &Ratelimiter, client: IpAddr, rate: f64, burst: f64) -> Result {
	let now = Instant::now();
	let mut buckets = table.lock()?;

	// A fully refilled bucket equals an absent one; prune those past the cap so
	// a source-address spray cannot grow the table without bound.
	if buckets.len() >= RATELIMIT_MAP_CAP {
		buckets.retain(|_, bucket| {
			let (last, toks) = *bucket;
			now.duration_since(last)
				.as_secs_f64()
				.mul_add(rate, toks)
				< burst
		});
	}

	let (last_time, tokens) = buckets
		.entry(client)
		.or_insert_with(|| (now, burst));

	let new_tokens = now
		.duration_since(*last_time)
		.as_secs_f64()
		.mul_add(rate, *tokens)
		.min(burst);

	if new_tokens < 1.0 {
		return Err(Error::Request(
			ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
			"Too many OIDC requests.".into(),
			StatusCode::TOO_MANY_REQUESTS,
		));
	}

	*last_time = now;
	*tokens = new_tokens - 1.0;

	Ok(())
}

/// Remove all session state for a user. For debug and developer use only;
/// deleting state can cause registration conflicts and unintended
/// re-registrations.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn delete_user_sessions(&self, user_id: &UserId) {
	self.user_sessions(user_id)
		.ready_filter_map(Result::ok)
		.ready_filter_map(|(_, session)| session.sess_id)
		.for_each(|sess_id| async move {
			self.sessions.delete(&sess_id).await;
		})
		.await;
}

/// Revoke all session tokens for a user.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn revoke_user_tokens(&self, user_id: &UserId) {
	self.user_sessions(user_id)
		.ready_filter_map(Result::ok)
		.for_each(|(provider, session)| async move {
			self.revoke_token((&provider, &session))
				.await
				.log_err()
				.ok();
		})
		.await;
}

/// Get user's authorizations. Lists pairs of `(Provider, Session)` for a user.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub fn user_sessions(
	&self,
	user_id: &UserId,
) -> impl Stream<Item = Result<(Provider, Session)>> + Send {
	self.sessions
		.get_by_user(user_id)
		.and_then(async |session| Ok((self.sessions.provider(&session).await?, session)))
}

/// Network request to a Provider returning userinfo for a Session. The session
/// must have a valid access token.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all, ret)]
pub async fn request_userinfo(
	&self,
	(provider, session): (&Provider, &Session),
) -> Result<UserInfo> {
	#[derive(Debug, Serialize)]
	struct Query;

	let url = provider
		.userinfo_url
		.clone()
		.ok_or_else(|| err!(Config("userinfo_url", "Missing userinfo URL in config")))?;

	self.request((Some(provider), Some(session)), Method::GET, url, Option::<Query>::None)
		.await
		.and_then(|value| serde_json::from_value(value).map_err(Into::into))
		.log_err()
}

/// Network request to a Provider returning information for a Session based on
/// its access token.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all, ret)]
pub async fn request_tokeninfo(
	&self,
	(provider, session): (&Provider, &Session),
) -> Result<UserInfo> {
	#[derive(Debug, Serialize)]
	struct Query;

	let url = provider
		.introspection_url
		.clone()
		.ok_or_else(|| {
			err!(Config("introspection_url", "Missing introspection URL in config"))
		})?;

	self.request((Some(provider), Some(session)), Method::GET, url, Option::<Query>::None)
		.await
		.and_then(|value| serde_json::from_value(value).map_err(Into::into))
		.log_err()
}

/// Network request asking a Provider to revoke a Session's upstream grant.
///
/// RFC 7009: the client authenticates with its own credentials and names the
/// token to revoke in the form body — the refresh token when one is held,
/// which ends the whole grant, otherwise the access token. No bearer is sent,
/// and a success answer has no body to parse. A session holding neither token
/// has nothing to revoke.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn revoke_token(&self, (provider, session): (&Provider, &Session)) -> Result {
	#[derive(Serialize)]
	struct RevokeQuery<'a> {
		client_id: &'a str,
		client_secret: &'a str,
		token: &'a str,
		token_type_hint: &'a str,
	}

	let (token, token_type_hint) = match (&session.refresh_token, &session.access_token) {
		| (Some(token), _) => (token.as_str(), "refresh_token"),
		| (None, Some(token)) => (token.as_str(), "access_token"),
		| (None, None) => return Ok(()),
	};

	let url = provider
		.revocation_url
		.clone()
		.ok_or_else(|| err!(Config("revocation_url", "Missing revocation URL in config")))?;

	let client_secret = provider.get_client_secret().await?;
	let body = serde_html_form::to_string(RevokeQuery {
		client_id: &provider.client_id,
		client_secret: &client_secret,
		token,
		token_type_hint,
	})?;

	self.services
		.client
		.oauth
		.post(url)
		.header(ACCEPT, "application/json")
		.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
		.body(body)
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

/// Network request to a Provider to obtain an access token for a Session using
/// a provided code.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all, ret)]
pub async fn request_token(
	&self,
	(provider, session): (&Provider, &Session),
	code: &str,
) -> Result<TokenResponse> {
	#[derive(Debug, Serialize)]
	struct TokenQuery<'a> {
		client_id: &'a str,
		client_secret: &'a str,
		grant_type: &'a str,
		code: &'a str,
		code_verifier: Option<&'a str>,
		redirect_uri: Option<&'a str>,
	}

	let client_secret = provider.get_client_secret().await?;

	let query = TokenQuery {
		client_id: &provider.client_id,
		client_secret: &client_secret,
		grant_type: "authorization_code",
		code,
		code_verifier: session.code_verifier.as_deref(),
		redirect_uri: provider.callback_url.as_ref().map(Url::as_str),
	};

	let url = provider
		.token_url
		.clone()
		.ok_or_else(|| err!(Config("token_url", "Missing token URL in config")))?;

	self.request((Some(provider), Some(session)), Method::POST, url, Some(query))
		.await
		.and_then(|value| serde_json::from_value(value).map_err(Into::into))
		.log_err()
}

/// Send a request to a provider; this is somewhat abstract since URL's are
/// formed prior to this call and could point at anything, however this function
/// uses the oauth-specific http client and is configured for JSON with special
/// casing for an `error` property in the response.
///
/// The response is deliberately not recorded: a token-endpoint reply is raw
/// JSON containing the access, refresh and ID tokens, and no such value may
/// reach a log at any level (plan.md, non-negotiable invariant 7). Callers that
/// need a decoded view record their own redacted type instead.
#[implement(Service)]
#[tracing::instrument(name = "request", level = "debug", skip(self, body))]
pub async fn request<Body>(
	&self,
	(provider, session): (Option<&Provider>, Option<&Session>),
	method: Method,
	url: Url,
	body: Option<Body>,
) -> Result<JsonValue>
where
	Body: Serialize,
{
	let mut request = self
		.services
		.client
		.oauth
		.request(method, url)
		.header(ACCEPT, "application/json");

	if let Some(body) = body.map(serde_html_form::to_string).transpose()? {
		request = request
			.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
			.body(body);
	}

	if let Some(session) = session
		&& let Some(access_token) = session.access_token.clone()
	{
		request = request.bearer_auth(access_token);
	}

	let limit = self.services.config.max_response_size;
	let http_response = request.send().await?.error_for_status()?;

	let body = read_response_capped(http_response, limit).await?;
	let response: JsonValue = serde_json::from_slice(&body)?;

	if let Some(response) = response.as_object().as_ref()
		&& let Some(error) = response.get("error").and_then(JsonValue::as_str)
	{
		let description = response
			.get("error_description")
			.and_then(JsonValue::as_str)
			.unwrap_or("(no description)");

		return Err!(Request(Forbidden("Error from provider: {error}: {description}",)));
	}

	Ok(response)
}

/// Verify the `id_token` a provider returned for this session.
///
/// Returns the verified claims, or `None` when the provider issued no
/// `id_token` and does not require one. The provider's JWKS is served from the
/// runtime cache; a token naming a key id the cached set does not hold forces
/// one bounded re-fetch, which is how a key rotation between our last fetch
/// and this login is picked up without turning forged key ids into traffic.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all, fields(provider = provider.id()))]
pub async fn verify_id_token(
	&self,
	(provider, session): (&Provider, &Session),
) -> Result<Option<IdTokenClaims>> {
	// `require_id_token` implies verification: demanding the assertion and then
	// not checking it would be worse than not demanding it.
	if !provider.verify_id_token && !provider.require_id_token {
		return Ok(None);
	}

	let Some(token) = session.id_token.as_deref() else {
		if provider.require_id_token {
			return Err!(Request(Unauthorized(
				"Provider returned no id_token but require_id_token is set."
			)));
		}

		return Ok(None);
	};

	let issuer = provider
		.issuer_url
		.as_ref()
		.map(Url::as_str)
		.ok_or_else(|| err!(Config("issuer_url", "Missing issuer URL in config")))?;

	let kid = id_token::key_id(token);
	let mut jwks = self.providers.jwks(provider, false).await?;

	if kid
		.as_deref()
		.is_some_and(|kid| jwks.find(kid).is_none())
	{
		debug_warn!(
			provider = provider.id(),
			"id_token names a key id absent from the cached JWKS; re-fetching.",
		);

		jwks = self.providers.jwks(provider, true).await?;
	}

	id_token::verify(token, &jwks, issuer, &provider.client_id, session.query_nonce.as_deref())
		.map(Some)
}

/// Outcome of re-checking an upstream authorization.
///
/// The cases are deliberately distinct: only an explicit refusal from the
/// provider may end every session of an account, because a provider outage
/// that looked like a refusal would log out every user at once.
#[derive(Clone, Debug)]
pub enum Recheck {
	/// The provider re-authorized the grant under its current policy.
	Allowed,

	/// The provider refused: the grant is revoked, or its policy no longer
	/// admits this user.
	Denied(String),

	/// The provider could not be reached, failed transiently, or the stored
	/// grant could not be read. The session is left intact and the caller
	/// answers a retryable error.
	Unavailable(String),

	/// The device holds no upstream grant to re-check — its grant was cleared,
	/// or it never received one. That ends this device, not the account.
	NoGrant(String),
}

/// Re-check the upstream grant a device's own sign-in obtained.
///
/// The grants re-checked are those bound to `device_id`. A device with none
/// falls back to grants obtained before grants were bound to devices, which
/// back every unbound device of the user. Returns `None` when no provider
/// configured with `require_upstream_refresh` applies — accounts that never
/// came through SSO, the sealed recovery account and appservice users
/// included — and [`Recheck::NoGrant`] when one applies but the device holds
/// no grant. An unreadable record always applies, and makes the answer at best
/// unavailable.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn recheck_device(&self, user_id: &UserId, device_id: &DeviceId) -> Option<Recheck> {
	let records: Vec<Result<(Provider, Session)>> = self.user_sessions(user_id).collect().await;

	let required = records.iter().any(|record| match record {
		| Ok((provider, _)) => provider.require_upstream_refresh,
		| Err(_) => true,
	});

	if !required {
		return None;
	}

	let bound = |session: &Session| session.device_id.as_deref() == Some(device_id);
	let unbound = |session: &Session| {
		session.device_id.is_none() && session.login_token_hash.is_none() && session.has_grant()
	};

	let has_bound = records
		.iter()
		.flatten()
		.any(|(_, session)| bound(session));

	let candidates: Vec<_> = records
		.into_iter()
		.filter(|record| match record {
			| Ok((_, session)) if has_bound => bound(session),
			| Ok((_, session)) => unbound(session),
			| Err(_) => true,
		})
		.collect();

	if candidates.is_empty() {
		return Some(Recheck::NoGrant("This device holds no upstream grant.".to_owned()));
	}

	Box::pin(recheck_authorizations(
		futures::stream::iter(candidates),
		|provider, session| async move { self.recheck_session((&provider, &session)).await },
	))
	.await
}

/// Fold the actual lookup/recheck path without dropping storage, decoding or
/// provider-resolution errors. A readable optional provider may be exempt;
/// an unreadable one must never be assumed optional. Keep examining readable
/// grants after an error so a later explicit denial still wins.
///
/// Precedence: a denial ends the fold; otherwise unavailability outranks an
/// authorization, which outranks a missing grant.
async fn recheck_authorizations<S, F, Fut>(sessions: S, mut recheck: F) -> Option<Recheck>
where
	S: Stream<Item = Result<(Provider, Session)>>,
	F: FnMut(Provider, Session) -> Fut,
	Fut: Future<Output = Recheck>,
{
	futures::pin_mut!(sessions);
	let mut outcome = None;
	while let Some(session) = sessions.next().await {
		let checked = match session {
			| Ok((provider, _)) if !provider.require_upstream_refresh => continue,
			| Ok((provider, session)) => recheck(provider, session).await,
			| Err(_) =>
				Recheck::Unavailable("Upstream authorization state could not be read.".into()),
		};
		match checked {
			| Recheck::Denied(_) => return Some(checked),
			| Recheck::Unavailable(_) => outcome = Some(checked),
			| Recheck::Allowed if !matches!(outcome, Some(Recheck::Unavailable(_))) =>
				outcome = Some(checked),
			| Recheck::NoGrant(_) if outcome.is_none() => outcome = Some(checked),
			| Recheck::Allowed | Recheck::NoGrant(_) => (),
		}
	}
	outcome
}

/// Re-check one upstream grant and persist the refreshed grant on success.
///
/// The re-check runs inside the grant identity's critical section, on the
/// record as last committed: two devices of one identity never exchange the
/// same rotating refresh token concurrently, and a grant cleared by a logout or
/// revocation while this re-check waited is found cleared rather than restored.
///
/// The `refresh_token` grant is preferred: Cloudflare Access re-evaluates its
/// Access policies while serving it ("Cloudflare will use the refresh token to
/// obtain a new access token after checking the user's identity against your
/// Access policies" — Cloudflare One docs, Generic OIDC application), so it is
/// a policy check and not merely a liveness check. A provider or configuration
/// that issued no refresh token falls back to a userinfo request with the
/// stored access token, which proves only that the token is still accepted;
/// that weaker guarantee is recorded here so a deployment relying on it is not
/// misled.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all, fields(provider = provider.id()))]
pub async fn recheck_session(&self, (provider, session): (&Provider, &Session)) -> Recheck {
	let Some(sess_id) = session.sess_id.as_deref() else {
		return Recheck::Denied("Session has no identifier to re-check.".to_owned());
	};

	let outcome = self
		.sessions
		.update(sess_id, async |current| Ok(self.recheck_locked(provider, current).await))
		.await;

	match outcome {
		| Ok(Some(recheck)) => recheck,
		| Ok(None) => Recheck::NoGrant("The upstream grant record no longer exists.".to_owned()),
		| Err(error) =>
			Recheck::Unavailable(format!("Upstream grant state could not be updated: {error}")),
	}
}

/// The re-check proper, on the current record. Returns the record to persist
/// when the provider refreshed it.
#[implement(Service)]
async fn recheck_locked(
	&self,
	provider: &Provider,
	session: Session,
) -> (Option<Session>, Recheck) {
	if session.grant_unreadable {
		return (
			None,
			Recheck::Unavailable(
				"The stored upstream grant cannot be opened with the configured keys.".to_owned(),
			),
		);
	}

	if !session.has_grant() {
		return (None, Recheck::NoGrant("The upstream grant was cleared.".to_owned()));
	}

	if let Some(refresh_token) = session.refresh_token.clone() {
		return match self
			.request_refresh((provider, &session), &refresh_token)
			.await
		{
			| Err(error) => (None, classify_upstream(&error)),
			| Ok(token) => {
				let sess_id = session.sess_id.clone();
				match session.apply_token_response(token) {
					| Ok(refreshed) => {
						info!(
							?sess_id,
							provider = provider.id(),
							"Upstream grant re-authorized."
						);
						(Some(refreshed), Recheck::Allowed)
					},
					| Err(error) => (
						None,
						Recheck::Unavailable(format!(
							"Provider token response could not be applied: {error}"
						)),
					),
				}
			},
		};
	}

	if session.access_token.is_none() {
		return (
			None,
			Recheck::NoGrant("No upstream access or refresh token is stored.".to_owned()),
		);
	}

	match self.request_userinfo((provider, &session)).await {
		| Ok(..) => (None, Recheck::Allowed),
		| Err(error) => (None, classify_upstream(&error)),
	}
}

/// Classify a provider failure as a policy denial or a transient failure.
///
/// Only an explicit `4xx` from the provider denies, and `408`/`429` are
/// excluded because they are the provider asking us to come back later. A
/// network failure, a `5xx`, a malformed response, and any local error are all
/// transient by construction: the alternative is a provider outage revoking
/// every session on the server.
#[must_use]
pub fn classify_upstream(error: &Error) -> Recheck {
	let status = match error {
		| Error::Reqwest(error) => error.status(),
		| Error::Request(.., status) => Some(*status),
		| _ => None,
	};

	match status {
		| Some(status)
			if status.is_client_error()
				&& status != StatusCode::REQUEST_TIMEOUT
				&& status != StatusCode::TOO_MANY_REQUESTS =>
			Recheck::Denied(format!("Provider refused the grant: {error}")),

		| _ => Recheck::Unavailable(format!("Provider could not be reached: {error}")),
	}
}

/// Network request exchanging a stored refresh token for a fresh grant.
///
/// No bearer is attached: RFC 6749 §6 authenticates the client with its
/// credentials in the request body, and sending the expiring access token as a
/// bearer here is what makes a re-check fail once it lapses.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn request_refresh(
	&self,
	(provider, _session): (&Provider, &Session),
	refresh_token: &str,
) -> Result<TokenResponse> {
	#[derive(Debug, Serialize)]
	struct RefreshQuery<'a> {
		client_id: &'a str,
		client_secret: &'a str,
		grant_type: &'a str,
		refresh_token: &'a str,
		#[serde(skip_serializing_if = "Option::is_none")]
		scope: Option<&'a str>,
	}

	let client_secret = provider.get_client_secret().await?;
	let scope = provider.scope.iter().join(" ");
	let scope = (!scope.is_empty()).then_some(scope);

	let query = RefreshQuery {
		client_id: &provider.client_id,
		client_secret: &client_secret,
		grant_type: "refresh_token",
		refresh_token,
		scope: scope.as_deref(),
	};

	let url = provider
		.token_url
		.clone()
		.ok_or_else(|| err!(Config("token_url", "Missing token URL in config")))?;

	self.request((Some(provider), None), Method::POST, url, Some(query))
		.await
		.and_then(|value| serde_json::from_value(value).map_err(Into::into))
}

/// Clear every stored upstream grant of a user.
///
/// Each grant is revoked at its provider when it publishes a revocation
/// endpoint, then its token material is dropped from the record. The identity
/// association is deliberately kept: deleting it would unbind `(iss, sub)` from
/// the account and the next login would register a new one.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn clear_user_grants(&self, user_id: &UserId) {
	self.clear_grants(user_id, None).await;
}

/// Clear the upstream grants bound to one device, as its logout or revocation
/// requires. Grants of the user's other devices are untouched.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn clear_device_grants(&self, user_id: &UserId, device_id: &DeviceId) {
	self.clear_grants(user_id, Some(device_id)).await;
}

#[implement(Service)]
async fn clear_grants(&self, user_id: &UserId, device_id: Option<&DeviceId>) {
	let records: Vec<Result<Session>> = self.sessions.get_by_user(user_id).collect().await;
	let mut cleared = 0_usize;

	for record in records {
		let session = match record {
			| Ok(session) => session,
			| Err(error) => {
				warn!(%user_id, "An upstream grant could not be read to clear it: {error}");
				continue;
			},
		};

		if device_id.is_some_and(|device_id| session.device_id.as_deref() != Some(device_id)) {
			continue;
		}

		let Some(sess_id) = session.sess_id.as_deref() else {
			continue;
		};

		let provider = self.sessions.provider(&session).await.ok();
		match self.clear_grant(provider.as_ref(), sess_id).await {
			| Ok(()) => cleared = cleared.saturating_add(1),
			| Err(error) => warn!(%user_id, "An upstream grant could not be cleared: {error}"),
		}
	}

	info!(
		audit = "grants_cleared",
		%user_id,
		device_id = ?device_id,
		cleared,
		"Cleared stored upstream grants.",
	);
}

/// Drop the grant of one record that nothing will use — the one a
/// user-interactive authentication step through SSO obtains only to prove the
/// identity. The record and its identity association stay.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn clear_session_grant(&self, sess_id: &str) -> Result {
	let session = self.sessions.get(sess_id).await?;
	let provider = self.sessions.provider(&session).await.ok();

	self.clear_grant(provider.as_ref(), sess_id).await
}

/// Revoke one stored grant at its provider and drop it from its record, inside
/// its identity's critical section. Revocation is best effort; the local grant
/// is dropped whatever the provider answers.
#[implement(Service)]
async fn clear_grant(&self, provider: Option<&Provider>, sess_id: &str) -> Result {
	self.sessions
		.update(sess_id, async |session| {
			if !session.has_grant()
				&& session.device_id.is_none()
				&& session.login_token_hash.is_none()
			{
				return Ok((None, ()));
			}

			if let Some(provider) = provider
				&& !session.grant_unreadable
			{
				self.revoke_token((provider, &session))
					.await
					.log_err()
					.ok();
			}

			Ok((Some(session.cleared()), ()))
		})
		.await
		.map(|_| ())
}

/// End every session of an account at once: remove all its Matrix devices, then
/// revoke and drop every stored upstream grant.
///
/// This is what an explicit refusal from the identity provider triggers, and
/// what operator revocation does. The account and its identity association are
/// kept. Returns the number of devices removed.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn revoke_user_sessions(&self, user_id: &UserId) -> usize {
	let devices: Vec<OwnedDeviceId> = self
		.services
		.users
		.all_device_ids(user_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	for device_id in &devices {
		self.services
			.users
			.remove_device(user_id, device_id)
			.await;
	}

	self.clear_user_grants(user_id).await;

	warn!(
		audit = "sessions_revoked",
		%user_id,
		devices = devices.len(),
		"Revoked every session of the account.",
	);

	devices.len()
}

/// Bind the grant a login token was issued with to the device its redemption
/// created.
///
/// Grants previously bound to the same device are superseded: revoked and
/// cleared. A token no grant was issued with — one from an existing session,
/// for instance — binds nothing. A grant whose token was consumed by another
/// redemption in the meantime refuses the binding.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self, token))]
pub async fn bind_login_device(
	&self,
	user_id: &UserId,
	token: &str,
	device_id: &DeviceId,
) -> Result {
	let Some(sess_id) = self
		.sessions
		.find_login_token(user_id, token)
		.await?
		.and_then(|session| session.sess_id)
	else {
		return Ok(());
	};

	let superseded: Vec<Session> = self
		.sessions
		.get_by_user(user_id)
		.ready_filter_map(Result::ok)
		.ready_filter(|session| {
			session.device_id.as_deref() == Some(device_id)
				&& session.sess_id.as_deref() != Some(sess_id.as_str())
		})
		.collect()
		.await;

	for session in superseded {
		if let Some(old) = session.sess_id.as_deref() {
			let provider = self.sessions.provider(&session).await.ok();
			self.clear_grant(provider.as_ref(), old)
				.await
				.log_err()
				.ok();
		}
	}

	let hash = login_token_hash(token);
	let bound = self
		.sessions
		.update(&sess_id, async move |current| {
			if current.login_token_hash.as_deref() != Some(hash.as_str()) {
				return Ok((None, false));
			}

			let current = Session {
				device_id: Some(device_id.to_owned()),
				login_token_hash: None,
				bound_at: Some(SystemTime::now()),
				..current
			};

			Ok((Some(current), true))
		})
		.await?;

	if bound != Some(true) {
		return Err!(Request(Forbidden("This sign-in's upstream grant is no longer available.")));
	}

	info!(audit = "grant_bound", %user_id, %device_id, "Bound the sign-in's upstream grant to its device.");

	Ok(())
}

/// One maintenance pass over stored grants, bounded to [`MAINTENANCE_BATCH`]
/// records.
///
/// Material stored unsealed, or sealed under a previous key, is resealed with
/// the current key. Records nothing needs any more are cleared or deleted:
/// authorizations that never reached their callback, grants whose login token
/// was never redeemed, grants whose device is gone, and cleared records that
/// are not their identity's association record, which is never deleted.
#[implement(Service)]
async fn maintain(&self) {
	let records: Vec<Session> = self
		.sessions
		.stream()
		.take(MAINTENANCE_BATCH)
		.collect()
		.await;

	let current_kid = self.sessions.current_kid().map(ToOwned::to_owned);
	let (mut resealed, mut cleared, mut deleted) = (0_usize, 0_usize, 0_usize);

	for session in records {
		match self.upkeep(session, current_kid.as_deref()).await {
			| Ok(Upkeep::Kept) => {},
			| Ok(Upkeep::Resealed) => resealed = resealed.saturating_add(1),
			| Ok(Upkeep::Cleared) => cleared = cleared.saturating_add(1),
			| Ok(Upkeep::Deleted) => deleted = deleted.saturating_add(1),
			| Err(error) => warn!("Upstream grant maintenance skipped a record: {error}"),
		}
	}

	if resealed > 0 || cleared > 0 || deleted > 0 {
		info!(resealed, cleared, deleted, "Maintained stored upstream grants.");
	}
}

/// What one maintenance step did to a record.
enum Upkeep {
	Kept,
	Resealed,
	Cleared,
	Deleted,
}

#[implement(Service)]
async fn upkeep(&self, session: Session, current_kid: Option<&str>) -> Result<Upkeep> {
	let Some(sess_id) = session.sess_id.clone() else {
		return Ok(Upkeep::Kept);
	};

	if session.user_info.is_none() {
		if session
			.authorize_expires_at
			.is_some_and(timepoint_has_passed)
		{
			self.sessions.delete(&sess_id).await;
			return Ok(Upkeep::Deleted);
		}

		return Ok(Upkeep::Kept);
	}

	let grace = Duration::from_millis(self.services.config.login_token_ttl)
		.saturating_add(UNREDEEMED_GRACE);

	let unredeemed = session.login_token_hash.is_some()
		&& session.device_id.is_none()
		&& session
			.bound_at
			.and_then(|at| at.checked_add(grace))
			.is_none_or(timepoint_has_passed);

	let device_gone = match (&session.user_id, &session.device_id) {
		| (Some(user_id), Some(device_id)) =>
			!self
				.services
				.users
				.device_exists(user_id, device_id)
				.await,
		| _ => false,
	};

	if unredeemed || device_gone {
		let provider = self.sessions.provider(&session).await.ok();
		self.clear_grant(provider.as_ref(), &sess_id)
			.await?;

		if self.sessions.is_identity_record(&session).await {
			return Ok(Upkeep::Cleared);
		}

		self.sessions.delete(&sess_id).await;
		return Ok(Upkeep::Deleted);
	}

	if !session.has_grant() && session.device_id.is_none() && session.login_token_hash.is_none() {
		if self.sessions.is_identity_record(&session).await {
			return Ok(Upkeep::Kept);
		}

		self.sessions.delete(&sess_id).await;
		return Ok(Upkeep::Deleted);
	}

	if session.has_grant()
		&& !session.grant_unreadable
		&& current_kid.is_some()
		&& session.sealed_with.as_deref() != current_kid
	{
		self.sessions
			.update(&sess_id, async |current| Ok((Some(current), ())))
			.await?;

		return Ok(Upkeep::Resealed);
	}

	Ok(Upkeep::Kept)
}

/// Generate a unique-id string determined by the combination of `Provider` and
/// `Session` instances.
#[inline]
pub fn unique_id((provider, session): (&Provider, &Session)) -> Result<String> {
	unique_id_parts((provider, session)).and_then(unique_id_iss_sub)
}

/// Generate a unique-id string determined by the combination of `Provider`
/// instance and `sub` string.
#[inline]
pub fn unique_id_sub((provider, sub): (&Provider, &str)) -> Result<String> {
	unique_id_sub_parts((provider, sub)).and_then(unique_id_iss_sub)
}

/// Generate a unique-id string determined by the combination of `issuer_url`
/// and `Session` instance.
#[inline]
pub fn unique_id_iss((iss, session): (&str, &Session)) -> Result<String> {
	unique_id_iss_parts((iss, session)).and_then(unique_id_iss_sub)
}

/// Generate a unique-id string determined by the `issuer_url` and the `sub`
/// strings directly.
pub fn unique_id_iss_sub((iss, sub): (&str, &str)) -> Result<String> {
	let hash = sha256::delimited([iss, sub].iter());
	let b64 = b64encode.encode(hash);

	Ok(b64)
}

fn unique_id_parts<'a>(
	(provider, session): (&'a Provider, &'a Session),
) -> Result<(&'a str, &'a str)> {
	identity_issuer(provider)
		.ok_or_else(|| err!(Config("issuer_url", "issuer_url not found for this provider.")))
		.and_then(|iss| unique_id_iss_parts((iss, session)))
}

fn unique_id_sub_parts<'a>(
	(provider, sub): (&'a Provider, &'a str),
) -> Result<(&'a str, &'a str)> {
	identity_issuer(provider)
		.ok_or_else(|| err!(Config("issuer_url", "issuer_url not found for this provider.")))
		.map(|iss| (iss, sub))
}

/// Issuer string used as input to the identity hash. Pinned per-brand for
/// providers whose published issuer has changed under us, so existing account
/// associations survive the change.
fn identity_issuer(provider: &Provider) -> Option<&str> {
	match provider.brand.as_str() {
		| "github" => Some("https://github.com/"),
		| _ => provider.issuer_url.as_ref().map(Url::as_str),
	}
}

fn unique_id_iss_parts<'a>((iss, session): (&'a str, &'a Session)) -> Result<(&'a str, &'a str)> {
	session
		.user_info
		.as_ref()
		.map(|user_info| user_info.sub.as_str())
		.ok_or_else(|| err!(Request(NotFound("user_info not found for this session."))))
		.map(|sub| (iss, sub))
}
