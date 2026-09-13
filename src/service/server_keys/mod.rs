mod acquire;
mod get;
mod keypair;
mod request;
mod sign;
#[cfg(test)]
mod tests;
mod verify;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::StreamExt;
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, MilliSecondsSinceUnixEpoch, OwnedServerSigningKeyId,
	ServerName, ServerSigningKeyId, UInt,
	api::federation::discovery::{OldVerifyKey, ServerSigningKeys, VerifyKey},
	room_version_rules::RoomVersionRules,
	signatures::{Ed25519KeyPair, PublicKeyMap, PublicKeySet},
};
use tuwunel_core::{
	Err, Result, err, implement,
	utils::{IterStream, timepoint_from_now},
};
use tuwunel_database::{Deserialized, Json, Map};

pub struct Service {
	keypair: Box<Ed25519KeyPair>,
	verify_keys: VerifyKeys,
	minimum_valid: Duration,
	services: Arc<crate::services::OnceServices>,
	db: Data,
}

struct Data {
	server_signingkeys: Arc<Map>,
}

pub type VerifyKeys = BTreeMap<OwnedServerSigningKeyId, VerifyKey>;
pub type PubKeyMap = PublicKeyMap;
pub type PubKeys = PublicKeySet;

/// The use a signing key is put to, which decides the keys that may serve and
/// the validity they need.
#[derive(Clone, Copy, Debug)]
pub enum KeyUse {
	/// Signing an X-Matrix request at the given time. Only a current verify key
	/// valid then may: old verify keys "are only valid for signing events"
	/// (server-server API, "Publishing Keys").
	Request(MilliSecondsSinceUnixEpoch),

	/// Signing an event sent at the given time: a current verify key valid
	/// until at least then, or an old verify key which expired no earlier (room
	/// version 5, "Signing key validity period"). `None` where the room version
	/// ignores `valid_until_ts`.
	Event(Option<MilliSecondsSinceUnixEpoch>),
}

/// Servers "MUST use the lesser of [`valid_until_ts`] and 7 days into the
/// future when determining if a key is valid" (server-server API).
const MAX_KEY_VALIDITY: Duration = Duration::from_hours(7 * 24);

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		let minimum_valid = Duration::from_hours(1);

		let (keypair, verify_keys) = tokio::task::block_in_place(|| {
			tokio::runtime::Handle::current()
				.block_on(keypair::init(args.db, &args.server.config.server_name))
		})?;
		debug_assert!(verify_keys.len() == 1, "only one active verify_key supported");

		Ok(Arc::new(Self {
			keypair,
			verify_keys,
			minimum_valid,
			services: args.services.clone(),
			db: Data {
				server_signingkeys: args.db["server_signingkeys"].clone(),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
#[inline]
#[must_use]
pub fn keypair(&self) -> &Ed25519KeyPair { &self.keypair }

/// Stages a new signing keypair and returns its key id. It becomes the active
/// key at the next start, when the current key is published as an old verify
/// key expiring then.
#[implement(Service)]
pub async fn stage_signing_key(&self) -> Result<OwnedServerSigningKeyId> {
	keypair::stage_next(&self.services.db).await
}

#[implement(Service)]
#[inline]
#[must_use]
pub fn active_key_id(&self) -> &ServerSigningKeyId { self.active_verify_key().0 }

#[implement(Service)]
#[inline]
#[must_use]
pub fn active_verify_key(&self) -> (&ServerSigningKeyId, &VerifyKey) {
	debug_assert!(self.verify_keys.len() <= 1, "more than one active verify_key");
	self.verify_keys
		.iter()
		.next()
		.map(|(id, key)| (id.as_ref(), key))
		.expect("missing active verify_key")
}

#[implement(Service)]
async fn add_signing_keys(&self, new_keys: ServerSigningKeys) {
	let origin = new_keys.server_name.clone();

	// (timo) Not atomic, but this is not critical
	let stored = self.signing_keys_for(&origin).await.ok();
	let keys = merge_signing_keys(stored, new_keys, max_valid_until_ts());

	self.db
		.server_signingkeys
		.raw_put(&origin, Json(&keys))
		.await
		.expect("database write error");
}

#[implement(Service)]
pub async fn required_keys_exist(
	&self,
	object: &CanonicalJsonObject,
	rules: &RoomVersionRules,
) -> bool {
	use ruma::signatures::required_keys;

	let Ok(required_keys) = required_keys(object, &rules.signatures) else {
		return false;
	};

	let Ok(usage) = event_key_use(object, rules) else {
		return false;
	};

	required_keys
		.iter()
		.flat_map(|(server, key_ids)| key_ids.iter().map(move |key_id| (server, key_id)))
		.stream()
		.all(|(server, key_id)| self.verify_key_exists(server, key_id, usage))
		.await
}

/// Whether a cached key of `origin` known as `key_id` is usable for `usage`.
#[implement(Service)]
pub async fn verify_key_exists(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
	usage: KeyUse,
) -> bool {
	self.cached_key(origin, key_id, usage)
		.await
		.is_some()
}

/// The cached key of `origin` known as `key_id`, if it is usable for `usage`.
/// Our own active key always is.
#[implement(Service)]
pub async fn cached_key(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
	usage: KeyUse,
) -> Option<VerifyKey> {
	if self.services.globals.server_is_ours(origin)
		&& let Some(key) = self.verify_keys.get(key_id)
	{
		return Some(key.clone());
	}

	self.signing_keys_for(origin)
		.await
		.ok()
		.and_then(|keys| usable_key(&keys, key_id, usage))
}

#[implement(Service)]
pub async fn verify_keys_for(&self, origin: &ServerName) -> VerifyKeys {
	let mut keys = self
		.signing_keys_for(origin)
		.await
		.map(|keys| merge_old_keys(keys).verify_keys)
		.unwrap_or(BTreeMap::new());

	if self.services.globals.server_is_ours(origin) {
		keys.extend(self.verify_keys.clone());
	}

	keys
}

#[implement(Service)]
pub async fn signing_keys_for(&self, origin: &ServerName) -> Result<ServerSigningKeys> {
	self.db
		.server_signingkeys
		.get(origin)
		.await
		.deserialized()
}

#[implement(Service)]
fn minimum_valid_ts(&self) -> MilliSecondsSinceUnixEpoch {
	let timepoint =
		timepoint_from_now(self.minimum_valid).expect("SystemTime should not overflow");

	MilliSecondsSinceUnixEpoch::from_system_time(timepoint).expect("UInt should not overflow")
}

/// The latest `valid_until_ts` a key response is trusted for.
fn max_valid_until_ts() -> MilliSecondsSinceUnixEpoch {
	let timepoint = timepoint_from_now(MAX_KEY_VALIDITY).expect("SystemTime should not overflow");

	MilliSecondsSinceUnixEpoch::from_system_time(timepoint).expect("UInt should not overflow")
}

/// The key use for verifying `object` as an event under `rules`: its
/// `origin_server_ts` where the room version enforces signing key validity.
fn event_key_use(object: &CanonicalJsonObject, rules: &RoomVersionRules) -> Result<KeyUse> {
	if !rules.enforce_key_validity {
		return Ok(KeyUse::Event(None));
	}

	let Some(CanonicalJsonValue::Integer(origin_server_ts)) = object.get("origin_server_ts")
	else {
		return Err!(BadServerResponse(
			"Event has no origin_server_ts to check its signing keys against."
		));
	};

	let origin_server_ts = UInt::try_from(i64::from(*origin_server_ts))
		.map_err(|e| err!(BadServerResponse("Event has an invalid origin_server_ts: {e}")))?;

	Ok(KeyUse::Event(Some(MilliSecondsSinceUnixEpoch(origin_server_ts))))
}

/// Merges a fetched key response into the keys stored for its server.
///
/// A response vouches for its `verify_keys` until the lesser of its
/// `valid_until_ts` and `latest`, seven days from now. Of the stored keys and
/// the response, the one valid longer is kept; a current key only the other
/// lists is kept as an old key expiring when that list's validity ends, so it
/// never inherits the longer validity.
fn merge_signing_keys(
	stored: Option<ServerSigningKeys>,
	mut new: ServerSigningKeys,
	latest: MilliSecondsSinceUnixEpoch,
) -> ServerSigningKeys {
	new.valid_until_ts = new.valid_until_ts.min(latest);

	let Some(stored) = stored else {
		return new;
	};

	let (mut kept, other) = if new.valid_until_ts >= stored.valid_until_ts {
		(new, stored)
	} else {
		(stored, new)
	};

	for (key_id, key) in other.verify_keys {
		if !kept.verify_keys.contains_key(&key_id) {
			kept.old_verify_keys
				.entry(key_id)
				.or_insert_with(|| OldVerifyKey::new(other.valid_until_ts, key.key));
		}
	}

	for (key_id, old) in other.old_verify_keys {
		kept.old_verify_keys.entry(key_id).or_insert(old);
	}

	kept
}

/// The key known as `key_id` in `keys`, if it may make a signature for
/// `usage`: a current key until `valid_until_ts`, an old key for events until
/// its `expired_ts`.
fn usable_key(
	keys: &ServerSigningKeys,
	key_id: &ServerSigningKeyId,
	usage: KeyUse,
) -> Option<VerifyKey> {
	let (at, old_keys_serve) = match usage {
		| KeyUse::Request(at) => (Some(at), false),
		| KeyUse::Event(at) => (at, true),
	};

	let valid_until = |until: MilliSecondsSinceUnixEpoch| at.is_none_or(|at| until >= at);

	keys.verify_keys
		.get(key_id)
		.filter(|_| valid_until(keys.valid_until_ts))
		.cloned()
		.or_else(|| {
			keys.old_verify_keys
				.get(key_id)
				.filter(|old| old_keys_serve && valid_until(old.expired_ts))
				.map(|old| VerifyKey::new(old.key.clone()))
		})
}

fn merge_old_keys(mut keys: ServerSigningKeys) -> ServerSigningKeys {
	keys.verify_keys.extend(
		keys.old_verify_keys
			.clone()
			.into_iter()
			.map(|(key_id, old)| (key_id, VerifyKey::new(old.key))),
	);

	keys
}

fn key_exists(keys: &ServerSigningKeys, key_id: &ServerSigningKeyId) -> bool {
	keys.verify_keys.contains_key(key_id) || keys.old_verify_keys.contains_key(key_id)
}
