use std::borrow::Borrow;

use ruma::{
	CanonicalJsonObject, MilliSecondsSinceUnixEpoch, ServerName, ServerSigningKeyId,
	api::federation::discovery::VerifyKey, room_version_rules::RoomVersionRules,
};
use tuwunel_core::{Err, Result, implement};

use super::{KeyUse, PubKeyMap, PubKeys, event_key_use};

/// The keys verifying `object` as an event under `version`. Where the room
/// version enforces signing key validity, each is valid for the event's
/// `origin_server_ts`.
#[implement(super::Service)]
pub async fn get_event_keys(
	&self,
	object: &CanonicalJsonObject,
	version: &RoomVersionRules,
) -> Result<PubKeyMap> {
	let usage = event_key_use(object, version)?;

	self.get_keys_for(object, version, usage).await
}

/// The keys verifying the signatures `object` requires under `version`, each
/// usable for `usage`.
#[implement(super::Service)]
pub(super) async fn get_keys_for(
	&self,
	object: &CanonicalJsonObject,
	version: &RoomVersionRules,
	usage: KeyUse,
) -> Result<PubKeyMap> {
	use ruma::signatures::required_keys;

	let required = match required_keys(object, &version.signatures) {
		| Ok(required) => required,
		| Err(e) => {
			return Err!(BadServerResponse("Failed to determine keys required to verify: {e}"));
		},
	};

	let batch = required
		.iter()
		.map(|(s, ids)| (s.borrow(), ids.iter().map(Borrow::borrow)));

	Ok(self.get_pubkeys(batch, usage).await)
}

#[implement(super::Service)]
pub async fn get_pubkeys<'a, S, K>(&self, batch: S, usage: KeyUse) -> PubKeyMap
where
	S: Iterator<Item = (&'a ServerName, K)> + Send,
	K: Iterator<Item = &'a ServerSigningKeyId> + Send,
{
	let mut keys = PubKeyMap::new();
	for (server, key_ids) in batch {
		let pubkeys = self.get_pubkeys_for(server, key_ids, usage).await;
		keys.insert(server.as_str().into(), pubkeys);
	}

	keys
}

#[implement(super::Service)]
pub async fn get_pubkeys_for<'a, I>(
	&self,
	origin: &ServerName,
	key_ids: I,
	usage: KeyUse,
) -> PubKeys
where
	I: Iterator<Item = &'a ServerSigningKeyId> + Send,
{
	let mut keys = PubKeys::new();
	for key_id in key_ids {
		if let Ok(verify_key) = self.get_key(origin, key_id, usage).await {
			keys.insert(key_id.as_str().into(), verify_key.key);
		}
	}

	keys
}

/// Any key of `origin` known as `key_id`, current or old, whatever its
/// validity.
#[implement(super::Service)]
pub async fn get_verify_key(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
) -> Result<VerifyKey> {
	self.get_key(origin, key_id, KeyUse::Event(None))
		.await
}

/// The current verify key of `origin` known as `key_id`, if it is valid now:
/// the only kind of key which may sign a request.
#[implement(super::Service)]
pub async fn get_request_key(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
) -> Result<VerifyKey> {
	self.get_key(origin, key_id, KeyUse::Request(MilliSecondsSinceUnixEpoch::now()))
		.await
}

/// The key of `origin` known as `key_id` usable for `usage`. The origin's keys
/// are fetched when the cached ones will not do.
#[implement(super::Service)]
async fn get_key(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
	usage: KeyUse,
) -> Result<VerifyKey> {
	let notary_first = self
		.services
		.server
		.config
		.query_trusted_key_servers_first;

	let notary_only = self
		.services
		.server
		.config
		.only_query_trusted_key_servers;

	if let Some(result) = self.cached_key(origin, key_id, usage).await {
		return Ok(result);
	}

	if notary_first
		&& let Ok(result) = self
			.get_key_from_notaries(origin, key_id, usage)
			.await
	{
		return Ok(result);
	}

	if !notary_only
		&& let Ok(result) = self
			.get_key_from_origin(origin, key_id, usage)
			.await
	{
		return Ok(result);
	}

	if !notary_first
		&& let Ok(result) = self
			.get_key_from_notaries(origin, key_id, usage)
			.await
	{
		return Ok(result);
	}

	Err!(BadServerResponse(debug_error!(
		?key_id,
		?origin,
		?usage,
		"Failed to fetch a valid federation signing-key"
	)))
}

#[implement(super::Service)]
async fn get_key_from_notaries(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
	usage: KeyUse,
) -> Result<VerifyKey> {
	for notary in &self.services.config.trusted_servers {
		if let Ok(server_keys) = self.notary_request(notary, origin).await {
			for server_key in server_keys {
				self.add_signing_keys(server_key).await;
			}

			if let Some(result) = self.cached_key(origin, key_id, usage).await {
				return Ok(result);
			}
		}
	}

	Err!(Request(NotFound("Failed to fetch a valid signing-key from notaries")))
}

#[implement(super::Service)]
async fn get_key_from_origin(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
	usage: KeyUse,
) -> Result<VerifyKey> {
	if let Ok(server_key) = self.server_request(origin).await {
		self.add_signing_keys(server_key).await;
		if let Some(result) = self.cached_key(origin, key_id, usage).await {
			return Ok(result);
		}
	}

	Err!(Request(NotFound("Failed to fetch a valid signing-key from origin")))
}
