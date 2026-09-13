use std::{
	collections::{BTreeMap, BTreeSet},
	convert::identity,
	fmt::Debug,
};

use futures::{FutureExt, StreamExt, TryFutureExt};
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, MilliSecondsSinceUnixEpoch, OwnedServerName,
	OwnedServerSigningKeyId, ServerName, ServerSigningKeyId,
	api::federation::discovery::{
		ServerSigningKeys, get_remote_server_keys,
		get_remote_server_keys_batch::{self, v2::QueryCriteria},
		get_server_keys,
	},
	serde::Raw,
	signatures::verify_json,
};
use tuwunel_core::{
	Err, Result, debug_warn, err, error, implement, info, trace,
	utils::stream::{IterStream, ReadyExt, TryBroadbandExt, TryReadyExt},
};

use super::{KeyUse, PubKeyMap, PubKeys};

#[implement(super::Service)]
pub(super) async fn batch_notary_request<'a, S, K>(
	&self,
	notary: &ServerName,
	batch: S,
) -> Result<Vec<ServerSigningKeys>>
where
	S: Iterator<Item = (&'a ServerName, K)> + Send,
	K: Iterator<Item = &'a ServerSigningKeyId> + Send,
{
	use get_remote_server_keys_batch::v2::Request;
	type RumaBatch = BTreeMap<OwnedServerName, BTreeMap<OwnedServerSigningKeyId, QueryCriteria>>;

	let criteria = QueryCriteria {
		minimum_valid_until_ts: Some(self.minimum_valid_ts()),
	};

	let mut server_keys = batch.fold(RumaBatch::new(), |mut batch, (server, key_ids)| {
		batch
			.entry(server.into())
			.or_default()
			.extend(key_ids.map(|key_id| (key_id.into(), criteria.clone())));

		batch
	});

	let total_keys = server_keys
		.values()
		.flat_map(|ids| ids.iter())
		.count();

	debug_assert!(total_keys > 0, "empty batch request to notary");

	let requested: BTreeSet<OwnedServerName> = server_keys.keys().cloned().collect();

	let batch_max = self
		.services
		.server
		.config
		.trusted_server_batch_size;

	let batch_concurrency = self
		.services
		.server
		.config
		.trusted_server_batch_concurrency;

	let batches: Vec<_> = server_keys
		.keys()
		.rev()
		.step_by(batch_max.saturating_sub(1))
		.skip(1)
		.chain(server_keys.keys().next())
		.cloned()
		.collect();

	let responses = batches
		.iter()
		.stream()
		.enumerate()
		.map(|(i, batch)| {
			let request = Request {
				server_keys: server_keys.split_off(batch),
			};

			if request.server_keys.is_empty() {
				return None;
			}

			trace!(
				%i, %notary, ?batch,
				remaining = ?server_keys,
				requesting = ?request.server_keys.keys(),
				"Request to notary server."
			);

			info!(
				%notary,
				remaining = %server_keys.len(),
				requesting = %request.server_keys.len(),
				"Sending request to notary server..."
			);

			Some(Ok(request))
		})
		.ready_filter_map(identity)
		.broadn_and_then(batch_concurrency, |request| {
			self.services
				.federation
				.execute_synapse(notary, request)
		})
		.ready_try_fold(Vec::new(), |mut results, response| {
			trace!(
				%notary, response = ?response.server_keys,
				"Response from notary server."
			);

			results.extend(response.server_keys);

			info!(
				"Received {0} keys out of {1} from notary server so far...",
				results.len(),
				total_keys,
			);

			Ok(results)
		})
		.inspect_err(|e| {
			error!(
				?notary, %batch_max, %batch_concurrency, %total_keys,
				"Requesting keys from notary server failed: {e}",
			);
		})
		.boxed()
		.await?;

	Ok(self
		.verify_notary_response(notary, &responses, |server| requested.contains(server))
		.await)
}

#[implement(super::Service)]
pub async fn notary_request(
	&self,
	notary: &ServerName,
	target: &ServerName,
) -> Result<impl Iterator<Item = ServerSigningKeys> + Clone + Debug + Send + use<>> {
	use get_remote_server_keys::v2::Request;

	let request = Request {
		server_name: target.into(),
		minimum_valid_until_ts: self.minimum_valid_ts(),
	};

	let responses = self
		.services
		.federation
		.execute(notary, request)
		.await?
		.server_keys;

	let response = self
		.verify_notary_response(notary, &responses, |server| server == target)
		.await;

	Ok(response.into_iter())
}

#[implement(super::Service)]
pub async fn server_request(&self, target: &ServerName) -> Result<ServerSigningKeys> {
	use get_server_keys::v2::Request;

	let server_signing_key = self
		.services
		.federation
		.execute(target, Request::new())
		.await
		.map(|response| response.server_key)
		.and_then(|key| verify_server_keys(&key, None))?;

	if server_signing_key.server_name != target {
		return Err!(BadServerResponse(debug_warn!(
			requested = ?target,
			response = ?server_signing_key.server_name,
			"Server responded with bogus server_name"
		)));
	}

	Ok(server_signing_key)
}

/// The key responses a notary returned for requested servers which are signed
/// both by their server and by the notary. Any other is discarded.
#[implement(super::Service)]
async fn verify_notary_response<F>(
	&self,
	notary: &ServerName,
	responses: &[Raw<ServerSigningKeys>],
	requested: F,
) -> Vec<ServerSigningKeys>
where
	F: Fn(&ServerName) -> bool + Send,
{
	let notary_keys = self.notary_keys(notary, responses).await;

	responses
		.iter()
		.filter_map(|response| {
			verify_server_keys(response, Some((notary, &notary_keys)))
				.inspect_err(|e| debug_warn!(%notary, "Discarding notary key response: {e}"))
				.ok()
		})
		.filter(|server_keys| {
			let requested = requested(&server_keys.server_name);
			if !requested {
				debug_warn!(
					%notary,
					server = %server_keys.server_name,
					"Discarding notary key response for a server not requested"
				);
			}

			requested
		})
		.collect()
}

/// The notary's current keys, valid now, which signed `responses`. A key not
/// cached is fetched from the notary itself, whose own key response is
/// self-signed and so needs no other notary.
#[implement(super::Service)]
async fn notary_keys(
	&self,
	notary: &ServerName,
	responses: &[Raw<ServerSigningKeys>],
) -> PubKeys {
	type Signatures = BTreeMap<OwnedServerName, BTreeMap<OwnedServerSigningKeyId, String>>;

	let key_ids: BTreeSet<OwnedServerSigningKeyId> = responses
		.iter()
		.filter_map(|response| {
			response
				.get_field::<Signatures>("signatures")
				.ok()
				.flatten()
		})
		.filter_map(|mut signatures| signatures.remove(notary))
		.flat_map(BTreeMap::into_keys)
		.collect();

	let usage = KeyUse::Request(MilliSecondsSinceUnixEpoch::now());
	let mut fetched = false;
	let mut keys = PubKeys::new();
	for key_id in &key_ids {
		let mut key = self.cached_key(notary, key_id, usage).await;
		if key.is_none() && !fetched {
			fetched = true;
			if let Ok(server_keys) = self.server_request(notary).await {
				self.add_signing_keys(server_keys).await;
				key = self.cached_key(notary, key_id, usage).await;
			}
		}

		if let Some(key) = key {
			keys.insert(key_id.as_str().into(), key.key);
		}
	}

	keys
}

/// Checks a key response is signed by its server with one of the verify keys
/// it lists and, when a notary relayed it, by the notary with one of
/// `notary_keys`. Signatures by any other server are not considered.
pub(super) fn verify_server_keys(
	response: &Raw<ServerSigningKeys>,
	notary: Option<(&ServerName, &PubKeys)>,
) -> Result<ServerSigningKeys> {
	let server_keys: ServerSigningKeys = response.deserialize()?;
	let origin = server_keys.server_name.as_str();

	let mut required = PubKeyMap::new();
	required.insert(
		origin.into(),
		server_keys
			.verify_keys
			.iter()
			.map(|(key_id, key)| (key_id.as_str().into(), key.key.clone()))
			.collect(),
	);

	if let Some((notary, notary_keys)) = notary {
		required
			.entry(notary.as_str().into())
			.or_default()
			.extend(notary_keys.clone());
	}

	let mut object: CanonicalJsonObject = serde_json::from_str(response.json().get())?;
	let Some(CanonicalJsonValue::Object(signatures)) = object.get_mut("signatures") else {
		return Err!(BadServerResponse("Key response for {origin} is not signed."));
	};

	signatures.retain(|entity, _| required.contains_key(entity.as_str()));
	if let Some(unsigned) = required
		.keys()
		.find(|entity| !signatures.contains_key(entity.as_str()))
	{
		return Err!(BadServerResponse("Key response for {origin} is not signed by {unsigned}."));
	}

	verify_json(&required, &object).map_err(|e| {
		err!(BadServerResponse("Key response for {origin} failed verification: {e}"))
	})?;

	Ok(server_keys)
}
