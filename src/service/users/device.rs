use std::{
	io::{self, Write},
	iter::once,
	net::IpAddr,
	sync::Arc,
	time::{Duration, SystemTime},
};

use futures::{Stream, StreamExt, pin_mut};
use ruma::{
	DeviceId, MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedUserId, UserId,
	api::client::device::Device, events::AnyToDeviceEvent, serde::Raw,
	to_device::DeviceIdOrAllDevices,
};
use serde::Serialize;
use serde_json::json;
use tuwunel_core::{
	Err, Error, Result, at, implement, trace,
	utils::{
		self, BoolExt, ReadyExt, random_string,
		result::LogErr,
		stream::TryIgnore,
		string::to_small_string,
		time::{
			duration_since_epoch, timepoint_from_epoch, timepoint_from_now, timepoint_has_passed,
		},
	},
};
use tuwunel_database::{
	Cbor, Deserialized, Ignore, Interfix, Json, Map, Txn, deserialize_from_slice, serialize_key,
};

/// generated device ID length
const DEVICE_ID_LENGTH: usize = 10;

/// generated user access token length
pub const TOKEN_LENGTH: usize = 32;

/// Adds a new device to a user.
#[implement(super::Service)]
#[tracing::instrument(level = "info", skip(self, access_token))]
pub async fn create_device(
	&self,
	user_id: &UserId,
	device_id: Option<&DeviceId>,
	(access_token, expires_in): (Option<&str>, Option<Duration>),
	refresh_token: Option<&str>,
	initial_device_display_name: Option<&str>,
	client_ip: Option<IpAddr>,
) -> Result<OwnedDeviceId> {
	let device_id = resolve_device_id(device_id);

	if !self.exists(user_id).await {
		return Err!(Request(InvalidParam(error!(
			"Called create_device for non-existent user {user_id}"
		))));
	}

	let notify = true;
	self.put_device_metadata(user_id, notify, &Device {
		device_id: device_id.clone(),
		display_name: initial_device_display_name.map(Into::into),
		last_seen_ts: Some(MilliSecondsSinceUnixEpoch::now()),
		last_seen_ip: client_ip.map(to_small_string),
	})
	.await?;

	if let Some(access_token) = access_token {
		self.set_access_token(user_id, &device_id, access_token, expires_in, refresh_token)
			.await?;
	}

	Ok(device_id)
}

fn resolve_device_id(device_id: Option<&DeviceId>) -> OwnedDeviceId {
	// Treat an empty device_id ("") as unspecified.
	device_id
		.filter(|device_id| !device_id.as_str().is_empty())
		.map(ToOwned::to_owned)
		.unwrap_or_else(|| OwnedDeviceId::from(random_string(DEVICE_ID_LENGTH)))
}

/// Removes a device from a user.
#[implement(super::Service)]
#[tracing::instrument(level = "info", skip(self))]
pub async fn remove_device(&self, user_id: &UserId, device_id: &DeviceId) -> Result {
	self.remove_devices(user_id, &[device_id.to_owned()])
		.await
}

/// Returns an iterator over all device ids of this user.
#[implement(super::Service)]
pub fn all_device_ids<'a>(
	&'a self,
	user_id: &'a UserId,
) -> impl Stream<Item = &DeviceId> + Send + 'a {
	let prefix = (user_id, Interfix);
	self.db
		.userdeviceid_metadata
		.keys_prefix(&prefix)
		.ignore_err()
		.map(|(_, device_id): (Ignore, &DeviceId)| device_id)
}

/// Find out which user an access or refresh token belongs to.
#[implement(super::Service)]
#[tracing::instrument(level = "trace", skip(self, token))]
pub async fn find_from_token(
	&self,
	token: &str,
) -> Result<(OwnedUserId, OwnedDeviceId, Option<SystemTime>)> {
	self.db
		.token_userdeviceid
		.get(token)
		.await
		.deserialized()
		.and_then(|(user_id, device_id, expires_at): (_, _, Option<u64>)| {
			let expires_at = expires_at
				.map(Duration::from_secs)
				.map(timepoint_from_epoch)
				.transpose()?;

			Ok((user_id, device_id, expires_at))
		})
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn remove_tokens(&self, user_id: &UserId, device_id: &DeviceId) -> Result {
	let _guard = self.device_mutation_locks.lock(user_id).await;
	let services = self.services.get();
	let mut txn = services.db.txn();
	self.append_token_removal(user_id, device_id, true, true, &mut txn)
		.await?;
	txn.execute().await
}

/// Replaces the access token of one device.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn set_access_token(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	access_token: &str,
	expires_in: Option<Duration>,
	refresh_token: Option<&str>,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	assert!(
		access_token.len() >= TOKEN_LENGTH,
		"Caller must supply an access_token >= {TOKEN_LENGTH} chars."
	);

	if let Some(refresh_token) = refresh_token {
		self.set_refresh_token(user_id, device_id, refresh_token)
			.await?;
	}

	let _guard = self.device_mutation_locks.lock(user_id).await;
	self.db
		.userdeviceid_metadata
		.qry(&(user_id, device_id))
		.await?;
	let expires_at = expires_in
		.map(timepoint_from_now)
		.transpose()?
		.map(duration_since_epoch)
		.as_ref()
		.map(Duration::as_secs);

	let userdeviceid = (user_id, device_id);

	// Fold the prior pointer token into the index for pre-index upgrades.
	let previous = match self
		.db
		.userdeviceid_token
		.qry(&userdeviceid)
		.await
		.deserialized::<String>()
	{
		| Ok(previous) => Some(previous),
		| Err(error) if error.is_not_found() => None,
		| Err(error) => return Err(error),
	};

	let mut txn = services_root.db.txn();

	if let Some(previous) = previous.as_deref() {
		let key = (user_id, device_id, previous);

		txn.put_raw(&self.db.userdeviceidtoken_index, key, []);
	}

	let key = (user_id, device_id, access_token);
	let value = (user_id, device_id, expires_at);

	txn.raw_put(&self.db.token_userdeviceid, access_token, value);
	txn.put_raw(&self.db.userdeviceidtoken_index, key, []);
	txn.put_raw(&self.db.userdeviceid_token, userdeviceid, access_token);

	txn.execute().await?;

	Ok(())
}

/// Revoke every access token of one device, without deleting the device. Take
/// care to not leave dangling devices if using this method.
#[implement(super::Service)]
pub async fn remove_access_token(&self, user_id: &UserId, device_id: &DeviceId) -> Result {
	let _guard = self.device_mutation_locks.lock(user_id).await;
	let services = self.services.get();
	let mut txn = services.db.txn();
	self.append_token_removal(user_id, device_id, true, false, &mut txn)
		.await?;
	txn.execute().await
}

/// Revoke a single access token by value, leaving the device and any other
/// tokens it holds intact.
#[implement(super::Service)]
pub async fn remove_access_token_value(&self, access_token: &str) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();
	let owner = match self
		.db
		.token_userdeviceid
		.get(access_token)
		.await
		.deserialized::<(OwnedUserId, OwnedDeviceId, Option<u64>)>()
	{
		| Ok(owner) => owner,
		| Err(error) if error.is_not_found() => return Ok(()),
		| Err(error) => return Err(error),
	};
	let _guard = self
		.device_mutation_locks
		.lock::<UserId>(&owner.0)
		.await;
	let current = match self
		.db
		.token_userdeviceid
		.get(access_token)
		.await
		.deserialized::<(OwnedUserId, OwnedDeviceId, Option<u64>)>()
	{
		| Ok(owner) => owner,
		| Err(error) if error.is_not_found() => return Ok(()),
		| Err(error) => return Err(error),
	};
	if current != owner {
		return Err(Error::bad_database("Token ownership changed during revocation"));
	}
	let mut txn = services_root.db.txn();
	txn.del(&self.db.userdeviceidtoken_index, (&*owner.0, &*owner.1, access_token));
	txn.del_raw(&self.db.token_userdeviceid, access_token);
	txn.execute().await
}

#[implement(super::Service)]
pub fn generate_access_token(&self, expires: bool) -> (String, Option<Duration>) {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let access_token = random_string(TOKEN_LENGTH);
	let expires_in = expires
		.then_some(services_root.server.config.access_token_ttl)
		.map(Duration::from_secs);

	(access_token, expires_in)
}

/// Replaces the refresh token of one device.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn set_refresh_token(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	refresh_token: &str,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	debug_assert!(refresh_token.starts_with("refresh_"), "refresh_token missing prefix");

	let _guard = self.device_mutation_locks.lock(user_id).await;
	self.db
		.userdeviceid_metadata
		.qry(&(user_id, device_id))
		.await?;
	let config = &services_root.server.config;
	let ttl = config.refresh_token_ttl;
	let idle_only = config.refresh_token_idle_only;

	// Absolute mode carries the prior deadline forward instead of sliding it.
	let prior_expires_at: Option<SystemTime> = (ttl != 0 && !idle_only)
		.then_async(|| self.find_refresh_token_expires_at(user_id, device_id))
		.await
		.flatten();

	// Capture the outgoing token before removal so it can be retained for one
	// generation, making a later replay detectable.
	let spent: Option<String> = self
		.db
		.userdeviceid_refresh
		.qry(&(user_id, device_id))
		.await
		.deserialized()
		.ok();

	// Also drops the prior spent entry.
	self.remove_refresh_token_unlocked(user_id, device_id)
		.await?;

	let expires_at = match (ttl, prior_expires_at) {
		| (0, _) => None,
		| (_, Some(prior)) => Some(prior),
		| (ttl, None) => Some(timepoint_from_now(Duration::from_secs(ttl))?),
	};

	let expires_at_secs = expires_at
		.map(duration_since_epoch)
		.as_ref()
		.map(Duration::as_secs);

	let userdeviceid = (user_id, device_id);
	let value = (user_id, device_id, expires_at_secs);
	let mut txn = services_root.db.txn();

	txn.raw_put(&self.db.token_userdeviceid, refresh_token, value);
	txn.put_raw(&self.db.userdeviceid_refresh, userdeviceid, refresh_token);

	// Retain the outgoing token as the device's spent token, pointing at its
	// successor so a double-submit can be distinguished from a replay.
	if let Some(spent) = spent {
		let spent_at = duration_since_epoch(SystemTime::now()).as_secs();
		let value = (user_id, device_id, refresh_token, spent_at);

		txn.raw_put(&self.db.spentrefresh_userdeviceid, &*spent, value);
		txn.put_raw(&self.db.userdeviceid_spentrefresh, userdeviceid, &*spent);
	}

	txn.execute().await?;

	Ok(())
}

/// Look up the expiry stored alongside the current refresh token for this
/// device, if one is recorded. Pre-rotation entries carry no expiry and
/// return `None`.
#[implement(super::Service)]
async fn find_refresh_token_expires_at(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
) -> Option<SystemTime> {
	let userdeviceid = (user_id, device_id);
	let old_token: String = self
		.db
		.userdeviceid_refresh
		.qry(&userdeviceid)
		.await
		.deserialized()
		.ok()?;

	let (_, _, expires_at_secs): (Ignore, Ignore, Option<u64>) = self
		.db
		.token_userdeviceid
		.get(&old_token)
		.await
		.deserialized()
		.ok()?;

	expires_at_secs
		.map(Duration::from_secs)
		.map(timepoint_from_epoch)?
		.ok()
}

/// Revoke the refresh token without deleting the device. Take care to not leave
/// dangling devices if using this method.
#[implement(super::Service)]
pub async fn remove_refresh_token(&self, user_id: &UserId, device_id: &DeviceId) -> Result {
	let _guard = self.device_mutation_locks.lock(user_id).await;
	self.remove_refresh_token_unlocked(user_id, device_id)
		.await
}

#[implement(super::Service)]
async fn remove_refresh_token_unlocked(&self, user_id: &UserId, device_id: &DeviceId) -> Result {
	let services = self.services.get();
	let mut txn = services.db.txn();
	self.append_token_removal(user_id, device_id, false, true, &mut txn)
		.await?;
	txn.execute().await
}

/// Classification of a refresh token presented for rotation at a token
/// endpoint.
pub enum RefreshToken {
	/// The device's current refresh token; rotate it.
	Current {
		user_id: OwnedUserId,
		device_id: OwnedDeviceId,
		expires_at: Option<SystemTime>,
	},

	/// A spent (already-rotated) token retained for one generation. `grace` is
	/// set when its successor is still current and it was spent within the
	/// configured window, marking a benign double-submit rather than a replay;
	/// `current` is the successor for which to re-issue an access token.
	Replayed {
		user_id: OwnedUserId,
		device_id: OwnedDeviceId,
		current: String,
		grace: bool,
	},

	/// Not a recognised refresh token.
	Unknown,
}

/// Classify a presented refresh token for the token-endpoint rotation path.
#[implement(super::Service)]
pub async fn classify_refresh_token(&self, presented: &str) -> RefreshToken {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	// The current refresh token resolves and matches the device's active
	// pointer (an access token resolves but will not match).
	if let Ok((user_id, device_id, expires_at)) = self.find_from_token(presented).await {
		let current: Option<String> = self
			.db
			.userdeviceid_refresh
			.qry(&(&user_id, &device_id))
			.await
			.deserialized()
			.ok();

		if current.as_deref() == Some(presented) {
			return RefreshToken::Current { user_id, device_id, expires_at };
		}
	}

	// Otherwise it may be the one retained spent token: a benign double-submit
	// inside the grace window, or a replay to be treated as a compromise.
	let Ok((user_id, device_id, successor, spent_at)) = self
		.db
		.spentrefresh_userdeviceid
		.get(presented)
		.await
		.deserialized::<(OwnedUserId, OwnedDeviceId, String, u64)>()
	else {
		return RefreshToken::Unknown;
	};

	let current: Option<String> = self
		.db
		.userdeviceid_refresh
		.qry(&(&user_id, &device_id))
		.await
		.deserialized()
		.ok();

	let grace_window = services_root
		.server
		.config
		.refresh_token_reuse_grace;
	let elapsed = duration_since_epoch(SystemTime::now())
		.as_secs()
		.saturating_sub(spent_at);

	let grace = grace_window != 0
		&& elapsed <= grace_window
		&& current.as_deref() == Some(successor.as_str());

	RefreshToken::Replayed {
		user_id,
		device_id,
		current: successor,
		grace,
	}
}

#[must_use]
pub fn generate_refresh_token() -> String { format!("refresh_{}", random_string(TOKEN_LENGTH)) }

/// One to-device message for a local recipient: a single device, or every
/// device the user has when the message is delivered.
pub struct ToDeviceTarget {
	pub user_id: OwnedUserId,
	pub device: DeviceIdOrAllDevices,
	pub content: serde_json::Value,
}

fn to_device_limit() -> Error {
	tuwunel_core::err!(Request(TooLarge("To-device delivery inventory or payload limit")))
}

fn to_device_admission_error(error: &tuwunel_bridge::Error) -> Error {
	tuwunel_core::err!(Request(TooLarge("To-device commit admission: {error}")))
}

fn admit_to_device_targets(targets: &[ToDeviceTarget], txnid: Option<&[u8]>) -> Result {
	if targets.len() > tuwunel_bridge::MAX_COMMIT_OPS {
		return Err(to_device_limit());
	}
	let mut bytes = txnid.map_or(0, <[u8]>::len);
	if txnid.is_some_and(|key| key.is_empty() || key.len() > tuwunel_bridge::MAX_KEY_BYTES) {
		return Err(to_device_limit());
	}
	for target in targets {
		let device_bytes = match &target.device {
			| DeviceIdOrAllDevices::DeviceId(id) => id.as_str().len(),
			| DeviceIdOrAllDevices::AllDevices => 1,
		};
		if target.user_id.as_bytes().len() > tuwunel_bridge::MAX_KEY_BYTES
			|| device_bytes > tuwunel_bridge::MAX_KEY_BYTES
		{
			return Err(to_device_limit());
		}
		bytes = bytes
			.saturating_add(target.user_id.as_bytes().len())
			.saturating_add(device_bytes);
		if bytes > tuwunel_bridge::request::MAX_BYTES {
			return Err(to_device_limit());
		}
	}
	Ok(())
}

#[derive(Serialize)]
struct ToDeviceBody<'a> {
	// Preserve the existing Json(json!({...})) map's stored field ordering.
	content: &'a serde_json::Value,
	sender: &'a UserId,
	#[serde(rename = "type")]
	kind: &'a str,
}

struct ToDeviceBodyWriter {
	bytes: Vec<u8>,
	exceeded: bool,
}

impl Write for ToDeviceBodyWriter {
	fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
		let length = self.bytes.len().saturating_add(bytes.len());
		if length > tuwunel_bridge::MAX_VALUE_BYTES {
			self.exceeded = true;
			return Err(io::Error::from(io::ErrorKind::FileTooLarge));
		}
		if length > self.bytes.capacity() {
			let capacity = length
				.max(self.bytes.capacity().saturating_mul(2))
				.min(tuwunel_bridge::MAX_VALUE_BYTES);
			self.bytes
				.try_reserve_exact(capacity.saturating_sub(self.bytes.len()))
				.map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
		}
		self.bytes.extend_from_slice(bytes);
		Ok(bytes.len())
	}

	fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

fn encode_to_device_body(
	sender: &UserId,
	kind: &str,
	content: &serde_json::Value,
) -> Result<Vec<u8>> {
	let mut writer = ToDeviceBodyWriter { bytes: Vec::new(), exceeded: false };
	match serde_json::to_writer(&mut writer, &ToDeviceBody { content, sender, kind }) {
		| Ok(()) => Ok(writer.bytes),
		| Err(_) if writer.exceeded => Err(to_device_limit()),
		| Err(error) => Err(error.into()),
	}
}

/// Store one to-device message in one device's inbox, returning its count.
#[implement(super::Service)]
pub async fn add_to_device_event(
	&self,
	sender: &UserId,
	target_user_id: &UserId,
	target_device_id: &DeviceId,
	event_type: &str,
	content: &serde_json::Value,
) -> u64 {
	let _guard = self
		.device_mutation_locks
		.lock(target_user_id)
		.await;

	// A delivery that waited behind revocation must not recreate an inbox.
	match self
		.db
		.userdeviceid_metadata
		.qry(&(target_user_id, target_device_id))
		.await
	{
		| Ok(_) => {},
		| Err(error) if error.is_not_found() => return 0,
		| Err(error) => panic!("Cannot check recipient device: {error}"),
	}

	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let count = services_root
		.globals
		.next_count()
		.await
		.expect("failed to obtain next sequence number");

	let delivery = (target_user_id, target_device_id, *count, content);
	self.to_device_txn(sender, event_type, once(delivery), None)
		.execute()
		.await
		.expect("database insert error");

	trace!(
		%target_user_id,
		%target_device_id,
		count = *count,
		%event_type,
		%sender,
		"to_device write",
	);

	*count
}

/// Admit one request's local inboxes and frozen interested-appservice copies
/// in the same transaction as its deduplication record.
///
/// Every inbox write and the request's transaction record (`txnid`, a
/// [`crate::transaction_ids::key`], recorded with an empty response) land in
/// one commit. An interrupted request therefore delivers to all of its
/// recipients or none, and a retry that finds the record delivers nothing
/// again. `AllDevices` resolves to the devices the user has now.
///
/// Complete shared inbox/forwarding admission precedes count allocation.
/// Only payload-free wake hints follow the durable commit; restart discovers
/// copies whose wake was interrupted.
#[implement(super::Service)]
pub async fn deliver_to_device(
	&self,
	sender: &UserId,
	event_type: &str,
	targets: &[ToDeviceTarget],
	txnid: Option<&[u8]>,
) -> Result {
	admit_to_device_targets(targets, txnid)?;
	let mut recipients = targets
		.iter()
		.map(|target| target.user_id.clone())
		.collect::<Vec<_>>();
	recipients.sort_unstable();
	recipients.dedup();
	let mut guards = Vec::new();
	for user in &recipients {
		guards.push(
			self.device_mutation_locks
				.lock::<UserId>(user)
				.await,
		);
	}

	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	// Complete inventories and the exact encoded inbox budget precede count
	// permits and transaction allocation. The transaction record shares the
	// same mixed-map budget and actual writer lease as the inbox rows.
	let mut budget = self.db.todeviceid_events.put_batch_budget()?;
	if let Some(txnid) = txnid {
		let map = tuwunel_bridge::catalog::map_id("userdevicetxnid_response")
			.ok_or_else(|| Error::bad_database("Missing transaction record catalog map"))?;
		budget
			.try_put_to(map.0, txnid.len(), &[])
			.map_err(|error| to_device_admission_error(&error))?;
	}
	let mut resolved = Vec::new();
	for target in targets {
		let (device_ids, forward) = match &target.device {
			| DeviceIdOrAllDevices::DeviceId(device_id) => (vec![device_id.clone()], true),
			| DeviceIdOrAllDevices::AllDevices =>
				(self.bounded_device_ids(&target.user_id).await?, true),
		};
		let mut available = Vec::new();
		for device_id in device_ids {
			match self
				.db
				.userdeviceid_metadata
				.qry(&(&*target.user_id, &*device_id))
				.await
			{
				| Ok(_) => available.push(device_id),
				| Err(error) if error.is_not_found() => {},
				| Err(error) => return Err(error),
			}
		}
		if available.is_empty() {
			continue;
		}
		let body = encode_to_device_body(sender, event_type, &target.content)?;
		for device_id in &available {
			let key = serialize_key((&*target.user_id, &**device_id, 0_u64))?;
			budget
				.try_put(key.len(), &body)
				.map_err(|error| to_device_admission_error(&error))?;
		}
		resolved.push((target, available, forward, body));
	}

	let mut forwarding = services_root
		.sending
		.prepare_to_device_forwarding(
			sender,
			event_type,
			resolved
				.iter()
				.map(|(target, devices, forward, _)| {
					(&*target.user_id, devices.as_slice(), &target.content, *forward)
				}),
			&mut budget,
		)
		.await?;
	let mut counts = Vec::new();
	let mut permits = Vec::new();
	let mut txn = services_root.db.txn();
	for (target, devices, _, body) in resolved {
		for device in devices {
			let count = services_root.globals.next_count().await?;
			txn.put_raw(&self.db.todeviceid_events, (&*target.user_id, &*device, *count), &body);
			counts.push(*count);
			permits.push(count);
		}
	}
	if let Some(txnid) = txnid {
		txn.insert_raw(&self.db.userdevicetxnid_response, txnid, []);
	}
	services_root
		.sending
		.append_to_device_forwarding(&mut forwarding, &counts, &mut txn)
		.await?;
	txn.check_bridge_admission()?;
	txn.execute_flushed().await?;
	drop(guards);
	drop(permits);
	services_root
		.sending
		.finish_to_device_forwarding(forwarding)
		.log_err()
		.ok();

	Ok(())
}

/// The single commit that stores to-device messages in their recipients'
/// inboxes, with the sending request's transaction record when `txnid` is
/// given.
///
/// Each delivery is (recipient user, recipient device, count, content).
/// Returned unexecuted so the commit's contents can be inspected;
/// [`Self::deliver_to_device`] executes it.
#[implement(super::Service)]
pub fn to_device_txn<'a, I>(
	&self,
	sender: &UserId,
	event_type: &str,
	deliveries: I,
	txnid: Option<&[u8]>,
) -> Txn
where
	I: IntoIterator<Item = (&'a UserId, &'a DeviceId, u64, &'a serde_json::Value)>,
{
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let mut txn = services_root.db.txn();

	for (target_user_id, target_device_id, count, content) in deliveries {
		txn.put(
			&self.db.todeviceid_events,
			(target_user_id, target_device_id, count),
			Json(json!({
				"type": event_type,
				"sender": sender,
				"content": content,
			})),
		);
	}

	// An empty response marks a to-device transaction; the room send routes
	// refuse its reuse as an incompatible endpoint.
	if let Some(txnid) = txnid {
		txn.insert_raw(&self.db.userdevicetxnid_response, txnid, []);
	}

	txn
}

#[implement(super::Service)]
pub fn get_to_device_events<'a>(
	&'a self,
	user_id: &'a UserId,
	device_id: &'a DeviceId,
	since: Option<u64>,
	to: Option<u64>,
) -> impl Stream<Item = (u64, Raw<AnyToDeviceEvent>)> + Send + 'a {
	type Key<'a> = (&'a UserId, &'a DeviceId, u64);

	let from = (user_id, device_id, since.map_or(0, |since| since.saturating_add(1)));

	self.db
		.todeviceid_events
		.stream_from(&from)
		.ignore_err()
		.ready_take_while(move |((user_id_, device_id_, count), _): &(Key<'_>, _)| {
			user_id == *user_id_ && device_id == *device_id_ && to.is_none_or(|to| *count <= to)
		})
		.map(|((_, _, count), event)| (count, event))
}

/// A complete diagnostic inbox inventory, bounded by the existing admin
/// inventory ceilings: 128 rows and 64 KiB of combined stored keys and JSON.
/// One lookahead detects overflow. Charge borrowed bytes before typed key
/// decoding or owning JSON; storage and codec errors refuse the whole result.
/// Sync consumers retain their existing streaming interface.
#[implement(super::Service)]
pub async fn bounded_to_device_events(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
) -> Result<Vec<(u64, Raw<AnyToDeviceEvent>)>> {
	let prefix = (user_id, device_id, Interfix);
	let rows = self
		.db
		.todeviceid_events
		.stream_prefix_capped::<&[u8], &[u8], _>(
			&prefix,
			super::MAX_ADMIN_DEVICE_ROWS.saturating_add(1),
		);
	pin_mut!(rows);
	let mut events = Vec::new();
	let mut bytes = 0_usize;
	while let Some(row) = rows.next().await {
		let (key, value) = row?;
		bytes = bytes
			.saturating_add(key.len())
			.saturating_add(value.len());
		if events.len() >= super::MAX_ADMIN_DEVICE_ROWS || bytes > super::MAX_ADMIN_DEVICE_BYTES {
			return Err(Error::bad_database(
				"To-device diagnostic inventory exceeds row/byte budget",
			));
		}
		let (owner, device, count): (&UserId, &DeviceId, u64) = deserialize_from_slice(key)?;
		if owner != user_id || device != device_id {
			return Err(Error::bad_database(
				"To-device inbox key does not match requested device",
			));
		}
		let event = serde_json::from_slice(value)
			.map_err(|_| Error::bad_database("Cannot deserialize stored to-device event JSON"))?;
		events.push((count, event));
	}
	Ok(events)
}

/// A bounded contiguous inbox page. `next_batch` only advances past admitted
/// events; it reaches `until` only when the entire captured range is exhausted.
#[derive(Debug)]
pub struct ToDevicePage {
	/// Admitted messages, in stored sequence order.
	pub events: Vec<(u64, Raw<AnyToDeviceEvent>)>,
	/// Inclusive watermark proved safe by this page.
	pub next_batch: u64,
	/// Whether the captured range was exhausted rather than page limited.
	pub complete: bool,
}

/// Maximum inbox rows retained in one sync response.
pub const MAX_TO_DEVICE_PAGE_ROWS: usize = 128;
/// Stored key and JSON bytes retained in one inbox page. A legal maximum-width
/// value fits alone. Lookahead borrows at most one extra storage-width row.
pub const MAX_TO_DEVICE_PAGE_BYTES: usize = tuwunel_bridge::request::MAX_BYTES;

/// Reads a contiguous checked page from `(since, until]` before any ACK write.
/// Source fetches stop at `limit + 1`; raw key/value widths, borrowed tuple
/// decoding and JSON validity are checked even for lookahead. Retained bytes
/// are at most 4 MiB; inspection adds at most one storage-width peek row.
/// JSON ownership follows byte/row admission, so the peek is never cloned or
/// skipped by the next cursor.
#[implement(super::Service)]
pub async fn to_device_page(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	since: u64,
	until: u64,
	limit: usize,
) -> Result<ToDevicePage> {
	if limit == 0 || limit > MAX_TO_DEVICE_PAGE_ROWS || since > until {
		return Err(Error::bad_database("Invalid to-device page bounds"));
	}
	if since == until {
		return Ok(ToDevicePage {
			events: Vec::new(),
			next_batch: until,
			complete: true,
		});
	}
	let prefix = serialize_key((user_id, device_id, Interfix))?;
	let from = serialize_key((user_id, device_id, since.saturating_add(1)))?;
	let rows = self
		.db
		.todeviceid_events
		.stream_capped_from::<&[u8], &[u8]>(Some(&from), limit.saturating_add(1));
	pin_mut!(rows);
	let mut page = ToDevicePage {
		events: Vec::new(),
		next_batch: until,
		complete: true,
	};
	let mut bytes = 0_usize;
	while let Some(row) = rows.next().await {
		let (key, value) = row?;
		if !key.starts_with(&prefix) {
			break;
		}
		if key.len() > tuwunel_bridge::MAX_KEY_BYTES
			|| value.len() > tuwunel_bridge::MAX_VALUE_BYTES
		{
			return Err(Error::bad_database("To-device row exceeds storage width"));
		}
		let (owner, device, count): (&UserId, &DeviceId, u64) = deserialize_from_slice(key)?;
		if owner != user_id || device != device_id || count <= since {
			return Err(Error::bad_database("To-device page key does not match requested range"));
		}
		if count > until {
			break;
		}
		let prospective_bytes = bytes
			.saturating_add(key.len())
			.saturating_add(value.len());
		if page.events.len() == limit || prospective_bytes > MAX_TO_DEVICE_PAGE_BYTES {
			// Validate the borrowed peek without allocating an owned Raw event.
			let _: &serde_json::value::RawValue =
				serde_json::from_slice(value).map_err(|_| {
					Error::bad_database("Cannot deserialize stored to-device event JSON")
				})?;
			let Some((last, _)) = page.events.last() else {
				return Err(Error::bad_database("To-device event exceeds page byte budget"));
			};
			page.next_batch = *last;
			page.complete = false;
			break;
		}
		let event = serde_json::from_slice(value)
			.map_err(|_| Error::bad_database("Cannot deserialize stored to-device event JSON"))?;
		page.events.push((count, event));
		bytes = prospective_bytes;
	}
	Ok(page)
}

/// Collects one atomic bounded page of already acknowledged inbox rows. Old
/// acknowledged backlog may remain for a later GC pass, but reads exclude it
/// by `since`; limiting GC therefore cannot omit or redeliver pending events.
/// No deletion is staged until every selected key has been checked.
#[implement(super::Service)]
pub async fn remove_to_device_events<Until>(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	until: Until,
) -> Result
where
	Until: Into<Option<u64>> + Send,
{
	let _guard = self.device_mutation_locks.lock(user_id).await;
	let until = until.into().unwrap_or(u64::MAX);
	let prefix = serialize_key((user_id, device_id, Interfix))?;
	let from = serialize_key((user_id, device_id, until))?;
	let keys = self
		.db
		.todeviceid_events
		.raw_keys_prefix_reverse(&prefix, &from, MAX_TO_DEVICE_PAGE_ROWS)
		.await?;
	for key in &keys {
		let (owner, device, count): (&UserId, &DeviceId, u64) = deserialize_from_slice(key)?;
		if owner != user_id || device != device_id || count > until {
			return Err(Error::bad_database("To-device ACK key does not match requested range"));
		}
	}
	let services = self.services.get();
	let mut txn = services.db.txn();
	for key in keys {
		txn.del_raw(&self.db.todeviceid_events, key);
	}
	txn.check_bridge_admission()?;
	txn.execute().await
}

#[implement(super::Service)]
pub async fn update_device_last_seen(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	last_seen_ip: Option<IpAddr>,
	last_seen_ts: Option<MilliSecondsSinceUnixEpoch>,
) -> Result {
	let mut device = self
		.get_device_metadata(user_id, device_id)
		.await?;

	if let Some(last_seen_ip) = last_seen_ip.map(to_small_string) {
		device.last_seen_ip.replace(last_seen_ip);
	}

	device
		.last_seen_ts
		.replace(last_seen_ts.unwrap_or_else(MilliSecondsSinceUnixEpoch::now));

	self.put_device_metadata(user_id, false, &device)
		.await?;

	Ok(())
}

#[implement(super::Service)]
pub async fn put_device_metadata(
	&self,
	user_id: &UserId,
	notify: bool,
	device: &Device,
) -> Result {
	let _guard = self.device_mutation_locks.lock(user_id).await;
	let key = (user_id, &device.device_id);
	if !notify {
		self.db.userdeviceid_metadata.qry(&key).await?;
	}
	self.db
		.userdeviceid_metadata
		.put(key, Json(device))
		.await?;

	if notify {
		increment(&self.db.userid_devicelistversion, user_id.as_bytes()).await?;
	}

	Ok(())
}

/// Get device metadata.
#[implement(super::Service)]
pub async fn get_device_metadata(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
) -> Result<Device> {
	self.db
		.userdeviceid_metadata
		.qry(&(user_id, device_id))
		.await
		.deserialized()
		.inspect(|device: &Device| {
			debug_assert_eq!(&device.device_id, device_id, "device_id mismatch");
		})
}

#[implement(super::Service)]
pub async fn device_exists(&self, user_id: &UserId, device_id: &DeviceId) -> bool {
	self.db
		.userdeviceid_metadata
		.contains(&(user_id, device_id))
		.await
}

#[implement(super::Service)]
pub async fn is_oidc_device(&self, user_id: &UserId, device_id: &DeviceId) -> bool {
	self.db
		.oidcdevice_userdeviceid
		.contains(&(user_id, device_id))
		.await
}

/// Returns the IdP that originally authenticated this device, if known.
/// Returns `None` for devices predating the idp_id field or non-OIDC devices.
#[implement(super::Service)]
pub async fn get_oidc_device_idp(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
) -> Option<String> {
	self.db
		.oidcdevice_userdeviceid
		.qry(&(user_id, device_id))
		.await
		.deserialized::<Json<String>>()
		.ok()
		.map(|Json(idp)| idp)
		.filter(|idp| !idp.is_empty())
}

#[implement(super::Service)]
pub async fn mark_oidc_device(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	idp_id: &str,
) -> Result {
	let _guard = self.device_mutation_locks.lock(user_id).await;
	self.db
		.userdeviceid_metadata
		.qry(&(user_id, device_id))
		.await?;

	self.db
		.oidcdevice_userdeviceid
		.put((user_id, device_id), Json(idp_id))
		.await?;

	Ok(())
}

/// Allow cross-signing key replacement without UIAA for the next 10 minutes.
/// Returns the expiry timestamp in milliseconds.
#[implement(super::Service)]
pub async fn allow_cross_signing_replacement(&self, user_id: &UserId) -> SystemTime {
	let duration = Duration::from_mins(10);
	let expires = timepoint_from_now(duration).expect("failed to create timepoint from now");

	self.db
		.oidccskeybypass_userid
		.raw_put(user_id, Cbor(expires))
		.await
		.expect("database write error");

	expires
}

/// Check if the user is allowed to replace cross-signing keys without UIAA.
#[implement(super::Service)]
pub async fn can_replace_cross_signing_keys(&self, user_id: &UserId) -> bool {
	let Ok(expires): Result<SystemTime, _> = self
		.db
		.oidccskeybypass_userid
		.get(user_id)
		.await
		.deserialized::<Cbor<_>>()
		.map(at!(0))
	else {
		return false;
	};

	if !timepoint_has_passed(expires) {
		return true;
	}

	self.db
		.oidccskeybypass_userid
		.remove(user_id)
		.await
		.expect("database write error");
	false
}

#[implement(super::Service)]
pub async fn get_devicelist_version(&self, user_id: &UserId) -> Result<u64> {
	self.db
		.userid_devicelistversion
		.get(user_id)
		.await
		.deserialized()
}

#[implement(super::Service)]
pub fn all_devices_metadata<'a>(
	&'a self,
	user_id: &'a UserId,
) -> impl Stream<Item = Device> + Send + 'a {
	let key = (user_id, Interfix);
	self.db
		.userdeviceid_metadata
		.stream_prefix(&key)
		.ignore_err()
		.map(|(_, val): (Ignore, Device)| val)
}

//TODO: this is an ABA
async fn increment(db: &Arc<Map>, key: &[u8]) -> Result {
	let old = db.get_blocking(key);
	let new = utils::increment(old.ok().as_deref());
	db.insert(key, new).await
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn absent_device_id_is_generated() {
		let device_id = resolve_device_id(None);

		assert_eq!(device_id.as_str().len(), DEVICE_ID_LENGTH);
	}

	#[test]
	fn empty_device_id_is_generated() {
		let device_id = resolve_device_id(Some("".into()));

		assert_eq!(device_id.as_str().len(), DEVICE_ID_LENGTH);
	}

	#[test]
	fn provided_device_id_is_preserved() {
		let device_id = resolve_device_id(Some("HELLOWORLD".into()));

		assert_eq!(device_id.as_str(), "HELLOWORLD");
	}
}

#[cfg(test)]
#[path = "to_device_admission_tests.rs"]
mod to_device_admission_tests;

#[cfg(test)]
#[path = "inbox_paging_tests.rs"]
mod inbox_paging_tests;
