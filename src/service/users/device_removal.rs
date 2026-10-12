//! Complete checked revocation inventories, prepared before the first deletion.
use std::sync::Arc;

use ruma::{DeviceId, OwnedDeviceId, OwnedUserId, UserId, events::RoomAccountDataEventType};
use tuwunel_core::{
	Error, Result, implement,
	utils::{MutexMapGuard, two_phase_counter::Permit},
};
use tuwunel_database::{Interfix, Json, Map, Txn, deserialize_from_slice, serialize_key};

use super::{DehydratedDevice, DeviceKeyUpdatePlan};

pub(super) const ROWS: usize = 900;
const BYTES: usize = 512 * 1024;
type Deletion = (Arc<Map>, Vec<u8>);

pub struct DeviceRemovalPlan {
	_user_guard: MutexMapGuard<OwnedUserId, ()>,
	user: OwnedUserId,
	deletions: Vec<Deletion>,
	devices: Vec<OwnedDeviceId>,
	pushkeys: Vec<String>,
	key_update: Option<DeviceKeyUpdatePlan>,
	push_cleanup: crate::sending::PreparedPushCleanup,
	remaining_devices: usize,
	version: Vec<u8>,
	account_counts: Vec<Permit>,
}

impl DeviceRemovalPlan {
	#[must_use]
	pub fn len(&self) -> usize { self.devices.len() }

	#[must_use]
	pub fn is_empty(&self) -> bool { self.devices.is_empty() }

	#[must_use]
	pub fn is_last_device(&self) -> bool { self.remaining_devices == 0 }

	#[must_use]
	pub fn remaining_devices(&self) -> usize { self.remaining_devices }
}

fn limit() -> Error { Error::bad_database("Device revocation inventory exceeds row/byte budget") }

fn charge(rows: &mut usize, bytes: &mut usize, width: usize) -> Result {
	*rows = rows.checked_add(1).ok_or_else(limit)?;
	*bytes = bytes.checked_add(width).ok_or_else(limit)?;
	if *rows > ROWS || *bytes > BYTES {
		return Err(limit());
	}
	Ok(())
}

async fn optional_raw(map: &Arc<Map>, key: &[u8]) -> Result<Option<Vec<u8>>> {
	match map.get(key).await {
		| Ok(value) if value.len() <= BYTES => Ok(Some(value.as_ref().to_vec())),
		| Ok(_) => Err(limit()),
		| Err(error) if error.is_not_found() => Ok(None),
		| Err(error) => Err(error),
	}
}

async fn checked_keys(
	map: &Arc<Map>,
	prefix: &[u8],
	rows: &mut usize,
	bytes: &mut usize,
) -> Result<Vec<Vec<u8>>> {
	let keys = map
		.raw_keys_prefix_after(prefix, None, ROWS.saturating_sub(*rows).saturating_add(1))
		.await?;
	for key in &keys {
		charge(rows, bytes, key.len())?;
	}
	Ok(keys)
}

async fn check_token_owner(
	map: &Arc<Map>,
	token: &[u8],
	user: &UserId,
	device: &DeviceId,
	spent: bool,
) -> Result {
	let raw = optional_raw(map, token).await?;
	if let Some(raw) = raw {
		let (owner, id) = if spent {
			let (owner, id, ..): (&UserId, &DeviceId, &str, u64) = deserialize_from_slice(&raw)?;
			(owner, id)
		} else {
			let (owner, id, _): (&UserId, &DeviceId, Option<u64>) = deserialize_from_slice(&raw)?;
			(owner, id)
		};
		if owner != user || id != device {
			return Err(Error::bad_database("Token index points to another device"));
		}
	}
	Ok(())
}

#[implement(super::Service)]
async fn token_deletions(
	&self,
	user: &UserId,
	device: &DeviceId,
	access: bool,
	refresh: bool,
) -> Result<Vec<Deletion>> {
	let mut deletes = Vec::new();
	let key = serialize_key((user, device))?.to_vec();
	let mut rows = 0;
	let mut bytes = 0;
	if access {
		let prefix = serialize_key((user, device, Interfix))?;
		let keys = checked_keys(&self.db.userdeviceidtoken_index, &prefix, &mut rows, &mut bytes)
			.await?;
		for key in keys {
			let (owner, id, token): (&UserId, &DeviceId, &str) = deserialize_from_slice(&key)?;
			if owner != user
				|| id != device
				|| token.len() > 1024
				|| serialize_key((owner, id, token))?.as_slice() != key.as_slice()
			{
				return Err(Error::bad_database("Invalid device token index"));
			}
			check_token_owner(&self.db.token_userdeviceid, token.as_bytes(), user, device, false)
				.await?;
			deletes.push((self.db.token_userdeviceid.clone(), token.as_bytes().to_vec()));
			deletes.push((self.db.userdeviceidtoken_index.clone(), key));
		}
		let token = optional_raw(&self.db.userdeviceid_token, &key).await?;
		if let Some(token) = token {
			if token.len() > 1024 || std::str::from_utf8(&token).is_err() {
				return Err(Error::bad_database("Invalid access token pointer"));
			}
			check_token_owner(&self.db.token_userdeviceid, &token, user, device, false).await?;
			deletes.push((self.db.token_userdeviceid.clone(), token));
		}
		deletes.push((self.db.userdeviceid_token.clone(), key.clone()));
	}
	if refresh {
		for (pointer, target) in [
			(&self.db.userdeviceid_refresh, &self.db.token_userdeviceid),
			(&self.db.userdeviceid_spentrefresh, &self.db.spentrefresh_userdeviceid),
		] {
			let token = optional_raw(pointer, &key).await?;
			if let Some(token) = token {
				if token.len() > 1024 || std::str::from_utf8(&token).is_err() {
					return Err(Error::bad_database("Invalid refresh token pointer"));
				}
				check_token_owner(
					target,
					&token,
					user,
					device,
					Arc::ptr_eq(target, &self.db.spentrefresh_userdeviceid),
				)
				.await?;
				deletes.push((target.clone(), token));
			}
			deletes.push((pointer.clone(), key.clone()));
		}
	}
	Ok(deletes)
}

#[implement(super::Service)]
pub(super) async fn append_token_removal(
	&self,
	user: &UserId,
	device: &DeviceId,
	access: bool,
	refresh: bool,
	txn: &mut Txn,
) -> Result {
	let deletes = self
		.token_deletions(user, device, access, refresh)
		.await?;
	for (map, key) in deletes {
		txn.del_raw(&map, key);
	}
	check_batch(txn)
}

#[implement(super::Service)]
pub(crate) async fn lock_device_mutations(
	&self,
	user: &UserId,
) -> MutexMapGuard<OwnedUserId, ()> {
	self.device_mutation_locks.lock(user).await
}

#[implement(super::Service)]
pub async fn prepare_all_device_removals(&self, user: &UserId) -> Result<DeviceRemovalPlan> {
	let guard = self.device_mutation_locks.lock(user).await;
	let devices = self.bounded_device_ids(user).await?;
	self.prepare_device_removals_locked(user, &devices, guard)
		.await
}

#[implement(super::Service)]
pub async fn prepare_device_removals(
	&self,
	user: &UserId,
	devices: &[OwnedDeviceId],
) -> Result<DeviceRemovalPlan> {
	let guard = self.device_mutation_locks.lock(user).await;
	self.prepare_device_removals_locked(user, devices, guard)
		.await
}

#[implement(super::Service)]
async fn prepare_device_removals_locked(
	&self,
	user: &UserId,
	devices: &[OwnedDeviceId],
	guard: MutexMapGuard<OwnedUserId, ()>,
) -> Result<DeviceRemovalPlan> {
	let services_guard = self.services.get();
	let services = services_guard.as_ref();
	if devices.len() > super::MAX_ADMIN_DEVICE_ROWS {
		return Err(limit());
	}
	let mut devices = devices.to_vec();
	devices.sort_unstable();
	devices.dedup();
	let existing = self.bounded_device_ids(user).await?;
	let remaining_devices = existing
		.iter()
		.filter(|id| !devices.contains(id))
		.count();
	let key_update = if devices.is_empty() {
		None
	} else {
		Some(self.prepare_device_key_update(user).await?)
	};
	let prior_version = optional_raw(&self.db.userid_devicelistversion, user.as_bytes()).await?;
	if prior_version
		.as_ref()
		.is_some_and(|value| value.len() != 8)
	{
		return Err(Error::bad_database("Invalid device list version"));
	}
	let version = tuwunel_core::utils::increment(prior_version.as_deref()).to_vec();
	// Filled after the complete pusher inventory, while the device guard stays
	// held.
	let mut deletions = Vec::new();
	let mut pushkeys = Vec::new();
	let mut account_counts = Vec::new();
	let mut rows = 0;
	let mut bytes = 0;
	for device in &devices {
		let token_deletions = self
			.token_deletions(user, device, true, true)
			.await?;
		for deletion in token_deletions {
			charge(&mut rows, &mut bytes, deletion.1.len())?;
			deletions.push(deletion);
		}
		let prefix = serialize_key((user, &**device, Interfix))?;
		for map in [&self.db.todeviceid_events, &self.db.userdeviceidalgorithm_fallback] {
			let keys = checked_keys(map, &prefix, &mut rows, &mut bytes).await?;
			for key in keys {
				if Arc::ptr_eq(map, &self.db.todeviceid_events) {
					let (owner, id, _): (&UserId, &DeviceId, u64) = deserialize_from_slice(&key)?;
					if owner != user || id != &**device {
						return Err(Error::bad_database("Invalid inbox key"));
					}
				} else {
					let (owner, id, _): (&UserId, &DeviceId, &str) =
						deserialize_from_slice(&key)?;
					if owner != user || id != &**device {
						return Err(Error::bad_database("Invalid fallback key"));
					}
				}
				deletions.push((map.clone(), key));
			}
		}
		if let Some(map) = &self.db.onetimekeyid4225_otk {
			let keys = checked_keys(map, &prefix, &mut rows, &mut bytes).await?;
			for key in keys {
				let (owner, id, ..): (&UserId, &DeviceId, u64, &ruma::OneTimeKeyId) =
					deserialize_from_slice(&key)?;
				if owner != user || id != &**device {
					return Err(Error::bad_database("Invalid one-time key inventory"));
				}
				deletions.push((map.clone(), key));
			}
		}
		for map in [
			&self.db.userdeviceid_metadata,
			&self.db.oidcdevice_userdeviceid,
			&self.db.keyid_key,
		] {
			deletions.push((map.clone(), serialize_key((user, &**device))?.to_vec()));
		}
		let event_type: RoomAccountDataEventType =
			format!("org.matrix.msc3890.local_notification_settings.{device}").into();
		let key = serialize_key((None::<&ruma::RoomId>, user, &event_type))?;
		let previous = optional_raw(&services.db["roomusertype_roomuserdataid"], &key).await?;
		if let Some(previous) = previous {
			charge(&mut rows, &mut bytes, previous.len())?;
			let (room, owner, _, kind): (
				Option<&ruma::RoomId>,
				&UserId,
				u64,
				RoomAccountDataEventType,
			) = deserialize_from_slice(&previous)?;
			if room.is_some() || owner != user || kind != event_type {
				return Err(Error::bad_database(
					"Invalid local notification account data pointer",
				));
			}
			deletions.push((services.db["roomuserdataid_accountdata"].clone(), previous));
		}
		account_counts.push(services.globals.next_count().await?);
	}
	let raw = optional_raw(&self.db.userid_dehydrateddevice, user.as_bytes()).await?;
	if let Some(raw) = raw {
		charge(&mut rows, &mut bytes, raw.len())?;
		let dehydrated: DehydratedDevice = serde_json::from_slice(&raw)?;
		if devices.contains(&dehydrated.device_id) {
			deletions.push((self.db.userid_dehydrateddevice.clone(), user.as_bytes().to_vec()));
		}
	}
	// Inventory the user's whole pusher index; an unreadable ownership pointer
	// must not become a short successful selection.
	let pushers = &services.db["senderkey_pusher"];
	let prefix = serialize_key((user, Interfix))?;
	let keys = checked_keys(pushers, &prefix, &mut rows, &mut bytes).await?;
	for key in keys {
		let (owner, pushkey): (&UserId, &str) = deserialize_from_slice(&key)?;
		if owner != user
			|| pushkey.len() > 512
			|| serialize_key((owner, pushkey))?.as_slice() != key.as_slice()
		{
			return Err(Error::bad_database("Invalid pusher key"));
		}
		let pointer = services.db["pushkey_deviceid"]
			.get(pushkey)
			.await?;
		charge(&mut rows, &mut bytes, pointer.len())?;
		let device: &DeviceId = deserialize_from_slice(&pointer)?;
		if devices.iter().any(|id| &**id == device) {
			pushkeys.push(pushkey.to_owned());
			deletions
				.push((services.db["pushkey_deviceid"].clone(), pushkey.as_bytes().to_vec()));
			deletions.push((pushers.clone(), key));
		}
	}
	let push_cleanup = services
		.sending
		.prepare_device_push_cleanup(user, &pushkeys, key_update.as_ref())
		.await?;
	Ok(DeviceRemovalPlan {
		_user_guard: guard,
		user: user.to_owned(),
		deletions,
		devices,
		pushkeys,
		key_update,
		push_cleanup,
		remaining_devices,
		version,
		account_counts,
	})
}

#[implement(super::Service)]
pub fn append_device_removals(&self, plan: &DeviceRemovalPlan, txn: &mut Txn) -> Result {
	let services_guard = self.services.get();
	let services = services_guard.as_ref();
	for (map, key) in &plan.deletions {
		txn.del_raw(map, key);
	}
	for (device, count) in plan.devices.iter().zip(&plan.account_counts) {
		let event_type: RoomAccountDataEventType =
			format!("org.matrix.msc3890.local_notification_settings.{device}").into();
		let key = (None::<&ruma::RoomId>, &*plan.user, &event_type);
		let data_key = (None::<&ruma::RoomId>, &*plan.user, **count, &event_type);
		txn.put(
			&services.db["roomuserdataid_accountdata"],
			data_key,
			Json(serde_json::json!({"type":event_type.to_string(),"content":{}})),
		);
		txn.put(&services.db["roomusertype_roomuserdataid"], key, data_key);
	}
	if let Some(key_update) = &plan.key_update {
		txn.insert_raw(&self.db.userid_devicelistversion, plan.user.as_bytes(), &plan.version);
		self.append_device_key_update(&plan.user, key_update, txn);
	}
	services
		.sending
		.append_device_push_cleanup(&plan.push_cleanup, txn)?;
	check_batch(txn)
}

pub(super) fn check_batch(txn: &Txn) -> Result {
	if txn.len() > ROWS || txn.size_in_bytes() > BYTES {
		return Err(limit());
	}
	txn.check_bridge_admission().map(|_| ())
}

#[implement(super::Service)]
pub async fn finish_device_removals(&self, user: &UserId, plan: DeviceRemovalPlan) -> Result {
	let services_guard = self.services.get();
	let services = services_guard.as_ref();
	for pushkey in &plan.pushkeys {
		services.pusher.forget_sent_badge(user, pushkey);
	}
	// Release sender admission before flushing key-change destinations.
	drop(plan.push_cleanup);
	if let Some(key_update) = plan.key_update {
		self.finish_device_key_update(user, key_update)
			.await?;
	}
	Ok(())
}

#[implement(super::Service)]
pub async fn remove_devices(&self, user: &UserId, devices: &[OwnedDeviceId]) -> Result {
	let plan = self
		.prepare_device_removals(user, devices)
		.await?;
	let services_guard = self.services.get();
	let mut txn = services_guard.as_ref().db.txn();
	self.append_device_removals(&plan, &mut txn)?;
	txn.execute().await?;
	self.finish_device_removals(user, plan).await
}

#[implement(super::Service)]
pub async fn remove_all_devices(&self, user: &UserId) -> Result {
	let plan = self.prepare_all_device_removals(user).await?;
	let services_guard = self.services.get();
	let mut txn = services_guard.as_ref().db.txn();
	self.append_device_removals(&plan, &mut txn)?;
	txn.execute().await?;
	self.finish_device_removals(user, plan).await
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn revocation_budget_refuses_before_retaining_an_overflow_row() {
		let mut rows = ROWS;
		let mut bytes = 0;
		charge(&mut rows, &mut bytes, 1).expect_err("complete inventory row overflow");
		let mut rows = 0;
		let mut bytes = BYTES;
		charge(&mut rows, &mut bytes, 1).expect_err("aggregate inventory byte overflow");
	}
}
