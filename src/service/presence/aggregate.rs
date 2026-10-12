//! Presence aggregation across devices.
//!
//! This module keeps per-device presence snapshots and computes a single
//! user-level presence view. Aggregation applies idle/offline thresholds,
//! favors higher-ranked states, and prunes stale devices to cap memory.

use std::{collections::HashMap, sync::RwLock};

use ruma::{OwnedDeviceId, OwnedUserId, UInt, UserId, presence::PresenceState};
use tuwunel_core::debug;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DeviceKey {
	Device(OwnedDeviceId),
	Remote,
	UnknownLocal,
}

/// Kinds of updates to the per-device `status_msg`.
///
/// `Unchanged` preserves what the device already has.
/// `Set` writes through, including `None` and `Some("")` to clear it.
#[derive(Debug, Clone)]
pub(crate) enum StatusMsg {
	Set(Option<String>),
	Unchanged,
}

#[derive(Debug, Clone)]
struct DevicePresence {
	state: PresenceState,
	currently_active: bool,
	last_active_ts: u64,
	last_update_ts: u64,
	status_msg: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct AggregatedPresence {
	pub(crate) state: PresenceState,
	pub(crate) currently_active: bool,
	pub(crate) last_active_ts: u64,
	pub(crate) status_msg: Option<String>,
	pub(crate) device_count: usize,
}

#[derive(Debug, Default)]
pub(crate) struct PresenceAggregator {
	inner: RwLock<HashMap<OwnedUserId, UserPresence>>,
}

#[derive(Debug, Default)]
struct UserPresence {
	devices: HashMap<DeviceKey, DevicePresence>,
	count: Option<u64>,
}

/// Restores only this request's device on refusal or cancellation. The caller
/// holds the user's mutation exclusion until this checkpoint is dropped.
pub(super) struct Checkpoint<'a> {
	aggregator: &'a PresenceAggregator,
	user: OwnedUserId,
	device: DeviceKey,
	before: Option<DevicePresence>,
	count: Option<u64>,
	accepted: bool,
}

impl Checkpoint<'_> {
	pub(super) fn accept(mut self, count: Option<u64>) {
		self.aggregator.committed_count(&self.user, count);
		self.accepted = true;
	}
}

impl Drop for Checkpoint<'_> {
	fn drop(&mut self) {
		if self.accepted {
			return;
		}
		let mut users = self.aggregator.inner.write().expect("locked");
		if let Some(before) = self.before.take() {
			let user = users.entry(self.user.clone()).or_default();
			user.devices.insert(self.device.clone(), before);
			user.count = self.count;
		} else if let Some(user) = users.get_mut(&self.user) {
			user.devices.remove(&self.device);
			user.count = self.count;
			if user.devices.is_empty() {
				users.remove(&self.user);
			}
		}
	}
}

impl PresenceAggregator {
	/// Create a new, empty aggregator.
	pub(crate) fn new() -> Self { Self::default() }

	/// Clear all tracked device state.
	pub(crate) fn clear(&self) { self.inner.write().expect("locked").clear(); }

	/// Discard derived device state when a persisted generation changed outside
	/// this cache, including an uncertain or cancelled remote commit.
	pub(super) fn reconcile(&self, user: &UserId, count: Option<u64>) {
		let mut users = self.inner.write().expect("locked");
		if users
			.get(user)
			.is_some_and(|entry| entry.count != count)
		{
			users.remove(user);
		}
	}

	pub(super) fn committed_count(&self, user: &UserId, count: Option<u64>) {
		if let Some(entry) = self.inner.write().expect("locked").get_mut(user) {
			entry.count = count;
		}
	}

	pub(super) fn invalidate(&self, user: &UserId) {
		self.inner.write().expect("locked").remove(user);
	}

	pub(super) fn checkpoint(&self, user: &UserId, device: &DeviceKey) -> Checkpoint<'_> {
		let users = self.inner.read().expect("locked");
		let before = users.get(user);
		Checkpoint {
			aggregator: self,
			user: user.to_owned(),
			device: device.clone(),
			before: before
				.and_then(|entry| entry.devices.get(device))
				.cloned(),
			count: before.and_then(|entry| entry.count),
			accepted: false,
		}
	}

	/// Update presence state for a single device.
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn update(
		&self,
		user_id: &UserId,
		device_key: DeviceKey,
		state: &PresenceState,
		currently_active: Option<bool>,
		last_active_ago: Option<UInt>,
		status_msg: StatusMsg,
		now_ms: u64,
	) {
		let mut guard = self.inner.write().expect("locked");
		let devices = &mut guard
			.entry(user_id.to_owned())
			.or_default()
			.devices;

		let last_active_ts = match last_active_ago {
			| None => now_ms,
			| Some(ago) => now_ms.saturating_sub(ago.into()),
		};

		let initial_status = match &status_msg {
			| StatusMsg::Set(msg) => msg.clone(),
			| StatusMsg::Unchanged => None,
		};

		let entry = devices
			.entry(device_key)
			.or_insert_with(|| DevicePresence {
				state: state.clone(),
				currently_active: currently_active.unwrap_or(false),
				last_active_ts,
				last_update_ts: now_ms,
				status_msg: initial_status,
			});

		entry.state = state.clone();
		entry.currently_active = currently_active.unwrap_or(false);
		entry.last_active_ts = last_active_ts;
		entry.last_update_ts = now_ms;
		if let StatusMsg::Set(msg) = status_msg {
			entry.status_msg = msg;
		}
	}

	/// Aggregate per-device state into a single presence snapshot.
	///
	/// Prunes devices that have not updated within the offline timeout to keep
	/// the map bounded.
	pub(crate) fn aggregate(
		&self,
		user_id: &UserId,
		now_ms: u64,
		idle_timeout_ms: u64,
		offline_timeout_ms: u64,
	) -> AggregatedPresence {
		let mut guard = self.inner.write().expect("locked");
		let Some(user) = guard.get_mut(user_id) else {
			return AggregatedPresence {
				state: PresenceState::Offline,
				currently_active: false,
				last_active_ts: now_ms,
				status_msg: None,
				device_count: 0,
			};
		};
		let devices = &mut user.devices;

		let mut best_state = PresenceState::Offline;
		let mut best_rank = state_rank(&best_state);
		let mut any_currently_active = false;
		let mut last_active_ts = 0_u64;
		let mut latest_status: Option<(u64, String)> = None;

		devices.retain(|_, device| {
			let last_active_age = now_ms.saturating_sub(device.last_active_ts);
			let last_update_age = now_ms.saturating_sub(device.last_update_ts);

			let effective_state = effective_device_state(
				&device.state,
				last_active_age,
				idle_timeout_ms,
				offline_timeout_ms,
			);

			let rank = state_rank(&effective_state);
			if rank > best_rank {
				best_rank = rank;
				best_state = effective_state.clone();
			}

			if (effective_state == PresenceState::Online
				|| effective_state == PresenceState::Busy)
				&& device.currently_active
				&& last_active_age < idle_timeout_ms
			{
				any_currently_active = true;
			}

			if let Some(msg) = device
				.status_msg
				.as_ref()
				.filter(|msg| !msg.is_empty())
			{
				match latest_status {
					| None => {
						latest_status = Some((device.last_update_ts, msg.clone()));
					},
					| Some((ts, _)) if device.last_update_ts > ts => {
						latest_status = Some((device.last_update_ts, msg.clone()));
					},
					| _ => {},
				}
			}

			if device.last_active_ts > last_active_ts {
				last_active_ts = device.last_active_ts;
			}

			// Drop devices that haven't updated in a long time to keep the map small.
			last_update_age < offline_timeout_ms
		});

		let device_count = devices.len();
		let status_msg = latest_status.map(|(_, msg)| msg);

		if device_count == 0 {
			guard.remove(user_id);
			return AggregatedPresence {
				state: PresenceState::Offline,
				currently_active: false,
				last_active_ts: now_ms,
				status_msg: None,
				device_count: 0,
			};
		}

		debug!(
			?user_id,
			device_count,
			state = ?best_state,
			currently_active = any_currently_active,
			last_active_ts,
			status_msg = status_msg.as_deref(),
			"Aggregated presence"
		);

		AggregatedPresence {
			state: best_state,
			currently_active: any_currently_active,
			last_active_ts: if last_active_ts == 0 { now_ms } else { last_active_ts },
			status_msg,
			device_count,
		}
	}
}

fn effective_device_state(
	state: &PresenceState,
	last_active_age: u64,
	idle_timeout_ms: u64,
	offline_timeout_ms: u64,
) -> PresenceState {
	match state {
		| PresenceState::Busy | PresenceState::Online =>
			if last_active_age >= idle_timeout_ms {
				PresenceState::Unavailable
			} else {
				state.clone()
			},
		| PresenceState::Unavailable =>
			if last_active_age >= offline_timeout_ms {
				PresenceState::Offline
			} else {
				PresenceState::Unavailable
			},
		| PresenceState::Offline => PresenceState::Offline,
		| _ => state.clone(),
	}
}

fn state_rank(state: &PresenceState) -> u8 {
	match state {
		| PresenceState::Busy => 3,
		| PresenceState::Online => 2,
		| PresenceState::Unavailable => 1,
		| _ => 0,
	}
}

#[cfg(test)]
mod tests {
	use ruma::{device_id, uint, user_id};

	use super::*;

	#[test]
	fn aggregates_rank_and_status_msg() {
		let aggregator = PresenceAggregator::new();
		let user = user_id!("@alice:example.com");
		let now = 1_000_u64;

		aggregator.update(
			user,
			DeviceKey::Device(device_id!("DEVICE_A").to_owned()),
			&PresenceState::Unavailable,
			Some(false),
			Some(uint!(50)),
			StatusMsg::Set(Some("away".into())),
			now,
		);

		aggregator.update(
			user,
			DeviceKey::Device(device_id!("DEVICE_B").to_owned()),
			&PresenceState::Online,
			Some(true),
			Some(uint!(10)),
			StatusMsg::Set(Some("online".into())),
			now + 10,
		);

		let aggregated = aggregator.aggregate(user, now + 10, 100, 300);

		assert_eq!(aggregated.state, PresenceState::Online);
		assert!(aggregated.currently_active);
		assert_eq!(aggregated.status_msg.as_deref(), Some("online"));
		assert_eq!(aggregated.device_count, 2);
	}

	#[test]
	fn degrades_online_to_unavailable_after_idle() {
		let aggregator = PresenceAggregator::new();
		let user = user_id!("@bob:example.com");
		let now = 10_000_u64;

		aggregator.update(
			user,
			DeviceKey::Device(device_id!("DEVICE_IDLE").to_owned()),
			&PresenceState::Online,
			Some(true),
			Some(uint!(500)),
			StatusMsg::Unchanged,
			now,
		);

		let aggregated = aggregator.aggregate(user, now + 500, 100, 1_000);

		assert_eq!(aggregated.state, PresenceState::Unavailable);
	}

	#[test]
	fn explicit_set_clears_status_msg() {
		let aggregator = PresenceAggregator::new();
		let user = user_id!("@alice:example.com");
		let device = DeviceKey::Device(device_id!("DEVICE_A").to_owned());
		let now = 1_000_u64;

		aggregator.update(
			user,
			device.clone(),
			&PresenceState::Online,
			Some(true),
			Some(uint!(0)),
			StatusMsg::Set(Some("busy".to_owned())),
			now,
		);

		let aggregated = aggregator.aggregate(user, now, 100, 300);
		assert_eq!(aggregated.status_msg.as_deref(), Some("busy"));

		aggregator.update(
			user,
			device.clone(),
			&PresenceState::Online,
			Some(true),
			Some(uint!(0)),
			StatusMsg::Set(Some(String::new())),
			now + 1,
		);

		let aggregated = aggregator.aggregate(user, now + 1, 100, 300);

		assert!(aggregated.status_msg.is_none());

		aggregator.update(
			user,
			device.clone(),
			&PresenceState::Online,
			Some(true),
			Some(uint!(0)),
			StatusMsg::Set(Some("back".to_owned())),
			now + 2,
		);

		aggregator.update(
			user,
			device,
			&PresenceState::Online,
			Some(true),
			Some(uint!(0)),
			StatusMsg::Set(None),
			now + 3,
		);

		let aggregated = aggregator.aggregate(user, now + 3, 100, 300);

		assert!(aggregated.status_msg.is_none());
	}

	#[test]
	fn unchanged_preserves_status_msg() {
		let aggregator = PresenceAggregator::new();
		let user = user_id!("@alice:example.com");
		let device = DeviceKey::Device(device_id!("DEVICE_A").to_owned());
		let now = 1_000_u64;

		aggregator.update(
			user,
			device.clone(),
			&PresenceState::Online,
			Some(true),
			Some(uint!(0)),
			StatusMsg::Set(Some("away".to_owned())),
			now,
		);

		aggregator.update(
			user,
			device,
			&PresenceState::Online,
			Some(true),
			Some(uint!(0)),
			StatusMsg::Unchanged,
			now + 1,
		);

		let aggregated = aggregator.aggregate(user, now + 1, 100, 300);

		assert_eq!(aggregated.status_msg.as_deref(), Some("away"));
	}

	#[test]
	fn drops_stale_devices_on_aggregate() {
		let aggregator = PresenceAggregator::new();
		let user = user_id!("@carol:example.com");

		aggregator.update(
			user,
			DeviceKey::Device(device_id!("DEVICE_STALE").to_owned()),
			&PresenceState::Online,
			Some(true),
			Some(uint!(10)),
			StatusMsg::Unchanged,
			0,
		);

		let aggregated = aggregator.aggregate(user, 1_000, 100, 100);

		assert_eq!(aggregated.device_count, 0);
		assert_eq!(aggregated.state, PresenceState::Offline);
	}
}
