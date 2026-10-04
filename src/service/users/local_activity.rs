use std::cmp::Reverse;

use futures::{StreamExt, pin_mut};
use ruma::{
	DeviceId, MilliSecondsSinceUnixEpoch, OwnedUserId, UserId,
	api::{
		client::device::{Device, LastSeenIp},
		error::{ErrorKind, LimitExceededErrorData},
	},
	uint,
};
use tuwunel_core::{Error, Result};
use tuwunel_database::{Ignore, Interfix};

use super::Service;

/// Maximum device rows included in one complete admin inventory.
pub const MAX_ADMIN_DEVICE_ROWS: usize = 128;
/// Maximum stored JSON bytes examined by one admin device inventory.
pub const MAX_ADMIN_DEVICE_BYTES: usize = 64 * 1024;
const MAX_ACTIVITY_DEVICE_ROWS: usize = 4096;
const MAX_ACTIVITY_DEVICE_BYTES: usize = 256 * 1024;
const MAX_ACTIVITY_REPLY_BYTES: usize = 16 * 1024;

/// Complete device metadata with the work performed to obtain it.
#[derive(Debug)]
pub struct DeviceMetadataInventory {
	/// Complete decoded metadata for the matching user.
	pub devices: Vec<Device>,
	/// Matching rows examined before a successful complete result.
	pub examined: usize,
	/// Stored JSON bytes examined, checked before decoding.
	pub encoded_bytes: usize,
}

/// A user's most recently observed device activity.
#[derive(Debug)]
pub struct LocalUserActivity {
	/// Active account from the complete local-user inventory.
	pub user_id: OwnedUserId,
	/// Maximum device timestamp; equal timestamps use the IP as a tie breaker.
	pub last_seen_ts: MilliSecondsSinceUnixEpoch,
	/// IP associated with the selected timestamp.
	pub last_seen_ip: Option<LastSeenIp>,
}

fn inventory_limit() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Device/activity inventory limit reached".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}

impl Service {
	/// Returns a complete device inventory, or refuses it. Row and byte budgets
	/// may be reduced to zero by a caller's remaining aggregate budget. One
	/// lookahead detects overflow; raw bytes are checked before JSON decoding.
	/// Storage/decoding failures never become a short successful inventory.
	pub async fn bounded_devices_metadata(
		&self,
		user_id: &UserId,
		limit: usize,
		byte_limit: usize,
	) -> Result<DeviceMetadataInventory> {
		let limit = limit.min(MAX_ADMIN_DEVICE_ROWS);
		let byte_limit = byte_limit.min(MAX_ADMIN_DEVICE_BYTES);
		let prefix = (user_id, Interfix);
		let rows = self
			.db
			.userdeviceid_metadata
			.stream_prefix_capped::<(Ignore, &DeviceId), &[u8], _>(
				&prefix,
				limit.saturating_add(1),
			);
		pin_mut!(rows);
		let mut inventory = DeviceMetadataInventory {
			devices: Vec::new(),
			examined: 0,
			encoded_bytes: 0,
		};
		while let Some(row) = rows.next().await {
			let ((_, key_device), json) = row?;
			inventory.examined = inventory.examined.saturating_add(1);
			inventory.encoded_bytes = inventory.encoded_bytes.saturating_add(json.len());
			if inventory.examined > limit || inventory.encoded_bytes > byte_limit {
				return Err(inventory_limit());
			}
			let device: Device = serde_json::from_slice(json)?;
			if device.device_id.as_str() != key_device.as_str() {
				return Err(Error::Database("Device metadata does not match its key".into()));
			}
			inventory.devices.push(device);
		}
		Ok(inventory)
	}

	/// The globally most recent active users within complete inventory budgets.
	/// Reads close before output. Device rows/bytes share one aggregate budget;
	/// sorting does not turn a truncated input into a successful top-N result.
	pub async fn recent_local_activity(&self, limit: usize) -> Result<Vec<LocalUserActivity>> {
		if !(1..=64).contains(&limit) {
			return Err(Error::BadRequest(
				ErrorKind::InvalidParam,
				"Activity output limit must be between 1 and 64",
			));
		}
		let users = self.bounded_local_users().await?;
		let mut remaining_rows = MAX_ACTIVITY_DEVICE_ROWS;
		let mut remaining_bytes = MAX_ACTIVITY_DEVICE_BYTES;
		let mut activity = Vec::new();
		for user_id in users {
			let inventory = self
				.bounded_devices_metadata(&user_id, remaining_rows, remaining_bytes)
				.await?;
			remaining_rows = remaining_rows.saturating_sub(inventory.examined);
			remaining_bytes = remaining_bytes.saturating_sub(inventory.encoded_bytes);
			let latest = inventory
				.devices
				.into_iter()
				.filter_map(|device| {
					device
						.last_seen_ts
						.map(|ts| (ts, device.last_seen_ip))
				})
				.max();
			if let Some((last_seen_ts, last_seen_ip)) = latest
				&& last_seen_ts.get() > uint!(0)
			{
				activity.push(LocalUserActivity { user_id, last_seen_ts, last_seen_ip });
			}
		}
		activity.sort_by_key(|item| Reverse(item.last_seen_ts));
		activity.truncate(limit);
		let reply_bytes = activity.iter().fold(0_usize, |bytes, item| {
			bytes
				.saturating_add(format!("{:?}", item.last_seen_ts).len())
				.saturating_add(item.user_id.localpart().len())
				.saturating_add(
					item.last_seen_ip
						.as_ref()
						.map_or(0, LastSeenIp::len),
				)
				.saturating_add(43)
		});
		if reply_bytes > MAX_ACTIVITY_REPLY_BYTES {
			return Err(inventory_limit());
		}
		Ok(activity)
	}
}
