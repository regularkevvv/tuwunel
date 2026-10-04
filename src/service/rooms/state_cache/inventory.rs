use futures::{StreamExt, pin_mut};
use ruma::{
	OwnedUserId, RoomId, UserId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use tuwunel_core::{Error, Result};
use tuwunel_database::{Ignore, Interfix, Map};

use super::Service;

/// Complete key-only membership count, including the work required to prove it.
#[derive(Clone, Copy)]
pub struct RoomMemberInventoryCount {
	/// Local members counted, including disabled and guest accounts.
	pub local_members: usize,
	/// All membership keys examined, including remote members.
	pub examined: usize,
	/// User-ID bytes examined; the constant encoded room prefix is excluded.
	pub user_id_bytes: usize,
}

impl Service {
	/// Complete joined-member IDs, including remote and disabled accounts.
	/// Reads at most 1,025 keys and retains at most 1,024 IDs / 128 KiB.
	/// Values do not affect the existing key-presence membership semantics.
	pub async fn bounded_room_members(&self, room: &RoomId) -> Result<Vec<OwnedUserId>> {
		const MAX_ROWS: usize = 1024;
		const MAX_BYTES: usize = 128 * 1024;
		let prefix = (room, Interfix);
		let keys = self
			.db
			.roomuserid_joinedcount
			.keys_prefix_capped::<(Ignore, &UserId), _>(&prefix, MAX_ROWS + 1);
		pin_mut!(keys);
		let mut members = Vec::new();
		let mut bytes = 0_usize;
		while let Some(key) = keys.next().await {
			let (_, user) = key?;
			bytes = bytes.saturating_add(user.as_bytes().len());
			if members.len() >= MAX_ROWS || bytes > MAX_BYTES {
				return Err(Error::Request(
					ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
					"Room member inventory limit reached".into(),
					http::StatusCode::TOO_MANY_REQUESTS,
				));
			}
			members.push(user.to_owned());
		}
		Ok(members)
	}

	/// Counts local joined users only after a complete fallible key scan.
	/// Includes at most 1,024 joined-member keys / 128 KiB user-ID bytes, or
	/// a caller's smaller remaining budgets, plus one overflow key.
	pub async fn bounded_local_member_count(
		&self,
		room: &RoomId,
		row_limit: usize,
		byte_limit: usize,
	) -> Result<RoomMemberInventoryCount> {
		self.bounded_local_membership_count(
			&self.db.roomuserid_joinedcount,
			room,
			row_limit,
			byte_limit,
		)
		.await
	}

	/// Counts local invitations only after a complete fallible key scan, with
	/// the same row/byte budgets as joined membership.
	pub async fn bounded_local_invited_member_count(
		&self,
		room: &RoomId,
		row_limit: usize,
		byte_limit: usize,
	) -> Result<RoomMemberInventoryCount> {
		self.bounded_local_membership_count(
			&self.db.roomuserid_invitecount,
			room,
			row_limit,
			byte_limit,
		)
		.await
	}

	/// Proves local membership exists, or proves both complete inventories
	/// empty. Failed or oversized reads never become an empty room.
	pub async fn has_local_membership_checked(&self, room: &RoomId) -> Result<bool> {
		let joined = self
			.bounded_local_member_count(room, 1024, 128 * 1024)
			.await?;
		if joined.local_members > 0 {
			return Ok(true);
		}
		let invited = self
			.bounded_local_invited_member_count(room, 1024, 128 * 1024)
			.await?;
		Ok(invited.local_members > 0)
	}

	async fn bounded_local_membership_count(
		&self,
		map: &std::sync::Arc<Map>,
		room: &RoomId,
		row_limit: usize,
		byte_limit: usize,
	) -> Result<RoomMemberInventoryCount> {
		let row_limit = row_limit.min(1024);
		let byte_limit = byte_limit.min(128 * 1024);
		let prefix = (room, Interfix);
		let keys =
			map.keys_prefix_capped::<(Ignore, &UserId), _>(&prefix, row_limit.saturating_add(1));
		pin_mut!(keys);
		let mut count = RoomMemberInventoryCount {
			local_members: 0,
			examined: 0,
			user_id_bytes: 0,
		};
		while let Some(key) = keys.next().await {
			let (_, user) = key?;
			count.examined = count.examined.saturating_add(1);
			count.user_id_bytes = count
				.user_id_bytes
				.saturating_add(user.as_bytes().len());
			if count.examined > row_limit || count.user_id_bytes > byte_limit {
				return Err(Error::Request(
					ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
					"Room member inventory limit reached".into(),
					http::StatusCode::TOO_MANY_REQUESTS,
				));
			}
			if self.services.globals.user_is_local(user) {
				count.local_members = count.local_members.saturating_add(1);
			}
		}
		Ok(count)
	}
}
