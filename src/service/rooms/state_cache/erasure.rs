use std::sync::Arc;

use futures::{TryStreamExt, pin_mut};
use ruma::{
	RoomId, ServerName, UserId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use tuwunel_core::{Error, Result};
use tuwunel_database::{Interfix, Map, Txn, serialize_key};

use super::Service;

#[derive(Default)]
struct ErasureBudget {
	rows: usize,
	bytes: usize,
}

impl ErasureBudget {
	fn cap(&self) -> usize {
		4096_usize
			.saturating_sub(self.rows)
			.saturating_add(1)
	}

	fn charge(&mut self, key: &[u8]) -> Result {
		self.rows = self.rows.saturating_add(1);
		self.bytes = self.bytes.saturating_add(key.len());
		if self.rows > 4096 || self.bytes > 512 * 1024 {
			return Err(Error::Request(
				ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
				"Membership erasure inventory limit reached".into(),
				http::StatusCode::TOO_MANY_REQUESTS,
			));
		}
		Ok(())
	}
}

impl Service {
	/// Check all membership erasure sources before shutdown can evict a user
	/// or remove an alias. An unexecuted transaction changes no storage.
	pub(crate) async fn preflight_room_erasure(&self, room: &RoomId, force: bool) -> Result {
		let _guard = self.membership_mutex.lock(room).await;
		let mut txn = self.services.db.txn();
		self.stage_membership_erasure(room, force, &mut txn)
			.await
	}

	/// The caller holds membership exclusion until this batch is committed.
	/// All five sources share a complete 4,096-key / 512-KiB inventory budget;
	/// local departures retained by non-force deletion still consume it.
	pub(super) async fn stage_membership_erasure(
		&self,
		room: &RoomId,
		force: bool,
		txn: &mut Txn,
	) -> Result {
		let mut budget = ErasureBudget::default();
		let prefix = (room, Interfix);
		let encoded = serialize_key(prefix)?;
		let keys = self
			.db
			.roomserverids
			.keys_prefix_raw_capped(&prefix, budget.cap());
		pin_mut!(keys);
		while let Some(key) = keys.try_next().await? {
			budget.charge(key)?;
			let name = std::str::from_utf8(&key[encoded.len()..])
				.map_err(|_| Error::bad_database("Invalid membership erasure server key"))?;
			let server: &ServerName = name
				.try_into()
				.map_err(|_| Error::bad_database("Invalid membership erasure server key"))?;
			txn.del(&self.db.roomserverids, (room, server));
			txn.del(&self.db.serverroomids, (server, room));
		}

		for (source, reverse, retain_local) in [
			(&self.db.roomuserid_invitecount, &self.db.userroomid_invitestate, false),
			(&self.db.roomuserid_joinedcount, &self.db.userroomid_joinedcount, false),
			(&self.db.roomuserid_knockedcount, &self.db.userroomid_knockedstate, false),
			(&self.db.roomuserid_leftcount, &self.db.userroomid_leftstate, !force),
		] {
			self.stage_erased_members(source, reverse, room, retain_local, &mut budget, txn)
				.await?;
		}
		for map in [
			&self.db.roomid_knockedcount,
			&self.db.roomid_invitedcount,
			&self.db.roomid_inviteviaservers,
			&self.db.roomid_joinedcount,
		] {
			txn.del_raw(map, room);
		}
		Ok(())
	}

	async fn stage_erased_members(
		&self,
		source: &Arc<Map>,
		reverse: &Arc<Map>,
		room: &RoomId,
		retain_local: bool,
		budget: &mut ErasureBudget,
		txn: &mut Txn,
	) -> Result {
		let prefix = (room, Interfix);
		let encoded = serialize_key(prefix)?;
		let keys = source.keys_prefix_raw_capped(&prefix, budget.cap());
		pin_mut!(keys);
		while let Some(key) = keys.try_next().await? {
			budget.charge(key)?;
			let name = std::str::from_utf8(&key[encoded.len()..])
				.map_err(|_| Error::bad_database("Invalid membership erasure user key"))?;
			let user: &UserId = name
				.try_into()
				.map_err(|_| Error::bad_database("Invalid membership erasure user key"))?;
			if retain_local && self.services.globals.user_is_local(user) {
				continue;
			}
			txn.del(source, (room, user));
			txn.del(reverse, (user, room));
		}
		Ok(())
	}
}
