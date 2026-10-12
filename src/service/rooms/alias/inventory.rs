use futures::{TryStreamExt, pin_mut};
use ruma::{
	OwnedRoomAliasId, RoomAliasId, RoomId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use tuwunel_core::{Error, Result};
use tuwunel_database::{Interfix, Txn, serialize_key};

use super::Service;

struct AliasRow {
	key: Vec<u8>,
	alias: OwnedRoomAliasId,
}

impl Service {
	/// Complete room alias inventory, validating both indexes before shutdown.
	/// All rows (including duplicates) consume 1,024-row / 128-KiB limits.
	pub async fn bounded_local_aliases_for_room(
		&self,
		room: &RoomId,
	) -> Result<Vec<OwnedRoomAliasId>> {
		let _guard = self.mutation.lock().await;
		let mut aliases: Vec<_> = self
			.alias_rows(room)
			.await?
			.into_iter()
			.map(|row| row.alias)
			.collect();
		aliases.sort();
		aliases.dedup();
		Ok(aliases)
	}

	pub(crate) async fn preflight_room_alias_shutdown(&self, room: &RoomId) -> Result {
		let _guard = self.mutation.lock().await;
		let _prepared = self.prepare_room_alias_shutdown(room).await?;
		Ok(())
	}

	/// Remove all aliases and directory publication in one atomic commit,
	/// keeping exclusion through inventory validation and acknowledgement.
	pub(crate) async fn remove_room_aliases(
		&self,
		room: &RoomId,
	) -> Result<Vec<OwnedRoomAliasId>> {
		let _guard = self.mutation.lock().await;
		let (txn, aliases) = self.prepare_room_alias_shutdown(room).await?;
		txn.execute().await?;
		Ok(aliases)
	}

	async fn prepare_room_alias_shutdown(
		&self,
		room: &RoomId,
	) -> Result<(Txn, Vec<OwnedRoomAliasId>)> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let mut txn = services_root.db.txn();
		let mut aliases = Vec::new();
		let rows = self.alias_rows(room).await?;
		for row in rows {
			txn.del_raw(&self.db.aliasid_alias, row.key);
			aliases.push(row.alias);
		}
		aliases.sort();
		aliases.dedup();
		for alias in &aliases {
			txn.del_raw(&self.db.alias_roomid, alias.alias());
			txn.del_raw(&self.db.alias_userid, alias.alias());
		}
		txn.del_raw(&services_root.db["publicroomids"], room);
		check_mutation_budget(&txn, 0)?;
		Ok((txn, aliases))
	}

	async fn alias_rows(&self, room: &RoomId) -> Result<Vec<AliasRow>> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let prefix = (room, Interfix);
		let encoded = serialize_key(prefix)?;
		let rows = self
			.db
			.aliasid_alias
			.stream_prefix_capped::<&[u8], &[u8], _>(&prefix, 1025);
		pin_mut!(rows);
		let mut out = Vec::new();
		let mut bytes = 0_usize;
		while let Some((key, value)) = rows.try_next().await? {
			bytes = bytes
				.saturating_add(key.len())
				.saturating_add(value.len());
			if out.len() >= 1024 || bytes > 128 * 1024 {
				return Err(Error::Request(
					ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
					"Room alias inventory limit reached".into(),
					http::StatusCode::TOO_MANY_REQUESTS,
				));
			}
			if key.len() != encoded.len().saturating_add(8) {
				return Err(Error::bad_database("Invalid room alias index key"));
			}
			let text = std::str::from_utf8(value)
				.map_err(|_| Error::bad_database("Invalid stored room alias"))?;
			let alias = RoomAliasId::parse(text)
				.map_err(|_| Error::bad_database("Invalid stored room alias"))?;
			if !services_root.globals.alias_is_local(&alias)
				|| self.resolve_local_alias(&alias).await? != room
			{
				return Err(Error::bad_database("Room alias indexes disagree"));
			}
			out.push(AliasRow { key: key.to_vec(), alias });
		}
		Ok(out)
	}

	/// Stage only this alias's inverse rows; other aliases remain reachable.
	/// The caller holds mutation exclusion through the atomic three-map commit.
	pub(super) async fn stage_removed_alias(
		&self,
		alias: &RoomAliasId,
		room: &RoomId,
		txn: &mut Txn,
	) -> Result {
		let mut matched = false;
		for row in self.alias_rows(room).await? {
			if row.alias == alias {
				matched = true;
				txn.del_raw(&self.db.aliasid_alias, row.key);
			}
		}
		if !matched {
			return Err(Error::bad_database("Room alias inverse index is missing"));
		}
		txn.del_raw(&self.db.alias_roomid, alias.alias());
		txn.del_raw(&self.db.alias_userid, alias.alias());
		// Leave capacity for the three replacement writes in set_alias_by.
		check_mutation_budget(txn, 3)?;
		Ok(())
	}
}

fn check_mutation_budget(txn: &Txn, reserved: usize) -> Result {
	if txn.len() > tuwunel_bridge::MAX_COMMIT_OPS.saturating_sub(reserved) {
		return Err(Error::Request(
			ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
			"Room alias mutation limit reached".into(),
			http::StatusCode::TOO_MANY_REQUESTS,
		));
	}
	Ok(())
}
