use std::collections::HashSet;

use futures::{TryStreamExt, pin_mut};
use ruma::{
	OwnedServerName, RoomId, ServerName, UserId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use tuwunel_core::{Error, Result};
use tuwunel_database::{Interfix, Map, serialize_key};

use super::Service;

const MAX_RECOUNT_ROWS: usize = 4096;
const MAX_RECOUNT_BYTES: usize = 512 * 1024;
pub(super) const GENERATION_BYTES: usize = 32;
pub(super) const RECOUNT_GENERATION: &str = "membership_recount_generation_v1";

/// Complete key-presence membership inventories, prepared before any aggregate
/// or server-index mutation. Values are not projected, matching membership's
/// existing key-presence semantics.
pub(super) struct RecountInventory {
	pub(super) joined: u64,
	pub(super) invited: u64,
	pub(super) knocked: u64,
	pub(super) joined_servers: HashSet<OwnedServerName>,
	pub(super) old_servers: Vec<OwnedServerName>,
}

#[derive(Default)]
struct RecountBudget {
	rows: usize,
	bytes: usize,
}

impl RecountBudget {
	fn cap(&self) -> usize {
		MAX_RECOUNT_ROWS
			.saturating_sub(self.rows)
			.saturating_add(1)
	}

	fn charge(&mut self, key: &[u8]) -> Result {
		self.rows = self.rows.saturating_add(1);
		self.bytes = self.bytes.saturating_add(key.len());
		if self.rows > MAX_RECOUNT_ROWS || self.bytes > MAX_RECOUNT_BYTES {
			return Err(Error::Request(
				ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
				"Membership recount inventory limit reached".into(),
				http::StatusCode::TOO_MANY_REQUESTS,
			));
		}
		Ok(())
	}
}

impl Service {
	/// A stamp is only a bounded cache hint for this service graph. Missing or
	/// prior-process stamps require reconciliation; corrupt encodings refuse.
	pub(super) async fn recount_is_current(&self, room: &RoomId) -> Result<bool> {
		match self.services.db["global"]
			.qry(&(RECOUNT_GENERATION, room))
			.await
		{
			| Ok(value)
				if value.len() == GENERATION_BYTES
					&& value.iter().all(u8::is_ascii_alphanumeric) =>
				Ok(value.as_ref() == self.recount_generation.as_bytes()),
			| Ok(_) => Err(Error::bad_database("Invalid membership recount generation")),
			| Err(error) if error.is_not_found() => Ok(false),
			| Err(error) => Err(error),
		}
	}

	/// All readers use the same room exclusion as membership and recounts.
	/// Validate the existing counter before replacing unmarked legacy counts:
	/// a corrupt/missing counter cannot silently become a healthy zero.
	pub(super) async fn read_reconciled_count(
		&self,
		room: &RoomId,
		map: &Map,
		invalid: &'static str,
	) -> Result<u64> {
		let guard = self.membership_mutex.lock(room).await;
		Box::pin(self.repair_joined_count_locked(room, &guard)).await?;
		let value = map.get(room).await?;
		let count = tuwunel_core::utils::bytes::u64_from_bytes(value.as_ref())
			.map_err(|_| Error::bad_database(invalid))?;
		if self.recount_is_current(room).await? {
			return Ok(count);
		}
		Box::pin(self.update_joined_count_locked(room, &guard)).await?;
		let value = map.get(room).await?;
		tuwunel_core::utils::bytes::u64_from_bytes(value.as_ref())
			.map_err(|_| Error::bad_database(invalid))
	}

	pub(super) async fn prepare_recount_inventory(
		&self,
		room: &RoomId,
	) -> Result<RecountInventory> {
		let mut budget = RecountBudget::default();
		let mut joined_servers = HashSet::new();
		let joined = self
			.recount_members(
				&self.db.roomuserid_joinedcount,
				room,
				&mut budget,
				Some(&mut joined_servers),
			)
			.await?;
		let invited = self
			.recount_members(&self.db.roomuserid_invitecount, room, &mut budget, None)
			.await?;
		let knocked = self
			.recount_members(&self.db.roomuserid_knockedcount, room, &mut budget, None)
			.await?;
		let prefix = (room, Interfix);
		let encoded_prefix = serialize_key(prefix)?;
		let keys = self
			.db
			.roomserverids
			.keys_prefix_raw_capped(&prefix, budget.cap());
		pin_mut!(keys);
		let mut old_servers = Vec::new();
		while let Some(key) = keys.try_next().await? {
			budget.charge(key)?;
			let server = std::str::from_utf8(&key[encoded_prefix.len()..])
				.map_err(|_| Error::bad_database("Invalid membership recount server key"))?;
			let server: &ServerName = server
				.try_into()
				.map_err(|_| Error::bad_database("Invalid membership recount server key"))?;
			old_servers.push(server.to_owned());
		}
		Ok(RecountInventory {
			joined,
			invited,
			knocked,
			joined_servers,
			old_servers,
		})
	}

	async fn recount_members(
		&self,
		map: &std::sync::Arc<Map>,
		room: &RoomId,
		budget: &mut RecountBudget,
		mut servers: Option<&mut HashSet<OwnedServerName>>,
	) -> Result<u64> {
		let prefix = (room, Interfix);
		let encoded_prefix = serialize_key(prefix)?;
		let keys = map.keys_prefix_raw_capped(&prefix, budget.cap());
		pin_mut!(keys);
		let mut count = 0_u64;
		while let Some(key) = keys.try_next().await? {
			budget.charge(key)?;
			let user = std::str::from_utf8(&key[encoded_prefix.len()..])
				.map_err(|_| Error::bad_database("Invalid membership recount user key"))?;
			let user: &UserId = user
				.try_into()
				.map_err(|_| Error::bad_database("Invalid membership recount user key"))?;
			if let Some(servers) = servers.as_mut() {
				servers.insert(user.server_name().to_owned());
			}
			count = count.saturating_add(1);
		}
		Ok(count)
	}
}
