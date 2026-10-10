//! Key-only recovery pages close their scan before scheduling any delivery.
use ruma::{OwnedServerName, RoomId, ServerName, UserId};
use tuwunel_core::{Error, Result, utils};
use tuwunel_database::{Deserialized, deserialize_from_slice};

use super::{Data, Destination, Key};

pub(in crate::sending) const DISCOVERY_PAGE_LIMIT: usize = 256;
const KEY_LIMIT: usize = 16 * 1024;
pub(in crate::sending) type RecoveryPage = Vec<(Key, Option<Destination>)>;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::sending) enum RecoverySource {
	#[default]
	Active,
	Queued,
	Edus,
	Rooms,
}

impl Data {
	pub(in crate::sending) async fn recovery_page(
		&self,
		source: RecoverySource,
		after: Option<&[u8]>,
		retired: u64,
	) -> Result<RecoveryPage> {
		let map = match source {
			| RecoverySource::Active => &self.servercurrentevent_data,
			| RecoverySource::Queued => &self.servernameevent_data,
			| RecoverySource::Edus => &self.servername_educount,
			| RecoverySource::Rooms => &self.db["serverroomids"],
		};
		let keys = map
			.raw_keys_after(after, DISCOVERY_PAGE_LIMIT)
			.await?;
		let mut page = Vec::with_capacity(keys.len());
		for key in keys {
			if key.len() > KEY_LIMIT {
				return Err(Error::bad_database("Outgoing recovery key exceeds limit"));
			}
			let destination = if matches!(source, RecoverySource::Edus | RecoverySource::Rooms) {
				let server = if source == RecoverySource::Rooms {
					let (server, _): (&ServerName, &RoomId) = deserialize_from_slice(&key)?;
					server.to_owned()
				} else {
					OwnedServerName::parse(utils::str_from_bytes(&key)?)?
				};
				let count: u64 = match self
					.servername_educount
					.get(&server)
					.await
					.deserialized()
				{
					| Err(error) if error.is_not_found() && source == RecoverySource::Rooms => 0,
					| Err(error) if error.is_not_found() => {
						page.push((key, None));
						continue;
					},
					| result => result?,
				};
				// Another sender may advance a valid watermark after this page's
				// retired-count cut. Compare corruption against the current count;
				// eligibility still uses the frozen cut and resumes next cycle.
				if count
					> self
						.services
						.get()
						.as_ref()
						.globals
						.current_count()
				{
					return Err(Error::bad_database(
						"Outgoing EDU watermark exceeds the retired counter",
					));
				}
				(count < retired).then_some(Destination::Federation(server))
			} else {
				Some(destination_from_key(&key)?)
			};
			page.push((key, destination));
		}
		Ok(page)
	}
}

fn destination_from_key(key: &[u8]) -> Result<Destination> {
	let bad = || Error::bad_database("Invalid outgoing recovery key");
	let (sigil, bytes) = match key.first() {
		| Some(b'+' | b'$') => (key[0], &key[1..]),
		| _ => (0, key),
	};
	let mut parts = bytes.splitn(if sigil == b'$' { 3 } else { 2 }, |byte| *byte == 0xFF);
	let owner = parts.next().ok_or_else(bad)?;
	let second = parts.next().ok_or_else(bad)?;
	match sigil {
		| b'$' => {
			let suffix = parts.next().ok_or_else(bad)?;
			if suffix.is_empty() {
				return Err(bad());
			}
			Ok(Destination::Push(
				UserId::parse(utils::str_from_bytes(owner)?)?,
				utils::string_from_bytes(second)?,
			))
		},
		| b'+' => {
			if second.is_empty() {
				return Err(bad());
			}
			Ok(Destination::Appservice(utils::string_from_bytes(owner)?))
		},
		| _ => {
			if second.is_empty() {
				return Err(bad());
			}
			Ok(Destination::Federation(OwnedServerName::parse(utils::str_from_bytes(owner)?)?))
		},
	}
}
