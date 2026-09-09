//! Bounded cleanup of expired/orphaned index rows and unaged legacy
//! challenges. Cursors carry no authorization and restart at the beginning
//! after process replacement. No scan remains open across a mutation.
use std::{sync::Arc, time::Duration};

use futures::{StreamExt, TryStreamExt};
use ruma::{OwnedDeviceId, OwnedUserId};
use tuwunel_core::{Result, implement};
use tuwunel_database::{Map, deserialize_from_slice};

use super::{
	Service,
	lifecycle::{self, Metadata},
	transitions::SessionKey,
};

pub(super) const INTERVAL: Duration = Duration::from_secs(30);
pub(super) const BATCH: usize = 64;

#[derive(Default)]
pub(super) struct Cursors {
	index: Option<Vec<u8>>,
	legacy: Option<Vec<u8>>,
}

/// Visit at most 64 rows of each UIAA map. Reads enforce expiry independently
/// of this collector; failed cleanup cannot turn an expired proof valid.
#[implement(Service)]
pub async fn sweep_sessions(&self) -> Result {
	let mut cursors = self.sweep_cursor.lock().await;
	let keys = page(&self.db.uiaasessionid_metadata, &mut cursors.index).await?;
	for key in keys {
		let session = std::str::from_utf8(&key)
			.ok()
			.filter(|session| lifecycle::valid_session(session));
		if let Some(session) = session {
			let _transition = self
				.transitions
				.lock(&SessionKey::new(session))
				.await;
			let value = match self.db.uiaasessionid_metadata.get(&key).await {
				| Ok(value) => value,
				| Err(error) if error.is_not_found() => continue,
				| Err(error) => return Err(error),
			};
			let metadata: Option<Metadata> = (value.len() <= lifecycle::MAX_RECORD_BYTES)
				.then(|| serde_json::from_slice(value.as_ref()).ok())
				.flatten();
			if let Some(metadata) = metadata.as_ref()
				&& metadata.active(lifecycle::now()?)
			{
				let legacy = tuwunel_database::keyval::serialize_key((
					&metadata.user,
					&metadata.device,
					session,
				))?;
				match self
					.db
					.userdevicesessionid_uiaainfo
					.get(&legacy)
					.await
				{
					| Ok(_) => continue,
					| Err(error) if error.is_not_found() => {},
					| Err(error) => return Err(error),
				}
			}
			let mut txn = self.db.database.txn();
			txn.del_raw(&self.db.uiaasessionid_metadata, &key);
			if let Some(metadata) = metadata.as_ref() {
				txn.del(
					&self.db.userdevicesessionid_uiaainfo,
					(&metadata.user, &metadata.device, session),
				);
			}
			txn.execute().await?;
			if let Some(metadata) = metadata {
				self.remove_uiaa_request(&metadata.user, &metadata.device, session);
			}
		} else {
			let mut txn = self.db.database.txn();
			txn.del_raw(&self.db.uiaasessionid_metadata, &key);
			txn.execute().await?;
		}
	}

	let keys = page(&self.db.userdevicesessionid_uiaainfo, &mut cursors.legacy).await?;
	for key in keys {
		self.sweep_legacy_row(&key).await?;
	}
	Ok(())
}

#[implement(Service)]
async fn sweep_legacy_row(&self, key: &[u8]) -> Result {
	let tuple = deserialize_from_slice::<(OwnedUserId, OwnedDeviceId, String)>(key).ok();
	if let Some((user, device, session)) =
		tuple.filter(|(_, _, session)| lifecycle::valid_session(session))
	{
		let _transition = self
			.transitions
			.lock(&SessionKey::new(&session))
			.await;
		// Let the index pass remove malformed index records. A legacy pass
		// must not mistake an I/O failure for absence or stall on decode errors.
		let metadata: Option<Metadata> = match self.db.uiaasessionid_metadata.get(&session).await
		{
			| Ok(value) if value.len() <= lifecycle::MAX_RECORD_BYTES => {
				let Ok(metadata) = serde_json::from_slice(value.as_ref()) else {
					return Ok(());
				};
				Some(metadata)
			},
			| Ok(_) => return Ok(()),
			| Err(error) if error.is_not_found() => None,
			| Err(error) => return Err(error),
		};
		let matches = metadata
			.as_ref()
			.is_some_and(|metadata| metadata.user == user && metadata.device == device);
		// Read the clock after acquiring the session lock and reading its
		// metadata. A challenge may have been created since this sweep began;
		// a sweep-start timestamp would incorrectly treat it as future-dated.
		let now = lifecycle::now()?;
		if matches
			&& metadata
				.as_ref()
				.is_some_and(|metadata| metadata.active(now))
		{
			return Ok(());
		}
		let mut txn = self.db.database.txn();
		txn.del_raw(&self.db.userdevicesessionid_uiaainfo, key);
		if matches {
			txn.del_raw(&self.db.uiaasessionid_metadata, &session);
		}
		txn.execute().await?;
		self.remove_uiaa_request(&user, &device, &session);
	} else {
		let mut txn = self.db.database.txn();
		txn.del_raw(&self.db.userdevicesessionid_uiaainfo, key);
		txn.execute().await?;
	}
	Ok(())
}

async fn page(map: &Arc<Map>, cursor: &mut Option<Vec<u8>>) -> Result<Vec<Vec<u8>>> {
	let start = cursor.clone().unwrap_or_default();
	let mut keys = map
		.raw_keys_from(&start)
		.take(BATCH.saturating_add(1))
		.map_ok(<[u8]>::to_vec)
		.try_collect::<Vec<_>>()
		.await?;
	keys.retain(|key| cursor.as_ref() != Some(key));
	keys.truncate(BATCH);
	*cursor = (keys.len() == BATCH)
		.then(|| keys.last().cloned())
		.flatten();
	Ok(keys)
}
