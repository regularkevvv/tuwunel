use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{Stream, TryStreamExt};
use ruma::{CanonicalJsonObject, EventId};
use tokio::sync::{Mutex, MutexGuard};
use tuwunel_core::{
	Error, Result, debug_info, implement, matrix::pdu::PduEvent, utils::time::now,
};
use tuwunel_database::{Database, Deserialized, Json, Map, Txn, deserialize_from_slice};

use crate::rooms::timeline::RoomMutexGuard;

pub struct Service {
	services: Arc<crate::services::OnceServices>,
	db: Arc<Database>,
	originals: Mutex<()>,
	eventid_originalpdu: Arc<Map>,
	timeredacted_eventid: Arc<Map>,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: args.services.clone(),
			db: args.db.clone(),
			originals: Mutex::new(()),
			eventid_originalpdu: args.db["eventid_originalpdu"].clone(),
			timeredacted_eventid: args.db["timeredacted_eventid"].clone(),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		if self.services.server.config.maintenance {
			self.services.server.until_shutdown().await;
			return Ok(());
		}
		loop {
			let retention_seconds = self.services.config.redaction_retention_seconds;

			if retention_seconds != 0 {
				debug_info!("Cleaning up retained events");

				let count = self.expire_originals().await?;

				debug_info!(?count, "Finished cleaning up retained events");
			}

			tokio::select! {
				() = tokio::time::sleep(Duration::from_hours(1)) => {},
				() = self.services.server.until_shutdown() => return Ok(())
			};
		}
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Visit chronological rows in bounded pages, closing each scan before a
/// mutation. Expiry removes the original and its housekeeping key atomically.
#[implement(Service)]
pub async fn expire_originals(&self) -> Result<usize> {
	let retention_seconds = self.services.config.redaction_retention_seconds;
	if self.services.server.config.maintenance || retention_seconds == 0 {
		return Ok(0);
	}
	let at = now().as_secs();
	let mut after = None;
	let mut count = 0_usize;
	loop {
		let rows = self
			.timeredacted_eventid
			.raw_rows_after(after.as_deref(), 64)
			.await?;
		if rows.is_empty() {
			return Ok(count);
		}
		let _originals = self.lock_originals().await;
		let pins = self.services.tasks.pinned_history_rooms().await?;
		for (key, value) in rows {
			let (time_redacted, event_id): (u64, &EventId) = deserialize_from_slice(&key)?;
			if !value.is_empty() {
				return Err(Error::bad_database("Invalid original retention index"));
			}
			if time_redacted.saturating_add(retention_seconds) >= at {
				return Ok(count);
			}
			if !pins.is_empty()
				&& self
					.original_room_is_pinned(event_id, &pins)
					.await?
			{
				after = Some(key);
				continue;
			}
			let mut txn = self.db.txn();
			txn.del_raw(&self.eventid_originalpdu, event_id);
			txn.del_raw(&self.timeredacted_eventid, &key);
			txn.execute().await?;
			count = count.saturating_add(1);
			after = Some(key);
		}
	}
}

#[implement(Service)]
pub async fn get_original_pdu(&self, event_id: &EventId) -> Result<PduEvent> {
	self.eventid_originalpdu
		.get(event_id)
		.await?
		.deserialized()
}

#[implement(Service)]
pub async fn get_original_pdu_json(&self, event_id: &EventId) -> Result<CanonicalJsonObject> {
	self.eventid_originalpdu
		.get(event_id)
		.await?
		.deserialized()
}

#[implement(Service)]
pub async fn save_original_pdu(
	&self,
	event_id: &EventId,
	pdu: &CanonicalJsonObject,
	_state_lock: &RoomMutexGuard,
) -> Result {
	if !self.services.config.save_unredacted_events {
		return Ok(());
	}

	let _originals = self.lock_originals().await;
	match self.eventid_originalpdu.get(event_id).await {
		| Ok(_) => return Ok(()),
		| Err(error) if error.is_not_found() => {},
		| Err(error) => return Err(error),
	}

	let now = now().as_secs();

	let mut txn = self.db.txn();
	txn.raw_put(&self.eventid_originalpdu, event_id, Json(pdu));
	txn.put_raw(&self.timeredacted_eventid, (now, event_id), []);
	txn.execute().await
}

#[implement(Service)]
pub fn retained_pdus_raw(&self) -> impl Stream<Item = Result<&[u8]>> + Send {
	self.eventid_originalpdu
		.raw_stream()
		.map_ok(|x| x.1)
}

/// Drops the retained unredacted original of a purged event. The paired
/// `timeredacted_eventid` index entry is left for the retention worker to reap
/// at its scheduled time.
#[implement(Service)]
pub async fn purge_original(&self, event_id: &EventId) -> Result {
	self.eventid_originalpdu.remove(event_id).await
}

/// Leave the chronological housekeeping row for the retention worker, while
/// removing the original in the same transaction as the canonical event.
#[implement(Service)]
pub(crate) fn append_purge_original(&self, txn: &mut Txn, event_id: &EventId) {
	txn.del_raw(&self.eventid_originalpdu, event_id);
}

#[implement(Service)]
pub(crate) async fn lock_originals(&self) -> MutexGuard<'_, ()> { self.originals.lock().await }

#[implement(Service)]
pub(crate) async fn original_snapshot(
	&self,
	event_id: &EventId,
) -> Result<Option<(PduEvent, tuwunel_core::utils::hash::sha256::Digest)>> {
	let value = match self.eventid_originalpdu.get(event_id).await {
		| Ok(value) => value,
		| Err(error) if error.is_not_found() => return Ok(None),
		| Err(error) => return Err(error),
	};
	if value.len() > tuwunel_bridge::MAX_VALUE_BYTES {
		return Err(Error::bad_database("History original exceeds backend value bound"));
	}
	let pdu = serde_json::from_slice(&value)
		.map_err(|_| Error::bad_database("Invalid retained history original"))?;
	Ok(Some((pdu, tuwunel_core::utils::hash::sha256::hash(value.as_ref()))))
}

#[derive(serde::Deserialize)]
struct OriginalBinding {
	event_id: ruma::OwnedEventId,
	room_id: ruma::OwnedRoomId,
}

#[implement(Service)]
async fn original_room_is_pinned(
	&self,
	event: &EventId,
	pins: &std::collections::BTreeSet<ruma::OwnedRoomId>,
) -> Result<bool> {
	let value = match self.eventid_originalpdu.get(event).await {
		| Ok(value) => value,
		| Err(error) if error.is_not_found() => return Ok(false),
		| Err(error) => return Err(error),
	};
	if value.len() > tuwunel_bridge::MAX_VALUE_BYTES {
		return Err(Error::bad_database("Original retention value exceeds backend bound"));
	}
	let binding: OriginalBinding = serde_json::from_slice(&value)
		.map_err(|_| Error::bad_database("Invalid original retention binding"))?;
	if binding.event_id != event {
		return Err(Error::bad_database("Original retention event binding changed"));
	}
	Ok(pins.contains(&binding.room_id))
}
