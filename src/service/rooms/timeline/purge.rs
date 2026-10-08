use ruma::{
	RoomId,
	api::error::{ErrorKind, LimitExceededErrorData},
	events::TimelineEventType,
};
use tuwunel_core::{
	Error, Result, implement,
	matrix::{
		Event,
		pdu::{PduCount, PduEvent},
	},
};
use tuwunel_database::Txn;

use super::{ExtractBody, RawPduId, bias_count};

/// Selectively purge events strictly before the captured stream boundary.
/// State and optionally local events remain. Each canonical PDU and its
/// derived cleanup commit together; refused cleanup leaves that event intact.
/// Operation-level progress and automatic job resumption remain separate.
#[implement(super::Service)]
pub async fn purge_history(
	&self,
	room_id: &RoomId,
	until: PduCount,
	delete_local_events: bool,
) -> Result<usize> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let _state = services_root.state.mutex.lock(room_id).await;
	let _insert = self.mutex_insert.lock(room_id).await;
	let short = services_root
		.short
		.get_shortroomid(room_id)
		.await?;
	let prefix = short.to_be_bytes();
	let mut after = None;
	let mut purged = 0_usize;
	loop {
		// Keep only one bounded event value alive and close the cursor before
		// mutation. Deleting a row cannot invalidate the exclusive next key.
		let rows = self
			.db
			.pduid_pdu
			.raw_rows_prefix_after(&prefix, after.as_deref(), 1)
			.await?;
		let Some((key, value)) = rows.into_iter().next() else {
			return Ok(purged);
		};
		let raw = RawPduId::from_bytes(&key)?;
		if raw.pdu_count() >= until {
			return Ok(purged);
		}
		if value.len() > tuwunel_bridge::MAX_VALUE_BYTES {
			return Err(purge_limit());
		}
		let pdu: PduEvent = serde_json::from_slice(&value)
			.map_err(|_| Error::bad_database("Invalid history purge PDU"))?;
		if pdu.room_id != room_id {
			return Err(Error::bad_database("History purge PDU room mismatch"));
		}
		let binding = self
			.db
			.eventid_pduid
			.get(&pdu.event_id)
			.await
			.map_err(|error| {
				if error.is_not_found() {
					Error::bad_database("History purge PDU binding is missing")
				} else {
					error
				}
			})?;
		if binding.as_ref() != key {
			return Err(Error::bad_database("History purge PDU indexes disagree"));
		}
		if pdu.state_key.is_none()
			&& (delete_local_events || !services_root.globals.user_is_local(&pdu.sender))
		{
			let txn = self
				.prepare_history_erasure(short, &raw, &pdu)
				.await?;
			check_purge_batch(&txn)?;
			// No status/progress row is advanced separately from this boundary.
			txn.execute().await?;
			purged = purged
				.checked_add(1)
				.ok_or_else(|| Error::bad_database("History purge count overflow"))?;
		}
		after = Some(key);
	}
}

#[implement(super::Service)]
async fn prepare_history_erasure(
	&self,
	short: u64,
	raw: &RawPduId,
	pdu: &PduEvent,
) -> Result<Txn> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let mut txn = self.prepare_history_base(raw, pdu).await?;
	services_root
		.pusher
		.stage_notification_erasure(&mut txn, raw, &pdu.room_id)
		.await?;
	self.append_history_search(&mut txn, short, raw, pdu)?;
	// Redaction can already have stripped the current body. Include the
	// retained original so interrupted older redactions cannot strand tokens.
	match services_root
		.retention
		.get_original_pdu(&pdu.event_id)
		.await
	{
		| Ok(original) => {
			if original.event_id != pdu.event_id || original.room_id != pdu.room_id {
				return Err(Error::bad_database("History purge original PDU binding mismatch"));
			}
			self.append_history_search(&mut txn, short, raw, &original)?;
		},
		| Err(error) if error.is_not_found() => {},
		| Err(error) => return Err(error),
	}
	services_root
		.pdu_metadata
		.append_purge_event_relations(
			&mut txn,
			short,
			raw.pdu_count(),
			&pdu.room_id,
			&pdu.event_id,
		)
		.await?;
	services_root
		.retention
		.append_purge_original(&mut txn, &pdu.event_id);
	Ok(txn)
}

#[implement(super::Service)]
fn append_history_search(
	&self,
	txn: &mut Txn,
	short: u64,
	raw: &RawPduId,
	pdu: &PduEvent,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	if pdu.kind == TimelineEventType::RoomMessage {
		let ExtractBody { body } = pdu.get_content()?;
		let Some(body) = body else {
			return Ok(());
		};
		services_root
			.search
			.append_deindex_pdu(txn, short, raw, &body)?;
	}
	Ok(())
}

/// Shared pre-commit limits for a single event's cleanup. Large fan-out needs
/// a journaled multi-batch operation before it can be admitted for resumption.
pub(crate) fn check_purge_batch(txn: &Txn) -> Result {
	if txn.len() > 900 || txn.size_in_bytes() > 512 * 1024 {
		return Err(purge_limit());
	}
	Ok(())
}

fn purge_limit() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"History purge event batch limit exceeded".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}

#[implement(super::Service)]
pub(super) async fn prepare_history_base(&self, raw: &RawPduId, pdu: &PduEvent) -> Result<Txn> {
	let mut txn = self.db.db.txn();
	txn.del_raw(&self.db.pduid_pdu, raw);
	txn.del_raw(&self.db.eventid_pduid, &pdu.event_id);
	txn.del_raw(&self.db.eventid_outlierpdu, &pdu.event_id);
	let ts: u64 = pdu.origin_server_ts.into();
	let key = (&pdu.room_id, ts, bias_count(raw.count()));
	let timestamp_binding = self
		.db
		.roomid_tscount_pducount
		.qry(&key)
		.await
		.map_err(|error| {
			if error.is_not_found() {
				Error::bad_database("History purge timestamp binding is missing")
			} else {
				error
			}
		})?;
	if timestamp_binding.as_ref() != raw.count() {
		return Err(Error::bad_database("History purge timestamp indexes disagree"));
	}
	txn.del(&self.db.roomid_tscount_pducount, key);
	Ok(txn)
}
