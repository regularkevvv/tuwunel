use ruma::{
	EventId, RoomId,
	canonical_json::{RedactedBecause, redact_in_place},
};
use tuwunel_core::{Result, err, implement, matrix::event::Event};

use crate::rooms::{short::ShortRoomId, timeline::RoomMutexGuard};

/// Replace a PDU with the redacted form.
#[implement(super::Service)]
#[tracing::instrument(name = "redact", level = "debug", skip(self))]
pub async fn redact_pdu<Pdu: Event + Send + Sync>(
	&self,
	event_id: &EventId,
	reason: &Pdu,
	shortroomid: ShortRoomId,
	state_lock: &RoomMutexGuard,
) -> Result {
	let services_guard = self.services.get();
	let originals = services_guard.retention.lock_originals().await;
	let mut txn = services_guard.db.txn();
	self.append_redaction(&mut txn, event_id, reason, shortroomid, state_lock, &originals)
		.await?;
	txn.check_bridge_admission()?;
	txn.execute_flushed().await
}

/// Stage the complete authorized target mutation with the redaction event.
/// The originals guard is retained by the caller through the canonical commit.
#[implement(super::Service)]
pub(crate) async fn append_redaction<Pdu: Event + Send + Sync>(
	&self,
	txn: &mut tuwunel_database::Txn,
	event_id: &EventId,
	reason: &Pdu,
	shortroomid: ShortRoomId,
	_state_lock: &RoomMutexGuard,
	originals: &tokio::sync::MutexGuard<'_, ()>,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let pdu_id = match self.get_pdu_id(event_id).await {
		| Ok(id) => id,
		| Err(error) if error.is_not_found() => return Ok(()),
		| Err(error) => return Err(error),
	};
	if pdu_id.shortroomid() != shortroomid.to_be_bytes() {
		return Err(err!(Database("Redaction target belongs to another room")));
	}

	let mut pdu = self
		.get_pdu_json_from_id(&pdu_id)
		.await
		.map_err(|e| {
			err!(Database(error!(?pdu_id, ?event_id, ?e, "PDU ID points to invalid PDU.")))
		})?;

	let target: tuwunel_core::matrix::pdu::PduEvent =
		serde_json::from_value(serde_json::to_value(&pdu)?)
			.map_err(|_| err!(Database("Invalid redaction target")))?;
	if target.event_id() != event_id || target.room_id() != reason.room_id() {
		return Err(err!(Database("Mismatched redaction target")));
	}
	services_root
		.retention
		.append_original_pdu(txn, event_id, &pdu, originals)
		.await?;

	let body = pdu["content"]
		.as_object()
		.and_then(|obj| obj.get("body"))
		.and_then(|body| body.as_str());

	if let Some(body) = body {
		services_root
			.search
			.append_deindex_pdu(txn, shortroomid, &pdu_id, body)?;
	}

	if *target.kind() == ruma::events::TimelineEventType::RoomTopic
		&& let Some(topic) = target
			.get_content()
			.ok()
			.and_then(crate::rooms::state_accessor::plain_text_topic)
	{
		services_root
			.search
			.append_deindex_pdu(txn, shortroomid, &pdu_id, &topic)?;
	}

	let room_id: &RoomId = pdu.get("room_id").try_into()?;

	let room_version_id = services_root
		.state
		.get_room_version(room_id)
		.await?;

	let room_version_rules = room_version_id.rules().ok_or_else(|| {
		err!(Request(UnsupportedRoomVersion(
			"Cannot redact event for unknown room version {room_version_id:?}."
		)))
	})?;

	services_root
		.pdu_metadata
		.append_delete_typed_relation(txn, &pdu_id, &pdu)
		.await?;

	redact_in_place(
		&mut pdu,
		&room_version_rules.redaction,
		Some(RedactedBecause::from_json(reason.to_canonical_object())),
	)
	.map_err(|err| err!("invalid event: {err}"))?;

	txn.raw_put(&self.db.pduid_pdu, pdu_id, tuwunel_database::Json(&pdu));
	txn.check_bridge_admission()?;
	Ok(())
}
