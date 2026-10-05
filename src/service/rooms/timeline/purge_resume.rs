//! Bounded steps for the typed history handler. The caller holds room state,
//! insertion and original-retention exclusion through preparation and commit.

use ruma::RoomId;
use tuwunel_core::{
	Error, Result, implement,
	matrix::{
		Event,
		pdu::{PduCount, PduEvent, RawPduId},
	},
	utils::hash::sha256,
};
use tuwunel_database::Txn;

use super::ExtractBody;
use crate::tasks::history::{History, Phase, Target};

struct Snapshot {
	pdu: PduEvent,
	canonical: sha256::Digest,
	original: Option<PduEvent>,
	original_hash: Option<sha256::Digest>,
}

#[implement(super::Service)]
pub(crate) async fn validate_history_progress(&self, room: &RoomId, history: &History) -> Result {
	if self.services.short.get_shortroomid(room).await? != history.shortroomid {
		return Err(Error::bad_database("History room binding changed"));
	}
	if let Some(target) = &history.current {
		let snapshot = self
			.history_snapshot(room, history.shortroomid, &target.key)
			.await?;
		validate_target(
			&snapshot,
			target,
			history,
			self.services
				.globals
				.user_is_local(&snapshot.pdu.sender),
		)?;
	}
	Ok(())
}

#[implement(super::Service)]
pub(crate) async fn prepare_history_step(
	&self,
	room: &RoomId,
	mut history: History,
) -> Result<(Txn, History)> {
	let mut txn = self.db.db.txn();
	let Some(mut target) = history.current.take() else {
		let rows = self
			.db
			.pduid_pdu
			.raw_rows_prefix_after(
				&history.shortroomid.to_be_bytes(),
				history.after.as_deref(),
				1,
			)
			.await?;
		let Some((key, _)) = rows.into_iter().next() else {
			history.done = true;
			return Ok((txn, history));
		};
		let raw = RawPduId::from_bytes(&key)?;
		if raw.pdu_count() >= PduCount::from(history.boundary) {
			history.done = true;
			return Ok((txn, history));
		}
		let snapshot = self
			.history_snapshot(room, history.shortroomid, &key)
			.await?;
		if snapshot.pdu.state_key.is_some()
			|| (!history.delete_local_events
				&& self
					.services
					.globals
					.user_is_local(&snapshot.pdu.sender))
		{
			history.after = Some(key);
		} else {
			history.current = Some(Target {
				key,
				event_id: snapshot.pdu.event_id,
				canonical: snapshot.canonical,
				original: snapshot.original_hash,
				phase: Phase::SearchCurrent,
				after: None,
			});
		}
		return Ok((txn, history));
	};
	let snapshot = self
		.history_snapshot(room, history.shortroomid, &target.key)
		.await?;
	validate_target(
		&snapshot,
		&target,
		&history,
		self.services
			.globals
			.user_is_local(&snapshot.pdu.sender),
	)?;
	let raw = RawPduId::from_bytes(&target.key)?;
	let (after, done) = match target.phase {
		| Phase::SearchCurrent | Phase::SearchOriginal => {
			let pdu = if target.phase == Phase::SearchCurrent {
				Some(&snapshot.pdu)
			} else {
				snapshot.original.as_ref()
			};
			let body = pdu
				.filter(|pdu| pdu.kind == ruma::events::TimelineEventType::RoomMessage)
				.map(Event::get_content::<ExtractBody>)
				.transpose()?
				.and_then(|body| body.body);
			match body {
				| Some(body) => self.services.search.append_deindex_page(
					&mut txn,
					history.shortroomid,
					&raw,
					&body,
					target.after.as_deref(),
				)?,
				| None => (None, true),
			}
		},
		| Phase::LegacyRelations | Phase::TypedRelations =>
			self.services
				.pdu_metadata
				.append_history_relation_page(
					&mut txn,
					history.shortroomid,
					raw.pdu_count(),
					target.phase == Phase::TypedRelations,
					target.after.as_deref(),
				)
				.await?,
		| Phase::Final => {
			txn = self
				.prepare_history_base(&raw, &snapshot.pdu)
				.await?;
			self.services
				.pdu_metadata
				.append_history_points(
					&mut txn,
					history.shortroomid,
					raw.pdu_count(),
					room,
					&target.event_id,
				)
				.await?;
			self.services
				.retention
				.append_purge_original(&mut txn, &target.event_id);
			history.purged = history
				.purged
				.checked_add(1)
				.ok_or_else(|| Error::bad_database("History total overflow"))?;
			history.after = Some(target.key);
			return Ok((txn, history));
		},
	};
	if done {
		target.after = None;
		target.phase = match target.phase {
			| Phase::SearchCurrent => Phase::SearchOriginal,
			| Phase::SearchOriginal => Phase::LegacyRelations,
			| Phase::LegacyRelations => Phase::TypedRelations,
			| Phase::TypedRelations | Phase::Final => Phase::Final,
		};
	} else {
		target.after = after;
	}
	history.current = Some(target);
	Ok((txn, history))
}

#[implement(super::Service)]
async fn history_snapshot(&self, room: &RoomId, short: u64, key: &[u8]) -> Result<Snapshot> {
	let raw = RawPduId::from_bytes(key)?;
	if raw.shortroomid() != short.to_be_bytes() {
		return Err(Error::bad_database("History PDU room key changed"));
	}
	let value = self.db.pduid_pdu.get(key).await?;
	if value.len() > tuwunel_bridge::MAX_VALUE_BYTES {
		return Err(Error::bad_database("History PDU exceeds backend value bound"));
	}
	let pdu: PduEvent =
		serde_json::from_slice(&value).map_err(|_| Error::bad_database("Invalid history PDU"))?;
	if pdu.room_id != room
		|| self
			.db
			.eventid_pduid
			.get(&pdu.event_id)
			.await?
			.as_ref() != key
	{
		return Err(Error::bad_database("History PDU indexes disagree"));
	}
	// Validate the timestamp binding before any derived cleanup too.
	let base = self.prepare_history_base(&raw, &pdu).await?;
	drop(base);
	let original = self
		.services
		.retention
		.original_snapshot(&pdu.event_id)
		.await?;
	if original.as_ref().is_some_and(|(original, _)| {
		original.event_id != pdu.event_id || original.room_id != pdu.room_id
	}) {
		return Err(Error::bad_database("History original binding changed"));
	}
	let (original, original_hash) =
		original.map_or((None, None), |(original, hash)| (Some(original), Some(hash)));
	Ok(Snapshot {
		pdu,
		canonical: sha256::hash(value.as_ref()),
		original,
		original_hash,
	})
}

fn validate_target(
	snapshot: &Snapshot,
	target: &Target,
	history: &History,
	local: bool,
) -> Result {
	if snapshot.pdu.event_id != target.event_id
		|| snapshot.canonical != target.canonical
		|| snapshot.original_hash != target.original
		|| snapshot.pdu.state_key.is_some()
		|| (!history.delete_local_events && local)
	{
		return Err(Error::bad_database("Frozen history target changed"));
	}
	Ok(())
}
