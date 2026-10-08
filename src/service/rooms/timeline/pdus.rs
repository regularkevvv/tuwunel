use futures::{
	Stream, TryFutureExt, TryStreamExt,
	future::Either::{Left, Right},
};
use ruma::{MilliSecondsSinceUnixEpoch, OwnedRoomId, RoomId, UInt, UserId, api::Direction};
use tuwunel_core::{
	Error, Result, at, err, implement,
	matrix::pdu::{PduCount, PduEvent},
	utils::{
		result::LogErr,
		stream::{TryIgnore, TryReadyExt, TryWidebandExt},
	},
	warn,
};
use tuwunel_database::{KeyVal, keyval::Val};

use super::{PduId, RawPduId};

pub type PdusIterItem = (PduCount, PduEvent);

/// Offset-binary `u64` of a PDU count, so key order matches signed value order
/// (backfilled negatives sort below normal positives).
#[must_use]
pub fn bias_count(count: [u8; 8]) -> u64 {
	i64::from_be_bytes(count)
		.wrapping_sub(i64::MIN)
		.cast_unsigned()
}

#[implement(super::Service)]
pub fn pdus_near_ts(
	&self,
	user_id: Option<&UserId>,
	room_id: &RoomId,
	ts: MilliSecondsSinceUnixEpoch,
	dir: Direction,
) -> impl Stream<Item = Result<PdusIterItem>> + Send {
	self.pdu_ids_near_ts(room_id, ts, dir)
		.map_ok(|(ts, pdu_id)| (ts, pdu_id.into()))
		.wide_and_then(async |(_, pdu_id): (_, RawPduId)| {
			self.get_pdu_from_id(&pdu_id)
				.map_ok(|pdu| (pdu_id, pdu))
				.await
		})
		.ready_and_then(move |item| Self::each_pdu(item, user_id, room_id))
}

#[implement(super::Service)]
pub fn pdu_ids_near_ts(
	&self,
	room_id: &RoomId,
	ts: MilliSecondsSinceUnixEpoch,
	dir: Direction,
) -> impl Stream<Item = Result<(MilliSecondsSinceUnixEpoch, PduId)>> + Send {
	let services_guard = self.services.get();
	crate::once_services::services_stream!(services_guard, services_root, {
		use Direction::{Backward, Forward};

		type KeyVal = ((OwnedRoomId, UInt, u64), i64);

		let ts: u64 = ts.get().into();

		services_root
			.short
			.get_shortroomid(room_id)
			.map_err(|e| err!(Request(NotFound("Room not found: {e:?}"))))
			.map_ok(move |shortroomid| {
				match dir {
					| Forward => Left(self.db.roomid_tscount_pducount.stream_from(&(
						room_id,
						ts,
						u64::MIN,
					))),
					| Backward => Right(self.db.roomid_tscount_pducount.rev_stream_from(&(
						room_id,
						ts,
						u64::MAX,
					))),
				}
				.ready_try_take_while(move |((room_id_, ..), _): &KeyVal| Ok(room_id == room_id_))
				.map_ok(move |((_, ts, _), count)| {
					(MilliSecondsSinceUnixEpoch(ts), PduId { shortroomid, count: count.into() })
				})
			})
			.try_flatten_stream()
	})
}

/// Returns an iterator over all PDUs in a room. Unknown rooms produce no
/// items.
#[implement(super::Service)]
#[inline]
pub fn all_pdus<'a>(
	&'a self,
	user_id: &'a UserId,
	room_id: &'a RoomId,
) -> impl Stream<Item = PdusIterItem> + Send + 'a {
	self.pdus(Some(user_id), room_id, None)
		.ignore_err()
}

/// Returns an iterator over all events and their tokens in a room that
/// happened after the event with id `from` in order.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn pdus<'a>(
	&'a self,
	user_id: Option<&'a UserId>,
	room_id: &'a RoomId,
	from: Option<PduCount>,
) -> impl Stream<Item = Result<PdusIterItem>> + Send + 'a {
	let from = from.unwrap_or_else(PduCount::min);
	self.count_to_id(room_id, from, Direction::Forward)
		.map_ok(move |current| {
			let prefix = current.shortroomid();
			self.db
				.pduid_pdu
				.raw_stream_from(&current)
				.ready_try_take_while(move |(key, _)| Ok(key.starts_with(&prefix)))
				.and_then(move |item| Box::pin(self.each_slice(item, user_id, room_id)))
		})
		.try_flatten_stream()
}

/// Returns an iterator over all events and their tokens in a room that
/// happened before the event with id `until` in reverse-order.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn pdus_rev<'a>(
	&'a self,
	user_id: Option<&'a UserId>,
	room_id: &'a RoomId,
	until: Option<PduCount>,
) -> impl Stream<Item = Result<PdusIterItem>> + Send + 'a {
	let until = until.unwrap_or_else(PduCount::max);
	self.count_to_id(room_id, until, Direction::Backward)
		.map_ok(move |current| {
			let prefix = current.shortroomid();
			self.db
				.pduid_pdu
				.rev_raw_stream_from(&current)
				.ready_try_take_while(move |(key, _)| Ok(key.starts_with(&prefix)))
				.and_then(move |item| Box::pin(self.each_slice(item, user_id, room_id)))
		})
		.try_flatten_stream()
}

#[implement(super::Service)]
pub fn pdus_raw(&self) -> impl Stream<Item = Result<Val<'_>>> + Send {
	self.db.pduid_pdu.raw_stream().map_ok(at!(1))
}

#[implement(super::Service)]
pub fn outlier_pdus_raw(&self) -> impl Stream<Item = Result<Val<'_>>> + Send {
	self.db
		.eventid_outlierpdu
		.raw_stream()
		.map_ok(at!(1))
}

#[implement(super::Service)]
async fn each_slice(
	&self,
	(pdu_id, pdu): KeyVal<'_>,
	user_id: Option<&UserId>,
	room_id: &RoomId,
) -> Result<PdusIterItem> {
	let pdu_id = RawPduId::from_bytes(pdu_id)?;
	let pdu = serde_json::from_slice::<PduEvent>(pdu)
		.map_err(|_| Error::bad_database("Invalid stored timeline event"))?;
	let accepted_id = self
		.get_pdu_id(&pdu.event_id)
		.await
		.map_err(|error| {
			if error.is_not_found() {
				Error::bad_database("Missing stored timeline event index")
			} else {
				error
			}
		})?;
	if accepted_id != pdu_id {
		return Err(Error::bad_database("Mismatched stored timeline event index"));
	}
	Self::each_pdu((pdu_id, pdu), user_id, room_id)
}

#[implement(super::Service)]
fn each_pdu(
	(pdu_id, mut pdu): (RawPduId, PduEvent),
	user_id: Option<&UserId>,
	room_id: &RoomId,
) -> Result<PdusIterItem> {
	if pdu.room_id.as_str() != room_id.as_str() {
		return Err(Error::bad_database("Mismatched stored timeline event room"));
	}
	pdu.remove_transaction_id_unless_sender(user_id);
	pdu.add_age().log_err().ok();

	Ok((pdu_id.pdu_count(), pdu))
}
