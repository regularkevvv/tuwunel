use std::pin::pin;

use futures::TryStreamExt;
use ruma::{
	EventId, RoomId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use tuwunel_core::{Error, Event, PduCount, Result, implement, utils::json::serialized_len};
use tuwunel_database::{Interfix, serialize_key};

use super::Service;

const MAX_FRONTIER_ROWS: usize = 4096;
const MAX_FRONTIER_BYTES: usize = 512 * 1024;

fn frontier_limit() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Room frontier read limit reached".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}

/// Checks the independently committed room frontier before serving a timeline
/// window. A missing newest accepted PDU is absent from the timeline scan, but
/// remains named by the frontier; it must not turn into an older valid cursor.
/// This does not require a state-after snapshot, so a complete legacy snapshot
/// plus timeline can still serve the state-after fallback.
#[implement(Service)]
pub async fn validate_timeline_frontier(
	&self,
	room_id: &RoomId,
	since: PduCount,
	until: Option<PduCount>,
) -> Result {
	let prefix = serialize_key((room_id, Interfix))?;
	let mut frontier = pin!(
		self.db
			.roomid_pduleaves
			.raw_stream_prefix(prefix.as_ref())
	);
	let shortroomid = self
		.services
		.short
		.get_shortroomid(room_id)
		.await?;
	let mut rows = 0_usize;
	let mut bytes = 0_usize;
	while let Some((key, value)) = frontier.try_next().await? {
		rows = rows.saturating_add(1);
		bytes = bytes
			.saturating_add(key.len())
			.saturating_add(value.len());
		if rows > MAX_FRONTIER_ROWS || bytes > MAX_FRONTIER_BYTES {
			return Err(frontier_limit());
		}
		let value = std::str::from_utf8(value)
			.map_err(|_| Error::bad_database("Invalid room frontier event ID"))?;
		let event_id = EventId::parse(value)
			.map_err(|_| Error::bad_database("Invalid room frontier event ID"))?;
		let expected_key = serialize_key((room_id, &event_id))?;
		if key != expected_key.as_ref() {
			return Err(Error::bad_database("Mismatched room frontier index"));
		}
		let pdu_id = self
			.services
			.timeline
			.get_pdu_id(&event_id)
			.await
			.map_err(|error| {
				if error.is_not_found() {
					Error::bad_database("Missing accepted room frontier index")
				} else {
					error
				}
			})?;
		if pdu_id.shortroomid() != shortroomid.to_be_bytes()
			|| !matches!(pdu_id.pdu_count(), PduCount::Normal(_))
		{
			return Err(Error::bad_database("Mismatched accepted room frontier index"));
		}
		let count = pdu_id.pdu_count();
		if count <= since || until.is_some_and(|until| count > until) {
			continue;
		}
		let pdu = self
			.services
			.timeline
			.get_pdu_from_id(&pdu_id)
			.await
			.map_err(|error| {
				if error.is_not_found() {
					Error::bad_database("Missing accepted room frontier event")
				} else {
					error
				}
			})?;
		bytes = bytes.saturating_add(
			serialized_len(pdu.as_pdu())
				.map_err(|_| Error::bad_database("Invalid room frontier event serialization"))?,
		);
		if bytes > MAX_FRONTIER_BYTES {
			return Err(frontier_limit());
		}
		if pdu.event_id() != event_id || pdu.room_id() != room_id {
			return Err(Error::bad_database("Mismatched accepted room frontier event"));
		}
	}
	Ok(())
}
