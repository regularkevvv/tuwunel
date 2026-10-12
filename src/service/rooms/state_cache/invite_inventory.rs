use std::fmt;

use ruma::{
	RoomId, UserId,
	api::error::{ErrorKind, LimitExceededErrorData},
	events::AnyStrippedStateEvent,
	serde::Raw,
};
use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use tuwunel_core::{Error, Result};

use super::Service;

/// Complete stored invitation input within row and encoded-byte budgets.
pub struct InviteStateInventory {
	/// At most 128 stripped events, or the caller's smaller remaining budget.
	pub events: Vec<Raw<AnyStrippedStateEvent>>,
	/// Stored JSON bytes examined, including whitespace and repeated content.
	pub bytes: usize,
}

struct Read {
	events: Vec<Raw<AnyStrippedStateEvent>>,
	overflow: bool,
}

struct Events(usize);

impl Service {
	/// Reads at most 64 KiB of stored JSON before decoding, retaining at most
	/// 128 raw events. Smaller caller budgets bound aggregate fallback work.
	/// Missing, malformed and failed reads remain errors.
	pub async fn bounded_invite_state(
		&self,
		user: &UserId,
		room: &RoomId,
		event_limit: usize,
		byte_limit: usize,
	) -> Result<InviteStateInventory> {
		let value = self
			.db
			.userroomid_invitestate
			.qry(&(user, room))
			.await
			.map_err(|error| {
				if error.is_not_found() {
					Error::bad_database("Missing known invitation state")
				} else {
					error
				}
			})?;
		let bytes = value.len();
		if bytes > byte_limit.min(64 * 1024) {
			return Err(limit());
		}
		let mut decoder = serde_json::Deserializer::from_slice(value.as_ref());
		let read = Events(event_limit.min(128))
			.deserialize(&mut decoder)
			.map_err(|_| Error::bad_database("Invalid stored invitation state"))?;
		decoder
			.end()
			.map_err(|_| Error::bad_database("Invalid stored invitation state"))?;
		if read.overflow {
			return Err(limit());
		}
		Ok(InviteStateInventory { events: read.events, bytes })
	}
}

impl<'de> DeserializeSeed<'de> for Events {
	type Value = Read;

	fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<Read, D::Error> {
		decoder.deserialize_seq(self)
	}
}

impl<'de> Visitor<'de> for Events {
	type Value = Read;

	fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("a stripped invitation-state array")
	}

	fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<Read, S::Error> {
		let mut read = Read { events: Vec::new(), overflow: false };
		while let Some(event) = sequence.next_element()? {
			if read.events.len() >= self.0 {
				read.overflow = true;
			} else {
				read.events.push(event);
			}
		}
		Ok(read)
	}
}

fn limit() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Invitation state inventory limit reached".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}
