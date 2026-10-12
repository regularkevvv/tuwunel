//! Preserve actual private/public read positions when adopting notification
//! cutoffs. Sync change stamps are deliberately never used as read positions.
use std::collections::BTreeMap;

use ruma::{
	EventId, RoomId, UInt, UserId,
	events::receipt::{ReceiptEvent, ReceiptType},
};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Error, Result, matrix::Event};
use tuwunel_database::{deserialize_from_slice, serialize_key};

use crate::Services;

const DONE: &[u8] = b"notification_read_cutoffs_v1";
const CURSOR: &[u8] = b"notification_read_cutoffs_cursor_v1";
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
	version: u8,
	phase: u8,
	after: Option<Vec<u8>>,
}

pub(super) async fn migrate(services: &Services, legacy: bool) -> Result {
	let global = &services.db["global"];
	match global.get(DONE).await {
		| Ok(value) if value.is_empty() => {
			return match global.get(CURSOR).await {
				| Err(error) if error.is_not_found() => Ok(()),
				| Err(error) => Err(error),
				| Ok(_) =>
					Err(Error::bad_database("Completed notification migration retains cursor")),
			};
		},
		| Ok(_) =>
			return Err(Error::bad_database("Invalid notification cutoff migration marker")),
		| Err(error) if error.is_not_found() => {},
		| Err(error) => return Err(error),
	}
	let mut cursor = match global.get(CURSOR).await {
		| Ok(value) => {
			if value.len() > 24 * 1024 {
				return Err(Error::bad_database(
					"Oversized notification cutoff migration cursor",
				));
			}
			let cursor: Cursor = serde_json::from_slice(&value).map_err(|_| {
				Error::bad_database("Invalid notification cutoff migration cursor")
			})?;
			if cursor.version != 1 || cursor.phase > 1 {
				return Err(Error::bad_database(
					"Unsupported notification cutoff migration cursor",
				));
			}
			if let Some(after) = &cursor.after {
				validate_key(after, cursor.phase)?;
			}
			cursor
		},
		| Err(error) if error.is_not_found() && !legacy => {
			global.insert(DONE, &[]).await?;
			return Ok(());
		},
		| Err(error) if error.is_not_found() => Cursor { version: 1, phase: 0, after: None },
		| Err(error) => return Err(error),
	};
	loop {
		let map = &services.db[if cursor.phase == 0 {
			"roomuserid_privateread"
		} else {
			"readreceiptid_readreceipt"
		}];
		let keys = map
			.raw_keys_after(cursor.after.as_deref(), 64)
			.await?;
		let mut cutoffs = BTreeMap::<Vec<u8>, u64>::new();
		for key in &keys {
			let (room, user, kind) = validate_key(key, cursor.phase)?;
			let value = map.get(key).await?;
			if key.len() > 4096 || value.len() > 64 * 1024 {
				return Err(Error::bad_database("Oversized legacy read receipt"));
			}
			let position = if cursor.phase == 0 {
				let (position, timestamp): (u64, Option<u64>) = deserialize_from_slice(&value)?;
				if timestamp.is_some_and(|timestamp| UInt::new(timestamp).is_none()) {
					return Err(Error::bad_database("Invalid legacy read timestamp"));
				}
				position
			} else {
				let receipt: ReceiptEvent = serde_json::from_slice(&value)
					.map_err(|_| Error::bad_database("Invalid legacy public read receipt"))?;
				if receipt.room_id != room || receipt.content.len() != 1 {
					return Err(Error::bad_database("Legacy read receipt room mismatch"));
				}
				let (event, by_type) = receipt
					.content
					.iter()
					.next()
					.ok_or_else(|| Error::bad_database("Empty legacy read receipt"))?;
				let readers = by_type
					.get(&ReceiptType::Read)
					.ok_or_else(|| Error::bad_database("Invalid legacy read receipt type"))?;
				let read = readers
					.get(user)
					.ok_or_else(|| Error::bad_database("Legacy read receipt user mismatch"))?;
				if by_type.len() != 1
					|| readers.len() != 1
					|| read.thread.as_str().unwrap_or("") != kind
				{
					return Err(Error::bad_database("Legacy read receipt scope mismatch"));
				}
				let raw = match services.timeline.get_pdu_id(event).await {
					| Ok(raw) => raw,
					| Err(error) if error.is_not_found() => continue,
					| Err(error) => return Err(error),
				};
				let pdu = services.timeline.get_pdu_from_id(&raw).await?;
				if pdu.room_id() != room {
					return Err(Error::bad_database("Legacy receipt event room mismatch"));
				}
				raw.pdu_count().into_unsigned()
			};
			if position > i64::MAX.unsigned_abs() {
				return Err(Error::bad_database("Invalid legacy receipt position"));
			}
			let key = serialize_key((room, user, kind))?.to_vec();
			let previous = cutoffs.entry(key).or_default();
			*previous = (*previous).max(position);
		}
		let mut txn = services.db.txn();
		let target = &services.db["roomuserid_notificationcutoff"];
		for (key, position) in cutoffs {
			let previous = match target.get(&key).await {
				| Ok(value) => {
					let value = u64::from_be_bytes(
						value
							.as_ref()
							.try_into()
							.map_err(|_| Error::bad_database("Invalid stored read cutoff"))?,
					);
					if value > i64::MAX.unsigned_abs() {
						return Err(Error::bad_database("Invalid stored read cutoff position"));
					}
					value
				},
				| Err(error) if error.is_not_found() => 0,
				| Err(error) => return Err(error),
			};
			txn.insert_raw(target, &key, previous.max(position).to_be_bytes());
		}
		if keys.is_empty() && cursor.phase == 1 {
			txn.insert_raw(global, DONE, []);
			txn.del_raw(global, CURSOR);
			return txn.execute().await;
		}
		if keys.is_empty() {
			cursor.phase = 1;
			cursor.after = None;
		} else {
			cursor.after = keys.last().cloned();
		}
		txn.insert_raw(global, CURSOR, serde_json::to_vec(&cursor)?);
		txn.execute().await?;
	}
}

fn validate_key(key: &[u8], phase: u8) -> Result<(&RoomId, &UserId, &str)> {
	if key.len() > 4096 {
		return Err(Error::bad_database("Oversized legacy receipt key"));
	}
	let (room, user, kind) = if phase == 0 {
		let (room, user, kind): (&RoomId, &UserId, &str) = deserialize_from_slice(key)?;
		if serialize_key((room, user, kind))?.as_slice() != key
			&& !(kind.is_empty() && serialize_key((room, user))?.as_slice() == key)
		{
			return Err(Error::bad_database("Invalid legacy private receipt key"));
		}
		(room, user, kind)
	} else {
		let (room, stamp, user, kind): (&RoomId, u64, &UserId, &str) =
			deserialize_from_slice(key)?;
		if stamp > i64::MAX.unsigned_abs()
			|| (serialize_key((room, stamp, user, kind))?.as_slice() != key
				&& !(kind.is_empty() && serialize_key((room, stamp, user))?.as_slice() == key))
		{
			return Err(Error::bad_database("Invalid legacy public receipt key"));
		}
		(room, user, kind)
	};
	if !kind.is_empty() && kind != "main" {
		<&EventId>::try_from(kind)
			.map_err(|_| Error::bad_database("Invalid legacy receipt thread"))?;
	}
	Ok((room, user, kind))
}
