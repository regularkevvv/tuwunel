//! Queue erasure shares admission/ACK exclusion. Journaled history scans one
//! bounded page per commit; its marker prevents admissions behind the cursor.
use tuwunel_core::{Error, Result, matrix::RawPduId, utils::hash::sha256::hash};
use tuwunel_database::Txn;

use super::{Data, decode_queued, parse_servercurrentevent};

const PREFIX: u8 = 0x0A;
const MAGIC: &[u8] = b"MSFE\x01";
const PAGE: usize = 16;
const ATOMIC_SCAN_LIMIT: usize = 4096;

fn marker_key(raw: &RawPduId) -> Vec<u8> {
	let mut key = Vec::with_capacity(17);
	key.push(PREFIX);
	key.extend_from_slice(raw.as_ref());
	key
}

fn marker_value(raw: &RawPduId) -> Vec<u8> {
	let mut value = MAGIC.to_vec();
	value.extend_from_slice(&hash(raw.as_ref()));
	value
}

impl Data {
	pub(crate) async fn event_erasure_started(&self, raw: &RawPduId) -> Result<bool> {
		match self.db["global"].get(&marker_key(raw)).await {
			| Ok(value) if value.as_ref() == marker_value(raw) => Ok(true),
			| Ok(_) => Err(Error::bad_database("Invalid event erasure marker")),
			| Err(error) if error.is_not_found() => Ok(false),
			| Err(error) => Err(error),
		}
	}

	pub(super) async fn require_deliverable_pdu(&self, raw: &RawPduId) -> Result {
		if self.event_erasure_started(raw).await? {
			return Err(Error::bad_database("Outgoing event is erased or being erased"));
		}
		self.db["pduid_pdu"]
			.exists(raw.as_ref())
			.await
			.map_err(|error| {
				if error.is_not_found() {
					Error::bad_database("Outgoing event is erased or missing")
				} else {
					error
				}
			})
	}

	/// Caller retains active_write through the progress commit. Retire the
	/// source immediately, keeping its role until canonical removal finishes.
	pub(crate) async fn stage_begin_event_erasure(
		&self,
		txn: &mut Txn,
		raw: &RawPduId,
	) -> Result {
		self.event_erasure_started(raw).await?;
		txn.insert_raw(&self.db["global"], marker_key(raw), marker_value(raw));
		self.stage_federation_source_erasure(txn, raw)
			.await
	}

	/// At most sixteen inspected keys and one matching delivery per page.
	/// A single attempt may own 24 body chunks with maximum-width keys; keeping
	/// only one retirement bounds the staged bytes as well as operations.
	pub(crate) async fn stage_event_queue_erasure_page(
		&self,
		txn: &mut Txn,
		raw: &RawPduId,
		active: bool,
		after: Option<&[u8]>,
	) -> Result<(Option<Vec<u8>>, bool)> {
		let map = if active {
			&self.servercurrentevent_data
		} else {
			&self.servernameevent_data
		};
		let keys = map.raw_keys_after(after, PAGE).await?;
		let exhausted = keys.len() < PAGE;
		let mut cursor = None;
		for (index, key) in keys.iter().enumerate() {
			cursor = Some(key.clone());
			// Both PDU variants retain their raw ID as the key suffix. A
			// different suffix proves this row cannot reference the target;
			// do not copy/decode unrelated EDU values or their corruption.
			if !key.ends_with(raw.as_ref()) {
				continue;
			}
			let value = map.get(key).await?;
			if value.len() > tuwunel_bridge::MAX_VALUE_BYTES {
				return Err(Error::bad_database(
					"Event erasure queue value exceeds storage limit",
				));
			}
			let (destination, event) = if active {
				parse_servercurrentevent(key, &value)?
			} else {
				let (_, event, destination) = decode_queued(Ok((key, &value)))?;
				(destination, event)
			};
			if event.pdu_id() != Some(raw) {
				continue;
			}
			if active {
				self.stage_erased_attempt(txn, &destination, key)
					.await?;
			}
			txn.del_raw(map, key);
			crate::rooms::timeline::check_purge_batch(txn)?;
			return Ok((cursor, exhausted && index.saturating_add(1) == keys.len()));
		}
		Ok((cursor, exhausted))
	}

	/// Synchronous erasure is one atomic commit. Refuse before writing if a
	/// complete inventory exceeds its bounded work/commit budget; the typed
	/// history executor handles larger queues with durable page progress.
	pub(super) async fn stage_event_queues_erasure(
		&self,
		txn: &mut Txn,
		raw: &RawPduId,
	) -> Result {
		for active in [false, true] {
			let mut after = None;
			for page in 0..(ATOMIC_SCAN_LIMIT / PAGE) {
				let (cursor, done) = self
					.stage_event_queue_erasure_page(txn, raw, active, after.as_deref())
					.await?;
				if done {
					break;
				}
				after = cursor;
				if after.is_none() {
					return Err(Error::bad_database("Event erasure cursor made no progress"));
				}
				if page.saturating_add(1) == ATOMIC_SCAN_LIMIT / PAGE {
					return Err(Error::bad_database(
						"Atomic event erasure inventory exceeds its work limit; use journaled \
						 history",
					));
				}
			}
		}
		Ok(())
	}

	pub(crate) fn stage_finish_event_erasure(&self, txn: &mut Txn, raw: &RawPduId) {
		txn.del_raw(&self.db["global"], marker_key(raw));
	}
}
