#[cfg(test)]
use std::collections::HashMap;

use futures::{StreamExt, TryStreamExt};
use tuwunel_core::{Error, Result, utils::ReadyExt};
use tuwunel_database::Row;

use super::{
	ACTIVE_PROMOTION_LIMIT, Data, Destination, SendingEvent, active, parse_servercurrentevent,
	within_prefix,
};

/// Exact active row bytes and, for HTTP transactions, persisted attempt owner.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(in crate::sending) struct ActiveAcknowledgement {
	pub(super) rows: Vec<Row>,
	pub(super) attempt: Option<super::attempt::AttemptRef>,
}

impl ActiveAcknowledgement {
	pub(in crate::sending) fn retain_prefix(&mut self, count: usize) -> Result {
		if self.attempt.is_some() || count == 0 || count > self.rows.len() {
			return Err(Error::bad_database(
				"Cannot split persisted or invalid transaction membership",
			));
		}
		self.rows.truncate(count);
		Ok(())
	}
}

#[cfg(test)]
impl ActiveAcknowledgement {
	pub(in crate::sending) fn selected_rows(&self) -> &[Row] { &self.rows }
}

impl Data {
	/// Snapshot a bounded prefix of physical admissions. Duplicate logical
	/// events retain distinct keys/identities; later active rows stay owed.
	pub(in crate::sending) async fn active_batch(
		&self,
		destination: &Destination,
	) -> Result<(Vec<SendingEvent>, ActiveAcknowledgement)> {
		const VALUE_BYTES: usize = super::attempt::BODY_LIMIT;
		const KEY_BYTES: usize = 128 * 1024;
		let _guard = self.active_write.lock().await;
		self.require_active_schema().await?;
		let prefix = destination.get_prefix();
		let mut active = self
			.servercurrentevent_data
			.raw_stream_from(&prefix)
			.ready_take_while(move |row| within_prefix(row, &prefix))
			.take(ACTIVE_PROMOTION_LIMIT)
			.boxed();
		let mut rows = Vec::new();
		let mut events = Vec::new();
		let mut key_bytes = 0_usize;
		let mut value_bytes = 0_usize;
		while let Some((key, value)) = active.try_next().await? {
			if key.len() > tuwunel_bridge::MAX_KEY_BYTES {
				return Err(Error::bad_database("Outgoing active key exceeds storage limit"));
			}
			let next_keys = key_bytes.saturating_add(key.len());
			let next_values = value_bytes.saturating_add(value.len());
			if next_keys > KEY_BYTES || next_values > VALUE_BYTES {
				if rows.is_empty() {
					return Err(Error::bad_database(
						"Outgoing active row exceeds batch byte budget",
					));
				}
				break;
			}
			self.validate_active_identity(value)?;
			let (owner, event) = parse_servercurrentevent(key, value)?;
			if &owner != destination {
				return Err(Error::bad_database("Outgoing active batch destination mismatch"));
			}
			rows.push((key.to_vec(), value.to_vec()));
			events.push(event);
			key_bytes = next_keys;
			value_bytes = next_values;
		}
		Ok((events, ActiveAcknowledgement { rows, attempt: None }))
	}

	// Small fixture preparation helper. Production snapshots select physical
	// rows through active_batch rather than matching the entire backlog.
	#[cfg(test)]
	pub(in crate::sending) async fn selected_acknowledgement(
		&self,
		destination: &Destination,
		events: &[SendingEvent],
	) -> Result<ActiveAcknowledgement> {
		let _guard = self.active_write.lock().await;
		self.require_active_schema().await?;
		let mut expected = HashMap::<&SendingEvent, usize>::new();
		for event in events
			.iter()
			.filter(|event| !matches!(event, SendingEvent::Flush))
		{
			let count = expected.entry(event).or_default();
			*count = count
				.checked_add(1)
				.ok_or_else(|| Error::bad_database("Selected membership overflow"))?;
		}
		let prefix = destination.get_prefix();
		let active = self
			.servercurrentevent_data
			.raw_stream_from(&prefix)
			.ready_take_while(move |row| within_prefix(row, &prefix))
			.map_ok(|(key, value)| (key.to_vec(), value.to_vec()))
			.try_collect::<Vec<_>>()
			.await?;
		let mut rows = Vec::with_capacity(active.len());
		for (key, value) in active {
			self.validate_active_identity(&value)?;
			let (_, event) = parse_servercurrentevent(&key, &value)?;
			let count = expected.get_mut(&event).ok_or_else(|| {
				Error::bad_database("Active delivery was not selected for this attempt")
			})?;
			*count = count.checked_sub(1).ok_or_else(|| {
				Error::bad_database("Active delivery exceeds selected membership")
			})?;
			rows.push((key, value));
		}
		if expected.values().any(|count| *count != 0) {
			return Err(Error::bad_database("Selected delivery has no durable active row"));
		}
		Ok(ActiveAcknowledgement { rows, attempt: None })
	}

	/// Validate every selected row before one atomic removal. Active writers
	/// and explicit cancellation share this guard; pending rows are never
	/// removed.
	pub(in crate::sending) async fn acknowledge_active(
		&self,
		destination: &Destination,
		acknowledgement: &ActiveAcknowledgement,
	) -> Result {
		let _guard = self.active_write.lock().await;
		self.require_active_schema().await?;
		let mut txn = self.db.txn();
		if !self
			.stage_attempt_ack(destination, acknowledgement, &mut txn)
			.await?
		{
			return Ok(());
		}
		let mut matched = false;
		let mut replaced = false;
		for (key, expected) in &acknowledgement.rows {
			let (owner, _) = parse_servercurrentevent(key, expected)?;
			if &owner != destination {
				return Err(Error::bad_database("Acknowledgement destination mismatch"));
			}
			match self.servercurrentevent_data.get(key).await {
				| Ok(value) if value.as_ref() == expected.as_slice() => {
					matched = true;
					txn.del_raw(&self.servercurrentevent_data, key);
				},
				| Ok(value) => {
					self.validate_active_identity(&value)?;
					let previous = active::identity(expected)?;
					let current = active::identity(&value)?;
					if current
						.is_some_and(|current| previous.is_none_or(|previous| current > previous))
					{
						// Cancellation/re-admission owns a newer identity, even
						// if key and logical event bytes were reused exactly.
						replaced = true;
						continue;
					}
					return Err(Error::bad_database(
						"Active delivery changed before acknowledgement",
					));
				},
				// Explicit destination cancellation may already have removed it.
				| Err(error) if error.is_not_found() => {},
				| Err(error) => return Err(error),
			}
		}
		if matched && !replaced {
			self.stage_clear_push_backoff(&mut txn, destination)?;
		}
		txn.execute().await
	}

	pub(super) fn validate_active_identity(&self, value: &[u8]) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		// Identities are persisted before dispatch. A different earlier write
		// may still hold the retired visibility frontier below this admission.
		if active::identity(value)?
			.is_some_and(|identity| identity > services_root.globals.pending_count().end)
		{
			return Err(Error::bad_database(
				"Active delivery identity exceeds the committed counter",
			));
		}
		Ok(())
	}
}

#[cfg(test)]
#[path = "ack_frontier_tests.rs"]
mod frontier_tests;
