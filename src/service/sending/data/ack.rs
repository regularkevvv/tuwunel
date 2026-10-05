use std::collections::HashMap;

use futures::TryStreamExt;
use tuwunel_core::{Error, Result, utils::ReadyExt};
use tuwunel_database::Row;

use super::{Data, Destination, SendingEvent, active, parse_servercurrentevent, within_prefix};

/// Exact durable row bytes selected before an attempt starts. This does not
/// freeze wire payloads or assign persistent transaction generations. New
/// active rows include their durable incarnation in these physical bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(in crate::sending) struct ActiveAcknowledgement {
	rows: Vec<Row>,
}

impl Data {
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
		Ok(ActiveAcknowledgement { rows })
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
		for (key, expected) in &acknowledgement.rows {
			let (owner, _) = parse_servercurrentevent(key, expected)?;
			if &owner != destination {
				return Err(Error::bad_database("Acknowledgement destination mismatch"));
			}
			match self.servercurrentevent_data.get(key).await {
				| Ok(value) if value.as_ref() == expected.as_slice() =>
					txn.del_raw(&self.servercurrentevent_data, key),
				| Ok(value) => {
					self.validate_active_identity(&value)?;
					let previous = active::identity(expected)?;
					let current = active::identity(&value)?;
					if current
						.is_some_and(|current| previous.is_none_or(|previous| current > previous))
					{
						// Cancellation/re-admission owns a newer identity, even
						// if key and logical event bytes were reused exactly.
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
		txn.execute().await
	}

	fn validate_active_identity(&self, value: &[u8]) -> Result {
		if active::identity(value)?
			.is_some_and(|identity| identity > self.services.globals.current_count())
		{
			return Err(Error::bad_database(
				"Active delivery identity exceeds the committed counter",
			));
		}
		Ok(())
	}
}
