use std::collections::HashMap;

use futures::TryStreamExt;
use tuwunel_core::{Error, Result};
use tuwunel_database::Row;

use super::{Data, Destination, SendingEvent, parse_servercurrentevent};

/// Exact durable row bytes selected before an attempt starts. This does not
/// freeze wire payloads or assign persistent transaction generations.
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
		let active = self
			.active_requests_for(destination)
			.try_collect::<Vec<_>>()
			.await?;
		let mut rows = Vec::with_capacity(active.len());
		for (key, event) in active {
			let count = expected.get_mut(&event).ok_or_else(|| {
				Error::bad_database("Active delivery was not selected for this attempt")
			})?;
			*count = count.checked_sub(1).ok_or_else(|| {
				Error::bad_database("Active delivery exceeds selected membership")
			})?;
			rows.push((key, event.value_bytes().to_vec()));
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
		let mut txn = self.db.txn();
		for (key, expected) in &acknowledgement.rows {
			let (owner, _) = parse_servercurrentevent(key, expected)?;
			if &owner != destination {
				return Err(Error::bad_database("Acknowledgement destination mismatch"));
			}
			match self.servercurrentevent_data.get(key).await {
				| Ok(value) if value.as_ref() == expected.as_slice() =>
					txn.del_raw(&self.servercurrentevent_data, key),
				| Ok(_) =>
					return Err(Error::bad_database(
						"Active delivery changed before acknowledgement",
					)),
				// Explicit destination cancellation may already have removed it.
				| Err(error) if error.is_not_found() => {},
				| Err(error) => return Err(error),
			}
		}
		txn.execute().await
	}
}
