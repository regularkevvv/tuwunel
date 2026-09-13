use std::{collections::VecDeque, sync::Arc};

use futures::{Stream, TryStreamExt, stream};
use tuwunel_core::{Result, implement};

use super::del_prefix::BATCH;

/// Deletes every entry of the map ([`Map::for_clear`]).
///
/// Entries written while this runs may be removed or may remain. Scan and
/// removal failures are returned; keys removed before a failure stay
/// removed.
#[implement(super::Map)]
#[tracing::instrument(level = "trace")]
pub async fn clear(self: &Arc<Self>) -> Result {
	self.for_clear()
		.try_for_each(async |_| Ok(()))
		.await
}

/// Deletes each entry of the map and yields its key.
///
/// Keys are read in batches of at most [`BATCH`], each after the last key of
/// the one before ([`Map::raw_keys_after`]), and each batch's scan is closed
/// before its keys are removed. So no removal drains a scan this operation
/// holds open, whatever the map's size. Entries written while this runs may
/// be removed or may remain. Polling drives deletion and exposes scan and
/// removal errors to the caller; keys removed before a failure stay removed.
#[implement(super::Map)]
#[tracing::instrument(level = "trace")]
pub fn for_clear(self: &Arc<Self>) -> impl Stream<Item = Result<Vec<u8>>> + Send + '_ {
	let start = Batch {
		after: None,
		keys: VecDeque::new(),
		ended: false,
	};

	stream::try_unfold(start, move |mut batch| async move {
		loop {
			if let Some(key) = batch.keys.pop_front() {
				self.remove(key.as_slice()).await?;
				return Ok(Some((key, batch)));
			}

			if batch.ended {
				return Ok(None);
			}

			let keys = self
				.raw_keys_after(batch.after.as_deref(), BATCH)
				.await?;

			batch.ended = keys.len() < BATCH;
			batch.after = keys.last().cloned();
			batch.keys = keys.into();
		}
	})
}

/// Where [`Map::for_clear`] is: the last key read, and the keys read but not
/// yet removed.
struct Batch {
	after: Option<Vec<u8>>,
	keys: VecDeque<Vec<u8>>,
	ended: bool,
}
