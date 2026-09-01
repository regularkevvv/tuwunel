use std::sync::Arc;

use futures::{Stream, TryStreamExt};
use tuwunel_core::{Result, implement};

use crate::keyval::Key;

/// Deletes all entries that exist when the clear scan begins.
///
/// The operation scans a consistent iterator view, so later writes can
/// remain. Scan and removal failures are returned; keys removed before a
/// failure stay removed.
#[implement(super::Map)]
#[tracing::instrument(level = "trace")]
pub async fn clear(self: &Arc<Self>) -> Result {
	self.for_clear()
		.try_for_each(async |_| Ok(()))
		.await
}

/// Deletes each entry visible to a clear scan and yields its key.
///
/// The iterator view is fixed when the stream begins, so later writes can
/// remain. Polling drives deletion and exposes scan and removal errors to
/// the caller. Each yielded key borrows cursor storage and must not be
/// retained across another poll.
#[implement(super::Map)]
#[tracing::instrument(level = "trace")]
pub fn for_clear(self: &Arc<Self>) -> impl Stream<Item = Result<Key<'_>>> + Send {
	self.raw_keys()
		.and_then(async move |key| {
			self.remove(&key).await?;
			Ok(key)
		})
}
