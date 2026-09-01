use std::{fmt::Debug, sync::Arc};

use futures::StreamExt;
use serde::Serialize;
use tuwunel_core::{Result, implement, utils::stream::TryIgnore};

/// Deletes every key visible under a serialized prefix.
///
/// The operation scans a consistent iterator view, so later writes can
/// remain. When debug assertions are disabled, scan errors are filtered
/// after any preceding keys have been removed. Removal failures are
/// returned; keys removed before a failure stay removed.
///
/// # Panics
///
/// Panics if the prefix cannot be serialized, or if a scan error occurs with
/// debug assertions enabled.
#[implement(super::Map)]
#[tracing::instrument(level = "trace", skip(self))]
pub async fn del_prefix<P>(self: &Arc<Self>, prefix: &P) -> Result
where
	P: Serialize + ?Sized + Debug + Sync,
{
	let keys = self.keys_prefix_raw(prefix).ignore_err();
	futures::pin_mut!(keys);
	while let Some(key) = keys.next().await {
		self.remove(&key).await?;
	}

	Ok(())
}
