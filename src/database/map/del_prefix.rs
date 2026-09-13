use std::{fmt::Debug, sync::Arc};

use futures::{StreamExt, TryStreamExt, future};
use rocksdb::Direction;
use serde::Serialize;
use tuwunel_core::{Result, implement, utils::stream::TryIgnore};

use super::{rows_after::successor, seek::seek_stream_bounded};
use crate::{
	backend::remote::scan::Bound,
	keyval::{Key, serialize_key},
	stream,
};

/// Keys one [`Map::del_prefix`] or [`Map::for_clear`] step reads before it
/// removes them.
pub(super) const BATCH: usize = 256;

/// Deletes every key under a serialized prefix.
///
/// Keys are read in batches of at most [`BATCH`], and each batch's scan is
/// closed before its keys are removed. So no removal drains a scan this
/// operation holds open, and no read goes past the prefix, however many keys
/// it has. Keys written under the prefix while this runs may be removed or
/// may remain. When debug assertions are disabled, a scan error ends the
/// operation after the keys before it have been removed. Removal failures
/// are returned; keys removed before a failure stay removed.
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
	let prefix = serialize_key(prefix).expect("failed to serialize query key");
	let mut from = prefix.to_vec();
	loop {
		let keys: Vec<Vec<u8>> = seek_stream_bounded::<stream::Keys<'_>, _>(
			self,
			Direction::Forward,
			Some(from.as_slice()),
			Bound { within: Some(&*prefix), cap: Some(BATCH) },
		)
		.try_take_while(|key: &Key<'_>| future::ok(key.starts_with(&prefix)))
		.ignore_err()
		.take(BATCH)
		.map(<[u8]>::to_vec)
		.collect()
		.await;

		for key in &keys {
			self.remove(key.as_slice()).await?;
		}

		match keys.last() {
			| Some(last) if keys.len() == BATCH => from = successor(last),
			| _ => return Ok(()),
		}
	}
}
