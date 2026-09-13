use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, future};
use rocksdb::Direction;
use tuwunel_core::{Result, implement};

use super::seek::seek_stream_bounded;
use crate::{backend::remote::scan::Bound, keyval::KeyVal, stream};

/// One owned row: key, then value.
pub type Row = (Vec<u8>, Vec<u8>);

/// Reads at most `limit` rows in key order after the key `after`, or from
/// the start of the map when `after` is `None`.
///
/// This is how work that must visit a whole map does so in bounded steps.
/// The scan reads no more than `limit` rows, even when a commit drains it,
/// and it is closed before this returns, so the caller may then write to the
/// map without that commit draining a scan it holds open. Pass the last key
/// back as `after` to resume; fewer than `limit` rows means the map ended.
#[implement(super::Map)]
#[tracing::instrument(skip(self, after), fields(%self), level = "trace")]
pub async fn raw_rows_after(
	self: &Arc<Self>,
	after: Option<&[u8]>,
	limit: usize,
) -> Result<Vec<Row>> {
	let from = after.map(successor);
	seek_stream_bounded::<stream::Items<'_>, _>(
		self,
		Direction::Forward,
		from.as_deref(),
		Bound::cap(limit),
	)
	.take(limit)
	.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
	.try_collect()
	.await
}

/// [`Map::raw_rows_after`] within one key prefix: at most `limit` rows under
/// `prefix` after `after`, or from the prefix's first row when it is `None`.
/// The scan reads nothing past the prefix.
#[implement(super::Map)]
#[tracing::instrument(skip(self, prefix, after), fields(%self), level = "trace")]
pub async fn raw_rows_prefix_after(
	self: &Arc<Self>,
	prefix: &[u8],
	after: Option<&[u8]>,
	limit: usize,
) -> Result<Vec<Row>> {
	let from = after.map_or_else(|| prefix.to_vec(), successor);
	seek_stream_bounded::<stream::Items<'_>, _>(
		self,
		Direction::Forward,
		Some(from.as_slice()),
		Bound { within: Some(prefix), cap: Some(limit) },
	)
	.try_take_while(|(key, _): &KeyVal<'_>| future::ok(key.starts_with(prefix)))
	.take(limit)
	.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
	.try_collect()
	.await
}

/// [`Map::raw_rows_after`] for keys alone.
#[implement(super::Map)]
pub async fn raw_keys_after(
	self: &Arc<Self>,
	after: Option<&[u8]>,
	limit: usize,
) -> Result<Vec<Vec<u8>>> {
	let from = after.map(successor);
	self.raw_keys_capped(from.as_deref(), limit).await
}

/// At most `limit` keys in key order from `from`, inclusive, or from the
/// start of the map when it is `None`. Like [`Map::raw_rows_after`], the scan
/// reads no more than `limit` rows and is closed before this returns; a
/// caller resumes from [`successor`] of the last key, or skips a key range
/// by resuming past it.
#[implement(super::Map)]
#[tracing::instrument(skip(self, from), fields(%self), level = "trace")]
pub async fn raw_keys_capped(
	self: &Arc<Self>,
	from: Option<&[u8]>,
	limit: usize,
) -> Result<Vec<Vec<u8>>> {
	seek_stream_bounded::<stream::Keys<'_>, _>(self, Direction::Forward, from, Bound::cap(limit))
		.take(limit)
		.map_ok(<[u8]>::to_vec)
		.try_collect()
		.await
}

/// The least key greater than `key` in bytewise order: `key` then `0x00`.
#[must_use]
pub fn successor(key: &[u8]) -> Vec<u8> {
	let mut next = Vec::with_capacity(key.len().saturating_add(1));
	next.extend_from_slice(key);
	next.push(0);
	next
}
