use std::sync::Arc;

use futures::{Stream, StreamExt};
use rocksdb::Direction;
use serde::Deserialize;
use tuwunel_core::{Result, implement};

use super::seek::{seek_stream, seek_stream_bounded};
use crate::{backend::remote::scan::Bound, keyval, keyval::KeyVal, stream};

/// Streams deserialized key-value entries in ascending database order.
///
/// Each raw pair is decoded with the database deserializer. Any borrowed key or
/// value must not be retained across another poll of the stream.
#[implement(super::Map)]
pub fn stream<'a, K, V>(
	self: &'a Arc<Self>,
) -> impl Stream<Item = Result<KeyVal<'_, K, V>>> + Send
where
	K: Deserialize<'a> + Send,
	V: Deserialize<'a> + Send,
{
	self.raw_stream()
		.map(keyval::result_deserialize::<K, V>)
}

/// Streams raw key-value entries in ascending database order.
///
/// The scan begins at the first key in the column family. Yielded keys and
/// values borrow cursor storage and must not be retained across another poll.
#[implement(super::Map)]
#[tracing::instrument(skip(self), fields(%self), level = "trace")]
pub fn raw_stream(self: &Arc<Self>) -> impl Stream<Item = Result<KeyVal<'_>>> + Send {
	seek_stream::<stream::Items<'_>, _>(self, Direction::Forward, None)
}

/// Streams at most `limit` deserialized rows from the beginning of the map.
///
/// The remote backend uses the same cap when fetching or draining this scan;
/// all backends stop after the cap. Entries borrow cursor storage as in
/// [`Map::stream`], so this retains no owned collection of row values.
#[implement(super::Map)]
pub fn stream_capped<'a, K, V>(
	self: &'a Arc<Self>,
	limit: usize,
) -> impl Stream<Item = Result<KeyVal<'_, K, V>>> + Send
where
	K: Deserialize<'a> + Send,
	V: Deserialize<'a> + Send,
{
	self.stream_capped_from(None, limit)
}

/// [`Map::stream_capped`] starting from an inclusive raw key. A caller uses
/// [`crate::successor`] to resume strictly after its prior cursor.
#[implement(super::Map)]
pub fn stream_capped_from<'a, K, V>(
	self: &'a Arc<Self>,
	from: Option<&[u8]>,
	limit: usize,
) -> impl Stream<Item = Result<KeyVal<'_, K, V>>> + Send + use<'a, K, V>
where
	K: Deserialize<'a> + Send,
	V: Deserialize<'a> + Send,
{
	seek_stream_bounded::<stream::Items<'_>, _>(self, Direction::Forward, from, Bound::cap(limit))
		.take(limit)
		.map(keyval::result_deserialize::<K, V>)
}
