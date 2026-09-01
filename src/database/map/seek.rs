use std::{
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
};

use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, future::Either};
use rocksdb::Direction;
use tuwunel_core::Result;

use super::{Map, cache_iter_options_default, iter_options_default};
use crate::{
	backend::metrics::STATS,
	map::Inner,
	pool::{Seek, into_send_seek},
	stream,
	stream::mem::{MemSeek, Project},
};

/// Builds a forward or reverse map stream from an optional raw seek key.
///
/// On the RocksDB backend, a block-cache probe selects inline iteration when
/// the initial seek is cached; otherwise the seek runs on the engine's
/// blocking pool. On the model backend the visible range is snapshotted at
/// creation. The projection type determines whether each item contains a key
/// alone or a key-value pair. Both paths share snapshot-at-creation
/// visibility and the caller contract that items are valid only until the
/// next poll.
pub(super) fn seek_stream<'a, C, T>(
	map: &'a Arc<Map>,
	dir: Direction,
	from: Option<&[u8]>,
) -> impl Stream<Item = Result<T>> + Send + use<'a, C, T>
where
	C: From<stream::State<'a>> + Stream<Item = Result<T>> + Send,
	T: Project<'a> + Send + Unpin + 'a,
{
	if let Inner::Mem(mem) = map.inner() {
		return Metered::new(Either::Right(MemSeek::new(map, &mem.store, dir, from)));
	}

	let opts = iter_options_default(map.engine());
	let state = stream::State::new(map, opts);
	if is_cached(map, dir, from) {
		let state = init(state, dir, from);
		return Metered::new(Either::Left(Either::Left(
			tokio::task::consume_budget()
				.map(move |()| C::from(state))
				.into_stream()
				.flatten(),
		)));
	}

	let seek = Seek {
		map: map.clone(),
		state: into_send_seek(state),
		dir,
		key: from.map(Into::into),
		res: None,
	};

	Metered::new(Either::Left(Either::Right(
		map.engine()
			.pool
			.execute_iter(seek)
			.ok_into::<C>()
			.into_stream()
			.try_flatten(),
	)))
}

/// Tests whether an initial seek can complete from block cache.
///
/// The probe uses the same direction and starting key as the real iterator
/// without filling cache. RocksDB-path internal helper.
#[tracing::instrument(
    name = "cached",
    level = "trace",
    skip_all,
    fields(%map),
)]
fn is_cached(map: &Arc<Map>, dir: Direction, from: Option<&[u8]>) -> bool {
	let opts = cache_iter_options_default(map.engine());
	let state = init(stream::State::new(map, opts), dir, from);

	!state.is_incomplete()
}

/// Initializes iterator state for the requested seek direction.
///
/// The optional raw key is interpreted as a lower bound when moving forward
/// and an upper bound when moving backward.
fn init<'a>(state: stream::State<'a>, dir: Direction, from: Option<&[u8]>) -> stream::State<'a> {
	match dir {
		| Direction::Forward => state.init_fwd(from),
		| Direction::Reverse => state.init_rev(from),
	}
}

/// Counts yielded items and records the scan length when the stream drops.
///
/// Only the count is recorded — never keys or values (backend metrics
/// contract).
struct Metered<S> {
	inner: S,
	items: usize,
}

impl<S> Metered<S> {
	fn new(inner: S) -> Self { Self { inner, items: 0 } }
}

impl<S: Stream> Stream for Metered<S> {
	type Item = S::Item;

	fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		// SAFETY: `inner` is structurally pinned: it is never moved out of
		// `self`, and `Metered`'s `Drop` only reads the `items` counter.
		// `items` itself is `Unpin` plain data.
		let this = unsafe { self.get_unchecked_mut() };
		// SAFETY: re-pinning the structurally pinned `inner` field of the
		// pinned `Metered` we just projected; it is never moved afterward.
		let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
		let polled = inner.poll_next(ctx);
		if matches!(polled, Poll::Ready(Some(_))) {
			this.items = this.items.saturating_add(1);
		}
		polled
	}

	fn size_hint(&self) -> (usize, Option<usize>) { self.inner.size_hint() }
}

impl<S> Drop for Metered<S> {
	fn drop(&mut self) { STATS.scan_items.record(self.items); }
}
