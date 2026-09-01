//! Model-backend scan streams.
//!
//! A scan on the model backend copies its visible range at creation, giving
//! the same snapshot-at-creation semantics as a RocksDB iterator: the stream
//! observes exactly the committed state at its creation and never a later
//! write.

use std::{marker::PhantomData, pin::Pin, sync::Arc};

use futures::{
	Stream,
	stream::FusedStream,
	task::{Context, Poll},
};
use rocksdb::Direction;
use tuwunel_core::Result;

use crate::{
	Map,
	backend::mem,
	keyval::{Key, KeyVal},
};

/// Projects one snapshot entry into a stream item type.
///
/// Implemented for the two item shapes the map streams yield: key-value
/// pairs and bare keys.
pub(crate) trait Project<'a>: Sized {
	fn project(entry: &'a (Box<[u8]>, Box<[u8]>)) -> Self;
}

impl<'a> Project<'a> for KeyVal<'a> {
	#[inline]
	fn project(entry: &'a (Box<[u8]>, Box<[u8]>)) -> Self { (&entry.0, &entry.1) }
}

impl<'a> Project<'a> for Key<'a> {
	#[inline]
	fn project(entry: &'a (Box<[u8]>, Box<[u8]>)) -> Self { &entry.0 }
}

/// One positioned model-backend scan.
///
/// The snapshot is immutable for the stream's life; yielded items borrow it.
/// As with the RocksDB cursor streams, an item is valid only until the next
/// poll and must be owned before it is retained.
pub(crate) struct MemSeek<'a, T> {
	snapshot: Vec<(Box<[u8]>, Box<[u8]>)>,
	at: usize,
	_marker: PhantomData<(&'a (), fn() -> T)>,
}

impl<'a, T> MemSeek<'a, T> {
	/// Copies the visible range for `map` in traversal order.
	///
	/// `from` follows RocksDB seek semantics: forward scans begin at the
	/// first key not less than `from`, reverse scans at the last key not
	/// greater than it.
	pub(crate) fn new(
		map: &'a Arc<Map>,
		store: &Arc<mem::Store>,
		dir: Direction,
		from: Option<&[u8]>,
	) -> Self {
		let id = map
			.id()
			.expect("model-backend maps are catalog maps");
		let reverse = matches!(dir, Direction::Reverse);

		Self {
			snapshot: store.snapshot(id, reverse, from),
			at: 0,
			_marker: PhantomData,
		}
	}
}

impl<'a, T> Stream for MemSeek<'a, T>
where
	T: Project<'a> + Unpin,
{
	type Item = Result<T>;

	fn poll_next(self: Pin<&mut Self>, _ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.get_mut();
		if this.at >= this.snapshot.len() {
			return Poll::Ready(None);
		}

		let entry: &(Box<[u8]>, Box<[u8]>) = &this.snapshot[this.at];
		this.at = this.at.saturating_add(1);

		// SAFETY: The snapshot is owned by this stream, never mutated after
		// construction, and its boxed slices never move when the stream does.
		// The borrow is extended to the map lifetime `'a` to satisfy
		// `Stream`'s lifetime-less `Item`, exactly as the RocksDB cursor
		// streams extend their borrows; the same caller contract applies: an
		// item is valid only until the next poll and must not outlive the
		// stream.
		let entry: &'a (Box<[u8]>, Box<[u8]>) = unsafe { std::mem::transmute(entry) };

		Poll::Ready(Some(Ok(T::project(entry))))
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let rest = self.snapshot.len().saturating_sub(self.at);
		(rest, Some(rest))
	}
}

impl<'a, T> FusedStream for MemSeek<'a, T>
where
	T: Project<'a> + Unpin,
{
	#[inline]
	fn is_terminated(&self) -> bool { self.at >= self.snapshot.len() }
}
