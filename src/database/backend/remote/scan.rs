//! Paged scans with snapshot-at-creation semantics (ADR-0012 "Reads").
//!
//! A remote scan fetches one page of `d1_scan_page` rows at a time and
//! continues from the last key seen with `inclusive = false`. The Worker has
//! no snapshots, but the Container is the single writer, so the only writes
//! that can interleave with a scan are its own: every open scan is
//! registered here, and before a commit touches a map, the backend *drains*
//! every open scan on that map (fetches its remaining pages into the scan's
//! buffer). A scan therefore observes exactly the state at its creation,
//! like a RocksDB iterator, and a caller that writes while iterating
//! (`del_prefix`, `for_clear`) can never deadlock: the drain runs inside
//! the commit, never inside the stream.
//!
//! Lock order, everywhere: the backend's commit barrier first, then one
//! scan's state. The stream's page fetch takes both (barrier shared); the
//! commit takes the barrier exclusively and then each drained scan's state.
//! A stream never holds either lock across a yield, so the committing task
//! may be the task iterating the scan.

use std::{
	collections::{BTreeSet, HashMap, VecDeque},
	marker::PhantomData,
	pin::Pin,
	sync::{
		Arc, Mutex, PoisonError,
		atomic::{AtomicU64, Ordering::Relaxed},
	},
	task::{Context, Poll},
};

use futures::{Stream, stream::FusedStream};
use tuwunel_core::Result;

use super::Backend;
use crate::{
	backend::{MapId, mem::Entry},
	stream::mem::Project,
};

/// Every open scan of one backend, keyed by a process-unique id.
#[derive(Default)]
pub(crate) struct Registry {
	next: AtomicU64,
	scans: Mutex<HashMap<u64, Arc<Scan>>>,
}

/// One open scan: its position and the rows fetched but not yet yielded.
pub(crate) struct Scan {
	pub(crate) map: MapId,
	pub(crate) reverse: bool,
	pub(crate) state: tokio::sync::Mutex<State>,
}

/// The mutable part of a scan, behind an async lock because page fetches
/// happen while it is held.
pub(crate) struct State {
	/// Rows fetched and not yet yielded, in scan order.
	pub(crate) buffer: VecDeque<Entry>,
	/// Seek position of the next page; `None` means the map's end.
	pub(crate) from: Option<Box<[u8]>>,
	/// Whether `from` itself may be returned (true for the first page only).
	pub(crate) inclusive: bool,
	/// No further page exists.
	pub(crate) exhausted: bool,
}

impl Registry {
	/// Registers a new scan positioned at `from` (seek semantics).
	pub(crate) fn register(
		&self,
		map: MapId,
		reverse: bool,
		from: Option<&[u8]>,
	) -> (u64, Arc<Scan>) {
		let scan = Arc::new(Scan {
			map,
			reverse,
			state: tokio::sync::Mutex::new(State {
				buffer: VecDeque::new(),
				from: from.map(Into::into),
				inclusive: true,
				exhausted: false,
			}),
		});

		let id = self.next.fetch_add(1, Relaxed);
		self.lock().insert(id, scan.clone());

		(id, scan)
	}

	/// Forgets a scan; called from the stream's drop.
	pub(crate) fn unregister(&self, id: u64) { self.lock().remove(&id); }

	/// Open scans on any of `maps`.
	pub(crate) fn touching(&self, maps: &BTreeSet<u16>) -> Vec<Arc<Scan>> {
		self.lock()
			.values()
			.filter(|scan| maps.contains(&scan.map.0))
			.cloned()
			.collect()
	}

	/// Number of open scans.
	#[cfg(test)]
	pub(crate) fn len(&self) -> usize { self.lock().len() }

	fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Arc<Scan>>> {
		self.scans
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
	}
}

/// Ties the stream to the map borrow and its projection without storing
/// either.
type SeekMarker<'a, T> = PhantomData<(&'a (), fn() -> T)>;

/// What the buffer fast path found without waiting on a lock.
enum Buffered {
	/// A row was ready.
	Row(Entry),
	/// The scan is finished.
	End,
	/// The lock was busy, or a page is needed.
	Unknown,
}

/// The future that produces the next row when the buffer is empty.
type Advance = Pin<Box<dyn Future<Output = Result<Option<Entry>>> + Send>>;

/// One positioned remote scan.
///
/// Yielded items borrow the row held in `current`, which is replaced on the
/// next poll: as with the RocksDB cursor streams, an item is valid only
/// until the next poll and must be owned before it is retained.
pub(crate) struct RemoteSeek<'a, T> {
	backend: Arc<Backend>,
	id: u64,
	scan: Arc<Scan>,
	current: Option<Entry>,
	pending: Option<Advance>,
	done: bool,
	_marker: SeekMarker<'a, T>,
}

impl<T> RemoteSeek<'_, T> {
	/// Registers a scan of `map` starting at `from` (RocksDB seek
	/// semantics: forward from the first key not less than `from`, reverse
	/// from the last key not greater than it).
	pub(crate) fn new(
		backend: Arc<Backend>,
		map: MapId,
		reverse: bool,
		from: Option<&[u8]>,
	) -> Self {
		let (id, scan) = backend.scans().register(map, reverse, from);

		Self {
			backend,
			id,
			scan,
			current: None,
			pending: None,
			done: false,
			_marker: PhantomData,
		}
	}
}

/// Fetches the next row through the locks, paging when the buffer is empty.
///
/// A free function rather than an associated one so the boxed future never
/// captures the stream's projection type or its map borrow: the future owns
/// only shared handles and is therefore `'static`.
async fn advance(backend: Arc<Backend>, scan: Arc<Scan>) -> Result<Option<Entry>> {
	let _barrier = backend.barrier().read().await;
	let mut state = scan.state.lock().await;
	if state.buffer.is_empty() && !state.exhausted {
		backend
			.fetch_page(&scan, &mut state, backend.scan_page())
			.await?;
	}

	// A page may come back empty; the buffer then stays empty, the scan is
	// exhausted, and the `None` below ends the stream.
	Ok(state.buffer.pop_front())
}

impl<'a, T> Stream for RemoteSeek<'a, T>
where
	T: Project<'a> + Unpin,
{
	type Item = Result<T>;

	fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.get_mut();
		if this.done {
			return Poll::Ready(None);
		}

		loop {
			if let Some(pending) = this.pending.as_mut() {
				let next = match pending.as_mut().poll(ctx) {
					| Poll::Pending => return Poll::Pending,
					| Poll::Ready(next) => next,
				};

				this.pending = None;
				return match next {
					| Ok(Some(entry)) => Poll::Ready(Some(Ok(this.yield_entry(entry)))),
					| Ok(None) => {
						this.done = true;
						Poll::Ready(None)
					},
					| Err(error) => {
						this.done = true;
						Poll::Ready(Some(Err(error)))
					},
				};
			}

			// Fast path: a buffered row needs no lock wait and no allocation.
			// The guard is released before the stream is touched again, so
			// the borrow never overlaps the yield.
			let buffered = match this.scan.state.try_lock() {
				| Ok(mut state) => match state.buffer.pop_front() {
					| Some(entry) => Buffered::Row(entry),
					| None if state.exhausted => Buffered::End,
					| None => Buffered::Unknown,
				},
				| Err(_) => Buffered::Unknown,
			};

			match buffered {
				| Buffered::Row(entry) => return Poll::Ready(Some(Ok(this.yield_entry(entry)))),
				| Buffered::End => {
					this.done = true;
					return Poll::Ready(None);
				},
				| Buffered::Unknown => {},
			}

			this.pending = Some(Box::pin(advance(this.backend.clone(), this.scan.clone())));
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		if self.done {
			return (0, Some(0));
		}

		let buffered = self
			.scan
			.state
			.try_lock()
			.map_or(0, |state| state.buffer.len());

		(buffered, None)
	}
}

impl<'a, T> RemoteSeek<'a, T>
where
	T: Project<'a>,
{
	/// Installs `entry` as the current row and projects a borrow of it.
	fn yield_entry(&mut self, entry: Entry) -> T {
		let entry: &Entry = self.current.insert(entry);

		// SAFETY: `current` owns the row; its boxed slices never move while the
		// row is installed, and the row is replaced only by the next poll. The
		// borrow is extended to the map lifetime `'a` to satisfy `Stream`'s
		// lifetime-less `Item`, exactly as the RocksDB cursor streams and the
		// model-backend stream extend theirs; the same caller contract
		// applies: an item is valid only until the next poll and must not
		// outlive the stream.
		let entry: &'a Entry = unsafe { std::mem::transmute(entry) };

		T::project(entry)
	}
}

impl<'a, T> FusedStream for RemoteSeek<'a, T>
where
	T: Project<'a> + Unpin,
{
	#[inline]
	fn is_terminated(&self) -> bool { self.done }
}

impl<T> Drop for RemoteSeek<'_, T> {
	fn drop(&mut self) { self.backend.scans().unregister(self.id); }
}
