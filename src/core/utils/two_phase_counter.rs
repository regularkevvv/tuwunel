//! Two-Phase Counter.

use std::{
	collections::VecDeque,
	ops::{Deref, Range},
	pin::Pin,
	sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use tokio::sync::Mutex as AsyncMutex;

use crate::{Result, checked, error, is_equal_to};

/// Persists a dispatched sequence number before it is handed out.
///
/// Asynchronous and fallible by the storage contract: the commit is a
/// database write, and a remote backend performs it over a network.
pub type CommitFn =
	Box<dyn Fn(u64) -> Pin<Box<dyn Future<Output = Result> + Send>> + Send + Sync>;

/// Notifies retirement-frontier advancement.
///
/// Runs from a permit destructor and must remain synchronous and
/// non-blocking; it only signals in-process state.
pub type ReleaseFn = Box<dyn Fn(u64) -> Result + Send + Sync>;

/// Two-Phase Counter.
///
/// This device solves the problem of a One-Phase Counter (or just a counter)
/// which is incremented to provide unique sequence numbers (or index numbers)
/// fundamental to server operation. For example, let's say a new Matrix Pdu
/// is received: the counter is incremented and its value becomes the PduId
/// used as a key for the Pdu value when writing to the database.
///
/// Problem: With a single counter shared by both writers and readers, pending
/// writes might still be in-flight and not visible to readers after the writer
/// incremented it. For example, client-sync sees the counter at a certain
/// value, but that value has no Pdu found because its write has not been
/// completed with global visibility. Client-sync will then move on to the next
/// counter value having missed the data from the current one.
pub struct Counter {
	/// Serializes dispatch so sequence numbers are drawn, persisted, and
	/// recorded in one strict order even though persistence awaits.
	dispatch_gate: AsyncMutex<()>,

	/// Self is intended to be `Arc<Counter>` with inner state mutable via
	/// Lock.
	inner: RwLock<State>,

	/// Callback to persist the next sequence number drawn from `dispatched`.
	/// This prevents pending numbers from being reused after server restart.
	commit: CommitFn,
}

/// Inner protected state for Two-Phase Counter.
struct State {
	/// Monotonic counter. The next sequence number is drawn by adding one to
	/// this value. That number will be persisted and added to `pending`.
	dispatched: u64,

	/// List of pending sequence numbers. One less than the minimum value in
	/// this list is the "retirement" sequence number where all writes have
	/// completed and all reads are globally visible.
	pending: VecDeque<u64>,

	/// Callback to notify updates of the retirement value. This is likely
	/// called from the destructor of a permit/guard; try not to panic.
	release: ReleaseFn,
}

#[clippy::has_significant_drop]
/// Holds a dispatched sequence number until its write operation retires.
///
/// The permit dereferences to its unique sequence number and records the
/// retirement frontier sampled at dispatch. Dropping it retires the sequence
/// through the shared counter so the retirement frontier advances in order.
pub struct Permit {
	/// Link back to the shared-state.
	state: Arc<Counter>,

	/// The retirement value computed as a courtesy when this permit was
	/// created.
	retired: u64,

	/// Sequence number of this permit.
	id: u64,
}

impl Counter {
	/// Construct a new Two-Phase counter state. The value of `init` is
	/// considered retired, and the next sequence number dispatched will be one
	/// greater.
	#[must_use]
	pub fn new(init: u64, commit: CommitFn, release: ReleaseFn) -> Arc<Self> {
		Arc::new(Self {
			dispatch_gate: AsyncMutex::new(()),
			inner: State::new(init, release).into(),
			commit,
		})
	}

	/// Obtain a sequence number to conduct write operations for the scope.
	///
	/// The number is durably persisted through the commit callback before it
	/// is returned; a failed commit dispatches nothing. Dispatch order equals
	/// persist order because both happen under the dispatch gate.
	pub async fn next(self: &Arc<Self>) -> Result<Permit> {
		let _order = self.dispatch_gate.lock().await;

		let (prev, retired) = {
			let inner = self.read();
			(inner.dispatched, inner.retired())
		};

		let id = checked!(prev + 1)?;

		debug_assert!(
			!self.read().check_pending(id),
			"sequence number cannot already be pending",
		);

		(self.commit)(id).await?;

		{
			let mut inner = self.write();
			inner.pending.push_back(id);
			inner.dispatched = id;
		};

		Ok(Permit { state: self.clone(), retired, id })
	}

	/// Load the current and dispatched values simultaneously
	#[inline]
	pub fn range(&self) -> Range<u64> {
		let inner = self.read();

		Range {
			start: inner.retired(),
			end: inner.dispatched,
		}
	}

	/// Load the highest sequence number safe for reading, also known as the
	/// retirement value with writes "globally visible."
	#[inline]
	pub fn current(&self) -> u64 { self.read().retired() }

	/// Load the highest sequence number (dispatched); may still be pending or
	/// may be retired.
	#[inline]
	pub fn dispatched(&self) -> u64 { self.read().dispatched }

	/// Borrow the state for reading, tolerating a poisoned lock.
	///
	/// Poisoning carries no information here: the pending list and dispatch
	/// value are only mutated after a successful commit, and `release` runs
	/// after all mutation, so an unwind through either callback leaves the
	/// state consistent. Honoring the flag instead turns a single failed
	/// write into a permanent outage of every sequence number.
	#[inline]
	fn read(&self) -> RwLockReadGuard<'_, State> {
		self.inner
			.read()
			.unwrap_or_else(PoisonError::into_inner)
	}

	/// Borrow the state for writing, tolerating a poisoned lock.
	///
	/// The reasoning is the same as [`Self::read`].
	#[inline]
	fn write(&self) -> RwLockWriteGuard<'_, State> {
		self.inner
			.write()
			.unwrap_or_else(PoisonError::into_inner)
	}
}

impl State {
	/// Create new state, starting from `init`. The next sequence number
	/// dispatched will be one greater than `init`.
	fn new(dispatched: u64, release: ReleaseFn) -> Self {
		Self {
			dispatched,
			pending: VecDeque::new(),
			release,
		}
	}

	/// Retire the sequence number `id`.
	///
	/// This runs from a destructor, so outside debug assertions a
	/// desynchronized pending list or a failing release callback is logged
	/// rather than raised as a panic.
	fn retire(&mut self, id: u64) {
		debug_assert!(self.check_pending(id), "sequence number must be currently pending");

		let Some(index) = self.pending_index(id) else {
			error!(id, "Sequence number was not pending for retirement.");
			return;
		};

		let removed = self.pending.remove(index);

		debug_assert_eq!(removed, Some(id), "sequence number removed must match id");

		// release only occurs when the oldest value retires
		if index != 0 {
			return;
		}

		// release occurs for the maximum retired value
		let release = if self.pending.is_empty() { self.dispatched } else { id };

		debug_assert!(release >= id, "sequence number released must not be less than id");

		(self.release)(release)
			.inspect_err(|error| error!(release, %error, "Failed to release sequence number."))
			.ok();
	}

	/// Calculate the retired sequence number, one less than the lowest pending
	/// sequence number. If nothing is pending the value of `dispatched` has
	/// been previously retired and is returned.
	fn retired(&self) -> u64 {
		debug_assert!(
			self.pending.iter().is_sorted(),
			"Pending values should be naturally sorted"
		);

		self.pending
			.front()
			.map(|val| val.saturating_sub(1))
			.unwrap_or(self.dispatched)
	}

	/// Get the position of `id` in the pending list.
	fn pending_index(&self, id: u64) -> Option<usize> {
		debug_assert!(
			self.pending.iter().is_sorted(),
			"Pending values should be naturally sorted"
		);

		self.pending.binary_search(&id).ok()
	}

	/// Check for `id` in the pending list sequentially (for debug and assertion
	/// purposes only)
	fn check_pending(&self, id: u64) -> bool { self.pending.iter().any(is_equal_to!(&id)) }
}

impl Permit {
	/// Access the retired sequence number sampled at this permit's creation.
	/// This may be outdated prior to access. Obtained as a courtesy under lock.
	#[inline]
	#[must_use]
	pub fn retired(&self) -> &u64 { &self.retired }

	/// Access the sequence number obtained by this permit; a unique value
	#[inline]
	#[must_use]
	pub fn id(&self) -> &u64 { &self.id }
}

impl Deref for Permit {
	type Target = u64;

	#[inline]
	fn deref(&self) -> &Self::Target { self.id() }
}

impl Drop for Permit {
	fn drop(&mut self) { self.state.write().retire(self.id); }
}
