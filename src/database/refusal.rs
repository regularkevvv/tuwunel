//! Commit refusals the integration tests arm (feature `commit_refusals`).
//!
//! A commit can fail while the process keeps running: the D1 bridge refuses
//! a batch SQLite rejected and keeps the writer, and the in-flight bound
//! refuses a call it never sent. Whatever runs after such a failure has to
//! leave durable state and every cache agreeing. This makes that failure
//! reachable on any backend: the next commit that writes an armed map is
//! refused whole, before it reaches the backend, so nothing in it applies and
//! later commits proceed.
//!
//! Only test dev-dependencies enable the feature, including optimized test
//! builds. A normal release build compiles none of this.

use std::{
	collections::BTreeSet,
	sync::{Mutex, PoisonError},
};

use tuwunel_core::{Err, Result};

/// Maps whose next commit is refused, one entry per refusal.
static ARMED: Mutex<Vec<Armed>> = Mutex::new(Vec::new());

struct Armed {
	map: &'static str,
	skip: usize,
}

/// Refuses the next commit that writes `map`, a batch or a single key.
///
/// Arming is process-wide, which suits the integration tests: each runs one
/// server in its own process. Arming a map twice refuses its next two commits.
pub fn refuse_next(map: &'static str) { refuse_after(map, 0); }

/// Refuse after `skip` otherwise permitted commits that write this map. Each
/// commit counts once even when its batch contains multiple keys in the map.
pub fn refuse_after(map: &'static str, skip: usize) {
	ARMED
		.lock()
		.unwrap_or_else(PoisonError::into_inner)
		.push(Armed { map, skip });
}

/// How many armed refusals have not fired yet.
#[must_use]
pub fn pending() -> usize {
	ARMED
		.lock()
		.unwrap_or_else(PoisonError::into_inner)
		.len()
}

/// Refuses a commit that writes an armed map, disarming that one refusal.
pub(crate) fn check<'a, I>(maps: I) -> Result
where
	I: IntoIterator<Item = &'a str>,
{
	let mut armed = ARMED
		.lock()
		.unwrap_or_else(PoisonError::into_inner);

	let mut seen = BTreeSet::new();
	for map in maps {
		if !seen.insert(map) {
			continue;
		}
		if let Some(at) = armed.iter().position(|armed| armed.map == map) {
			if armed[at].skip > 0 {
				armed[at].skip = armed[at].skip.saturating_sub(1);
				continue;
			}
			armed.remove(at);

			return Err!(Database("commit refused before dispatch: {map} was armed to refuse"));
		}
	}

	Ok(())
}

mod pause {
	use std::sync::{Arc, Mutex};

	use tokio::sync::{Notify, oneshot};
	use tuwunel_core::{Error, Result};

	static ARMED: Mutex<Option<Gate>> = Mutex::new(None);
	struct Gate {
		map: &'static str,
		entered: oneshot::Sender<()>,
		release: Arc<Notify>,
	}

	/// An owned pause at one actual transaction's pre-dispatch boundary.
	/// Dropping the owner releases the transaction or disarms an unused gate.
	pub struct CommitPause {
		entered: oneshot::Receiver<()>,
		release: Arc<Notify>,
	}
	impl CommitPause {
		/// Wait until the transaction reaches its pre-dispatch boundary.
		pub async fn entered(&mut self) -> Result {
			(&mut self.entered)
				.await
				.map_err(|_| Error::bad_database("Owned commit pause was abandoned"))
		}
	}
	impl Drop for CommitPause {
		fn drop(&mut self) {
			let mut gate = ARMED.lock().expect("owned commit pause");
			if gate
				.as_ref()
				.is_some_and(|gate| Arc::ptr_eq(&gate.release, &self.release))
			{
				gate.take();
			}
			self.release.notify_one();
		}
	}

	/// Test-only integration control, enabled by the existing dev dependency.
	///
	/// It pauses the next Txn touching this map after preparation and refusal
	/// checks, before backend dispatch. No environment switch or provider
	/// input.
	pub fn pause_next(map: &'static str) -> CommitPause {
		let (entered, receiver) = oneshot::channel();
		let release = Arc::new(Notify::new());
		let mut gate = ARMED.lock().expect("owned commit pause");
		assert!(gate.is_none(), "one process-owned transaction pause");
		*gate = Some(Gate { map, entered, release: release.clone() });
		CommitPause { entered: receiver, release }
	}

	pub(crate) async fn pause_before_dispatch<'a, I>(maps: I)
	where
		I: IntoIterator<Item = &'a str> + Send,
	{
		let pause = {
			let mut gate = ARMED.lock().expect("owned commit pause");
			if gate
				.as_ref()
				.is_some_and(|gate| maps.into_iter().any(|map| map == gate.map))
			{
				gate.take()
			} else {
				None
			}
		};
		if let Some(pause) = pause {
			pause.entered.send(()).ok();
			pause.release.notified().await;
		}
	}
}

pub(crate) use pause::pause_before_dispatch;
pub use pause::{CommitPause, pause_next};

#[cfg(test)]
mod tests {
	use super::{check, pending, refuse_after};
	#[test]
	fn delayed_refusal_counts_commits_instead_of_batch_operations() {
		refuse_after("delayed-refusal-fixture", 1);
		check(["unrelated-map", "delayed-refusal-fixture", "delayed-refusal-fixture"])
			.expect("first matching commit is allowed once");
		assert_eq!(pending(), 1);
		check(["unrelated-map"]).expect("unrelated commit does not consume refusal");
		check(["delayed-refusal-fixture"]).expect_err("second matching commit refuses");
		assert_eq!(pending(), 0);
		check(["delayed-refusal-fixture"]).expect("refusal was consumed");
	}
}
