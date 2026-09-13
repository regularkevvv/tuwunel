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
//! Only the `tuwunel` package's dev-dependencies enable the feature, so a
//! release build compiles none of this.

use std::sync::{Mutex, PoisonError};

use tuwunel_core::{Err, Result};

/// Maps whose next commit is refused, one entry per refusal.
static ARMED: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

/// Refuses the next commit that writes `map`, a batch or a single key.
///
/// Arming is process-wide, which suits the integration tests: each runs one
/// server in its own process. Arming a map twice refuses its next two commits.
pub fn refuse_next(map: &'static str) {
	ARMED
		.lock()
		.unwrap_or_else(PoisonError::into_inner)
		.push(map);
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

	for map in maps {
		if let Some(at) = armed.iter().position(|armed| *armed == map) {
			armed.remove(at);

			return Err!(Database("commit refused before dispatch: {map} was armed to refuse"));
		}
	}

	Ok(())
}
