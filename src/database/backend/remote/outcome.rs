//! A dispatched commit must reach a known outcome before another write.
//!
//! Dropping an HTTP future is not proof that D1 did not apply its batch.
//! Cancellation, exhausted transport retries, malformed replies and storage
//! ambiguity therefore stop this writer before its commit barrier is released.

use super::{Backend, client::CallError};

pub(super) struct CommitOutcome<'a> {
	backend: &'a Backend,
	resolved: bool,
}

impl<'a> CommitOutcome<'a> {
	pub(super) fn dispatched(backend: &'a Backend) -> Self { Self { backend, resolved: false } }

	pub(super) fn acknowledged(&mut self) { self.resolved = true; }

	pub(super) fn refused(&mut self, error: &CallError) {
		use tuwunel_bridge::Error;
		// Storage errors can follow a lost batch response and a failed digest
		// lookup. They are not evidence that the original batch never applied.
		// The exception is a class SQLite itself decided (a constraint, the
		// schema, a read-only database): the batch was refused whole and did not
		// apply, so the outcome is known and the writer stays.
		self.resolved = matches!(
			error,
			CallError::Bridge(
				Error::StaleLease { .. }
					| Error::DigestMismatch
					| Error::TooLarge { .. }
					| Error::Invalid(_)
			)
		) || matches!(error, CallError::Bridge(error) if error.is_storage_rejection());
	}
}

impl Drop for CommitOutcome<'_> {
	fn drop(&mut self) {
		if !self.resolved {
			self.backend.stop_indeterminate_commit();
		}
	}
}
