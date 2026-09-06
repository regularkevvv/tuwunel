//! Backend-neutral storage contract (ADR-0002).
//!
//! The concrete `Database`/`Map`/`Txn` facade remains the only service-facing
//! surface. Beneath it, this module defines the narrow contract every storage
//! backend implements so the facade's observable semantics do not depend on
//! RocksDB:
//!
//! - **Maps** are addressed by stable numeric [`MapId`]s ([`ids`]); names are a
//!   facade convenience. Foreign column families opened outside the catalog
//!   carry no id and exist only on the RocksDB backend.
//! - **Values** are owned or pinned byte sequences behind [`crate::Handle`]; no
//!   backend-specific pin type crosses the facade.
//! - **Reads** are point or multi-point lookups plus forward/reverse
//!   lexicographic byte-order scans with snapshot-at-creation visibility: a
//!   scan observes exactly the committed state at its creation and never a
//!   later write. Missing keys are the facade's not-found error, distinct from
//!   transport failures.
//! - **Mutations** are queued as neutral [`Op`]s and committed as one atomic
//!   multi-map batch through [`Txn`](crate::Txn). Partial application is
//!   forbidden. Single-key writes are the degenerate one-op batch and share the
//!   same durability rules.
//! - **Acknowledgement** means durably committed for the backend's configured
//!   durability level; watcher notification happens strictly after it.
//! - **Failure** is returned, never panicked, and never silently retried when
//!   the outcome of a commit is ambiguous. (The remote backend adds idempotency
//!   keys for ambiguous retries in phase 2; the contract reserves the concept
//!   here.)
//! - **Administration** (compaction, checkpoints, physical properties, backups,
//!   WAL control) is a per-backend capability, not part of this contract.
//!   Callers must tolerate `Unsupported`.
//!
//! Three implementations exist: the production RocksDB engine
//! (`crate::engine`, reached through the `Rocks` arms below), the in-memory
//! model backend ([`mem`]), which doubles as the semantic oracle for the
//! differential contract suite, and the [`remote`] D1 backend that reaches
//! the canonical database through the Worker bridge (ADR-0012).
//! `Compare-and-set` and `increment` mutations are still absent from the
//! enum: no caller needs them because the single-writer process serializes
//! read-modify-write at the service layer (ADR-0012, "Commits"), and defining
//! them without a consumer would freeze semantics nothing exercises.

pub mod ids;
pub mod mem;
pub(crate) mod metrics;
pub mod remote;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use crate::{
	Engine,
	keyval::{KeyBuf, ValBuf},
};

/// Stable numeric identity of one logical map.
///
/// Ids are immutable and append-only; see [`ids`]. Remote backends address
/// rows by this value, so it must never be derived from catalog position at
/// runtime.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MapId(pub u16);

/// One queued, backend-neutral mutation.
///
/// Keys and values are already encoded by the facade codec; the backend
/// treats them as opaque ordered bytes.
#[derive(Debug)]
#[expect(
	clippy::large_enum_variant,
	reason = "puts dominate real batches; boxing the inline value buffer would defeat it"
)]
pub enum Op {
	/// Insert or replace one key with one value.
	Put {
		/// Encoded key bytes, opaque ordered data to the backend.
		key: KeyBuf,
		/// Encoded value bytes.
		val: ValBuf,
	},
	/// Remove one key; removing an absent key is a successful no-op.
	Delete {
		/// Encoded key bytes to remove.
		key: KeyBuf,
	},
}

impl Op {
	/// Borrows the key bytes addressed by this mutation.
	#[inline]
	#[must_use]
	pub fn key(&self) -> &[u8] {
		match self {
			| Self::Put { key, .. } | Self::Delete { key } => key,
		}
	}

	/// Payload length used for capacity estimates.
	#[inline]
	#[must_use]
	pub(crate) fn size(&self) -> usize {
		match self {
			| Self::Put { key, val } => key.len().saturating_add(val.len()),
			| Self::Delete { key } => key.len(),
		}
	}
}

/// Identifies the backend instance owning a map or transaction.
///
/// Two facade objects may interoperate only when their sinks refer to the
/// same backend instance, mirroring the old same-engine assertion.
#[derive(Clone)]
pub(crate) enum Sink {
	/// The production RocksDB engine.
	Rocks(Arc<Engine>),
	/// The in-memory model backend.
	Mem(Arc<mem::Store>),
	/// The remote D1 backend behind the Worker bridge.
	Remote(Arc<remote::Backend>),
}

impl Sink {
	/// Whether two sinks are the same backend instance.
	#[inline]
	pub(crate) fn same(&self, other: &Self) -> bool {
		match (self, other) {
			| (Self::Rocks(a), Self::Rocks(b)) => Arc::ptr_eq(a, b),
			| (Self::Mem(a), Self::Mem(b)) => Arc::ptr_eq(a, b),
			| (Self::Remote(a), Self::Remote(b)) => Arc::ptr_eq(a, b),
			| _ => false,
		}
	}
}
