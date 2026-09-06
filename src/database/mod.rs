//! Persistent storage primitives for Tuwunel.
//!
//! The crate presents typed maps, serialization helpers, and atomic
//! transactions over a backend-neutral seam (ADR-0002). A database opens on
//! one of the backends in [`backend`]: the production RocksDB engine, or the
//! remote D1 backend that reaches the canonical database through the Worker
//! bridge (ADR-0012). `database_backend` selects between them, and the
//! RocksDB engine is a capability that only the RocksDB backend offers.

#![deny(missing_docs)]

extern crate rust_rocksdb as rocksdb;

tuwunel_core::mod_ctor! {}
tuwunel_core::mod_dtor! {}
tuwunel_core::rustc_flags_capture! {}

pub mod backend;
mod cork;
mod de;
mod deserialized;
mod engine;
mod handle;
pub mod keyval;
mod map;
pub mod maps;
mod pool;
mod ser;
mod stream;
#[cfg(test)]
mod tests;
mod txn;
pub(crate) mod util;

use std::{ops::Index, sync::Arc};

use log as _;
use tuwunel_core::{Result, Server, err};

pub use self::{
	backend::remote::LeaseStatus,
	cork::Cork,
	de::{Ignore, IgnoreAll, from_slice as deserialize_from_slice},
	deserialized::Deserialized,
	engine::Engine,
	handle::Handle,
	keyval::{KeyBuf, KeyVal, Slice, serialize_key, serialize_val},
	map::{Get, Map, Qry, compact},
	ser::{Cbor, Interfix, Json, SEP, Separator, serialize, serialize_to, serialize_to_vec},
	txn::Txn,
};
pub(crate) use self::{engine::context::Context, util::or_else};
use crate::{
	backend::{Sink, remote},
	maps::{Maps, MapsKey, MapsVal, open as open_maps, open_remote as open_remote_maps},
};

/// Configuration value selecting the remote D1 backend.
const BACKEND_D1: &str = "d1";

/// Configuration value selecting the local RocksDB backend (the default).
const BACKEND_ROCKSDB: &str = "rocksdb";

/// An open Tuwunel database and its configured maps.
///
/// Each instance owns maps created by one backend. Typed accessors preserve
/// that ownership relationship for individual reads and atomic transactions.
pub struct Database {
	maps: Maps,
	inner: Inner,
}

/// The backend an open database runs on.
enum Inner {
	/// The local RocksDB engine and the context that outlives it.
	Rocks {
		engine: Arc<Engine>,
		_ctx: Arc<Context>,
	},

	/// The remote D1 backend behind the Worker bridge (ADR-0012).
	Remote(Arc<remote::Backend>),
}

impl Database {
	/// Loads an existing database or creates a new one.
	///
	/// `database_backend` selects the backend. On `"rocksdb"` (the default)
	/// the engine opens at `database_path` and the configured map catalog is
	/// indexed by column-family identity. On `"d1"` no RocksDB engine, worker
	/// pool, block cache or filesystem path is opened at all: the remote
	/// backend performs the bridge handshake, refuses to serve on a protocol
	/// or schema mismatch, acquires the writer lease, and the catalog is
	/// opened by stable map id.
	pub async fn open(server: &Arc<Server>) -> Result<Arc<Self>> {
		if server
			.config
			.database_backend
			.eq_ignore_ascii_case(BACKEND_D1)
		{
			let backend = remote::Backend::open(server).await?;
			let maps = open_remote_maps(&backend);

			return Ok(Arc::new(Self { maps, inner: Inner::Remote(backend) }));
		}

		let ctx = Context::new(server)?;
		let engine = Engine::open(ctx.clone(), maps::MAPS).await?;
		let maps = open_maps(&engine)?;

		Ok(Arc::new(Self {
			maps,
			inner: Inner::Rocks { engine, _ctx: ctx },
		}))
	}

	/// Returns the RocksDB engine backing this database.
	///
	/// The engine is a backend capability, not part of the storage contract
	/// (ADR-0002): database-wide operations such as backups, checkpoints,
	/// memory reporting and physical inspection exist only on RocksDB. On any
	/// other backend this is the capability error, which callers report as
	/// "unsupported on this backend".
	#[inline]
	pub fn engine(&self) -> Result<&Arc<Engine>> {
		match &self.inner {
			| Inner::Rocks { engine, .. } => Ok(engine),
			| Inner::Remote(_) => Err(err!(
				"operation is a RocksDB backend capability, unsupported on this backend"
			)),
		}
	}

	/// Names the backend this database opened on.
	///
	/// Reported by `GET /_tuwunel/readiness` so an operator can tell a local
	/// RocksDB instance from the canonical D1 one at a glance.
	#[inline]
	#[must_use]
	pub fn backend(&self) -> &'static str {
		match &self.inner {
			| Inner::Rocks { .. } => BACKEND_ROCKSDB,
			| Inner::Remote(_) => BACKEND_D1,
		}
	}

	/// Returns the writer lease state, or `None` off the remote backend.
	///
	/// Only the remote backend has a lease: RocksDB is exclusive by holding
	/// its own directory lock (ADR-0003 fences the shared database, not the
	/// local one).
	#[inline]
	#[must_use]
	pub fn lease_status(&self) -> Option<LeaseStatus> {
		match &self.inner {
			| Inner::Rocks { .. } => None,
			| Inner::Remote(backend) => Some(backend.lease_status()),
		}
	}

	/// Stops background lease renewal and releases the writer lease.
	///
	/// A no-op off the remote backend. Releasing is best effort: a successor
	/// otherwise waits out the lease's natural expiry.
	pub async fn close(&self) {
		if let Inner::Remote(backend) = &self.inner {
			backend.close().await;
		}
	}

	#[inline]
	/// Creates an empty transaction for this database.
	///
	/// The transaction is bound to this database's backend and accepts writes
	/// only for maps owned by it. Queued operations remain unapplied until the
	/// transaction is executed.
	pub fn txn(&self) -> Txn { Txn::new_with_sink(self.sink()) }

	/// Returns the backend instance owning every map in this database.
	#[inline]
	fn sink(&self) -> Sink {
		match &self.inner {
			| Inner::Rocks { engine, .. } => Sink::Rocks(engine.clone()),
			| Inner::Remote(backend) => Sink::Remote(backend.clone()),
		}
	}

	#[inline]
	/// Retrieves a configured map by name.
	///
	/// The returned map belongs to this database's engine. An unknown name
	/// produces a not-found database error.
	pub fn get(&self, name: &str) -> Result<&Arc<Map>> {
		self.maps
			.get(name)
			.ok_or_else(|| err!(Request(NotFound("column not found"))))
	}

	/// Opens an existing column family outside the configured map catalog.
	///
	/// Migration readers use this for foreign database families that are not
	/// described by `MAPS`. An absent family returns `None` without creating
	/// it. Backends other than RocksDB have no foreign families at all —
	/// every map they address is a catalog map with a stable id — so they
	/// answer `None` as well, and the migrations that look for imported
	/// Conduit or conduwuit columns skip themselves there.
	pub fn open_cf(&self, name: &'static str) -> Result<Option<Arc<Map>>> {
		let Inner::Rocks { engine, .. } = &self.inner else {
			return Ok(None);
		};

		engine
			.has_cf(name)
			.then(|| Map::open(engine, name))
			.transpose()
	}

	#[inline]
	/// Iterates over configured map names and handles.
	///
	/// Entries follow the catalog's sorted map order. Every yielded handle
	/// belongs to this database's engine.
	pub fn iter(&self) -> impl Iterator<Item = (&MapsKey, &MapsVal)> + Send + '_ {
		self.maps.iter()
	}

	#[inline]
	/// Iterates over the configured map names.
	///
	/// Names follow the catalog's sorted map order. The iterator borrows this
	/// database for the duration of the traversal.
	pub fn keys(&self) -> impl Iterator<Item = &MapsKey> + Send + '_ { self.maps.keys() }

	#[inline]
	#[must_use]
	/// Reports whether the backend rejects writes.
	///
	/// On RocksDB, writes are rejected when the database is opened read-only
	/// or as a secondary instance. On the remote backend they are rejected
	/// while the writer lease is uncertain (a renewal failed) or lost
	/// (ADR-0003). The value applies to every map owned by this database.
	pub fn is_read_only(&self) -> bool {
		match &self.inner {
			| Inner::Rocks { engine, .. } => engine.is_read_only(),
			| Inner::Remote(backend) => !backend.is_writable(),
		}
	}

	#[inline]
	#[must_use]
	/// Reports whether this database is a secondary RocksDB instance.
	///
	/// A secondary instance follows another database and does not act as its
	/// primary writer. Only the RocksDB backend has the concept; the remote
	/// backend is always the single primary writer it holds the lease for.
	pub fn is_secondary(&self) -> bool {
		match &self.inner {
			| Inner::Rocks { engine, .. } => engine.is_secondary(),
			| Inner::Remote(_) => false,
		}
	}
}

impl Database {
	/// Writes the backend operation metrics snapshot if configured.
	///
	/// Shutdown paths that cannot guarantee this database's drop (dangling
	/// shutdown references) call this explicitly; drop also invokes it.
	pub fn dump_operation_metrics(&self) { backend::metrics::dump_on_close(); }
}

impl Drop for Database {
	fn drop(&mut self) { backend::metrics::dump_on_close(); }
}

impl Index<&str> for Database {
	type Output = Arc<Map>;

	/// Retrieves a configured map by name.
	///
	/// Indexing offers concise access when the map name is a static database
	/// invariant. Use [`Database::get`] when absence should be handled as an
	/// error.
	///
	/// # Panics
	///
	/// Panics if this database has no configured map with the requested name.
	fn index(&self, name: &str) -> &Self::Output {
		self.maps
			.get(name)
			.expect("column in database does not exist")
	}
}
