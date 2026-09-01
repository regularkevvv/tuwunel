mod clear;
pub mod compact;
mod contains;
mod count;
mod del;
mod del_prefix;
mod get;
mod get_batch;
mod insert;
mod keys;
mod keys_from;
mod keys_prefix;
mod open;
mod options;
mod put;
mod qry;
mod qry_batch;
mod remove;
mod rev_keys;
mod rev_keys_from;
mod rev_keys_prefix;
mod rev_stream;
mod rev_stream_from;
mod rev_stream_prefix;
mod seek;
mod stream;
mod stream_from;
mod stream_prefix;
mod watch;

use std::{
	ffi::CStr,
	fmt,
	fmt::{Debug, Display},
	path::Path,
	sync::{Arc, Weak},
};

use rocksdb::{
	AsColumnFamilyRef, ColumnFamily, DBCommon, ReadOptions, WriteOptions, checkpoint::Checkpoint,
};
use tuwunel_core::{Result, err};

pub(crate) use self::options::{
	cache_iter_options_default, cache_read_options_default, iter_options_default,
	read_options_default, write_options_default,
};
use self::watch::Watch;
/// Stream extensions for batched map reads.
///
/// `Get` accepts raw keys, while `Qry` serializes structured keys before
/// lookup. Both yield value handles through an asynchronous stream.
pub use self::{get_batch::Get, qry_batch::Qry};
use crate::{
	Engine,
	backend::{MapId, Sink, ids, mem},
	util::map_err,
};

/// Provides typed and raw access to one logical map.
///
/// A map retains the backend state addressing its storage: the RocksDB
/// backend keeps its column-family handle and prepared read/write options,
/// while the model backend keeps its store. Watchers and identity are
/// backend-neutral facade state.
pub struct Map {
	name: &'static str,
	id: Option<MapId>,
	watch: Watch,
	selfref: Weak<Self>,
	inner: Inner,
}

/// Backend-specific per-map storage state.
pub(crate) enum Inner {
	/// One RocksDB column family plus its prepared options.
	Rocks(RocksMap),
	/// One keyspace of the in-memory model store.
	Mem(MemMap),
}

/// RocksDB backend state for one map.
pub(crate) struct RocksMap {
	pub(crate) cf: Arc<ColumnFamily>,
	pub(crate) engine: Arc<Engine>,
	pub(crate) read_options: ReadOptions,
	pub(crate) cache_read_options: ReadOptions,
	pub(crate) write_options: WriteOptions,
}

/// Model backend state for one map.
pub(crate) struct MemMap {
	pub(crate) store: Arc<mem::Store>,
}

impl Map {
	/// Opens a map for a named RocksDB column family.
	///
	/// The returned map keeps the engine alive for at least as long as its
	/// column-family handle. Its read and write options are initialized from
	/// the engine configuration. Catalog maps carry their stable [`MapId`];
	/// foreign migration families carry none.
	pub(crate) fn open(engine: &Arc<Engine>, name: &'static str) -> Result<Arc<Self>> {
		Ok(Arc::new_cyclic(|selfref| Self {
			name,
			id: ids::map_id(name),
			watch: Watch::default(),
			selfref: selfref.clone(),
			inner: Inner::Rocks(RocksMap {
				cf: open::open(engine, name),
				engine: engine.clone(),
				read_options: read_options_default(engine),
				cache_read_options: cache_read_options_default(engine),
				write_options: write_options_default(engine),
			}),
		}))
	}

	/// Opens a catalog map on the in-memory model backend.
	///
	/// The contract suite uses this to drive the model through the same
	/// facade paths as production RocksDB.
	///
	/// # Panics
	///
	/// Panics when `name` is not a catalog map: the model backend addresses
	/// storage by stable [`MapId`] only.
	#[cfg_attr(not(test), expect(dead_code, reason = "contract-suite constructor until a runtime backend selector lands in phase 2"))]
	pub(crate) fn open_mem(store: &Arc<mem::Store>, name: &'static str) -> Arc<Self> {
		Arc::new_cyclic(|selfref| Self {
			name,
			id: Some(ids::map_id(name).expect("model-backend maps must be catalog maps")),
			watch: Watch::default(),
			selfref: selfref.clone(),
			inner: Inner::Mem(MemMap { store: store.clone() }),
		})
	}

	/// Flush this map's memtable to SST files (a RocksDB LSM-tree flush).
	///
	/// Forces the column family's buffered writes out of memory into the
	/// on-disk LSM tree. An LSM flush, not a libc `fflush(3)` or `fsync(2)`,
	/// and distinct from the engine's `flush` and `sync`, which act on the
	/// write-ahead log. A backend capability: unsupported off RocksDB.
	#[tracing::instrument(level = "info", skip_all, fields(map = self.name()))]
	pub fn sort(&self) -> Result {
		let rocks = self.rocks_capability()?;
		let flushoptions = rocksdb::FlushOptions::default();
		DBCommon::flush_cf_opt(&rocks.engine.db, &&*rocks.cf, &flushoptions).map_err(map_err)
	}

	/// Exports this map's column family to a physical checkpoint at `path`.
	///
	/// RocksDB flushes the column family before exporting its live SST files.
	/// A backend capability: unsupported off RocksDB.
	#[tracing::instrument(level = "info", skip(self))]
	pub fn checkpoint(&self, path: &Path) -> Result {
		let rocks = self.rocks_capability()?;
		let checkpoint = Checkpoint::new(&rocks.engine.db).map_err(map_err)?;

		checkpoint
			.export_column_family(&&*rocks.cf, path)
			.map(drop)
			.map_err(map_err)
	}

	/// Reads an integer RocksDB property for this map.
	///
	/// The property query is scoped to this map's column family. A backend
	/// capability: unsupported off RocksDB.
	#[inline]
	pub fn property_integer(&self, name: &CStr) -> Result<u64> {
		let rocks = self.rocks_capability()?;
		rocks.engine.property_integer(&&*rocks.cf, name)
	}

	/// Reads a string RocksDB property for this map.
	///
	/// The property query is scoped to this map's column family. A backend
	/// capability: unsupported off RocksDB.
	#[inline]
	pub fn property(&self, name: &str) -> Result<String> {
		let rocks = self.rocks_capability()?;
		rocks.engine.property(&&*rocks.cf, name)
	}

	/// Returns the column-family name of this map.
	///
	/// The name is fixed when the map opens and lives for the duration of the
	/// process.
	#[inline]
	pub fn name(&self) -> &str { self.name }

	/// Returns this map's stable numeric identity, if it is a catalog map.
	///
	/// Foreign migration column families have none and are invisible to
	/// remote backends and watcher notification.
	#[inline]
	pub(crate) fn id(&self) -> Option<MapId> { self.id }

	/// Returns the backend instance owning this map.
	#[inline]
	pub(crate) fn sink(&self) -> Sink {
		match &self.inner {
			| Inner::Rocks(rocks) => Sink::Rocks(rocks.engine.clone()),
			| Inner::Mem(mem) => Sink::Mem(mem.store.clone()),
		}
	}

	/// Upgrades this map's own shared handle.
	///
	/// Maps are always constructed behind an `Arc`, so the upgrade cannot
	/// fail while `self` is reachable.
	#[inline]
	pub(crate) fn cloned_arc(&self) -> Arc<Self> {
		self.selfref
			.upgrade()
			.expect("map is alive while borrowed")
	}

	/// Returns the backend-specific state for this map.
	#[inline]
	pub(crate) fn inner(&self) -> &Inner { &self.inner }

	/// Returns the RocksDB state for internal RocksDB-path callers.
	///
	/// # Panics
	///
	/// Panics when the map is not on the RocksDB backend; internal dispatch
	/// must route non-RocksDB maps elsewhere before reaching these paths.
	#[inline]
	pub(crate) fn rocks(&self) -> &RocksMap {
		match &self.inner {
			| Inner::Rocks(rocks) => rocks,
			| Inner::Mem(_) => unreachable!("rocks path reached for a model-backend map"),
		}
	}

	/// Returns the RocksDB state or a capability error.
	#[inline]
	fn rocks_capability(&self) -> Result<&RocksMap> {
		match &self.inner {
			| Inner::Rocks(rocks) => Ok(rocks),
			| Inner::Mem(_) =>
				Err(err!("operation is a RocksDB backend capability, unsupported here")),
		}
	}

	/// Returns the engine that owns this map on the RocksDB backend.
	///
	/// # Panics
	///
	/// Panics when the map is not on the RocksDB backend.
	#[inline]
	pub(crate) fn engine(&self) -> &Arc<Engine> { &self.rocks().engine }

	/// Returns this map's RocksDB column-family handle.
	///
	/// # Panics
	///
	/// Panics when the map is not on the RocksDB backend.
	#[inline]
	pub(crate) fn cf(&self) -> impl AsColumnFamilyRef + '_ { &*self.rocks().cf }
}

impl Debug for Map {
	fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(out, "Map {{name: {0}}}", self.name)
	}
}

impl Display for Map {
	fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result { write!(out, "{0}", self.name) }
}
