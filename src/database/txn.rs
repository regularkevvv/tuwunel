//! Atomic database writes as one backend-neutral mutation batch.
//!
//! A transaction queues [`Op`]s for maps owned by one backend and commits
//! them only when [`Txn::execute`] consumes it. Typed operations use the
//! database codec, while raw operations preserve caller-provided bytes. The
//! backend lowers the queued batch to its native atomic form: one RocksDB
//! `WriteBatch`, one model-store critical section, or (phase 2) one remote
//! transactional commit.

use std::{fmt::Debug, iter::once, sync::Arc};

use rocksdb::WriteBatch;
use serde::Serialize;
use tuwunel_core::{Result, implement};

use crate::{
	Engine, Map,
	backend::{Op, Sink, metrics::STATS},
	keyval::{serialize_key, serialize_val},
	util::or_else,
};

/// Atomic write batch spanning one or more maps from one database.
///
/// Every queued map must belong to the captured backend because operations
/// are interpreted within that database. Dropping an unexecuted transaction
/// leaves the database unchanged.
#[must_use = "does nothing until execute()"]
pub struct Txn {
	ops: Vec<(Arc<Map>, Op)>,
	sink: Sink,
}

/// Capacity-estimate header size retained from the RocksDB batch encoding.
const HEADER: usize = 12;

/// Worst-case per-record overhead retained from the RocksDB batch encoding.
const PER_OP: usize = 16;

/// Creates an empty transaction for one database engine.
///
/// Operations can be appended through the typed or raw queueing methods. The
/// transaction remains inert until [`Txn::execute`] consumes it.
#[implement(Txn)]
pub fn new(engine: &Arc<Engine>) -> Self {
	Self {
		ops: Vec::new(),
		sink: Sink::Rocks(engine.clone()),
	}
}

/// Creates an empty transaction addressed to an explicit backend sink.
///
/// The contract suite uses this to drive the model backend through the same
/// facade paths as production RocksDB.
#[implement(Txn)]
pub(crate) fn new_with_sink(sink: Sink) -> Self { Self { ops: Vec::new(), sink } }

/// Creates an empty transaction with reserved batch capacity.
///
/// `capacity_bytes` approximates the payload volume the caller expects to
/// queue. The reservation affects allocation only and does not queue an
/// operation.
#[implement(Txn)]
pub fn with_capacity_bytes(engine: &Arc<Engine>, capacity_bytes: usize) -> Self {
	Self {
		ops: Vec::with_capacity(capacity_bytes.saturating_div(PER_OP.saturating_mul(4)).max(1)),
		sink: Sink::Rocks(engine.clone()),
	}
}

/// Queues raw key and value pairs for one map from a single pass.
///
/// The database codec is not applied, and the batch copies each supplied
/// byte sequence. Empty input produces an empty transaction whose execution
/// is a no-op.
#[implement(Txn)]
pub fn insert<I, K, V>(map: &Map, items: I) -> Self
where
	I: IntoIterator<Item = (K, V)>,
	K: AsRef<[u8]>,
	V: AsRef<[u8]>,
{
	items
		.into_iter()
		.fold(Self::new_with_sink(map.sink()), |mut txn, (key, val)| {
			txn.insert_raw(map, key, val);
			txn
		})
}

/// Queues a raw slice for one map with a precomputed capacity estimate.
///
/// The estimate includes payload lengths and worst-case record overhead
/// before the items are copied into the batch. Empty input produces an empty
/// transaction.
#[implement(Txn)]
pub fn insert_slice<K, V>(map: &Map, items: &[(K, V)]) -> Self
where
	K: AsRef<[u8]>,
	V: AsRef<[u8]>,
{
	let mut txn = Self::new_with_sink(map.sink());
	txn.ops.reserve(items.len());

	for (key, val) in items {
		txn.insert_raw(map, key, val);
	}

	txn
}

/// Queues raw entries across maps from a nonempty single pass.
///
/// The first item selects the database backend, and every subsequent map must
/// belong to that same backend. The database codec is not applied to keys or
/// values.
///
/// # Panics
///
/// Panics when `items` is empty or when any map belongs to a different
/// database backend.
#[implement(Txn)]
pub fn insert_each<'a, I, K, V>(items: I) -> Self
where
	I: IntoIterator<Item = (&'a Map, K, V)>,
	K: AsRef<[u8]>,
	V: AsRef<[u8]>,
{
	let mut items = items.into_iter();
	let (map, key, val) = items
		.next()
		.expect("insert_each: at least one item");

	let mut txn = Self::new_with_sink(map.sink());

	txn.insert_raw(map, key, val);
	txn.extend(items);

	txn
}

/// Queues a nonempty raw slice across maps with a capacity estimate.
///
/// The first item selects the database backend, and every map must belong to
/// that same backend. The database codec is not applied to keys or values.
///
/// # Panics
///
/// Panics when `items` is empty or when any map belongs to a different
/// database backend.
#[implement(Txn)]
pub fn insert_each_slice<K, V>(items: &[(&Map, K, V)]) -> Self
where
	K: AsRef<[u8]>,
	V: AsRef<[u8]>,
{
	let map = items
		.first()
		.expect("insert_each_slice: at least one item")
		.0;

	let mut txn = Self::new_with_sink(map.sink());
	txn.ops.reserve(items.len());

	txn.extend(
		items
			.iter()
			.map(|(map, key, val)| (*map, key, val)),
	);

	txn
}

/// Serializes and queues entries across maps from a nonempty pass.
///
/// The first item selects the database backend, and every map must belong to
/// that same backend. All keys and values are encoded with the database
/// record codec before being copied into the batch.
///
/// # Panics
///
/// Panics when `items` is empty, a map belongs to another database backend,
/// or serialization of a key or value fails.
#[implement(Txn)]
pub fn put_each<'a, I, K, V>(items: I) -> Self
where
	I: IntoIterator<Item = (&'a Map, K, V)>,
	K: Serialize + Debug,
	V: Serialize,
{
	let mut items = items.into_iter();
	let (map, key, val) = items.next().expect("put_each: at least one item");
	let txn = Self::new_with_sink(map.sink());

	once((map, key, val))
		.chain(items)
		.fold(txn, |mut txn, (map, key, val)| {
			txn.put(map, key, val);
			txn
		})
}

/// Serializes and queues one insertion.
///
/// The key and value use the database record codec, and the operation remains
/// pending until [`Txn::execute`]. The map must belong to the transaction's
/// database backend.
///
/// # Panics
///
/// Panics when the map belongs to another database backend or serialization
/// of the key or value fails.
#[implement(Txn)]
pub fn put<K, V>(&mut self, map: &Map, key: K, val: V)
where
	K: Serialize + Debug,
	V: Serialize,
{
	self.assert_map(map);

	let key = serialize_key(key).expect("failed to serialize batch key");
	let val = serialize_val(val).expect("failed to serialize batch val");

	self.push(map, Op::Put { key, val });
}

/// Serializes the key and queues one raw-value insertion.
///
/// The key uses the database record codec, while the value bytes are copied
/// unchanged into the batch. The operation remains pending until
/// [`Txn::execute`], and the map must belong to the transaction's database
/// backend.
///
/// # Panics
///
/// Panics when the map belongs to another database backend or serialization
/// of the key fails.
#[implement(Txn)]
pub fn put_raw<K, V>(&mut self, map: &Map, key: K, val: V)
where
	K: Serialize + Debug,
	V: AsRef<[u8]>,
{
	self.assert_map(map);

	let key = serialize_key(key).expect("failed to serialize batch key");

	self.push(map, Op::Put { key, val: val.as_ref().into() });
}

/// Queues one raw-key insertion after serializing the value.
///
/// The key bytes are copied unchanged into the batch, while the value uses
/// the database record codec. The operation remains pending until
/// [`Txn::execute`], and the map must belong to the transaction's database
/// backend.
///
/// # Panics
///
/// Panics when the map belongs to another database backend or serialization
/// of the value fails.
#[implement(Txn)]
pub fn raw_put<K, V>(&mut self, map: &Map, key: K, val: V)
where
	K: AsRef<[u8]>,
	V: Serialize,
{
	self.assert_map(map);

	let val = serialize_val(val).expect("failed to serialize batch val");

	self.push(map, Op::Put { key: key.as_ref().into(), val });
}

/// Serializes and queues one deletion.
///
/// The key uses the database record codec, and the operation remains pending
/// until [`Txn::execute`]. The map must belong to the transaction's database
/// backend.
///
/// # Panics
///
/// Panics when the map belongs to another database backend or serialization
/// of the key fails.
#[implement(Txn)]
pub fn del<K>(&mut self, map: &Map, key: K)
where
	K: Serialize + Debug,
{
	self.assert_map(map);

	let key = serialize_key(key).expect("failed to serialize batch key");

	self.push(map, Op::Delete { key });
}

/// Queues one deletion for an already serialized key.
///
/// The key bytes are copied into the batch without invoking the database
/// codec. The map must belong to the transaction's database backend.
///
/// # Panics
///
/// Panics when the map belongs to another database backend.
#[implement(Txn)]
pub fn del_raw<K>(&mut self, map: &Map, key: K)
where
	K: AsRef<[u8]>,
{
	self.assert_map(map);
	self.push(map, Op::Delete { key: key.as_ref().into() });
}

/// Queue one unencoded key and value after enforcing map ownership.
///
/// Both byte sequences are copied into the batch without invoking the
/// database codec. The operation remains pending until [`Txn::execute`].
///
/// # Panics
///
/// Panics when the map belongs to another database backend.
#[implement(Txn)]
pub fn insert_raw<K, V>(&mut self, map: &Map, key: K, val: V)
where
	K: AsRef<[u8]>,
	V: AsRef<[u8]>,
{
	self.assert_map(map);
	self.push(map, Op::Put {
		key: key.as_ref().into(),
		val: val.as_ref().into(),
	});
}

/// Commits the batch atomically, flushes unless corked, and notifies
/// matching watchers.
///
/// An empty transaction returns without touching the backend. For a nonempty
/// batch, notifications occur only after the write and any required flush
/// succeed. Commit and flush failures are returned to the caller; the batch
/// is dropped unapplied or, on a flush failure, applied but unnotified only
/// after the backend has already accepted it durably into its write path.
#[implement(Txn)]
#[tracing::instrument(
	level = "trace",
	skip_all,
	fields(
		ops = self.len(),
		bytes = self.size_in_bytes(),
	)
)]
pub async fn execute(self) -> Result {
	if self.is_empty() {
		return Ok(());
	}

	STATS.txn_ops.record(self.len());
	STATS
		.txn_bytes
		.record(self.size_in_bytes());

	match &self.sink {
		| Sink::Rocks(engine) => {
			let mut batch = WriteBatch::with_capacity_bytes(self.size_in_bytes());
			for (map, op) in &self.ops {
				match op {
					| Op::Put { key, val } => batch.put_cf(&map.cf(), key, val),
					| Op::Delete { key } => batch.delete_cf(&map.cf(), key),
				}
			}

			engine
				.db
				.write_opt(&batch, &engine.write_options)
				.or_else(or_else)?;

			if !engine.corked() {
				engine.flush()?;
			}
		},
		| Sink::Mem(store) => {
			store.commit(self.ops.iter().map(|(map, op)| {
				(
					map.id().expect("model-backend maps have catalog ids"),
					match op {
						| Op::Put { key, val } => Op::Put { key: key.clone(), val: val.clone() },
						| Op::Delete { key } => Op::Delete { key: key.clone() },
					},
				)
			}));
		},
	}

	self.notify();

	Ok(())
}

/// Notifies watchers after a successful commit for queued keys that resolve
/// to catalog maps.
///
/// Operations are visited in queue order. Maps outside the startup catalog
/// (foreign column families opened for migrations) are skipped, matching the
/// historical notification behavior.
#[implement(Txn)]
fn notify(&self) {
	for (map, key) in self.keys() {
		map.notify(key);
	}
}

/// Iterate queued put and delete keys in insertion order.
///
/// The iterator borrows keys directly from the queued operations without
/// materializing a container. Operations on maps outside the startup catalog
/// are omitted.
#[implement(Txn)]
pub fn keys(&self) -> impl Iterator<Item = (Arc<Map>, &[u8])> + '_ {
	self.ops
		.iter()
		.filter(|(map, _)| map.id().is_some())
		.map(|(map, op)| (map.clone(), op.key()))
}

/// Returns the number of operations queued in the batch.
///
/// Both insertions and deletions count as one operation. Inspecting the
/// count does not execute the transaction.
#[implement(Txn)]
#[inline]
#[must_use]
pub fn len(&self) -> usize { self.ops.len() }

/// Reports whether the batch contains no queued operations.
///
/// A newly created or cleared transaction is empty. Executing an empty
/// transaction performs no database work.
#[implement(Txn)]
#[inline]
#[must_use]
pub fn is_empty(&self) -> bool { self.ops.is_empty() }

/// Returns the estimated encoded size of the batch in bytes.
///
/// The estimate uses the RocksDB batch encoding's header and worst-case
/// per-record overhead plus payload lengths. Inspecting it does not execute
/// the transaction.
#[implement(Txn)]
#[inline]
#[must_use]
pub fn size_in_bytes(&self) -> usize {
	self.ops
		.iter()
		.fold(HEADER, |bytes, (_, op)| {
			bytes
				.saturating_add(PER_OP)
				.saturating_add(op.size())
		})
}

/// Removes every queued operation from the transaction.
///
/// The captured database backend remains attached, so the transaction can be
/// populated again. Executing it before another operation is queued is a
/// no-op.
#[implement(Txn)]
#[inline]
pub fn clear(&mut self) { self.ops.clear(); }

/// Appends one verified operation to the queue.
#[implement(Txn)]
#[inline]
fn push(&mut self, map: &Map, op: Op) { self.ops.push((map.cloned_arc(), op)); }

/// Verifies that a map belongs to the transaction's database backend.
///
/// Operations are interpreted within one database, so accepting a foreign
/// map could target a same-named map in another backend instance.
///
/// # Panics
///
/// Panics when `map` belongs to a different database backend.
#[implement(Txn)]
#[inline]
fn assert_map(&self, map: &Map) {
	assert!(
		self.sink.same(&map.sink()),
		"transaction map belongs to a different database"
	);
}

/// Extends this transaction with raw insertions across maps.
///
/// Each tuple queues its raw key and value through [`Txn::insert_raw`]. Use
/// [`Txn::put_each`] when the keys and values need serialization. Every map
/// must belong to the transaction's database backend.
///
/// # Panics
///
/// Panics when any map belongs to another database backend.
impl<'a, K, V> Extend<(&'a Map, K, V)> for Txn
where
	K: AsRef<[u8]>,
	V: AsRef<[u8]>,
{
	fn extend<I>(&mut self, items: I)
	where
		I: IntoIterator<Item = (&'a Map, K, V)>,
	{
		for (map, key, val) in items {
			self.insert_raw(map, key, val);
		}
	}
}

/// Extends this transaction with raw-key deletions across maps.
///
/// Each tuple queues its raw key through [`Txn::del_raw`]. Every map must
/// belong to the transaction's database backend.
///
/// # Panics
///
/// Panics when any map belongs to another database backend.
impl<'a, K> Extend<(&'a Map, K)> for Txn
where
	K: AsRef<[u8]>,
{
	fn extend<I>(&mut self, items: I)
	where
		I: IntoIterator<Item = (&'a Map, K)>,
	{
		for (map, key) in items {
			self.del_raw(map, key);
		}
	}
}
