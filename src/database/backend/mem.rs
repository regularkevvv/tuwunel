//! In-memory model backend.
//!
//! A deliberately simple, obviously-correct implementation of the backend
//! contract over `BTreeMap`s. It exists to keep the contract honest: the
//! differential suite in `tests` runs every semantic case against RocksDB and
//! this model and requires identical results, so a RocksDB-only assumption
//! cannot hide inside the facade. It is also the semantic oracle for the
//! frozen operation traces.
//!
//! Not a production backend: nothing is durable, scans clone their snapshot,
//! and one `RwLock` serializes all writes. It is reachable only through
//! test constructors, never through server configuration.

use std::{
	collections::BTreeMap,
	ops::Bound,
	sync::{Arc, PoisonError, RwLock},
};

use super::MapId;

/// Byte-ordered contents of one logical map.
type Tree = BTreeMap<Box<[u8]>, Box<[u8]>>;

/// The model backend: every map's tree behind one lock.
///
/// The single lock gives batches their atomicity and readers a consistent
/// view; contention is irrelevant at test scale.
#[derive(Default)]
pub struct Store {
	trees: RwLock<BTreeMap<u16, Tree>>,
}

impl Store {
	/// Creates an empty model store.
	#[must_use]
	pub fn new() -> Arc<Self> { Arc::new(Self::default()) }

	/// Reads one key, returning owned bytes.
	pub(crate) fn get(&self, map: MapId, key: &[u8]) -> Option<Box<[u8]>> {
		self.read()
			.get(&map.0)
			.and_then(|tree| tree.get(key))
			.cloned()
	}

	/// Applies one atomic batch of mutations across maps.
	///
	/// The write lock spans the whole batch, so a concurrent reader observes
	/// either none or all of it: the model's equivalent of a committed
	/// RocksDB write batch.
	pub(crate) fn commit<I>(&self, ops: I)
	where
		I: IntoIterator<Item = (MapId, super::Op)>,
	{
		let mut trees = self.write();
		for (map, op) in ops {
			let tree = trees.entry(map.0).or_default();
			match op {
				| super::Op::Put { key, val } => {
					tree.insert(key.as_slice().into(), val.as_slice().into());
				},
				| super::Op::Delete { key } => {
					tree.remove(key.as_slice());
				},
			}
		}
	}

	/// Copies the scan range visible at this instant, in traversal order.
	///
	/// `from` matches RocksDB seek semantics: forward scans start at the
	/// first key not less than `from`; reverse scans start at the last key
	/// not greater than `from` (`seek_for_prev`). `None` starts at the
	/// corresponding end of the map.
	pub(crate) fn snapshot(
		&self,
		map: MapId,
		reverse: bool,
		from: Option<&[u8]>,
	) -> Vec<(Box<[u8]>, Box<[u8]>)> {
		let trees = self.read();
		let Some(tree) = trees.get(&map.0) else {
			return Vec::new();
		};

		let pairs: Vec<(Box<[u8]>, Box<[u8]>)> = if reverse {
			let range = match from {
				| Some(from) =>
					tree.range::<[u8], _>((Bound::Unbounded, Bound::Included(from))),
				| None => tree.range::<[u8], _>(..),
			};
			let mut pairs: Vec<_> = range
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect();
			pairs.reverse();
			pairs
		} else {
			let range = match from {
				| Some(from) =>
					tree.range::<[u8], _>((Bound::Included(from), Bound::Unbounded)),
				| None => tree.range::<[u8], _>(..),
			};
			range.map(|(k, v)| (k.clone(), v.clone())).collect()
		};

		pairs
	}

	fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<u16, Tree>> {
		self.trees
			.read()
			.unwrap_or_else(PoisonError::into_inner)
	}

	fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<u16, Tree>> {
		self.trees
			.write()
			.unwrap_or_else(PoisonError::into_inner)
	}
}
