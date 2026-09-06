//! Bounded process-local read cache of the remote backend (ADR-0012).
//!
//! A point read costs one bridge round trip per miss, so the backend keeps a
//! `(map, key) -> value | absent` cache with both positive and negative
//! entries. Coherence follows from the single-writer rule: the only writes
//! that can change a cached key are this process's own commits, and the
//! backend invalidates every key a commit touches; it also refuses to fill
//! from a read that overlapped a commit (`Backend::read_stamp`).
//!
//! The policy is a sharded CLOCK (second-chance FIFO) bounded by bytes, not
//! entries: a hit sets a reference bit without relinking anything under the
//! shard lock, and the byte budget (`d1_read_cache_mb`) is the number the
//! operator reasons about. It is hand-written rather than pulled from a
//! crate because nothing in the dependency graph offers byte-bounded
//! eviction, the policy is under a hundred lines, and the workspace's
//! arithmetic lints are easier to satisfy in code we own.

use std::{
	collections::{HashMap, VecDeque},
	hash::{DefaultHasher, Hash, Hasher},
	iter::repeat_with,
	sync::{Arc, Mutex, PoisonError},
};

use crate::{backend::MapId, keyval::KeyBuf};

/// Lock-striping width; a power of two keeps the modulo cheap.
const SHARDS: usize = 32;

/// Approximate per-entry bookkeeping added to the payload size: two `Arc`
/// headers, the hash-table slot, and the ring slot.
const ENTRY_OVERHEAD: usize = 96;

/// The cache: `SHARDS` independent CLOCK rings behind their own locks.
pub(crate) struct Cache {
	shards: Box<[Mutex<Shard>]>,
	/// Byte budget of one shard; zero disables caching entirely.
	budget: usize,
}

/// One lock stripe.
#[derive(Default)]
struct Shard {
	/// Composite key (`map` big-endian, then the key bytes) to its slot.
	map: HashMap<Arc<[u8]>, Slot>,
	/// CLOCK hand order; may hold keys already removed from `map`.
	ring: VecDeque<Arc<[u8]>>,
	/// Bytes accounted to live entries.
	bytes: usize,
}

/// One cached read result.
struct Slot {
	/// `None` caches a confirmed absence.
	val: Option<Box<[u8]>>,
	/// Set on hit; cleared by the CLOCK hand, which evicts on the next pass.
	referenced: bool,
}

impl Cache {
	/// Creates a cache with a total byte budget of `capacity_bytes`.
	pub(crate) fn new(capacity_bytes: usize) -> Self {
		let shards = repeat_with(|| Mutex::new(Shard::default()))
			.take(SHARDS)
			.collect();

		Self {
			shards,
			budget: capacity_bytes.checked_div(SHARDS).unwrap_or(0),
		}
	}

	/// Looks one key up.
	///
	/// The outer `None` is a cache miss; the inner `None` is a cached absence.
	#[expect(
		clippy::option_option,
		reason = "the two levels mean different things: whether the cache knows the key, and \
		          whether the key exists. Flattening them would erase the negative entry, which \
		          is the point of caching a confirmed absence."
	)]
	pub(crate) fn get(&self, map: MapId, key: &[u8]) -> Option<Option<Box<[u8]>>> {
		if self.budget == 0 {
			return None;
		}

		let composite = composite(map, key);
		let mut shard = self.shard(&composite);
		let slot = shard.map.get_mut(composite.as_slice())?;
		slot.referenced = true;

		Some(slot.val.clone())
	}

	/// Records one read result; `None` records an absence.
	///
	/// An entry larger than one shard's budget is never cached.
	pub(crate) fn insert(&self, map: MapId, key: &[u8], val: Option<&[u8]>) {
		let composite = composite(map, key);
		let size = entry_size(composite.len(), val.map_or(0, <[u8]>::len));
		if self.budget == 0 || size > self.budget {
			return;
		}

		let mut shard = self.shard(&composite);
		let composite: Arc<[u8]> = composite.as_slice().into();
		let slot = Slot {
			val: val.map(Into::into),
			referenced: false,
		};
		match shard.map.insert(composite.clone(), slot) {
			| Some(old) => {
				let old_size = entry_size(composite.len(), old.val.map_or(0, |v| v.len()));
				shard.bytes = shard.bytes.saturating_sub(old_size);
			},
			| None => shard.ring.push_back(composite),
		}

		shard.bytes = shard.bytes.saturating_add(size);
		shard.evict(self.budget);
	}

	/// Forgets one key; a later read refetches it.
	pub(crate) fn invalidate(&self, map: MapId, key: &[u8]) {
		if self.budget == 0 {
			return;
		}

		let composite = composite(map, key);
		let mut shard = self.shard(&composite);
		if let Some(old) = shard.map.remove(composite.as_slice()) {
			let size = entry_size(composite.len(), old.val.map_or(0, |v| v.len()));
			shard.bytes = shard.bytes.saturating_sub(size);
		}

		// Invalidation leaves its ring slot behind; sweep once the garbage
		// clearly dominates so the ring stays proportional to the live set.
		let limit = shard
			.map
			.len()
			.saturating_mul(2)
			.saturating_add(1024);
		if shard.ring.len() > limit {
			let Shard { map, ring, .. } = &mut *shard;
			ring.retain(|k| map.contains_key(&**k));
		}
	}

	/// Locks the shard owning `composite`.
	fn shard(&self, composite: &[u8]) -> std::sync::MutexGuard<'_, Shard> {
		let mut hasher = DefaultHasher::new();
		composite.hash(&mut hasher);
		let width = u64::try_from(self.shards.len()).unwrap_or(1);
		let index = usize::try_from(hasher.finish().checked_rem(width).unwrap_or(0)).unwrap_or(0);

		self.shards[index]
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
	}
}

impl Shard {
	/// Advances the CLOCK hand until the shard fits its budget.
	///
	/// Each entry gets one second chance per pass; the loop is bounded by two
	/// passes so a shard can never spin.
	fn evict(&mut self, budget: usize) {
		let mut steps = self.ring.len().saturating_mul(2);
		while self.bytes > budget && steps > 0 {
			steps = steps.saturating_sub(1);
			let Some(key) = self.ring.pop_front() else {
				break;
			};

			match self.map.get_mut(&*key) {
				| Some(slot) if slot.referenced => {
					slot.referenced = false;
					self.ring.push_back(key);
				},
				| Some(_) =>
					if let Some(old) = self.map.remove(&*key) {
						let size = entry_size(key.len(), old.val.map_or(0, |v| v.len()));
						self.bytes = self.bytes.saturating_sub(size);
					},
				// A slot invalidated after it entered the ring.
				| None => {},
			}
		}
	}
}

/// Builds the composite lookup key: the map id big-endian, then the key.
fn composite(map: MapId, key: &[u8]) -> KeyBuf {
	let mut buf = KeyBuf::with_capacity(key.len().saturating_add(2));
	buf.extend_from_slice(&map.0.to_be_bytes());
	buf.extend_from_slice(key);
	buf
}

/// Bytes accounted to one entry.
fn entry_size(key_len: usize, val_len: usize) -> usize {
	key_len
		.saturating_add(val_len)
		.saturating_add(ENTRY_OVERHEAD)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn hit_miss_absence_and_invalidate() {
		let cache = Cache::new(1 << 20);
		let map = MapId(7);
		assert!(cache.get(map, b"k").is_none(), "empty cache misses");

		cache.insert(map, b"k", Some(b"v"));
		assert_eq!(cache.get(map, b"k"), Some(Some(b"v".to_vec().into())));
		assert!(cache.get(MapId(8), b"k").is_none(), "map id is part of the key");

		cache.insert(map, b"gone", None);
		assert_eq!(cache.get(map, b"gone"), Some(None), "absence is cached");

		cache.invalidate(map, b"k");
		assert!(cache.get(map, b"k").is_none(), "invalidated key misses");
	}

	#[test]
	fn eviction_respects_the_byte_budget() {
		let budget = SHARDS.saturating_mul(4096);
		let cache = Cache::new(budget);
		let map = MapId(1);
		for i in 0_u32..4096 {
			cache.insert(map, &i.to_be_bytes(), Some(&[0; 64]));
		}

		let live: usize = cache
			.shards
			.iter()
			.map(|s| {
				s.lock()
					.unwrap_or_else(PoisonError::into_inner)
					.bytes
			})
			.sum();
		assert!(live <= budget, "cache exceeded its budget: {live} > {budget}");
		assert!(live > 0, "cache evicted everything");

		let oversized = vec![0_u8; 8192];
		cache.insert(map, b"big", Some(&oversized));
		assert!(cache.get(map, b"big").is_none(), "entries over a shard budget are skipped");
	}

	#[test]
	fn zero_budget_disables_caching() {
		let cache = Cache::new(0);
		cache.insert(MapId(0), b"k", Some(b"v"));
		assert!(cache.get(MapId(0), b"k").is_none());
	}
}
