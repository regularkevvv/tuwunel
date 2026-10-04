//! Facade-level backend operation metrics (plan phase 1, deliverable 6).
//!
//! Counts operations, payload sizes, batch sizes, and scan lengths at the
//! backend-neutral seam so the D1 feasibility envelope can be measured from
//! real traffic. Only sizes and counts are recorded — never keys, values, or
//! map contents. Recording uses relaxed atomics under a shared gate; a
//! snapshot briefly takes its exclusive side, so each record is either fully
//! included or fully excluded, even while traffic continues.
//!
//! A JSON snapshot is written to the path in `TUWUNEL_DB_METRICS_FILE` when
//! the database closes, and is available programmatically through
//! [`snapshot`].

use std::sync::{
	RwLock,
	atomic::{AtomicU64, Ordering::Relaxed},
};

static SNAPSHOT_GATE: RwLock<()> = RwLock::new(());

/// Power-of-two size histogram. Bucket zero includes zero and one; the last
/// bucket includes every value at least 2^31 rather than claiming an upper
/// bound.
const BUCKETS: usize = 32;

/// One counter set with a size histogram.
#[derive(Default)]
pub(crate) struct OpStat {
	count: AtomicU64,
	bytes: AtomicU64,
	hist: [AtomicU64; BUCKETS],
}

impl OpStat {
	pub(crate) fn record(&self, size: usize) {
		let Ok(_guard) = SNAPSHOT_GATE.read() else {
			return; // A poisoned snapshot is reported as unavailable, never valid zero.
		};
		self.record_locked(size);
	}

	fn record_locked(&self, size: usize) {
		self.count.fetch_add(1, Relaxed);
		self.bytes
			.fetch_add(u64::try_from(size).unwrap_or(u64::MAX), Relaxed);
		let bucket: usize = usize::BITS
			.saturating_sub(size.leading_zeros())
			.saturating_sub(1)
			.try_into()
			.unwrap_or(usize::MAX);
		self.hist[bucket.min(BUCKETS.saturating_sub(1))].fetch_add(1, Relaxed);
	}

	fn json(&self) -> serde_json::Value {
		let hist: Vec<u64> = self
			.hist
			.iter()
			.map(|b| b.load(Relaxed))
			.collect();
		let top = hist
			.iter()
			.rposition(|&v| v > 0)
			.map_or(0, |i| i.saturating_add(1));
		serde_json::json!({
			"count": self.count.load(Relaxed),
			"bytes": self.bytes.load(Relaxed),
			"log2_hist": &hist[..top],
		})
	}
}

/// Global operation statistics at the backend seam.
#[derive(Default)]
pub(crate) struct Stats {
	/// Point reads (result size; misses record size 0).
	pub(crate) get: OpStat,
	/// Point reads answered from the block-cache tier.
	pub(crate) get_cached: OpStat,
	/// Multi-point read batches (batch length).
	pub(crate) get_batch: OpStat,
	/// Single-key writes (key+value size).
	pub(crate) write: OpStat,
	/// Transaction commits (op count).
	pub(crate) txn_ops: OpStat,
	/// Transaction commits (payload bytes).
	pub(crate) txn_bytes: OpStat,
	/// Scans created (items ultimately yielded, recorded at stream drop).
	pub(crate) scan_items: OpStat,
	/// Watcher registrations (prefix length).
	pub(crate) watch: OpStat,
	/// Remote backend: scan pages fetched (rows in the page).
	pub(crate) remote_page: OpStat,
	/// Remote backend: open scans drained by a commit (rows materialized).
	pub(crate) remote_drain: OpStat,
	/// Remote backend: drained scans truncated at the drain budget (rows
	/// materialized before the budget ran out).
	pub(crate) remote_drain_truncated: OpStat,
	/// Every commit-drain call, including empty scans, failures and
	/// cancellation; histogram and total are elapsed microseconds, not payload
	/// bytes.
	pub(crate) remote_drain_call_us: OpStat,
	/// Subset of calls that found at least one unfinished scan.
	pub(crate) remote_drain_active_us: OpStat,
	/// Successfully completed calls that found no unfinished scan.
	pub(crate) remote_drain_empty_us: OpStat,
	/// Subset of calls that failed or were cancelled before completing.
	pub(crate) remote_drain_failed_us: OpStat,
	/// Remote backend: point reads answered from the process-local read cache
	/// (result size; a cached absence records 0).
	pub(crate) remote_cache_hit: OpStat,
	/// Remote backend: point reads that had to cross the bridge (always 0
	/// bytes; only the count is meaningful).
	pub(crate) remote_cache_miss: OpStat,
}

pub(crate) static STATS: Stats = Stats {
	get: OpStat::new(),
	get_cached: OpStat::new(),
	get_batch: OpStat::new(),
	write: OpStat::new(),
	txn_ops: OpStat::new(),
	txn_bytes: OpStat::new(),
	scan_items: OpStat::new(),
	watch: OpStat::new(),
	remote_page: OpStat::new(),
	remote_drain: OpStat::new(),
	remote_drain_truncated: OpStat::new(),
	remote_drain_call_us: OpStat::new(),
	remote_drain_active_us: OpStat::new(),
	remote_drain_empty_us: OpStat::new(),
	remote_drain_failed_us: OpStat::new(),
	remote_cache_hit: OpStat::new(),
	remote_cache_miss: OpStat::new(),
};

/// Record related drain counters together so a snapshot cannot split the
/// call from its active/failed subsets.
pub(crate) fn record_drain(micros: usize, active: bool, completed: bool) {
	let Ok(_guard) = SNAPSHOT_GATE.read() else {
		return;
	};
	STATS.remote_drain_call_us.record_locked(micros);
	if active {
		STATS.remote_drain_active_us.record_locked(micros);
	} else if completed {
		STATS.remote_drain_empty_us.record_locked(micros);
	}
	if !completed {
		STATS.remote_drain_failed_us.record_locked(micros);
	}
}

impl OpStat {
	const fn new() -> Self {
		#[expect(clippy::declare_interior_mutable_const)]
		const ZERO: AtomicU64 = AtomicU64::new(0);
		Self {
			count: AtomicU64::new(0),
			bytes: AtomicU64::new(0),
			hist: [ZERO; BUCKETS],
		}
	}
}

/// Returns the current backend-operation statistics as JSON.
#[must_use]
pub(crate) fn snapshot() -> serde_json::Value {
	let Ok(_guard) = SNAPSHOT_GATE.write() else {
		return serde_json::json!({"unavailable": "poisoned_metrics"});
	};
	serde_json::json!({
		"get": STATS.get.json(),
		"get_cached": STATS.get_cached.json(),
		"get_batch": STATS.get_batch.json(),
		"write": STATS.write.json(),
		"txn_ops": STATS.txn_ops.json(),
		"txn_bytes": STATS.txn_bytes.json(),
		"scan_items": STATS.scan_items.json(),
		"watch": STATS.watch.json(),
		"remote_page": STATS.remote_page.json(),
		"remote_drain": STATS.remote_drain.json(),
		"remote_drain_truncated": STATS.remote_drain_truncated.json(),
		"remote_drain_call_us": STATS.remote_drain_call_us.json(),
		"remote_drain_active_us": STATS.remote_drain_active_us.json(),
		"remote_drain_empty_us": STATS.remote_drain_empty_us.json(),
		"remote_drain_failed_us": STATS.remote_drain_failed_us.json(),
		"remote_cache_hit": STATS.remote_cache_hit.json(),
		"remote_cache_miss": STATS.remote_cache_miss.json(),
	})
}

/// Writes the snapshot to `TUWUNEL_DB_METRICS_FILE` if set.
///
/// Called from the facade's drop and from the router's shutdown sequence,
/// because a shutdown with dangling references never runs the drop.
/// Failures are logged and swallowed: metrics must never take down a
/// shutdown path.
pub(crate) fn dump_on_close() {
	let Ok(path) = std::env::var("TUWUNEL_DB_METRICS_FILE") else {
		return;
	};

	let json = snapshot();
	if let Err(error) = std::fs::write(&path, json.to_string()) {
		tuwunel_core::error!(%error, path, "failed writing database metrics snapshot");
	}
}

#[cfg(test)]
mod tests {
	use std::{sync::Arc, thread};

	use super::{OpStat, SNAPSHOT_GATE};

	#[test]
	fn concurrent_snapshot_cannot_split_a_record() {
		let stat = Arc::new(OpStat::new());
		let workers: Vec<_> = std::iter::repeat_with(|| {
			let stat = Arc::clone(&stat);
			thread::spawn(move || {
				for _ in 0..10_000 {
					stat.record(8);
				}
			})
		})
		.take(4)
		.collect();
		let mut snapshots = 0_usize;
		while workers.iter().any(|worker| !worker.is_finished()) {
			let _guard = SNAPSHOT_GATE.write().expect("snapshot gate");
			let value = stat.json();
			let count = value["count"].as_u64().expect("count");
			assert_eq!(value["bytes"].as_u64(), Some(count * 8));
			let sum: u64 = value["log2_hist"]
				.as_array()
				.expect("histogram")
				.iter()
				.map(|bucket| bucket.as_u64().expect("bucket"))
				.sum();
			assert_eq!(sum, count);
			snapshots = snapshots.saturating_add(1);
		}
		for worker in workers {
			worker.join().expect("metric recorder");
		}
		assert_eq!(stat.json()["count"], 40_000);
		assert!(snapshots > 0, "concurrent recording was never sampled");
	}
}
