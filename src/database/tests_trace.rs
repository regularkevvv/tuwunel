//! Frozen operation traces for backend-refactor parity (plan phase 1).
//!
//! Replays deterministic pseudo-random operation sequences through the public
//! facade and serializes every observable result. The emitted JSON is frozen
//! before the storage-seam refactor and must match byte-for-byte afterward:
//! identical bytes, ordering, missing-key behavior, and scan contents.
//!
//! Run explicitly (the test is `#[ignore]`d for normal suites):
//!
//! ```text
//! TUWUNEL_TRACE_OUT=/path/out.json  cargo test -p tuwunel-database trace_golden -- --ignored
//! TUWUNEL_TRACE_GOLDEN=/path/out.json cargo test -p tuwunel-database trace_golden -- --ignored
//! ```
//!
//! With `TUWUNEL_TRACE_OUT` the trace is written; with `TUWUNEL_TRACE_GOLDEN`
//! it is compared against the frozen file and any divergence fails the test.
//! Traces avoid `_CACHE`/TTL maps so no compaction policy can evict entries.
//!
//! `TUWUNEL_TRACE_BACKEND=remote` replays the same traces through the remote
//! D1 backend against the fake bridge of `backend::remote::tests`, so the
//! frozen evidence covers the third backend too:
//!
//! ```text
//! TUWUNEL_TRACE_BACKEND=remote TUWUNEL_TRACE_GOLDEN=../../tests/storage-contract/goldens/trace.json \
//!   cargo test -p tuwunel_database trace_golden -- --ignored
//! ```

use std::{env::var, fmt::Write as _, sync::Arc};

use futures::TryStreamExt;

use super::{TestDb, new_test_database};
use crate::{Database, Map, backend::remote::tests::Fake};

/// Environment variable selecting the backend the traces replay through.
const ENV_BACKEND: &str = "TUWUNEL_TRACE_BACKEND";

/// Rows per remote scan page during a replay; large enough that a whole-map
/// scan is a couple of pages, small enough that continuation is exercised.
const REMOTE_SCAN_PAGE: u32 = 64;

/// Mebibytes of remote read cache during a replay.
const REMOTE_CACHE_MB: u32 = 16;

/// Deterministic 64-bit generator (xorshift*), no external dependencies.
struct Rng(u64);

impl Rng {
	fn new(seed: u64) -> Self { Self(seed.max(1)) }

	fn next(&mut self) -> u64 {
		let mut x = self.0;
		x ^= x >> 12;
		x ^= x << 25;
		x ^= x >> 27;
		self.0 = x;
		x.wrapping_mul(0x2545_F491_4F6C_DD1D)
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next()
			.checked_rem(n)
			.expect("trace bounds are never zero")
	}
}

/// Uniform index into a non-empty slice of `len` items; consumes one draw.
fn index_below(rng: &mut Rng, len: usize) -> usize {
	let len = u64::try_from(len).expect("slice length fits u64");
	usize::try_from(rng.below(len)).expect("index fits usize")
}

/// Maps exercised by the traces: plain presets only, no TTL, no FIFO cache.
const TRACE_MAPS: &[&str] = &["alias_roomid", "pduid_pdu", "global"];

const OPS_PER_SEED: usize = 400;
const SEEDS: &[u64] = &[0xDEC0_DE01, 0xDEC0_DE02, 0xDEC0_DE03];

fn hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len().saturating_mul(2));
	for byte in bytes {
		write!(out, "{byte:02x}").expect("hex write");
	}
	out
}

/// Generates a key with adversarial byte patterns: 0x00/0xFF runs, separator
/// bytes, shared prefixes, lengths 1..=64.
fn gen_key(rng: &mut Rng) -> Vec<u8> {
	let len = usize::try_from(rng.below(64))
		.expect("key length fits usize")
		.saturating_add(1);
	let mut key = Vec::with_capacity(len);
	for _ in 0..len {
		key.push(match rng.below(8) {
			| 0 => 0x00,
			| 1 => 0xFF,
			| 2 => 0xFE,
			| 3 => b'p',
			| 4 => b'q',
			| _ => u8::try_from(rng.below(256)).expect("below 256 fits u8"),
		});
	}
	key
}

/// Generates a value: usually short, occasionally kilobytes; empty values
/// are legal records.
fn gen_val(rng: &mut Rng) -> Vec<u8> {
	let len = if rng.below(8) == 0 {
		usize::try_from(rng.below(2049)).expect("value length fits usize")
	} else {
		usize::try_from(rng.below(193)).expect("value length fits usize")
	};
	let mut val = Vec::with_capacity(len);
	for _ in 0..len {
		val.push(u8::try_from(rng.below(256)).expect("below 256 fits u8"));
	}
	val
}

/// A previously written key, or a fresh one three times out of four misses.
fn pick_key(rng: &mut Rng, written: &[Vec<u8>]) -> Vec<u8> {
	if !written.is_empty() && rng.below(4) < 3 {
		written[index_below(rng, written.len())].clone()
	} else {
		gen_key(rng)
	}
}

async fn run_trace(db: &TestDb, seed: u64) -> String {
	let maps: Vec<Arc<Map>> = TRACE_MAPS
		.iter()
		.map(|name| db.database.get(name).expect("trace map").clone())
		.collect();

	let mut rng = Rng::new(seed);
	let mut written: Vec<Vec<Vec<u8>>> = vec![Vec::new(); maps.len()];
	let mut out = String::new();
	write!(out, "{{\"schema\":1,\"seed\":{seed},\"results\":[").expect("write");

	for op_no in 0..OPS_PER_SEED {
		if op_no > 0 {
			out.push(',');
		}

		let mi = index_below(&mut rng, maps.len());
		let map = &maps[mi];

		match rng.below(10) {
			// put
			| 0..=2 => {
				let key = gen_key(&mut rng);
				let val = gen_val(&mut rng);
				map.insert(&key, &val).await.expect("trace put");
				written[mi].push(key.clone());
				write!(out, "{{\"op\":\"put\",\"map\":{mi},\"key\":\"{}\"}}", hex(&key))
					.expect("write");
			},
			// delete
			| 3 => {
				let key = pick_key(&mut rng, &written[mi]);
				map.remove(&key).await.expect("trace del");
				write!(out, "{{\"op\":\"del\",\"map\":{mi},\"key\":\"{}\"}}", hex(&key))
					.expect("write");
			},
			// multi-map transaction
			| 4 => {
				let mut txn = db.database.txn();
				let count = rng.below(4).saturating_add(2);
				let mut keys = String::new();
				for i in 0..count {
					let ti = index_below(&mut rng, maps.len());
					let key = gen_key(&mut rng);
					if rng.below(4) == 0 {
						txn.del_raw(&maps[ti], &key);
					} else {
						let val = gen_val(&mut rng);
						txn.insert_raw(&maps[ti], &key, &val);
						written[ti].push(key.clone());
					}
					if i > 0 {
						keys.push(',');
					}
					write!(keys, "\"{}:{}\"", ti, hex(&key)).expect("write");
				}
				txn.execute().await.expect("trace txn");
				write!(out, "{{\"op\":\"txn\",\"keys\":[{keys}]}}").expect("write");
			},
			// point read
			| 5 | 6 => {
				let key = pick_key(&mut rng, &written[mi]);
				let res = map.get(&key).await;
				match res {
					| Ok(handle) => write!(
						out,
						"{{\"op\":\"get\",\"map\":{mi},\"key\":\"{}\",\"val\":\"{}\"}}",
						hex(&key),
						hex(&handle)
					)
					.expect("write"),
					| Err(e) if e.is_not_found() => write!(
						out,
						"{{\"op\":\"get\",\"map\":{mi},\"key\":\"{}\",\"val\":null}}",
						hex(&key)
					)
					.expect("write"),
					| Err(e) => panic!("trace get error: {e}"),
				}
			},
			// bounded forward scan from a key
			| 7 => {
				let from = pick_key(&mut rng, &written[mi]);
				let items: Vec<(Vec<u8>, Vec<u8>)> = map
					.raw_stream_from(&from)
					.map_ok(|(k, v)| (k.to_vec(), v.to_vec()))
					.try_collect()
					.await
					.expect("trace stream");
				let items: Vec<_> = items.into_iter().take(20).collect();
				write_scan(&mut out, "scan_from", mi, &from, &items);
			},
			// bounded reverse scan from a key
			| 8 => {
				let from = pick_key(&mut rng, &written[mi]);
				let items: Vec<(Vec<u8>, Vec<u8>)> = map
					.rev_raw_stream_from(&from)
					.map_ok(|(k, v)| (k.to_vec(), v.to_vec()))
					.try_collect()
					.await
					.expect("trace stream");
				let items: Vec<_> = items.into_iter().take(20).collect();
				write_scan(&mut out, "rev_scan_from", mi, &from, &items);
			},
			// prefix scan with a short prefix
			| _ => {
				let mut prefix = pick_key(&mut rng, &written[mi]);
				prefix.truncate(
					usize::try_from(rng.below(4))
						.expect("prefix length fits usize")
						.saturating_add(1),
				);
				let items: Vec<(Vec<u8>, Vec<u8>)> = map
					.raw_stream_prefix(&prefix)
					.map_ok(|(k, v)| (k.to_vec(), v.to_vec()))
					.try_collect()
					.await
					.expect("trace stream");
				let items: Vec<_> = items.into_iter().take(40).collect();
				write_scan(&mut out, "scan_prefix", mi, &prefix, &items);
			},
		}
	}

	out.push_str("]}");
	out
}

fn write_scan(out: &mut String, op: &str, mi: usize, bound: &[u8], items: &[(Vec<u8>, Vec<u8>)]) {
	write!(out, "{{\"op\":\"{op}\",\"map\":{mi},\"bound\":\"{}\",\"items\":[", hex(bound))
		.expect("write");
	for (i, (k, v)) in items.iter().enumerate() {
		if i > 0 {
			out.push(',');
		}
		write!(out, "[\"{}\",\"{}\"]", hex(k), hex(v)).expect("write");
	}
	out.push_str("]}");
}

/// Opens the database the traces replay through.
///
/// The returned fake bridge must outlive the database on the remote backend;
/// it is the Worker for that replay.
async fn trace_database() -> tuwunel_core::Result<(TestDb, Option<Fake>)> {
	if var(ENV_BACKEND).as_deref() != Ok("remote") {
		return Ok((new_test_database("trace-golden").await?, None));
	}

	let fake = Fake::start().await?;
	let server = crate::backend::remote::tests::remote_server(
		&fake.url,
		REMOTE_SCAN_PAGE,
		REMOTE_CACHE_MB,
	)?;
	let database = Database::open(&server).await?;

	Ok((TestDb { database, _server: server }, Some(fake)))
}

#[tokio::test]
#[ignore = "explicit parity evidence; run via scripts/traces.sh"]
async fn trace_golden() -> tuwunel_core::Result {
	let (db, _fake) = trace_database().await?;

	let mut all = String::from("[");
	for (i, &seed) in SEEDS.iter().enumerate() {
		if i > 0 {
			all.push(',');
		}
		let trace = run_trace(&db, seed).await;
		all.push_str(&trace);
	}
	all.push(']');

	if let Ok(path) = var("TUWUNEL_TRACE_OUT") {
		std::fs::write(&path, &all).expect("write trace output");
		return Ok(());
	}

	let golden_path = var("TUWUNEL_TRACE_GOLDEN")
		.expect("set TUWUNEL_TRACE_OUT to record or TUWUNEL_TRACE_GOLDEN to verify");
	let golden = std::fs::read_to_string(&golden_path).expect("read golden trace");

	assert!(
		golden == all,
		"operation trace diverged from frozen golden ({} vs {} bytes)",
		golden.len(),
		all.len(),
	);

	Ok(())
}
