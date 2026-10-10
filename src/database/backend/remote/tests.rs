//! Remote-backend contract cases using the shared loopback bridge oracle.

use std::{collections::BTreeSet, sync::Arc};

use axum::http::StatusCode;
use futures::{FutureExt, StreamExt, TryStreamExt};
use tuwunel_bridge::{self as bridge, Mutation, Request};
use tuwunel_core::{Result, Server, config::Figment};

pub(crate) use super::fixture::{Fake, Faults, TOKEN};
use super::{Backend, DRAIN_BYTES, DRAIN_ROWS, fixture::CommitStage, is_truncated};
use crate::{Map, Txn, backend::Sink};

/// Builds a server whose configuration selects the remote backend at `url`.
///
/// `scan_page` is explicit because the page-continuation cases need a page
/// small enough that continuation is the common path, not the exception.
pub(crate) fn remote_server(url: &str, scan_page: u32, cache_mb: u32) -> Result<Arc<Server>> {
	let raw_config = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_backend", "d1"))
		.merge(("d1_bridge_url", url))
		.merge(("d1_bridge_token", TOKEN))
		.merge(("d1_scan_page", scan_page))
		.merge(("d1_read_cache_mb", cache_mb))
		.merge(("d1_lease_ttl_ms", 15_000))
		.merge(("test", ["fresh", "cleanup"]));

	crate::tests::test_server(&raw_config)
}

/// Starts a fake bridge and opens a remote backend against it.
async fn rig(scan_page: u32, cache_mb: u32) -> Result<(Fake, Arc<Server>, Arc<Backend>)> {
	let fake = Fake::start().await?;
	let server = remote_server(&fake.url, scan_page, cache_mb)?;
	let backend = Backend::open(&server).await?;

	Ok((fake, server, backend))
}

/// The map every case here writes through; `pduid_pdu` is a plain preset.
const MAP: &str = "pduid_pdu";

#[tokio::test]
async fn put_admission_uses_current_writer_and_preserves_exact_commit_boundary() -> Result {
	let (fake, _server, backend) = rig(4, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	let value = vec![0; bridge::MAX_VALUE_BYTES];
	let mut prefix = map.put_batch_budget()?;
	prefix.try_put(1, &value).expect("first put");
	prefix.try_put(1, &value).expect("second put");
	let mut initial = prefix;
	initial
		.try_put(4, &vec![0; 65_536])
		.expect("initial tail");
	let tail_len = 65_536 + bridge::request::MAX_BYTES - initial.encoded_size().expect("size");
	let tail = vec![0; tail_len];
	let before = prefix.encoded_size().expect("prefix size");
	prefix
		.try_put(4, &vec![0; tail_len + 1])
		.expect_err("one byte over");
	assert_eq!(prefix.operations(), 2);
	assert_eq!(prefix.encoded_size().expect("unchanged size"), before);
	assert_eq!(fake.applied(), 0);
	prefix
		.try_put(4, &tail)
		.expect("exact supported limit after refusal");
	assert_eq!(prefix.encoded_size().expect("exact size"), bridge::request::MAX_BYTES);
	let ops = vec![
		Mutation::Put {
			map: map_id(),
			key: vec![0].into(),
			val: value.clone().into(),
		},
		Mutation::Put {
			map: map_id(),
			key: vec![1].into(),
			val: value.into(),
		},
		Mutation::Put {
			map: map_id(),
			key: vec![2; 4].into(),
			val: tail.into(),
		},
	];
	let request = Request::Commit {
		request_id: vec![0; bridge::REQUEST_ID_LEN].into(),
		lease: backend.writable_lease()?,
		digest: bridge::digest(&ops).to_vec().into(),
		ops: ops.clone(),
	};
	assert_eq!(
		bridge::request::encode(&request)
			.expect("actual lease wire")
			.len(),
		bridge::request::MAX_BYTES
	);
	backend.commit(ops).await?;
	assert_eq!(fake.applied(), 1);
	let rows = map
		.raw_keys()
		.try_fold(0_usize, |count, _| async move { Ok(count.saturating_add(1)) })
		.await?;
	assert_eq!(rows, 3);
	backend.close().await;
	assert!(map.put_batch_budget().is_err(), "released lease admitted a producer batch");
	Ok(())
}

/// The `u64` prefix the `del_prefix` case deletes under.
const DOOMED: u64 = 7;

/// Its stable id, the one that reaches the wire.
fn map_id() -> u16 {
	crate::backend::ids::map_id(MAP)
		.expect("catalog map")
		.0
}

/// Adversarial key set: separator bytes, 0x00/0xFF runs, shared prefixes.
fn edge_keys() -> Vec<Vec<u8>> {
	vec![
		vec![0x00],
		vec![0x00, 0x00],
		vec![0x01],
		b"p".to_vec(),
		b"p\x00".to_vec(),
		b"p\xFF".to_vec(),
		b"prefix".to_vec(),
		b"prefix\x00".to_vec(),
		b"prefixed".to_vec(),
		b"q".to_vec(),
		vec![0xFE],
		vec![0xFF],
		vec![0xFF, 0x00],
		vec![0xFF, 0xFF],
	]
}

/// Opens a backend that must fail, returning the refusal's message.
///
/// `Backend` is deliberately not `Debug` (it holds the bridge token), so a
/// failed open cannot be unwrapped with `expect_err`.
async fn refused(server: &Arc<Server>, why: &str) -> String {
	match Backend::open(server).await {
		| Ok(_) => panic!("{why}"),
		| Err(error) => error.to_string(),
	}
}

#[tokio::test]
async fn hello_mismatch_refuses_to_open() -> Result {
	let fake = Fake::start().await?;
	let server = remote_server(&fake.url, 256, 1)?;

	fake.faults(Faults {
		schema_version: Some(bridge::SCHEMA_VERSION.saturating_add(1)),
		..Faults::default()
	});
	let error = refused(&server, "a schema mismatch refuses to serve").await;
	assert!(error.contains("schema"), "{error}");

	fake.faults(Faults {
		protocol: Some(bridge::PROTOCOL_VERSION.saturating_add(2)),
		..Faults::default()
	});
	let error = refused(&server, "a protocol mismatch refuses to serve").await;
	assert!(error.contains("protocol"), "{error}");

	// N+1 is the rollout window: the Worker activates first (ADR-0006).
	fake.faults(Faults {
		protocol: Some(bridge::PROTOCOL_VERSION.saturating_add(1)),
		..Faults::default()
	});
	let backend = Backend::open(&server)
		.await
		.expect("the next protocol version is accepted with a warning");
	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn hello_waits_for_a_bridge_that_does_not_answer_yet() -> Result {
	let fake = Fake::start().await?;
	let server = remote_server(&fake.url, 256, 1)?;

	// More failures than one call retries: a Container can start before its
	// Worker routes the bridge host, and must wait for it rather than exit.
	fake.faults(Faults {
		fail_hellos: super::client::MAX_ATTEMPTS.saturating_mul(2),
		..Faults::default()
	});
	let backend = Backend::open(&server)
		.await
		.expect("the handshake is retried until the bridge answers");
	backend.close().await;

	Ok(())
}

#[test]
fn the_container_environment_names_are_the_ones_the_image_bakes_in() {
	// The container image sets BRIDGE_URL, BRIDGE_TOKEN and
	// TUWUNEL_DATABASE_BACKEND=d1. The first two are read by the bridge
	// client when the matching configuration keys are unset; the third is
	// figment's mapping of the `database_backend` field, exercised by every
	// case in this module through `remote_server`.
	assert_eq!(bridge::ENV_URL, "BRIDGE_URL");
	assert_eq!(bridge::ENV_TOKEN, "BRIDGE_TOKEN");
}

#[tokio::test]
async fn the_d1_backend_opens_no_rocksdb_and_never_creates_database_path() -> Result {
	let fake = Fake::start().await?;

	// The container image bakes in TUWUNEL_DATABASE_PATH, so the path is
	// always configured on d1; it must nonetheless never be created.
	let root = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".into());
	let path = format!("{root}/tuwunel-d1-never-{}", std::process::id());
	std::fs::remove_dir_all(&path).ok();

	let raw_config = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_backend", "d1"))
		.merge(("database_path", &path))
		.merge(("d1_bridge_url", &fake.url))
		.merge(("d1_bridge_token", TOKEN));

	let server = crate::tests::test_server(&raw_config)?;
	let db = crate::Database::open(&server).await?;

	assert_eq!(db.backend(), "d1");
	assert!(
		!std::path::Path::new(&path).exists(),
		"the d1 backend created the RocksDB database directory"
	);

	// The engine is a RocksDB capability; every admin path that needs it
	// reports "unsupported on this backend" instead of panicking.
	let error = db
		.engine()
		.err()
		.expect("the engine is unavailable off RocksDB");
	assert!(
		error
			.to_string()
			.contains("unsupported on this backend"),
		"{error}"
	);

	// The lease is held, the database is writable, and a transaction lands on
	// the remote sink.
	let lease = db
		.lease_status()
		.expect("the remote backend reports its lease");
	assert!(lease.held && !lease.uncertain, "{lease:?}");
	assert!(!db.is_read_only());
	assert!(!db.is_secondary());

	let map = db.get(MAP)?;
	let mut txn = db.txn();
	txn.insert_raw(map, b"through-the-facade", b"value");
	txn.execute().await?;

	assert_eq!(&*map.get(&b"through-the-facade".to_vec()).await?, b"value");
	assert_eq!(fake.rows(map_id()).len(), 1);
	assert!(
		!std::path::Path::new(&path).exists(),
		"a committed transaction created the RocksDB database directory"
	);

	db.close().await;

	Ok(())
}

#[tokio::test]
async fn page_continuation_over_adversarial_keys() -> Result {
	// A page of two rows makes continuation the rule, not the exception.
	let (fake, _server, backend) = rig(2, 1).await?;
	let map = Map::open_remote(&backend, MAP);

	for key in edge_keys() {
		map.insert(&key, &key).await?;
	}

	let mut expect = fake.rows(map_id());
	expect.sort();

	let forward: Vec<(Vec<u8>, Vec<u8>)> = map
		.raw_stream()
		.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
		.try_collect()
		.await?;

	assert_eq!(forward, expect, "paged forward scan is not the whole map in byte order");

	let mut mirror = expect.clone();
	mirror.reverse();
	let reverse: Vec<(Vec<u8>, Vec<u8>)> = map
		.rev_raw_stream()
		.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
		.try_collect()
		.await?;

	assert_eq!(reverse, mirror, "paged reverse scan is not the forward mirror");

	// Seek boundaries survive continuation as well.
	for from in [&b"p"[..], b"prefix\x00", b"\xFF", b"\x00", b"zz-absent"] {
		let seen: Vec<Vec<u8>> = map
			.raw_keys_from(&from)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;

		let want: Vec<Vec<u8>> = expect
			.iter()
			.map(|(key, _)| key.clone())
			.filter(|key| key.as_slice() >= from)
			.collect();

		assert_eq!(seen, want, "keys_from diverges for {from:?}");

		let seen: Vec<Vec<u8>> = map
			.rev_raw_keys_from(&from)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;

		let mut want: Vec<Vec<u8>> = expect
			.iter()
			.map(|(key, _)| key.clone())
			.filter(|key| key.as_slice() <= from)
			.collect();
		want.reverse();

		assert_eq!(seen, want, "rev_keys_from diverges for {from:?}");
	}

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn a_scan_created_after_drain_must_not_silently_observe_the_later_commit() -> Result {
	let (fake, _server, backend) = rig(2, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	map.insert(&b"key".to_vec(), b"before").await?;
	let gate = fake.pause_commit();
	let writer = {
		let map = map.clone();
		tokio::spawn(async move { map.insert(&b"key".to_vec(), b"after").await })
	};
	tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
		.await
		.expect("commit reached post-drain gate");
	// Creation completes while the actual write is known not to have applied.
	// A scan may explicitly refuse admission, but may never return a different
	// snapshot as a successful result.
	let mut scan = Box::pin(map.raw_stream());
	gate.release.notify_one();
	writer.await??;
	match scan.next().await {
		| Some(Ok((key, value))) => {
			assert_eq!(key, b"key");
			assert_eq!(value, b"before", "scan admitted after drain observed a later write");
		},
		| Some(Err(error)) => assert_eq!(error.status_code(), StatusCode::TOO_MANY_REQUESTS),
		| None => panic!("an empty result is not explicit admission refusal"),
	}
	backend.close().await;
	Ok(())
}

#[tokio::test]
async fn cancelling_a_dispatched_commit_must_not_leave_the_writer_writable() -> Result {
	let (fake, _server, backend) = rig(2, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	let gate = fake.pause_commit_on(map_id(), CommitStage::BeforeApply);
	let writer = {
		let map = map.clone();
		tokio::spawn(async move { map.insert(&b"key".to_vec(), b"value").await })
	};
	tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
		.await
		.expect("commit reached post-drain gate");
	writer.abort();
	assert!(
		writer
			.await
			.expect_err("cancelled task")
			.is_cancelled()
	);
	let writable = backend.is_writable();
	// Release the owned fake before asserting, including on a failing baseline.
	gate.release.notify_one();
	assert!(!writable, "cancelled dispatched commit left the writer writable");
	backend.close().await;
	Ok(())
}

#[tokio::test]
async fn known_commit_refusal_reopens_admission_but_storage_ambiguity_stops_the_writer() -> Result
{
	let (fake, _server, backend) = rig(2, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	fake.faults(Faults { reject_commit: true, ..Faults::default() });
	assert!(
		map.insert(&b"key".to_vec(), b"refused")
			.await
			.is_err()
	);
	assert!(backend.is_writable());
	assert_eq!(fake.applied(), 0);
	let rows: Vec<Vec<u8>> = map
		.raw_keys()
		.map_ok(<[u8]>::to_vec)
		.try_collect()
		.await?;
	assert!(rows.is_empty());
	fake.faults(Faults {
		ambiguous_commit: true,
		..Faults::default()
	});
	assert!(
		map.insert(&b"key".to_vec(), b"unknown")
			.await
			.is_err()
	);
	assert_eq!(fake.applied(), 1);
	assert!(!backend.is_writable());
	let mut scan = Box::pin(map.raw_keys());
	scan.next()
		.await
		.expect("explicit closed-admission result")
		.expect_err("closed admission refuses reads");
	assert!(scan.next().await.is_none());
	assert!(
		map.insert(&b"later".to_vec(), b"must-not-apply")
			.await
			.is_err()
	);
	assert_eq!(fake.applied(), 1);
	backend.close().await;
	Ok(())
}

#[tokio::test]
async fn a_storage_rejection_sqlite_decided_keeps_the_writer() -> Result {
	let (fake, _server, backend) = rig(2, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	fake.faults(Faults {
		storage_rejection: true,
		..Faults::default()
	});
	assert!(
		map.insert(&b"key".to_vec(), b"rejected")
			.await
			.is_err()
	);
	// Nothing applied and the outcome is known, so the writer stays: unlike
	// the undecided storage failure above, which stops it.
	assert_eq!(fake.applied(), 0);
	assert!(backend.is_writable());
	fake.faults(Faults::default());
	map.insert(&b"key".to_vec(), b"accepted").await?;
	assert_eq!(fake.applied(), 1);
	backend.close().await;
	Ok(())
}

#[tokio::test]
async fn exhausted_or_malformed_commit_replies_stop_without_double_application() -> Result {
	for wrong_reply in [false, true] {
		let (fake, _server, backend) = rig(2, 0).await?;
		let map = Map::open_remote(&backend, MAP);
		fake.faults(Faults {
			lose_all_commit_replies: !wrong_reply,
			wrong_commit_reply: wrong_reply,
			..Faults::default()
		});
		assert!(
			map.insert(&b"key".to_vec(), b"unknown")
				.await
				.is_err()
		);
		assert_eq!(fake.applied(), 1, "transport retries reuse the same operation identity");
		assert!(!backend.is_writable());
		assert!(
			map.insert(&b"later".to_vec(), b"must-not-apply")
				.await
				.is_err()
		);
		assert_eq!(fake.applied(), 1);
		backend.close().await;
	}
	Ok(())
}

#[tokio::test]
async fn cancellation_before_dispatch_does_not_stop_the_writer() -> Result {
	let (fake, _server, backend) = rig(2, 0).await?;
	let barrier = backend.barrier.write().await;
	let map = Map::open_remote(&backend, MAP);
	let key = b"key".to_vec();
	let mut commit = Box::pin(map.insert(&key, b"not-dispatched"));
	assert!(futures::poll!(&mut commit).is_pending());
	drop(commit);
	assert!(backend.is_writable());
	assert_eq!(fake.applied(), 0);
	drop(barrier);
	map.insert(&b"key".to_vec(), b"works").await?;
	assert_eq!(fake.applied(), 1);
	backend.close().await;
	Ok(())
}

#[tokio::test]
async fn commit_during_an_open_scan_sees_the_pre_commit_snapshot() -> Result {
	// One page holds two rows, so every one of these scans continues across
	// the commits that interleave with it.
	let (fake, _server, backend) = rig(2, 1).await?;
	let map = Map::open_remote(&backend, MAP);

	for key in edge_keys() {
		map.insert(&key, b"x").await?;
	}

	// `DOOMED` is a `u64` prefix, the shape services use; its encoding is
	// eight big-endian bytes, which none of the adversarial keys start with.
	for suffix in [&b"a"[..], b"b", b"c"] {
		let mut key = DOOMED.to_be_bytes().to_vec();
		key.extend_from_slice(suffix);
		map.insert(&key, b"x").await?;
	}

	let before = fake.rows(map_id()).len();

	// `del_prefix` writes while iterating: it must delete exactly the rows
	// its scan saw at creation, and must not deadlock doing so.
	map.del_prefix(&DOOMED).await?;

	let rows = fake.rows(map_id());
	assert_eq!(rows.len(), before.saturating_sub(3), "del_prefix removed the wrong rows");
	assert!(
		!rows
			.iter()
			.any(|(key, _)| key.starts_with(&DOOMED.to_be_bytes())),
		"del_prefix left rows under its prefix"
	);

	// A commit on an unrelated key landing mid-scan is not visible to the
	// scan that was already open (RocksDB iterator semantics).
	let late: Vec<u8> = b"\x00\x00\x00-late".to_vec();
	let mut stream = Box::pin(map.raw_keys());
	let first = stream
		.next()
		.await
		.expect("the scan has rows")?
		.to_vec();

	map.insert(&late, b"late").await?;

	let mut seen = vec![first];
	while let Some(key) = stream.next().await {
		seen.push(key?.to_vec());
	}
	drop(stream);

	assert_eq!(seen.len(), rows.len(), "the open scan lost or gained rows");
	assert!(!seen.contains(&late), "an open scan observed a later write");

	// `for_clear` deletes each row its scan yields, which is every row that
	// existed when the scan began.
	let cleared: Vec<Vec<u8>> = map.for_clear().try_collect().await?;

	assert_eq!(cleared.len(), seen.len().saturating_add(1), "for_clear missed rows");
	assert!(fake.rows(map_id()).is_empty(), "clear left rows behind");

	// `clear` on an empty map is a no-op that still terminates.
	map.clear().await?;
	assert_eq!(map.count().await, 0);

	// Every scan unregistered itself, so a later commit drains nothing.
	assert_eq!(backend.scans().len(), 0, "a dropped scan stayed registered");

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn large_multi_key_reads_chunk_by_bytes_and_preserve_order() -> Result {
	let (fake, _server, backend) = rig(256, 0).await?;
	let keys: Vec<Vec<u8>> = (0_u16..300)
		.map(|n| {
			let mut key = vec![0; bridge::MAX_KEY_BYTES];
			key[..2].copy_from_slice(&n.to_be_bytes());
			key
		})
		.collect();
	fake.fill(
		map_id(),
		keys.iter()
			.map(|key| (key.clone(), key[..2].to_vec())),
	);
	let absent = vec![0xFF; bridge::MAX_KEY_BYTES];
	let mut query: Vec<&[u8]> = keys.iter().rev().map(Vec::as_slice).collect();
	query.push(&keys[0]);
	query.push(&absent);
	let rows = backend
		.get_many(crate::backend::MapId(map_id()), &query)
		.await?;
	assert_eq!(rows.len(), query.len());
	for (key, value) in query.iter().zip(&rows) {
		if *key == absent.as_slice() {
			assert!(value.is_none());
		} else {
			assert_eq!(value.as_deref(), Some(&key[..2]));
		}
	}
	backend.close().await;
	Ok(())
}

#[tokio::test]
async fn oversized_read_replies_reduce_batches_without_losing_order_or_duplicates() -> Result {
	for bounded_get in [false, true] {
		let (fake, _server, backend) = rig(256, 0).await?;
		fake.faults(Faults { bounded_get, ..Faults::default() });
		fake.fill(
			map_id(),
			[b'a', b'b', b'c']
				.into_iter()
				.map(|key| (vec![key], vec![key; bridge::MAX_VALUE_BYTES])),
		);
		let query: &[&[u8]] = &[b"c", b"absent", b"a", b"b", b"c"];
		let rows = backend
			.get_many(crate::backend::MapId(map_id()), query)
			.await?;
		assert_eq!(rows.len(), query.len());
		for (key, value) in query.iter().zip(&rows) {
			if *key == b"absent" {
				assert!(value.is_none());
			} else {
				let value = value.as_ref().expect("present key");
				assert_eq!(value.len(), bridge::MAX_VALUE_BYTES);
				assert!(value.iter().all(|byte| byte == &key[0]));
			}
		}
		backend.close().await;
	}
	Ok(())
}

#[tokio::test]
async fn oversized_legacy_scan_pages_reduce_without_losing_snapshot_or_cursor() -> Result {
	let (fake, _server, backend) = rig(4, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	fake.fill(
		map_id(),
		[b'a', b'b', b'c']
			.into_iter()
			.map(|key| (vec![key], vec![key; bridge::MAX_VALUE_BYTES])),
	);
	let reverse: Vec<Vec<u8>> = map
		.rev_raw_keys()
		.map_ok(<[u8]>::to_vec)
		.try_collect()
		.await?;
	assert_eq!(reverse, vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);
	let mut stream = Box::pin(map.raw_stream());
	let first = stream.next().await.expect("first row")?;
	let mut seen = vec![(first.0.to_vec(), first.1.to_vec())];
	map.insert(&b"b".to_vec(), b"changed").await?;
	map.insert(&b"d".to_vec(), b"new").await?;
	while let Some(row) = stream.next().await {
		let (key, value) = row?;
		seen.push((key.to_vec(), value.to_vec()));
	}
	assert_eq!(seen.len(), 3);
	for ((key, value), expected) in seen.iter().zip([b'a', b'b', b'c']) {
		assert_eq!(key, &[expected]);
		assert_eq!(value.len(), bridge::MAX_VALUE_BYTES);
		assert!(value.iter().all(|byte| *byte == expected));
	}
	backend.close().await;
	Ok(())
}

#[tokio::test]
async fn read_cache_serves_hits_and_a_commit_invalidates_them() -> Result {
	let (fake, _server, backend) = rig(256, 4).await?;
	let map = Map::open_remote(&backend, MAP);

	map.insert(&b"cached".to_vec(), b"first").await?;
	assert_eq!(&*map.get(&b"cached".to_vec()).await?, b"first");
	assert_eq!(&*map.get(&b"cached".to_vec()).await?, b"first", "second read differs");

	// Overwrite through the facade: the commit must invalidate the entry.
	map.insert(&b"cached".to_vec(), b"second").await?;
	assert_eq!(
		&*map.get(&b"cached".to_vec()).await?,
		b"second",
		"the read cache served a value the commit replaced"
	);

	// A cached absence is invalidated by the write that fills it.
	assert!(
		map.get(&b"absent".to_vec())
			.await
			.unwrap_err()
			.is_not_found()
	);
	map.insert(&b"absent".to_vec(), b"now-there")
		.await?;
	assert_eq!(
		&*map.get(&b"absent".to_vec()).await?,
		b"now-there",
		"the read cache served a negative entry the commit filled"
	);

	// A delete invalidates the positive entry.
	map.remove(&b"cached".to_vec()).await?;
	assert!(
		map.get(&b"cached".to_vec())
			.await
			.unwrap_err()
			.is_not_found(),
		"the read cache served a value the commit deleted"
	);

	assert_eq!(fake.rows(map_id()).len(), 1);

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn get_batch_preserves_order_with_misses() -> Result {
	let (_fake, _server, backend) = rig(256, 4).await?;
	let map = Map::open_remote(&backend, MAP);

	for key in [&b"a"[..], b"c", b"e"] {
		map.insert(&key, key).await?;
	}

	let asked: Vec<Vec<u8>> = [&b"e"[..], b"b", b"a", b"b", b"c", b"zzz", b"a"] // duplicates and misses
		.into_iter()
		.map(<[u8]>::to_vec)
		.collect();

	let got: Vec<Option<Vec<u8>>> = map
		.get_batch(futures::stream::iter(asked.clone()))
		.map(|result| result.ok().map(|handle| handle.to_vec()))
		.collect()
		.await;

	let want: Vec<Option<Vec<u8>>> = asked
		.iter()
		.map(|key| matches!(key.as_slice(), b"a" | b"c" | b"e").then(|| key.clone()))
		.collect();

	assert_eq!(got, want, "batched reads are out of order or lost a miss");

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn a_reply_lost_after_the_commit_reports_one_success() -> Result {
	let (fake, _server, backend) = rig(256, 1).await?;
	let map = Map::open_remote(&backend, MAP);

	fake.faults(Faults {
		swallow_commit_replies: 1,
		..Faults::default()
	});

	// The first attempt applies and its reply is dropped; the retry carries
	// the same request id and comes back as a duplicate.
	map.insert(&b"once".to_vec(), b"value").await?;

	assert_eq!(fake.applied(), 1, "the batch applied more than once");
	assert_eq!(&*map.get(&b"once".to_vec()).await?, b"value");
	assert_eq!(fake.rows(map_id()), vec![(b"once".to_vec(), b"value".to_vec())]);

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn a_stale_lease_makes_the_backend_read_only() -> Result {
	let (fake, _server, backend) = rig(256, 1).await?;
	let map = Map::open_remote(&backend, MAP);

	map.insert(&b"before".to_vec(), b"value").await?;
	assert!(backend.is_writable(), "the lease is held before the fault");

	fake.faults(Faults { stale_lease: true, ..Faults::default() });

	let watcher = map.watch_raw_prefix(b"after");
	futures::pin_mut!(watcher);

	let mut txn = Txn::new_with_sink(Sink::Remote(backend.clone()));
	txn.insert_raw(&map, b"after", b"value");
	let error = txn
		.execute()
		.await
		.expect_err("a stale lease refuses the commit");
	assert!(error.to_string().contains("bridge commit"), "{error}");

	assert!(watcher.now_or_never().is_none(), "watchers fired for a refused commit");
	assert!(
		!backend.is_writable(),
		"the backend still accepts writes after losing the lease"
	);
	assert!(!backend.lease_status().held, "the lease still reports itself held");
	assert_eq!(fake.applied(), 1, "the refused commit applied anyway");

	// Every later write fails fast, without reaching the bridge.
	let error = map
		.insert(&b"later".to_vec(), b"value")
		.await
		.expect_err("writes stay refused");
	assert!(error.to_string().contains("writer lease"), "{error}");
	assert_eq!(fake.applied(), 1);

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn a_failed_renewal_makes_the_lease_uncertain_and_commits_fail_fast() -> Result {
	// A one-second lease renews three times a second, so one failing renewal
	// is observed within a fraction of the test's patience.
	let fake = Fake::start().await?;
	let raw_config = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_backend", "d1"))
		.merge(("d1_bridge_url", &fake.url))
		.merge(("d1_bridge_token", TOKEN))
		.merge(("d1_lease_ttl_ms", 1_000))
		.merge(("d1_request_timeout_ms", 200))
		.merge(("test", ["fresh", "cleanup"]));

	let server = crate::tests::test_server(&raw_config)?;
	let backend = Backend::open(&server).await?;
	let map = Map::open_remote(&backend, MAP);

	map.insert(&b"before".to_vec(), b"value").await?;

	fake.faults(Faults { fail_renewals: true, ..Faults::default() });

	// Wait for the renewal loop to notice, bounded so a hang fails the test.
	let uncertain = tokio::time::timeout(std::time::Duration::from_secs(10), async {
		while backend.is_writable() {
			tokio::time::sleep(std::time::Duration::from_millis(25)).await;
		}
	})
	.await;

	assert!(uncertain.is_ok(), "a failing renewal never marked the lease uncertain");
	assert!(backend.lease_status().uncertain || !backend.lease_status().held);

	let error = map
		.insert(&b"during".to_vec(), b"value")
		.await
		.expect_err("commits fail fast while the lease is uncertain");
	assert!(error.to_string().contains("writer lease"), "{error}");
	assert_eq!(fake.applied(), 1, "a commit escaped an uncertain lease");

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn a_second_writer_is_refused_the_lease() -> Result {
	let fake = Fake::start().await?;
	let server = remote_server(&fake.url, 256, 1)?;
	let first = Backend::open(&server).await?;

	// The competing acquisition waits out the incumbent and then gives up:
	// four lease lifetimes is longer than this test may run, so the error is
	// the acquisition patience, not a silent second writer.
	let second =
		tokio::time::timeout(std::time::Duration::from_millis(500), Backend::open(&server)).await;

	assert!(second.is_err(), "a second writer took the lease from a live holder");

	first.close().await;

	// Once released, the successor takes it immediately with a higher epoch.
	let epoch = first.lease_status().epoch;
	let third = Backend::open(&server).await?;
	assert!(third.lease_status().epoch > epoch, "the fencing epoch did not increase");
	third.close().await;

	Ok(())
}

/// A request accepted by the old transport can arrive after a successor has
/// erased canonical data. The server-side epoch fence must reject the entire
/// stale batch, including its delivery rows and immutable attempted body.
#[tokio::test]
async fn successor_erasure_fences_an_already_dispatched_independent_writer() -> Result {
	let (fake, _first_server, first) = rig(4, 1).await?;
	let canonical = Map::open_remote(&first, "pduid_pdu");
	let pending = Map::open_remote(&first, "servernameevent_data");
	let attempts = Map::open_remote(&first, "sendingtransaction_record");
	let key = b"owned-erasure-target";
	let mut original = Txn::new_with_sink(Sink::Remote(first.clone()));
	for map in [&canonical, &pending, &attempts] {
		original.insert_raw(map, key, b"before-erasure");
	}
	original.execute().await?;
	let first_epoch = first.lease_status().epoch;
	let gate = fake.pause_commit_on(map_id(), CommitStage::BeforeApply);
	let mut delayed = Txn::new_with_sink(Sink::Remote(first.clone()));
	for map in [&canonical, &pending, &attempts] {
		delayed.insert_raw(map, key, b"stale-resurrection");
	}
	let delayed = tokio::spawn(delayed.execute());
	tokio::time::timeout(std::time::Duration::from_secs(10), gate.entered.notified())
		.await
		.expect("old writer dispatched its owned batch");
	fake.expire_lease();
	let successor_server = remote_server(&fake.url, 4, 1)?;
	let successor = Backend::open(&successor_server).await?;
	let mut erase = Txn::new_with_sink(Sink::Remote(successor.clone()));
	for name in ["pduid_pdu", "servernameevent_data", "sendingtransaction_record"] {
		let map = Map::open_remote(&successor, name);
		erase.del_raw(&map, key);
		erase.insert_raw(&map, b"unrelated", b"preserved");
	}
	erase.execute().await?;
	let applied = fake.applied();
	// Always release the owned request before checking its result. No accepted
	// task survives cleanup, even if this control exposes a regression.
	gate.release.notify_one();
	let refused = tokio::time::timeout(std::time::Duration::from_secs(10), delayed)
		.await
		.expect("stale batch receives the successor's epoch refusal")
		.expect("owned stale writer joined");
	first.close().await;
	successor.close().await;
	refused.expect_err("an independent old writer cannot resurrect erased data");
	assert!(successor.lease_status().epoch > first_epoch);
	assert_eq!(fake.applied(), applied, "the stale atomic batch never applied");
	for name in ["pduid_pdu", "servernameevent_data", "sendingtransaction_record"] {
		let map = crate::backend::ids::map_id(name)
			.expect("catalog map")
			.0;
		assert_eq!(fake.rows(map), vec![(b"unrelated".to_vec(), b"preserved".to_vec())]);
	}
	Ok(())
}

#[tokio::test]
async fn a_saturated_bound_refuses_unsent_and_the_lease_still_renews() -> Result {
	// The request timeout bounds how long a data call waits for a slot.
	let fake = Fake::start().await?;
	let raw_config = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_backend", "d1"))
		.merge(("d1_bridge_url", &fake.url))
		.merge(("d1_bridge_token", TOKEN))
		.merge(("d1_read_cache_mb", 0))
		.merge(("d1_request_timeout_ms", 200))
		.merge(("test", ["fresh", "cleanup"]));

	let server = crate::tests::test_server(&raw_config)?;
	let backend = Backend::open(&server).await?;
	let map = Map::open_remote(&backend, MAP);
	let slots = backend.client.in_flight();
	let held = slots
		.acquire_many(u32::try_from(slots.available_permits()).expect("slot count"))
		.await
		.expect("every slot");

	// A refused commit sent nothing: its outcome is known, the error is the
	// retryable limit, and the writer stays.
	let error = map
		.insert(&b"key".to_vec(), b"refused")
		.await
		.expect_err("no slot for the commit");
	assert_eq!(error.status_code(), StatusCode::TOO_MANY_REQUESTS, "{error}");
	assert!(backend.is_writable(), "a refused commit stopped the writer");

	// So is a commit cancelled while it queues for a slot, past its drain.
	let maps = BTreeSet::from([map_id()]);
	let key = b"queued".to_vec();
	let mut commit = Box::pin(map.insert(&key, b"cancelled"));
	assert!(futures::poll!(&mut commit).is_pending());
	assert!(backend.scans().begin_write(&maps).is_err(), "the commit is not queued");
	drop(commit);
	assert!(backend.is_writable(), "a cancelled queued commit stopped the writer");
	drop(backend.scans().begin_write(&maps)?);

	// Reads are refused the same way; the lease renews regardless.
	let Err(error) = map.get(&b"key".to_vec()).await else {
		panic!("no slot for the read");
	};
	assert_eq!(error.status_code(), StatusCode::TOO_MANY_REQUESTS, "{error}");
	let error = map
		.contains_checked(&("key",))
		.await
		.expect_err("a refused presence read cannot become a missing key");
	assert_eq!(error.status_code(), StatusCode::TOO_MANY_REQUESTS, "{error}");
	backend
		.lease
		.renew()
		.await
		.expect("renewal takes no slot");
	assert_eq!(fake.applied(), 0);

	drop(held);
	map.insert(&b"key".to_vec(), b"accepted").await?;
	assert_eq!(fake.applied(), 1);
	assert!(map.contains_checked(&("key",)).await?);
	assert!(!map.contains_checked(&("absent",)).await?);
	map.insert(&b"empty".to_vec(), b"").await?;
	assert!(map.contains_checked(&("empty",)).await?, "an empty value still exists");
	backend.close().await;

	Ok(())
}

/// Row key `n` as four big-endian bytes, so keys sort in number order.
fn numbered(n: usize) -> Vec<u8> {
	u32::try_from(n)
		.expect("a test-sized count")
		.to_be_bytes()
		.to_vec()
}

/// Reads a scan to its end or its first refusal: the keys, then the error.
async fn read_until_refused<S, K>(stream: &mut S) -> (Vec<Vec<u8>>, Option<tuwunel_core::Error>)
where
	S: futures::Stream<Item = Result<K>> + Unpin,
	K: AsRef<[u8]>,
{
	let mut keys = Vec::new();
	while let Some(key) = stream.next().await {
		match key {
			| Ok(key) => keys.push(key.as_ref().to_vec()),
			| Err(error) => return (keys, Some(error)),
		}
	}

	(keys, None)
}

#[tokio::test]
async fn a_drain_past_its_row_budget_truncates_the_scan_and_the_commit_reads_no_more() -> Result {
	let page: usize = 256;
	let (fake, _server, backend) = rig(256, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	let total = DRAIN_ROWS.saturating_mul(3);
	fake.fill(map_id(), (0..total).map(|n| (numbered(n), b"x".to_vec())));

	let mut stream = Box::pin(map.raw_keys());
	let first = stream
		.next()
		.await
		.expect("the map has rows")?
		.to_vec();

	// A commit on the map lands while the scan is open. Its drain reads the
	// budget, not the other two thirds of the map.
	let late = b"\xFF-late".to_vec();
	let before = fake.served();
	map.insert(&late, b"late").await?;
	let drained = fake.served().saturating_sub(before);
	assert_eq!(drained, DRAIN_ROWS, "the drain did not stop at its row budget");

	// The reader keeps every pre-commit row the drain read, then is refused
	// in a typed, retryable way; it never ends as if the map were shorter.
	let (rest, error) = read_until_refused(&mut stream).await;
	let error = error.expect("a truncated scan ended as if it were complete");
	assert!(is_truncated(&error), "{error}");
	assert_eq!(error.status_code(), StatusCode::TOO_MANY_REQUESTS);

	let mut seen = vec![first];
	seen.extend(rest);
	assert_eq!(seen.len(), page.saturating_add(drained));
	assert_eq!(seen, (0..seen.len()).map(numbered).collect::<Vec<_>>());
	assert!(stream.next().await.is_none(), "a refused scan yielded more");
	drop(stream);

	// Restarting reads the post-commit map, the late row included.
	let again: Vec<Vec<u8>> = map
		.raw_keys()
		.map_ok(<[u8]>::to_vec)
		.try_collect()
		.await?;
	assert_eq!(again.len(), total.saturating_add(1));
	assert_eq!(again.last(), Some(&late));

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn a_drain_past_its_byte_budget_truncates_the_scan() -> Result {
	let (fake, _server, backend) = rig(256, 0).await?;
	fake.faults(Faults { bounded_scan: true, ..Faults::default() });
	let map = Map::open_remote(&backend, MAP);
	let value = vec![0x5A_u8; 64 * 1024];
	let row_bytes = value.len().saturating_add(4);
	let total = DRAIN_BYTES
		.saturating_div(value.len())
		.saturating_mul(3);
	fake.fill(map_id(), (0..total).map(|n| (numbered(n), value.clone())));

	let mut stream = Box::pin(map.raw_keys());
	stream.next().await.expect("the map has rows")?;
	let first_page = fake.served();

	map.insert(&b"\xFF-late".to_vec(), b"late")
		.await?;
	let drained = fake.served().saturating_sub(first_page);
	let bytes = drained.saturating_mul(row_bytes);
	assert!(drained < DRAIN_ROWS, "the row budget stopped this drain, not the byte budget");
	assert!(bytes >= DRAIN_BYTES, "the drain stopped short of its byte budget");
	assert!(
		bytes <= DRAIN_BYTES.saturating_add(bridge::response::DATA_BYTES),
		"the drain read {bytes} bytes, more than its budget and one page"
	);

	let (rest, error) = read_until_refused(&mut stream).await;
	assert!(error.as_ref().is_some_and(is_truncated), "{error:?}");
	assert_eq!(rest.len().saturating_add(1), first_page.saturating_add(drained));

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn a_drain_stops_at_its_readers_prefix_and_never_truncates_it() -> Result {
	// Four-row pages, so the prefix's rows span several.
	let (fake, _server, backend) = rig(4, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	let keyed = |lead: &[u8], n: usize| [lead, numbered(n).as_slice()].concat();
	let short: Vec<Vec<u8>> = (0..10).map(|n| keyed(b"a", n)).collect();
	fake.fill(
		map_id(),
		short
			.iter()
			.map(|key| (key.clone(), b"x".to_vec())),
	);
	fake.fill(
		map_id(),
		(0..DRAIN_ROWS.saturating_mul(2)).map(|n| (keyed(b"b", n), b"x".to_vec())),
	);

	let mut stream = Box::pin(map.raw_keys_prefix(&b"a"[..]));
	let first = stream.next().await.expect("a row")?.to_vec();

	let before = fake.served();
	map.insert(&b"b-late".to_vec(), b"late").await?;
	let drained = fake.served().saturating_sub(before);
	assert!(
		drained < short.len().saturating_add(4),
		"the drain read {drained} rows, past its reader's prefix"
	);

	let (rest, error) = read_until_refused(&mut stream).await;
	assert!(error.is_none(), "a prefix read was truncated: {error:?}");
	let mut seen = vec![first];
	seen.extend(rest);
	assert_eq!(seen, short);

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn capped_batches_read_no_more_than_their_cap_and_resume_after_their_cursor() -> Result {
	let (fake, _server, backend) = rig(256, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	let total: usize = 1000;
	let cap: usize = 100;
	fake.fill(map_id(), (0..total).map(|n| (numbered(n), b"x".to_vec())));

	let mut after: Option<Vec<u8>> = None;
	let mut visited = Vec::new();
	let mut batches: usize = 0;
	loop {
		let before = fake.served();
		let rows = map.raw_rows_after(after.as_deref(), cap).await?;
		assert!(fake.served().saturating_sub(before) <= cap, "a batch read past its cap");
		assert_eq!(backend.scans().len(), 0, "a batch left its scan open");

		batches = batches.saturating_add(1);
		if batches == 1 {
			// Writes between batches: the one behind the cursor is not
			// visited, the one ahead of it is.
			map.insert(&[numbered(0), vec![0xAA]].concat(), b"behind")
				.await?;
			map.insert(&numbered(total), b"ahead").await?;
		}

		let ended = rows.len() < cap;
		after = rows.last().map(|(key, _)| key.clone());
		visited.extend(rows.into_iter().map(|(key, _)| key));
		if ended {
			break;
		}
	}

	assert_eq!(visited, (0..=total).map(numbered).collect::<Vec<_>>());
	assert_eq!(batches, total.saturating_div(cap).saturating_add(1));

	let tail = map
		.raw_keys_after(Some(&numbered(total.saturating_sub(2))), cap)
		.await?;
	assert_eq!(tail, vec![numbered(total.saturating_sub(1)), numbered(total)]);

	let under = map
		.raw_rows_prefix_after(&numbered(0), None, cap)
		.await?;
	assert_eq!(under.len(), 2, "a prefix batch left its prefix");

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn maximum_width_page_cursor_resumes_without_widening_the_bridge_request() -> Result {
	let (fake, _server, backend) = rig(4, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	let first = vec![b'x'; bridge::MAX_KEY_BYTES];
	let mut next = first.clone();
	*next.last_mut().expect("nonempty cursor") = b'y';
	fake.fill(map_id(), [
		(first.clone(), b"first".to_vec()),
		(next.clone(), b"next".to_vec()),
		(b"z".to_vec(), b"last".to_vec()),
	]);
	assert_eq!(map.raw_keys_after(None, 1).await?, vec![first.clone()]);
	let before = fake.served();
	assert_eq!(map.raw_keys_after(Some(&first), 1).await?, vec![next.clone()]);
	assert!(fake.served().saturating_sub(before) <= 2);
	assert_eq!(map.raw_rows_after(Some(&first), 1).await?, vec![(
		next.clone(),
		b"next".to_vec()
	)]);
	assert_eq!(
		map.raw_keys_prefix_after(b"x", Some(&first), 1)
			.await?,
		vec![next.clone()]
	);
	assert_eq!(
		map.raw_rows_prefix_after(b"x", Some(&first), 1)
			.await?,
		vec![(next.clone(), b"next".to_vec())]
	);
	// An absent cursor must not skip the next row; an empty page reads none.
	map.remove(&first).await?;
	assert_eq!(map.raw_keys_after(Some(&first), 1).await?, vec![next]);
	let before = fake.served();
	assert!(
		map.raw_keys_after(Some(&first), 0)
			.await?
			.is_empty()
	);
	assert_eq!(fake.served(), before);
	assert_eq!(backend.scans().len(), 0, "owned pages close their scans");
	backend.close().await;
	Ok(())
}

#[tokio::test]
async fn del_prefix_and_clear_past_the_drain_budget_remove_every_row() -> Result {
	let (fake, _server, backend) = rig(256, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	let doomed = DRAIN_ROWS.saturating_add(300);
	let under = |n: usize| [DOOMED.to_be_bytes().as_slice(), numbered(n).as_slice()].concat();
	let fill = || {
		fake.fill(map_id(), (0..doomed).map(|n| (under(n), b"x".to_vec())));
		fake.fill(
			map_id(),
			edge_keys()
				.into_iter()
				.map(|key| (key, b"x".to_vec())),
		);
	};

	// Each removal commits while more rows than the drain budget remain under
	// the prefix; removing batch by batch never leaves a scan to drain.
	fill();
	map.del_prefix(&DOOMED).await?;
	let left = fake.rows(map_id());
	assert!(
		left.iter()
			.all(|(key, _)| !key.starts_with(&DOOMED.to_be_bytes())),
		"del_prefix left rows under its prefix"
	);
	assert_eq!(left.len(), edge_keys().len(), "del_prefix took rows outside its prefix");

	fill();
	let cleared: Vec<Vec<u8>> = map.for_clear().try_collect().await?;
	assert_eq!(cleared.len(), doomed.saturating_add(edge_keys().len()));
	assert!(fake.rows(map_id()).is_empty(), "clear left rows behind");
	assert_eq!(backend.scans().len(), 0, "a batch left its scan open");

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn capped_typed_stream_bounds_fetch_and_commit_drain_without_shortening_its_snapshot()
-> Result {
	let (fake, _server, backend) = rig(256, 0).await?;
	let map = Map::open_remote(&backend, MAP);
	let cap = 1025_usize;
	fake.fill(map_id(), (0..5000).map(|index| (numbered(index), b"before".to_vec())));
	let before = fake.served();
	let mut stream = Box::pin(map.stream_capped::<&[u8], &[u8]>(cap));
	let (key, value) = stream.next().await.expect("nonempty map")?;
	assert_eq!(value, b"before");
	let mut seen = vec![key.to_vec()];
	// Updating this map drains the declared cap, not the rest of its inventory.
	map.insert(&numbered(500), b"after").await?;
	assert_eq!(fake.served().saturating_sub(before), cap, "a drain read beyond the typed cap");
	while let Some(row) = stream.next().await {
		let (key, value) = row?;
		assert_eq!(value, b"before", "the capped reader lost its pre-commit snapshot");
		seen.push(key.to_vec());
	}
	assert_eq!(seen, (0..cap).map(numbered).collect::<Vec<_>>());
	drop(stream);
	assert_eq!(backend.scans().len(), 0, "the capped reader left a scan registered");
	let before = fake.served();
	let mut empty = Box::pin(map.stream_capped::<&[u8], &[u8]>(0));
	assert!(empty.next().await.is_none());
	drop(empty);
	assert_eq!(fake.served(), before, "a zero cap fetched rows");
	let from = crate::successor(&numbered(cap.saturating_sub(1)));
	let before = fake.served();
	let next: Vec<Vec<u8>> = map
		.stream_capped_from::<&[u8], &[u8]>(Some(&from), 32)
		.map_ok(|(key, _)| key.to_vec())
		.try_collect()
		.await?;
	assert_eq!(
		next,
		(cap..cap.saturating_add(32))
			.map(numbered)
			.collect::<Vec<_>>()
	);
	assert_eq!(fake.served().saturating_sub(before), 32, "cursor page fetched beyond its cap");
	assert_eq!(backend.scans().len(), 0, "cursor page left its scan registered");
	let prefix = "devices/";
	fake.fill(
		map_id(),
		(0..130).map(|index| (format!("{prefix}{index:03}").into_bytes(), b"device".to_vec())),
	);
	let before = fake.served();
	let devices: Vec<Vec<u8>> = map
		.stream_prefix_capped::<&[u8], &[u8], _>(&(prefix,), 32)
		.map_ok(|(key, _)| key.to_vec())
		.try_collect()
		.await?;
	assert_eq!(
		devices,
		(0..32)
			.map(|index| format!("{prefix}{index:03}").into_bytes())
			.collect::<Vec<_>>()
	);
	assert_eq!(fake.served().saturating_sub(before), 32, "prefix page fetched beyond its cap");
	assert_eq!(backend.scans().len(), 0, "prefix page left its scan registered");
	let before = fake.served();
	let keys: Vec<Vec<u8>> = map
		.keys_prefix_capped::<&[u8], _>(&(prefix,), 32)
		.map_ok(<[u8]>::to_vec)
		.try_collect()
		.await?;
	assert_eq!(keys, devices, "key-only prefix bounds differ from row bounds");
	assert_eq!(
		fake.served().saturating_sub(before),
		32,
		"key-only prefix fetched beyond its cap"
	);
	assert_eq!(backend.scans().len(), 0, "key-only prefix left its scan registered");
	backend.close().await;
	Ok(())
}
