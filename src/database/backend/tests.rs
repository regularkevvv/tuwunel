//! Backend contract suite (plan phase 1, deliverable 5).
//!
//! Every case runs the same facade operations against the production RocksDB
//! backend and the in-memory model backend and requires identical observable
//! results: byte ordering, prefix boundaries, reverse scans, multi-map
//! atomic batches, missing-key classification, post-commit visibility, and
//! watcher timing. A backend-specific assumption inside the facade fails
//! here before it can reach a remote backend.

use std::sync::Arc;

use futures::{FutureExt, TryStreamExt};
use tuwunel_core::Result;

use super::{ids, mem};
use crate::{Map, Txn, backend::Sink, tests::new_test_database};

/// The maps exercised by the differential cases; plain presets only.
const MAPS: &[&str] = &["alias_roomid", "pduid_pdu", "global"];

/// One rocks map and its model twin, mutated and queried in lockstep.
struct Pair {
	rocks: Arc<Map>,
	mem: Arc<Map>,
}

struct Rig {
	_db: crate::tests::TestDb,
	rocks_db: Arc<crate::Database>,
	store: Arc<mem::Store>,
	pairs: Vec<Pair>,
}

async fn rig(tag: &str) -> Result<Rig> {
	let db = new_test_database(tag).await?;
	let store = mem::Store::new();
	let pairs = MAPS
		.iter()
		.map(|name| {
			Ok(Pair {
				rocks: db.database.get(name)?.clone(),
				mem: Map::open_mem(&store, name),
			})
		})
		.collect::<Result<Vec<_>>>()?;

	Ok(Rig {
		rocks_db: db.database.clone(),
		_db: db,
		store,
		pairs,
	})
}

/// Adversarial key set: separator bytes, 0x00/0xFF runs, shared prefixes,
/// boundary lengths.
fn edge_keys() -> Vec<Vec<u8>> {
	let mut keys: Vec<Vec<u8>> = vec![
		vec![0x00],
		vec![0x00, 0x00],
		vec![0x01],
		vec![0xFE],
		vec![0xFF],
		vec![0xFF, 0x00],
		vec![0xFF, 0xFF],
		vec![0xFF, 0xFF, 0xFF],
		b"p".to_vec(),
		b"p\x00".to_vec(),
		b"p\xFF".to_vec(),
		b"p\xFFq".to_vec(),
		b"prefix".to_vec(),
		b"prefix\x00".to_vec(),
		b"prefix\xFF".to_vec(),
		b"prefixed".to_vec(),
		b"q".to_vec(),
	];
	keys.push(vec![0xAB; 64]);
	keys.push(vec![0xFF; 64]);
	keys
}

async fn fwd(map: &Arc<Map>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	map.raw_stream()
		.map_ok(|(k, v)| (k.to_vec(), v.to_vec()))
		.try_collect()
		.await
}

async fn rev(map: &Arc<Map>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	map.rev_raw_stream()
		.map_ok(|(k, v)| (k.to_vec(), v.to_vec()))
		.try_collect()
		.await
}

async fn assert_maps_equal(pair: &Pair, ctx: &str) -> Result {
	let rocks_fwd = fwd(&pair.rocks).await?;
	let mem_fwd = fwd(&pair.mem).await?;
	assert_eq!(rocks_fwd, mem_fwd, "{ctx}: forward scans diverge");

	let mut expect_rev = rocks_fwd.clone();
	expect_rev.reverse();
	let rocks_rev = rev(&pair.rocks).await?;
	let mem_rev = rev(&pair.mem).await?;
	assert_eq!(rocks_rev, expect_rev, "{ctx}: rocks reverse scan is not the fwd mirror");
	assert_eq!(mem_rev, expect_rev, "{ctx}: mem reverse scan is not the fwd mirror");

	let mut sorted = rocks_fwd.clone();
	sorted.sort();
	assert_eq!(rocks_fwd, sorted, "{ctx}: forward scan not in lexicographic byte order");

	Ok(())
}

#[tokio::test]
async fn contract_point_ops_and_ordering() -> Result {
	let rig = rig("contract-point").await?;
	let pair = &rig.pairs[0];

	for (i, key) in edge_keys().iter().enumerate() {
		let val = vec![i as u8; i % 7];

		pair.rocks.insert(key, &val).await?;
		pair.mem.insert(key, &val).await?;

		let got = pair.rocks.get(key).await?;
		assert_eq!(&*got, &*val, "rocks readback");
		let got = pair.mem.get(key).await?;
		assert_eq!(&*got, &*val, "mem readback");
	}

	assert_maps_equal(pair, "after inserts").await?;

	// Missing keys are the not-found error on both backends.
	let missing: &[u8] = b"\xFF\xFF\xFF\xFF-missing";
	assert!(pair.rocks.get(&missing).await.unwrap_err().is_not_found());
	assert!(pair.mem.get(&missing).await.unwrap_err().is_not_found());
	assert!(!pair.rocks.contains(&missing).await);
	assert!(!pair.mem.contains(&missing).await);

	// Deleting an absent key succeeds; deleting a present key removes it.
	pair.rocks.remove(&missing).await?;
	pair.mem.remove(&missing).await?;
	let victim = edge_keys().swap_remove(3);
	pair.rocks.remove(&victim).await?;
	pair.mem.remove(&victim).await?;
	assert!(pair.rocks.get(&victim).await.unwrap_err().is_not_found());
	assert!(pair.mem.get(&victim).await.unwrap_err().is_not_found());

	assert_maps_equal(pair, "after deletes").await?;

	// Empty values are legal records distinct from absence.
	let empty_key: &[u8] = b"empty-value";
	pair.rocks.insert(&empty_key, []).await?;
	pair.mem.insert(&empty_key, []).await?;
	assert_eq!(&*pair.rocks.get(&empty_key).await?, b"");
	assert_eq!(&*pair.mem.get(&empty_key).await?, b"");

	Ok(())
}

#[tokio::test]
async fn contract_prefix_and_seek_boundaries() -> Result {
	let rig = rig("contract-prefix").await?;
	let pair = &rig.pairs[1];

	for key in edge_keys() {
		pair.rocks.insert(&key, &key).await?;
		pair.mem.insert(&key, &key).await?;
	}

	for prefix in [
		&b"p"[..],
		b"prefix",
		b"\xFF",
		b"\xFF\xFF",
		b"\x00",
		b"p\xFF",
		b"absent-prefix",
	] {
		let rocks: Vec<Vec<u8>> = pair
			.rocks
			.raw_keys_prefix(&prefix)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;
		let mem: Vec<Vec<u8>> = pair
			.mem
			.raw_keys_prefix(&prefix)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;
		assert_eq!(rocks, mem, "prefix scan diverges for {prefix:?}");
		assert!(
			rocks.iter().all(|k| k.starts_with(prefix)),
			"prefix scan leaked keys for {prefix:?}"
		);

		// Reverse prefix scans agree as well.
		let rocks_rev: Vec<Vec<u8>> = pair
			.rocks
			.rev_raw_keys_prefix(&prefix)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;
		let mem_rev: Vec<Vec<u8>> = pair
			.mem
			.rev_raw_keys_prefix(&prefix)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;
		assert_eq!(rocks_rev, mem_rev, "reverse prefix scan diverges for {prefix:?}");
	}

	// Seek-from boundaries: forward from-inclusive, reverse seek_for_prev.
	for from in [&b"p"[..], b"prefix\x00", b"\xFF", b"\x00", b"zz-absent"] {
		let rocks: Vec<Vec<u8>> = pair
			.rocks
			.raw_keys_from(&from)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;
		let mem: Vec<Vec<u8>> = pair
			.mem
			.raw_keys_from(&from)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;
		assert_eq!(rocks, mem, "keys_from diverges for {from:?}");

		let rocks_rev: Vec<Vec<u8>> = pair
			.rocks
			.rev_raw_keys_from(&from)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;
		let mem_rev: Vec<Vec<u8>> = pair
			.mem
			.rev_raw_keys_from(&from)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;
		assert_eq!(rocks_rev, mem_rev, "rev_keys_from diverges for {from:?}");
	}

	Ok(())
}

#[tokio::test]
async fn contract_multi_map_batch_and_watchers() -> Result {
	let rig = rig("contract-batch").await?;

	// Queue one batch across all three maps on each backend.
	let mut rocks_txn = rig.rocks_db.txn();
	let mut mem_txn = Txn::new_with_sink(Sink::Mem(rig.store.clone()));

	let watch_rocks = rig.pairs[0].rocks.watch_raw_prefix(b"batch");
	let watch_mem = rig.pairs[0].mem.watch_raw_prefix(b"batch");
	futures::pin_mut!(watch_rocks, watch_mem);

	for (i, pair) in rig.pairs.iter().enumerate() {
		let key = format!("batch-key-{i}");
		let val = format!("batch-val-{i}");
		rocks_txn.insert_raw(&pair.rocks, &key, &val);
		mem_txn.insert_raw(&pair.mem, &key, &val);
	}

	// Nothing is visible or notified before execute.
	assert!(
		rig.pairs[0]
			.rocks
			.get(&"batch-key-0")
			.await
			.unwrap_err()
			.is_not_found(),
		"rocks batch visible before commit"
	);
	assert!(
		rig.pairs[0]
			.mem
			.get(&"batch-key-0")
			.await
			.unwrap_err()
			.is_not_found(),
		"mem batch visible before commit"
	);
	assert!(watch_rocks.as_mut().now_or_never().is_none(), "rocks watcher fired early");
	assert!(watch_mem.as_mut().now_or_never().is_none(), "mem watcher fired early");

	rocks_txn.execute().await?;
	mem_txn.execute().await?;

	// Everything is visible after commit; watchers have fired.
	for (i, pair) in rig.pairs.iter().enumerate() {
		let key = format!("batch-key-{i}");
		let val = format!("batch-val-{i}");
		assert_eq!(&*pair.rocks.get(&key.as_bytes()).await?, val.as_bytes());
		assert_eq!(&*pair.mem.get(&key.as_bytes()).await?, val.as_bytes());
	}
	assert!(watch_rocks.now_or_never().is_some(), "rocks watcher missed commit");
	assert!(watch_mem.now_or_never().is_some(), "mem watcher missed commit");

	for (i, pair) in rig.pairs.iter().enumerate() {
		assert_maps_equal(pair, &format!("map {i} after batch")).await?;
	}

	Ok(())
}

#[tokio::test]
async fn contract_del_prefix_parity() -> Result {
	let rig = rig("contract-delprefix").await?;
	let pair = &rig.pairs[2];

	for key in edge_keys() {
		pair.rocks.insert(&key, b"x").await?;
		pair.mem.insert(&key, b"x").await?;
	}

	for map in [&pair.rocks, &pair.mem] {
		let doomed: Vec<Vec<u8>> = map
			.raw_keys_prefix(&&b"prefix"[..])
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;
		for key in doomed {
			map.remove(&key).await?;
		}
	}
	assert_maps_equal(pair, "after prefix delete").await?;

	pair.rocks.clear().await?;
	pair.mem.clear().await?;
	assert_maps_equal(pair, "after clear").await?;
	assert_eq!(pair.rocks.count().await, 0, "clear left rocks entries");
	assert_eq!(pair.mem.count().await, 0, "clear left mem entries");

	Ok(())
}

#[test]
fn map_id_table_is_a_catalog_bijection() {
	use std::collections::BTreeSet;

	let mut seen = BTreeSet::new();
	for (name, id) in ids::MAP_IDS {
		assert!(seen.insert(id.0), "duplicate MapId {} for {name}", id.0);
	}

	// Every catalog descriptor, tombstones included, has exactly one id.
	for i in 0..ids::MAP_IDS.len() {
		let (name, _) = ids::MAP_IDS[i];
		assert_eq!(
			ids::map_id(name).expect("catalog name must have an id").0 as usize,
			i,
			"table order and id diverge for {name}"
		);
	}

	assert_eq!(
		ids::MAP_IDS.len(),
		crate::maps::MAPS.len(),
		"catalog and id table cardinality diverge; append the new map's id"
	);

	for desc in crate::maps::MAPS {
		assert!(
			ids::map_id(desc.name).is_some(),
			"catalog map {} has no assigned MapId; append it to backend::ids",
			desc.name
		);
	}
}
