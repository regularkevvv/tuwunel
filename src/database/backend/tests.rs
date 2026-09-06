//! Backend contract suite (plan phase 1, deliverable 5; phase 2 extends it
//! to the remote backend).
//!
//! Every case runs the same facade operations against the production RocksDB
//! backend, the in-memory model backend, and the remote D1 backend driven by
//! the fake bridge of [`super::remote::tests`], and requires identical
//! observable results: byte ordering, prefix boundaries, reverse scans,
//! multi-map atomic batches, missing-key classification, post-commit
//! visibility, and watcher timing. A backend-specific assumption inside the
//! facade fails here.
//!
//! The remote leg runs with a three-row scan page so continuation is the
//! common path in every case rather than a special one.

use std::sync::Arc;

use futures::{FutureExt, TryStreamExt};
use tuwunel_core::{Result, Server};

use super::{
	Sink, ids, mem,
	remote::{self, tests::Fake},
};
use crate::{Map, Txn, tests::new_test_database};

/// The maps exercised by the differential cases; plain presets only.
const MAPS: &[&str] = &["alias_roomid", "pduid_pdu", "global"];

/// Rows per remote scan page while the suite runs.
const SCAN_PAGE: u32 = 3;

/// One map opened on every backend, mutated and queried in lockstep.
struct Trio {
	rocks: Arc<Map>,
	mem: Arc<Map>,
	remote: Arc<Map>,
}

impl Trio {
	/// The three handles in a fixed order; index 0 is the reference.
	fn all(&self) -> [&Arc<Map>; 3] { [&self.rocks, &self.mem, &self.remote] }
}

/// Names used in assertion messages, parallel to [`Trio::all`].
const BACKENDS: [&str; 3] = ["rocks", "mem", "remote"];

struct Rig {
	_db: crate::tests::TestDb,
	rocks_db: Arc<crate::Database>,
	store: Arc<mem::Store>,
	_fake: Fake,
	_server: Arc<Server>,
	backend: Arc<remote::Backend>,
	trios: Vec<Trio>,
}

async fn rig(tag: &str) -> Result<Rig> {
	let db = new_test_database(tag).await?;
	let store = mem::Store::new();

	let fake = Fake::start().await?;
	let server = remote::tests::remote_server(&fake.url, SCAN_PAGE, 1)?;
	let backend = remote::Backend::open(&server).await?;

	let trios = MAPS
		.iter()
		.map(|name| {
			Ok(Trio {
				rocks: db.database.get(name)?.clone(),
				mem: Map::open_mem(&store, name),
				remote: Map::open_remote(&backend, name),
			})
		})
		.collect::<Result<Vec<_>>>()?;

	Ok(Rig {
		rocks_db: db.database.clone(),
		_db: db,
		store,
		_fake: fake,
		_server: server,
		backend,
		trios,
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

async fn assert_maps_equal(trio: &Trio, ctx: &str) -> Result {
	let reference = fwd(&trio.rocks).await?;

	let mut expect_rev = reference.clone();
	expect_rev.reverse();

	for (map, name) in trio.all().into_iter().zip(BACKENDS) {
		assert_eq!(fwd(map).await?, reference, "{ctx}: {name} forward scan diverges");
		assert_eq!(
			rev(map).await?,
			expect_rev,
			"{ctx}: {name} reverse scan is not the fwd mirror"
		);
	}

	let mut sorted = reference.clone();
	sorted.sort();
	assert_eq!(reference, sorted, "{ctx}: forward scan not in lexicographic byte order");

	Ok(())
}

#[tokio::test]
async fn contract_point_ops_and_ordering() -> Result {
	let rig = rig("contract-point").await?;
	let trio = &rig.trios[0];

	for (i, key) in edge_keys().iter().enumerate() {
		let val = vec![u8::try_from(i).expect("edge key index fits u8"); i % 7];

		for (map, name) in trio.all().into_iter().zip(BACKENDS) {
			map.insert(key, &val).await?;
			let got = map.get(key).await?;
			assert_eq!(&*got, &*val, "{name} readback");
		}
	}

	assert_maps_equal(trio, "after inserts").await?;

	// Missing keys are the not-found error on every backend.
	let missing: &[u8] = b"\xFF\xFF\xFF\xFF-missing";
	for (map, name) in trio.all().into_iter().zip(BACKENDS) {
		assert!(
			map.get(&missing)
				.await
				.unwrap_err()
				.is_not_found(),
			"{name} misclassified a missing key"
		);
		assert!(!map.contains(&missing).await, "{name} claims a missing key exists");
	}

	// Deleting an absent key succeeds; deleting a present key removes it.
	let victim = edge_keys().swap_remove(3);
	for (map, name) in trio.all().into_iter().zip(BACKENDS) {
		map.remove(&missing).await?;
		map.remove(&victim).await?;
		assert!(map.get(&victim).await.unwrap_err().is_not_found(), "{name} kept a deleted key");
	}

	assert_maps_equal(trio, "after deletes").await?;

	// Empty values are legal records distinct from absence.
	let empty_key: &[u8] = b"empty-value";
	for (map, name) in trio.all().into_iter().zip(BACKENDS) {
		map.insert(&empty_key, []).await?;
		assert_eq!(&*map.get(&empty_key).await?, b"", "{name} lost an empty value");
	}

	Ok(())
}

#[tokio::test]
async fn contract_prefix_and_seek_boundaries() -> Result {
	let rig = rig("contract-prefix").await?;
	let trio = &rig.trios[1];

	for key in edge_keys() {
		for map in trio.all() {
			map.insert(&key, &key).await?;
		}
	}

	for prefix in
		[&b"p"[..], b"prefix", b"\xFF", b"\xFF\xFF", b"\x00", b"p\xFF", b"absent-prefix"]
	{
		let mut reference: Option<Vec<Vec<u8>>> = None;
		let mut reference_rev: Option<Vec<Vec<u8>>> = None;

		for (map, name) in trio.all().into_iter().zip(BACKENDS) {
			let keys: Vec<Vec<u8>> = map
				.raw_keys_prefix(&prefix)
				.map_ok(<[u8]>::to_vec)
				.try_collect()
				.await?;

			assert!(
				keys.iter().all(|k| k.starts_with(prefix)),
				"{name} prefix scan leaked keys for {prefix:?}"
			);

			match &reference {
				| None => reference = Some(keys),
				| Some(expect) =>
					assert_eq!(&keys, expect, "{name} prefix scan diverges for {prefix:?}"),
			}

			let keys_rev: Vec<Vec<u8>> = map
				.rev_raw_keys_prefix(&prefix)
				.map_ok(<[u8]>::to_vec)
				.try_collect()
				.await?;

			match &reference_rev {
				| None => reference_rev = Some(keys_rev),
				| Some(expect) => assert_eq!(
					&keys_rev, expect,
					"{name} reverse prefix scan diverges for {prefix:?}"
				),
			}
		}
	}

	// Seek-from boundaries: forward from-inclusive, reverse seek_for_prev.
	for from in [&b"p"[..], b"prefix\x00", b"\xFF", b"\x00", b"zz-absent"] {
		let mut reference: Option<Vec<Vec<u8>>> = None;
		let mut reference_rev: Option<Vec<Vec<u8>>> = None;

		for (map, name) in trio.all().into_iter().zip(BACKENDS) {
			let keys: Vec<Vec<u8>> = map
				.raw_keys_from(&from)
				.map_ok(<[u8]>::to_vec)
				.try_collect()
				.await?;

			match &reference {
				| None => reference = Some(keys),
				| Some(expect) =>
					assert_eq!(&keys, expect, "{name} keys_from diverges for {from:?}"),
			}

			let keys_rev: Vec<Vec<u8>> = map
				.rev_raw_keys_from(&from)
				.map_ok(<[u8]>::to_vec)
				.try_collect()
				.await?;

			match &reference_rev {
				| None => reference_rev = Some(keys_rev),
				| Some(expect) =>
					assert_eq!(&keys_rev, expect, "{name} rev_keys_from diverges for {from:?}"),
			}
		}
	}

	Ok(())
}

#[tokio::test]
async fn contract_multi_map_batch_and_watchers() -> Result {
	let rig = rig("contract-batch").await?;

	// Queue one batch across all three maps on each backend.
	let mut txns = [
		rig.rocks_db.txn(),
		Txn::new_with_sink(Sink::Mem(rig.store.clone())),
		Txn::new_with_sink(Sink::Remote(rig.backend.clone())),
	];

	let watch_rocks = rig.trios[0].rocks.watch_raw_prefix(b"batch");
	let watch_mem = rig.trios[0].mem.watch_raw_prefix(b"batch");
	let watch_remote = rig.trios[0].remote.watch_raw_prefix(b"batch");
	futures::pin_mut!(watch_rocks, watch_mem, watch_remote);

	for (i, trio) in rig.trios.iter().enumerate() {
		let key = format!("batch-key-{i}");
		let val = format!("batch-val-{i}");
		for (txn, map) in txns.iter_mut().zip(trio.all()) {
			txn.insert_raw(map, &key, &val);
		}
	}

	// Nothing is visible or notified before execute.
	for (map, name) in rig.trios[0].all().into_iter().zip(BACKENDS) {
		assert!(
			map.get(&"batch-key-0")
				.await
				.unwrap_err()
				.is_not_found(),
			"{name} batch visible before commit"
		);
	}
	assert!(watch_rocks.as_mut().now_or_never().is_none(), "rocks watcher fired early");
	assert!(watch_mem.as_mut().now_or_never().is_none(), "mem watcher fired early");
	assert!(watch_remote.as_mut().now_or_never().is_none(), "remote watcher fired early");

	for txn in txns {
		txn.execute().await?;
	}

	// Everything is visible after commit; watchers have fired.
	for (i, trio) in rig.trios.iter().enumerate() {
		let key = format!("batch-key-{i}");
		let val = format!("batch-val-{i}");
		for (map, name) in trio.all().into_iter().zip(BACKENDS) {
			assert_eq!(
				&*map.get(&key.as_bytes()).await?,
				val.as_bytes(),
				"{name} lost a committed batch entry"
			);
		}
	}
	assert!(watch_rocks.now_or_never().is_some(), "rocks watcher missed commit");
	assert!(watch_mem.now_or_never().is_some(), "mem watcher missed commit");
	assert!(watch_remote.now_or_never().is_some(), "remote watcher missed commit");

	for (i, trio) in rig.trios.iter().enumerate() {
		assert_maps_equal(trio, &format!("map {i} after batch")).await?;
	}

	Ok(())
}

#[tokio::test]
async fn contract_del_prefix_parity() -> Result {
	let rig = rig("contract-delprefix").await?;
	let trio = &rig.trios[2];

	for key in edge_keys() {
		for map in trio.all() {
			map.insert(&key, b"x").await?;
		}
	}

	// Removing while iterating: every backend's scan keeps the view it began
	// with, so the same keys disappear on all three.
	for map in trio.all() {
		let doomed: Vec<Vec<u8>> = map
			.raw_keys_prefix(&&b"prefix"[..])
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;
		for key in doomed {
			map.remove(&key).await?;
		}
	}
	assert_maps_equal(trio, "after prefix delete").await?;

	for (map, name) in trio.all().into_iter().zip(BACKENDS) {
		map.clear().await?;
		assert_eq!(map.count().await, 0, "clear left {name} entries");
	}
	assert_maps_equal(trio, "after clear").await?;

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
			usize::from(
				ids::map_id(name)
					.expect("catalog name must have an id")
					.0
			),
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
