#![cfg(test)]

use std::{
	env::temp_dir, fs::remove_dir_all, path::PathBuf, process::id as process_id, sync::Arc,
};

use futures::{StreamExt, pin_mut};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Error, Result, http, ruma::room_id};
use tuwunel_service::{
	Services,
	rooms::state_compressor::{CompressedState, ShortStateInfo},
};

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn room_state_reconstruction_is_bounded_and_write_admission_preserves_readability() -> Result {
	let db_path = DatabasePath(temp_dir().join(format!("tuwunel-state-bounds-{}", process_id())));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option.extend([
		format!("database_path={:?}", db_path.0),
		"log=\"warn\"".into(),
		"allow_local_presence=false".into(),
		"allow_outgoing_presence=false".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let outcome = exercise(&services).await;
		let shutdown = server.server.shutdown();
		drop(services);
		let run = async_run(&server).await;
		let stop = async_stop(&server).await;
		outcome.and(shutdown).and(run).and(stop)
	});
	drop(server);
	drop(runtime);
	result
}

fn event(key: u64) -> [u8; 16] {
	let mut event = [0_u8; 16];
	event[..8].copy_from_slice(&key.to_be_bytes());
	event[8..].copy_from_slice(&key.to_be_bytes());
	event
}

async fn row(
	services: &Services,
	hash: u64,
	parent: u64,
	added: &[[u8; 16]],
	removed: &[[u8; 16]],
) -> Result {
	let mut encoded = parent.to_be_bytes().to_vec();
	for event in added {
		encoded.extend_from_slice(event);
	}
	if !removed.is_empty() {
		encoded.extend_from_slice(&0_u64.to_be_bytes());
		for event in removed {
			encoded.extend_from_slice(event);
		}
	}
	services.db["shortstatehash_statediff"]
		.insert(&hash.to_be_bytes(), encoded)
		.await
}

async fn exercise(services: &Services) -> Result {
	let cells: Vec<_> = (1..=4096).map(event).collect();
	row(services, 10_000, 0, &cells, &[]).await?;
	let base = services
		.state_compressor
		.load_shortstatehash_info(10_000)
		.await?;
	assert_eq!(base.last().expect("base layer").full_state.len(), 4096);
	// A removal makes room for the new cell, even if the intermediate union is
	// larger.
	row(services, 10_001, 10_000, &[event(4097)], &[event(1)]).await?;
	let replaced = services
		.state_compressor
		.load_shortstatehash_info(10_001)
		.await?;
	let full = &replaced
		.last()
		.expect("replacement layer")
		.full_state;
	assert_eq!(full.len(), 4096);
	assert!(!full.contains(&event(1)));
	assert!(full.contains(&event(4097)));
	assert_eq!(
		services
			.state_compressor
			.load_shortstatehash_info(10_001)
			.await?
			.last()
			.expect("cached replacement")
			.full_state,
		*full
	);
	row(services, 10_002, 10_001, &[event(4098)], &[]).await?;
	assert_eq!(
		refused(
			services
				.state_compressor
				.load_shortstatehash_info(10_002)
				.await,
			"reconstructed-state overflow cannot return a prefix"
		)
		.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	let rows = services
		.state_accessor
		.state_full_ids_strict(10_002);
	pin_mut!(rows);
	assert_eq!(
		rows.next()
			.await
			.expect("error item")
			.expect_err("bounded state refusal")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	assert!(rows.next().await.is_none());
	let oversized = vec![0_u8; 131_089];
	services.db["shortstatehash_statediff"]
		.insert(&10_003_u64.to_be_bytes(), oversized)
		.await?;
	assert_eq!(
		refused(
			services
				.state_compressor
				.load_shortstatehash_info(10_003)
				.await,
			"encoded bytes are checked before decoding"
		)
		.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	row(services, 10_004, 10_004, &[], &[]).await?;
	assert_eq!(
		refused(
			services
				.state_compressor
				.load_shortstatehash_info(10_004)
				.await,
			"a cyclic parent chain must terminate with a corruption error"
		)
		.status_code(),
		http::StatusCode::INTERNAL_SERVER_ERROR
	);
	layer_and_cache_budgets(services, &cells).await?;
	write_admission(services, base).await
}

async fn layer_and_cache_budgets(services: &Services, cells: &[[u8; 16]]) -> Result {
	for index in 0..17_u64 {
		let hash = 20_000_u64.saturating_add(index);
		let parent = if index == 0 { 0 } else { hash.saturating_sub(1) };
		row(services, hash, parent, &[], &[]).await?;
	}
	assert_eq!(
		refused(
			services
				.state_compressor
				.load_shortstatehash_info(20_016)
				.await,
			"cold parent chain cannot exceed its depth budget"
		)
		.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	assert_eq!(
		services
			.state_compressor
			.load_shortstatehash_info(20_015)
			.await?
			.len(),
		16
	);
	assert_eq!(
		refused(
			services
				.state_compressor
				.load_shortstatehash_info(20_016)
				.await,
			"cached parents cannot bypass the chain-depth budget"
		)
		.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	row(services, 30_000, 0, cells, &[]).await?;
	for index in 1..8_u64 {
		let hash = 30_000_u64.saturating_add(index);
		row(services, hash, hash.saturating_sub(1), &[], &[]).await?;
	}
	assert_eq!(
		services
			.state_compressor
			.load_shortstatehash_info(30_006)
			.await?
			.len(),
		7
	);
	assert_eq!(
		refused(
			services
				.state_compressor
				.load_shortstatehash_info(30_007)
				.await,
			"retained cells across layers have a shared budget"
		)
		.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	let oversized: CompressedState = (1..=4097).map(event).collect();
	services
		.state_compressor
		.stateinfo_cache
		.lock()
		.expect("unpoisoned cache")
		.insert(40_000, vec![ShortStateInfo {
			shortstatehash: 40_000,
			full_state: Arc::new(oversized),
			..Default::default()
		}]);
	assert_eq!(
		refused(
			services
				.state_compressor
				.load_shortstatehash_info(40_000)
				.await,
			"cached snapshots must meet the same budget"
		)
		.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	Ok(())
}

async fn write_admission(services: &Services, parents: Vec<ShortStateInfo>) -> Result {
	let oversized: Arc<CompressedState> = Arc::new((1..=4097).map(event).collect());
	assert_eq!(
		refused(
			services
				.state_compressor
				.save_state(room_id!("!bounded:localhost"), oversized.clone())
				.await,
			"a new whole state must be readable before any allocation"
		)
		.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	let refused = services
		.short
		.get_or_create_shortstatehash(&[0xB1; 32], |txn, hash| {
			services.state_compressor.save_state_from_diff(
				txn,
				hash,
				oversized.clone(),
				Arc::default(),
				1,
				Vec::new(),
			)
		})
		.await
		.expect_err("oversized root state must abort atomic allocation");
	assert_eq!(refused.status_code(), http::StatusCode::TOO_MANY_REQUESTS);
	services
		.short
		.get_shortstatehash(&[0xB1; 32])
		.await
		.expect_err("refused state hash must not publish");
	let added = Arc::new(CompressedState::from([event(4097)]));
	let refused = services
		.short
		.get_or_create_shortstatehash(&[0xB2; 32], |txn, hash| {
			services.state_compressor.save_state_from_diff(
				txn,
				hash,
				added.clone(),
				Arc::default(),
				1,
				parents.clone(),
			)
		})
		.await
		.expect_err("small delta cannot push its parent beyond the complete-state budget");
	assert_eq!(refused.status_code(), http::StatusCode::TOO_MANY_REQUESTS);
	services
		.short
		.get_shortstatehash(&[0xB2; 32])
		.await
		.expect_err("refused delta hash must not publish");
	let (hash, existed) = services
		.short
		.get_or_create_shortstatehash(&[0xB3; 32], |txn, hash| {
			services.state_compressor.save_state_from_diff(
				txn,
				hash,
				added.clone(),
				Arc::new(CompressedState::from([event(1)])),
				1,
				parents,
			)
		})
		.await?;
	assert!(!existed);
	let loaded = services
		.state_compressor
		.load_shortstatehash_info(hash)
		.await?;
	let full = &loaded
		.last()
		.expect("admitted complete state")
		.full_state;
	assert_eq!(full.len(), 4096);
	assert!(full.contains(&event(4097)));
	assert!(!full.contains(&event(1)));
	assert!(services.globals.pending_count().is_empty());
	Ok(())
}

#[track_caller]
fn refused<T>(result: Result<T>, message: &str) -> Error {
	match result {
		| Err(error) => error,
		| Ok(_) => panic!("{message}"),
	}
}
