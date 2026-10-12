#![cfg(test)]

use std::{
	env::var,
	fs::remove_dir_all,
	path::PathBuf,
	process::id as process_id,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, PduEvent, Result, http,
	ruma::{
		CanonicalJsonObject, event_id, events::TimelineEventType, room_id, serde::Raw, uint,
		user_id,
	},
};
use tuwunel_service::{Services, rooms::state_compressor::CompressedState};

const SUCCESS_HASH: [u8; 32] = [0xA5; 32];
const FAILURE_HASH: [u8; 32] = [0x5A; 32];

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn state_hash_allocation_persists_an_atomic_pair() -> Result {
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path = DatabasePath(
		PathBuf::from(root).join(format!("tuwunel-state-atomic-allocation-{}", process_id())),
	);

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	// Prevent presence startup from consuming the global count under test.
	args.option.extend([
		format!("database_path={:?}", db_path.0),
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

	drop(runtime);

	result
}

async fn exercise(services: &Services) -> Result {
	let statediff = Arc::new(CompressedState::new());
	let (shortstatehash, already_existed) = services
		.short
		.get_or_create_shortstatehash(&SUCCESS_HASH, |txn, shortstatehash| {
			services.state_compressor.save_state_from_diff(
				txn,
				shortstatehash,
				statediff.clone(),
				statediff.clone(),
				1,
				Vec::new(),
			)
		})
		.await?;

	if already_existed {
		return Err!("new state hash reported as existing");
	}

	if services
		.short
		.get_shortstatehash(&SUCCESS_HASH)
		.await?
		!= shortstatehash
	{
		return Err!("state hash mapping did not resolve to its allocation");
	}

	let state = services
		.state_compressor
		.load_shortstatehash_info(shortstatehash)
		.await?;

	if state.len() != 1 || !state[0].full_state.is_empty() {
		return Err!("empty state diff did not load as one empty layer");
	}

	if !services.globals.pending_count().is_empty() {
		return Err!("successful allocation left a count permit pending");
	}

	let failure = services
		.short
		.get_or_create_shortstatehash(&FAILURE_HASH, |_, _| Err!("deliberate state diff failure"))
		.await;

	if failure.is_ok() {
		return Err!("failure callback did not abort allocation");
	}

	if services
		.short
		.get_shortstatehash(&FAILURE_HASH)
		.await
		.is_ok()
	{
		return Err!("failure callback published the state hash mapping");
	}

	if !services.globals.pending_count().is_empty() {
		return Err!("failure callback left a count permit pending");
	}

	let (existing, already_existed) = services
		.short
		.get_or_create_shortstatehash(&SUCCESS_HASH, |_, _| {
			Err!("existing state hash invoked the state diff callback")
		})
		.await?;

	if !already_existed || existing != shortstatehash {
		return Err!("existing state hash did not retain its allocation");
	}
	refuse_corrupt_state_hashes(services, shortstatehash).await?;
	refuse_corrupt_event_append(services).await?;

	let appended = services
		.state
		.append_to_state(&state_pdu()?)
		.await?;

	let state = services
		.state_compressor
		.load_shortstatehash_info(appended)
		.await?;

	let Some(state) = state.last() else {
		return Err!("appended state had no diff layer");
	};

	if state.shortstatehash != appended || state.full_state.len() != 1 {
		return Err!("appended state event was not loaded");
	}

	Ok(())
}

async fn refuse_corrupt_event_append(services: &Services) -> Result {
	let pdu = state_pdu()?;
	let short = services
		.short
		.get_or_create_shorteventid(&pdu.event_id)
		.await?;
	let mappings = &services.db["eventid_shorteventid"];
	let saved = mappings.get(&pdu.event_id).await?.to_vec();
	let reverse = &services.db["shorteventid_eventid"];
	let saved_reverse = reverse.get(&short.to_be_bytes()).await?.to_vec();
	let corrupt = vec![0_u8];
	mappings.raw_put(&pdu.event_id, &corrupt).await?;
	let before = services.globals.current_count();
	let error = services
		.state
		.append_to_state(&pdu)
		.await
		.expect_err("local append must not overwrite a corrupt event ID mapping");
	assert_eq!(error.status_code(), http::StatusCode::INTERNAL_SERVER_ERROR);
	assert_eq!(services.globals.current_count(), before);
	assert_eq!(mappings.get(&pdu.event_id).await?.as_ref(), corrupt.as_slice());
	assert_eq!(reverse.get(&short.to_be_bytes()).await?.as_ref(), saved_reverse.as_slice());
	services.db["shorteventid_shortstatehash"]
		.get(&short.to_be_bytes())
		.await
		.expect_err("refused local append cannot publish event state");
	assert!(services.globals.pending_count().is_empty());
	mappings.raw_put(&pdu.event_id, &saved).await?;
	Ok(())
}

async fn refused_allocation(services: &Services) -> Result {
	let before = services.globals.current_count();
	let called = AtomicBool::new(false);
	let error = services
		.short
		.get_or_create_shortstatehash(&SUCCESS_HASH, |_, _| {
			called.store(true, Ordering::Relaxed);
			Err!("corrupt allocation invoked its callback")
		})
		.await
		.expect_err("corrupt existing state allocation must refuse");
	assert_eq!(error.status_code(), http::StatusCode::INTERNAL_SERVER_ERROR);
	assert!(!called.load(Ordering::Relaxed), "refusal cannot invoke a write callback");
	assert_eq!(services.globals.current_count(), before, "refusal cannot allocate a counter");
	assert!(services.globals.pending_count().is_empty(), "refusal cannot leak count permits");
	Ok(())
}

async fn refuse_corrupt_state_hashes(services: &Services, hash: u64) -> Result {
	let mappings = &services.db["statehash_shortstatehash"];
	let saved = mappings.get(&SUCCESS_HASH).await?.to_vec();
	let mut trailing = saved.clone();
	trailing.push(0);
	let mut separator = saved.clone();
	separator.push(0xFF);
	for invalid in [Vec::new(), vec![0_u8], trailing, separator, u64::MAX.to_be_bytes().to_vec()]
	{
		mappings.raw_put(&SUCCESS_HASH, &invalid).await?;
		services.clear_cache().await;
		refused_allocation(services).await?;
		assert_eq!(mappings.get(&SUCCESS_HASH).await?.as_ref(), invalid.as_slice());
		mappings.raw_put(&SUCCESS_HASH, &saved).await?;
	}
	let states = &services.db["shortstatehash_statediff"];
	let key = hash.to_be_bytes();
	let saved_state = states.get(&key).await?.to_vec();
	for invalid in [None, Some(vec![b'{'])] {
		if let Some(bytes) = &invalid {
			states.raw_put(&key, bytes).await?;
		} else {
			states.remove(&key).await?;
		}
		services.clear_cache().await;
		refused_allocation(services).await?;
		assert_eq!(mappings.get(&SUCCESS_HASH).await?.as_ref(), saved.as_slice());
		if let Some(bytes) = &invalid {
			assert_eq!(states.get(&key).await?.as_ref(), bytes.as_slice());
		} else {
			states
				.get(&key)
				.await
				.expect_err("missing state cannot be recreated");
		}
		states.raw_put(&key, &saved_state).await?;
	}
	services.clear_cache().await;
	let (restored, existed) = services
		.short
		.get_or_create_shortstatehash(&SUCCESS_HASH, |_, _| Err!("restored state needs no write"))
		.await?;
	assert!(existed);
	assert_eq!(restored, hash);
	Ok(())
}

fn state_pdu() -> Result<PduEvent> {
	Ok(PduEvent {
		kind: TimelineEventType::RoomCreate,
		content: Raw::new(&CanonicalJsonObject::new())?,
		event_id: event_id!("$atomic-state-allocation:localhost").to_owned(),
		room_id: room_id!("!atomic-state-allocation:localhost").to_owned(),
		sender: user_id!("@atomic-state-allocation:localhost").to_owned(),
		state_key: Some("".into()),
		redacts: None,
		prev_events: Default::default(),
		auth_events: Default::default(),
		origin_server_ts: uint!(0),
		depth: uint!(1),
		hashes: Default::default(),
		origin: None,
		unsigned: None,
		rejected: false,
	})
}
