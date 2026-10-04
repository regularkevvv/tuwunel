#![cfg(test)]

use std::{env::var, fs::remove_dir_all, path::PathBuf, process::id as process_id};

use futures::{StreamExt, TryStreamExt, pin_mut};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, http,
	ruma::{OwnedEventId, RoomId, event_id, events::StateEventType, room_id},
};
use tuwunel_service::Services;

const OCCURRENCES: usize = 8;

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn batch_duplicates_share_one_shorteventid() -> Result {
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path = DatabasePath(
		PathBuf::from(root).join(format!("tuwunel-short-id-allocation-{}", process_id())),
	);

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.push(format!("database_path={:?}", db_path.0));
	args.option
		.extend(["allow_local_presence=false".into(), "allow_outgoing_presence=false".into()]);

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
	let event_id = event_id!("$short-id-allocation-batch:localhost");
	// a repeated event misses the batched lookup on every occurrence
	let batch = [event_id; OCCURRENCES];

	let shorts = services
		.short
		.multi_get_or_create_shorteventid(batch.iter().copied());

	pin_mut!(shorts);

	let Some(first) = shorts.next().await else {
		return Err!("batch yielded no short ids");
	};
	let first = first?;

	while let Some(short) = shorts.try_next().await? {
		if short != first {
			return Err!("one event id took more than one short id within a batch");
		}
	}

	let resolved: OwnedEventId = services
		.short
		.get_eventid_from_short(first)
		.await?;

	if resolved != event_id {
		return Err!("short id did not resolve back to its event id");
	}
	corrupt_batch_mappings(services, event_id).await?;
	batch_limits(services, event_id).await?;
	lazy_atomic_allocation(services).await?;
	state_key_allocation(services).await?;
	state_key_limits(services).await?;
	room_allocation(services).await?;
	room_reverse_limits(services).await?;

	Ok(())
}

async fn refused_state_key(services: &Services, key: &str) -> Result {
	let before = services.globals.current_count();
	let error = services
		.short
		.get_or_create_shortstatekey(&StateEventType::RoomTopic, key)
		.await
		.expect_err("corrupt state key allocation must refuse");
	assert_eq!(
		error.status_code(),
		http::StatusCode::INTERNAL_SERVER_ERROR,
		"stored corruption must be a server error"
	);
	assert_eq!(services.globals.current_count(), before, "refusal cannot allocate a new ID");
	assert!(services.globals.pending_count().is_empty(), "refusal cannot leak a permit");
	Ok(())
}

async fn state_key_allocation(services: &Services) -> Result {
	let kind = StateEventType::RoomTopic;
	let key = "short-state-allocation";
	let short = services
		.short
		.get_or_create_shortstatekey(&kind, key)
		.await?;
	let forward = &services.db["statekey_shortstatekey"];
	let reverse = &services.db["shortstatekey_statekey"];
	let forward_key = tuwunel_database::serialize_key((&kind, key))?;
	let reverse_key = short.to_be_bytes();
	let saved = forward
		.get(forward_key.as_slice())
		.await?
		.to_vec();
	let saved_reverse = reverse.get(&reverse_key).await?.to_vec();
	let mut trailing = saved.clone();
	trailing.push(0);
	let mut separator = saved.clone();
	separator.push(0xFF);
	for invalid in [Vec::new(), vec![0_u8], trailing, separator] {
		forward
			.raw_put(forward_key.as_slice(), invalid.as_slice())
			.await?;
		refused_state_key(services, key).await?;
		assert_eq!(
			forward
				.get(forward_key.as_slice())
				.await?
				.as_ref(),
			invalid.as_slice(),
			"refusal must preserve the corrupt forward record"
		);
		assert_eq!(
			reverse.get(&reverse_key).await?.as_ref(),
			saved_reverse.as_slice(),
			"refusal must preserve the reverse record"
		);
		forward
			.raw_put(forward_key.as_slice(), saved.as_slice())
			.await?;
	}
	let foreign = tuwunel_database::serialize_val((&kind, "foreign-state-key"))?.to_vec();
	let mut trailing = saved_reverse.clone();
	trailing.push(0xFF);
	for invalid in [None, Some(Vec::new()), Some(foreign), Some(trailing)] {
		if let Some(bytes) = &invalid {
			reverse
				.raw_put(&reverse_key, bytes.as_slice())
				.await?;
		} else {
			reverse.remove(&reverse_key).await?;
		}
		refused_state_key(services, key).await?;
		assert_eq!(
			forward
				.get(forward_key.as_slice())
				.await?
				.as_ref(),
			saved.as_slice(),
			"corrupt reverse cannot replace the forward record"
		);
		if let Some(bytes) = &invalid {
			assert_eq!(
				reverse.get(&reverse_key).await?.as_ref(),
				bytes.as_slice(),
				"refusal cannot repair a corrupt reverse record"
			);
		} else {
			reverse
				.get(&reverse_key)
				.await
				.expect_err("missing reverse must remain missing");
		}
		reverse
			.raw_put(&reverse_key, saved_reverse.as_slice())
			.await?;
	}
	assert_eq!(
		services
			.short
			.get_or_create_shortstatekey(&kind, key)
			.await?,
		short,
		"restoration must reuse the existing identity"
	);
	let before = services.globals.current_count();
	let (left, right) = tokio::join!(
		services
			.short
			.get_or_create_shortstatekey(&kind, "state-concurrent"),
		services
			.short
			.get_or_create_shortstatekey(&kind, "state-concurrent")
	);
	assert_eq!(left?, right?, "concurrent state keys must share one identity");
	assert_eq!(
		services.globals.current_count(),
		before.saturating_add(1),
		"one identity must consume one sequence number"
	);
	let rejected = "state-refused-commit";
	tuwunel_database::refusal::refuse_next("statekey_shortstatekey");
	services
		.short
		.get_or_create_shortstatekey(&kind, rejected)
		.await
		.expect_err("refused state key pair commit must propagate");
	assert_eq!(
		tuwunel_database::refusal::pending(),
		0,
		"the actual commit must consume refusal"
	);
	let rejected_key = tuwunel_database::serialize_key((&kind, rejected))?;
	forward
		.get(rejected_key.as_slice())
		.await
		.expect_err("refusal cannot publish the forward pair");
	reverse
		.get(&services.globals.current_count().to_be_bytes())
		.await
		.expect_err("refusal cannot publish the reverse pair");
	assert!(services.globals.pending_count().is_empty(), "refusal must retire its permit");
	let short = services
		.short
		.get_or_create_shortstatekey(&kind, rejected)
		.await?;
	let decoded = services
		.short
		.get_statekey_from_short(short)
		.await?;
	assert_eq!(decoded.0, kind, "retry must publish the original state type");
	assert_eq!(decoded.1.as_str(), rejected, "retry must publish the original state key");
	Ok(())
}

async fn state_key_limits(services: &Services) -> Result {
	let kind = StateEventType::RoomTopic;
	let exact = "x".repeat(512 * 1024 - kind.to_cow_str().len() - 1);
	let short = services
		.short
		.get_or_create_shortstatekey(&kind, &exact)
		.await?;
	assert_eq!(
		services
			.short
			.get_or_create_shortstatekey(&kind, &exact)
			.await?,
		short,
		"exact encoded key budget must preserve a complete mapping"
	);
	let decoded = services
		.short
		.get_statekey_from_short(short)
		.await?;
	assert_eq!(decoded.1.as_str(), exact, "exact-budget reverse mapping must stay complete");
	let overflow = format!("{exact}x");
	let before = services.globals.current_count();
	let error = services
		.short
		.get_or_create_shortstatekey(&kind, &overflow)
		.await
		.expect_err("one encoded byte over the state-key budget must refuse");
	assert_eq!(
		error.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS,
		"oversize state keys must produce the typed budget error"
	);
	assert_eq!(
		services.globals.current_count(),
		before,
		"state-key overflow cannot allocate an identity"
	);
	let key = tuwunel_database::serialize_key((&kind, &overflow))?;
	services.db["statekey_shortstatekey"]
		.get(key.as_slice())
		.await
		.expect_err("overflow cannot publish a forward mapping");
	assert!(
		services.globals.pending_count().is_empty(),
		"overflow cannot leak count permits"
	);
	Ok(())
}

async fn room_allocation(services: &Services) -> Result {
	let room = room_id!("!short-room-allocation:localhost");
	let short = services
		.short
		.get_or_create_shortroomid(room)
		.await?;
	let mappings = &services.db["roomid_shortroomid"];
	let saved = mappings.get(room).await?.to_vec();
	let mut trailing = saved.clone();
	trailing.push(0);
	let mut separator = saved.clone();
	separator.push(0xFF);
	for invalid in [Vec::new(), vec![0_u8], trailing, separator] {
		mappings.raw_put(room, invalid.as_slice()).await?;
		let before = services.globals.current_count();
		let error = services
			.short
			.get_or_create_shortroomid(room)
			.await
			.expect_err("corrupt room mapping must not be replaced");
		assert_eq!(
			error.status_code(),
			http::StatusCode::INTERNAL_SERVER_ERROR,
			"corrupt room IDs must produce a server error"
		);
		assert_eq!(services.globals.current_count(), before, "refusal cannot allocate a room ID");
		assert_eq!(
			mappings.get(room).await?.as_ref(),
			invalid.as_slice(),
			"refusal must preserve the corrupt room mapping"
		);
		mappings.raw_put(room, saved.as_slice()).await?;
	}
	assert_eq!(
		services
			.short
			.get_or_create_shortroomid(room)
			.await?,
		short,
		"restored room ID must retain its allocation"
	);
	assert_eq!(
		services
			.short
			.get_roomid_from_short(short)
			.await?
			.as_str(),
		room.as_str(),
		"healthy reverse lookup must identify the room"
	);
	let concurrent = room_id!("!short-room-concurrent:localhost");
	let before = services.globals.current_count();
	let (left, right) = tokio::join!(
		services
			.short
			.get_or_create_shortroomid(concurrent),
		services
			.short
			.get_or_create_shortroomid(concurrent)
	);
	assert_eq!(left?, right?, "concurrent room allocations must share one ID");
	assert_eq!(
		services.globals.current_count(),
		before.saturating_add(1),
		"one room must consume one sequence number"
	);
	let rejected = room_id!("!short-room-refused:localhost");
	tuwunel_database::refusal::refuse_next("roomid_shortroomid");
	services
		.short
		.get_or_create_shortroomid(rejected)
		.await
		.expect_err("refused room mapping commit must propagate");
	assert_eq!(
		tuwunel_database::refusal::pending(),
		0,
		"the actual room commit must consume refusal"
	);
	mappings
		.get(rejected)
		.await
		.expect_err("refusal cannot publish a room mapping");
	assert!(services.globals.pending_count().is_empty(), "room refusal cannot leak a permit");
	let short = services
		.short
		.get_or_create_shortroomid(rejected)
		.await?;
	assert_eq!(
		services
			.short
			.get_roomid_from_short(short)
			.await?
			.as_str(),
		rejected.as_str(),
		"retry must resolve the published room identity"
	);
	Ok(())
}

async fn write_room_inventory(services: &Services, rows: usize, key_len: usize) -> Result {
	let mappings = &services.db["roomid_shortroomid"];
	for start in (0..rows).step_by(64) {
		let mut txn = services.db.txn();
		for index in start..rows.min(start.saturating_add(64)) {
			let room = RoomId::parse(format!("!{index:04x}{}", "x".repeat(key_len - 5)))?;
			let short = 100_000_u64.saturating_add(index.try_into()?);
			txn.insert_raw(mappings, room.as_bytes(), short.to_be_bytes());
		}
		txn.execute().await?;
	}
	Ok(())
}

async fn room_reverse_limits(services: &Services) -> Result {
	// This entire map belongs to the fresh disposable maintenance fixture.
	let mappings = &services.db["roomid_shortroomid"];
	let baseline: Vec<_> = mappings
		.raw_stream()
		.map_ok(|(key, value)| (key.to_vec(), value.to_vec()))
		.try_collect()
		.await?;
	mappings.clear().await?;
	write_room_inventory(services, 4096, 20).await?;
	let first = services
		.short
		.get_roomid_from_short(100_000)
		.await?;
	assert_eq!(
		first.as_str().len(),
		20,
		"exact row limit must return the complete matching room"
	);
	let overflow = room_id!("!zz-room-row-overflow:localhost");
	mappings.raw_put(overflow, 200_000_u64).await?;
	let error = services
		.short
		.get_roomid_from_short(100_000)
		.await
		.expect_err("a known prefix cannot hide room inventory overflow");
	assert_eq!(
		error.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS,
		"one row over the limit must refuse"
	);
	mappings.remove(overflow).await?;
	mappings.raw_put(overflow, &[0_u8][..]).await?;
	let error = services
		.short
		.get_roomid_from_short(100_000)
		.await
		.expect_err("a known room cannot hide a corrupt mapping tail");
	// Row overflow is observed before decoding the extra record.
	assert_eq!(
		error.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS,
		"row accounting must precede value decoding"
	);
	mappings.clear().await?;
	write_room_inventory(services, 4096, 120).await?;
	let first = services
		.short
		.get_roomid_from_short(100_000)
		.await?;
	assert_eq!(first.as_str().len(), 120, "exact 512 KiB scan must remain complete");
	mappings.remove(&first).await?;
	let extra = RoomId::parse(format!("{}x", first.as_str()))?;
	mappings.raw_put(&extra, 100_000_u64).await?;
	let error = services
		.short
		.get_roomid_from_short(100_000)
		.await
		.expect_err("one byte over the room inventory budget must refuse");
	assert_eq!(
		error.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS,
		"byte overflow must refuse even after finding the requested room"
	);
	mappings.clear().await?;
	for (key, value) in &baseline {
		mappings
			.raw_put(key.as_slice(), value.as_slice())
			.await?;
	}
	let room = room_id!("!short-room-allocation:localhost");
	let short = services.short.get_shortroomid(room).await?;
	let tail = room_id!("!zz-room-corrupt-tail:localhost");
	mappings.raw_put(tail, &[0_u8][..]).await?;
	let error = services
		.short
		.get_roomid_from_short(short)
		.await
		.expect_err("under-budget corrupt tail cannot be filtered after a match");
	assert_eq!(
		error.status_code(),
		http::StatusCode::INTERNAL_SERVER_ERROR,
		"corrupt tail must be classified as stored corruption"
	);
	mappings.remove(tail).await?;
	mappings.raw_put(tail, short).await?;
	let error = services
		.short
		.get_roomid_from_short(short)
		.await
		.expect_err("duplicate compact room identities must refuse");
	assert_eq!(
		error.status_code(),
		http::StatusCode::INTERNAL_SERVER_ERROR,
		"duplicate compact room identities must not choose one room"
	);
	mappings.remove(tail).await?;
	assert_eq!(
		services
			.short
			.get_roomid_from_short(short)
			.await?
			.as_str(),
		room.as_str(),
		"restored inventory must resolve the original room"
	);
	Ok(())
}

async fn refused_batch(
	services: &Services,
	events: &[&tuwunel_core::ruma::EventId],
	expected: tuwunel_core::http::StatusCode,
) -> Result {
	let before = services.globals.current_count();
	let stream = services
		.short
		.multi_get_or_create_shorteventid(events.iter().copied());
	pin_mut!(stream);
	let error = stream
		.try_next()
		.await
		.expect_err("preflight must refuse before the first ID");
	assert_eq!(error.status_code(), expected);
	assert_eq!(services.globals.current_count(), before, "preflight refusal cannot allocate");
	assert!(
		services.globals.pending_count().is_empty(),
		"preflight cannot leave count permits"
	);
	Ok(())
}

async fn corrupt_batch_mappings(
	services: &Services,
	event: &tuwunel_core::ruma::EventId,
) -> Result {
	let forward = &services.db["eventid_shorteventid"];
	let short = services.short.get_shorteventid(event).await?;
	let reverse = &services.db["shorteventid_eventid"];
	let key = short.to_be_bytes();
	let saved = forward.get(event).await?.to_vec();
	let saved_reverse = reverse.get(&key).await?.to_vec();
	let missing = event_id!("$short-batch-preflight-missing:localhost");
	let mut trailing = saved.clone();
	trailing.push(0);
	let mut separator = saved.clone();
	separator.push(0xFF);
	for invalid in [Vec::new(), vec![0_u8], trailing, separator] {
		forward.raw_put(event, &invalid).await?;
		refused_batch(
			services,
			&[missing, event],
			tuwunel_core::http::StatusCode::INTERNAL_SERVER_ERROR,
		)
		.await?;
		refused_single(services, event).await?;
		assert_eq!(forward.get(event).await?.as_ref(), invalid.as_slice());
		assert_eq!(reverse.get(&key).await?.as_ref(), saved_reverse.as_slice());
		services
			.short
			.get_shorteventid(missing)
			.await
			.expect_err("preflight cannot create the first missing ID");
		forward.raw_put(event, &saved).await?;
	}
	for invalid in [None, Some(b"$wrong-short-batch:localhost".to_vec()), Some(Vec::new())] {
		if let Some(bytes) = &invalid {
			reverse.raw_put(&key, bytes).await?;
		} else {
			reverse.remove(&key).await?;
		}
		refused_batch(
			services,
			&[missing, event],
			tuwunel_core::http::StatusCode::INTERNAL_SERVER_ERROR,
		)
		.await?;
		refused_single(services, event).await?;
		assert_eq!(forward.get(event).await?.as_ref(), saved.as_slice());
		services
			.short
			.get_shorteventid(missing)
			.await
			.expect_err("bad reverse cannot create a preceding ID");
		if let Some(bytes) = &invalid {
			assert_eq!(reverse.get(&key).await?.as_ref(), bytes.as_slice());
		} else {
			reverse
				.get(&key)
				.await
				.expect_err("missing reverse cannot be repaired by allocation");
		}
		reverse.raw_put(&key, &saved_reverse).await?;
	}
	assert_eq!(
		services
			.short
			.get_or_create_shorteventid(event)
			.await?,
		short
	);
	Ok(())
}

async fn refused_single(services: &Services, event: &tuwunel_core::ruma::EventId) -> Result {
	let before = services.globals.current_count();
	let error = services
		.short
		.get_or_create_shorteventid(event)
		.await
		.expect_err("single allocation must refuse a corrupt existing pair");
	assert_eq!(error.status_code(), tuwunel_core::http::StatusCode::INTERNAL_SERVER_ERROR);
	assert_eq!(services.globals.current_count(), before, "single refusal cannot allocate");
	assert!(
		services.globals.pending_count().is_empty(),
		"single refusal cannot leak permits"
	);
	Ok(())
}

async fn batch_limits(services: &Services, event: &tuwunel_core::ruma::EventId) -> Result {
	let expected = services.short.get_shorteventid(event).await?;
	let boundary = vec![event; 4096];
	let actual: Vec<_> = services
		.short
		.multi_get_or_create_shorteventid(boundary.iter().copied())
		.try_collect()
		.await?;
	assert_eq!(
		actual,
		vec![expected; 4096],
		"complete boundary input preserves every duplicate"
	);
	refused_batch(
		services,
		&vec![event; 4097],
		tuwunel_core::http::StatusCode::TOO_MANY_REQUESTS,
	)
	.await?;
	let long = tuwunel_core::ruma::EventId::parse(format!("${}", "x".repeat(127)))?;
	let extra = tuwunel_core::ruma::EventId::parse(format!("${}", "y".repeat(128)))?;
	let short = services
		.short
		.get_or_create_shorteventid(&long)
		.await?;
	let mut inputs = vec![long.as_ref(); 4096];
	let actual: Vec<_> = services
		.short
		.multi_get_or_create_shorteventid(inputs.iter().copied())
		.try_collect()
		.await?;
	assert_eq!(actual, vec![short; 4096], "exact 512 KiB observed IDs remain complete");
	inputs[0] = extra.as_ref();
	refused_batch(services, &inputs, tuwunel_core::http::StatusCode::TOO_MANY_REQUESTS).await?;
	services
		.short
		.get_shorteventid(&extra)
		.await
		.expect_err("byte overflow cannot allocate");
	Ok(())
}

async fn lazy_atomic_allocation(services: &Services) -> Result {
	let first = event_id!("$short-batch-first-only:localhost");
	let second = event_id!("$short-batch-not-consumed:localhost");
	let inputs = [first, second];
	let stream = services
		.short
		.multi_get_or_create_shorteventid(inputs.into_iter());
	pin_mut!(stream);
	let short = stream
		.try_next()
		.await?
		.expect("first demanded allocation");
	let resolved: OwnedEventId = services
		.short
		.get_eventid_from_short(short)
		.await?;
	assert_eq!(resolved, first);
	services
		.short
		.get_shorteventid(second)
		.await
		.expect_err("unconsumed IDs cannot allocate ahead");
	let concurrent = event_id!("$short-batch-concurrent:localhost");
	let before = services.globals.current_count();
	let (left, right) = tokio::join!(
		services
			.short
			.get_or_create_shorteventid(concurrent),
		services
			.short
			.get_or_create_shorteventid(concurrent)
	);
	assert_eq!(left?, right?, "checked concurrent allocations retain one identity");
	assert_eq!(services.globals.current_count(), before.saturating_add(1));
	let rejected = event_id!("$short-batch-refused-commit:localhost");
	tuwunel_database::refusal::refuse_next("eventid_shorteventid");
	services
		.short
		.get_or_create_shorteventid(rejected)
		.await
		.expect_err("refused atomic mapping commit must return an error");
	services.db["shorteventid_eventid"]
		.get(&services.globals.current_count().to_be_bytes())
		.await
		.expect_err("refused mapping cannot publish its reverse direction");
	assert_eq!(tuwunel_database::refusal::pending(), 0);
	services
		.short
		.get_shorteventid(rejected)
		.await
		.expect_err("refusal cannot publish a forward mapping");
	assert!(services.globals.pending_count().is_empty());
	let short = services
		.short
		.get_or_create_shorteventid(rejected)
		.await?;
	let resolved: OwnedEventId = services
		.short
		.get_eventid_from_short(short)
		.await?;
	assert_eq!(resolved, rejected, "retry publishes a complete mapping pair");
	Ok(())
}
