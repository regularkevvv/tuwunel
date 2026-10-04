#![cfg(test)]

use std::{env::var, fs::remove_dir_all, path::PathBuf, process::id as process_id};

use futures::{StreamExt, TryStreamExt, pin_mut};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result,
	ruma::{OwnedEventId, event_id},
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
