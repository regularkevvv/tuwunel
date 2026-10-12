//! Atomic producer admission must fit the bridge before queue/counter writes.
//! These cases own native scratch databases and never dispatch transports.
use std::{
	iter::repeat_n,
	sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

use futures::{StreamExt, TryStreamExt, stream::iter};
use tuwunel_core::Result;

use super::{Destination, EduBuf, SendingEvent, edu_tests::Fixture};

fn edu(bytes: usize) -> SendingEvent {
	let prefix = br#"{"type":"m.typing","content":{"room_id":"!admission:localhost","user_ids":[],"padding":""#;
	let suffix = br#""}}"#;
	let mut value = Vec::with_capacity(bytes);
	value.extend_from_slice(prefix);
	value.resize(
		bytes
			.checked_sub(suffix.len())
			.expect("fixture width"),
		b'a',
	);
	value.extend_from_slice(suffix);
	assert_eq!(value.len(), bytes);
	SendingEvent::Edu(EduBuf::from_slice(&value))
}

async fn queued(fixture: &Fixture) -> Result<usize> {
	fixture.services.db["servernameevent_data"]
		.raw_keys()
		.try_fold(0_usize, |count, _| async move { Ok(count.saturating_add(1)) })
		.await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operation_limit_refuses_before_queue_and_counter_mutation() -> Result {
	let fixture = Fixture::new().await?;
	let destination = Destination::Push(
		ruma::user_id!("@admission:localhost").to_owned(),
		"admission-operation-limit".into(),
	);
	let event = SendingEvent::BadgeRefresh;
	let before = fixture.services.globals.current_count();
	let result = fixture
		.services
		.sending
		.db
		.queue_requests(repeat_n(
			(&event, &destination),
			tuwunel_bridge::MAX_COMMIT_OPS.saturating_add(1),
		))
		.await;
	let rows = queued(&fixture).await?;
	let after = fixture.services.globals.current_count();
	fixture.finish().await;
	assert!(
		result.is_err(),
		"native producer accepted {} rows beyond the operation limit and advanced the counter \
		 by {}",
		rows,
		after.saturating_sub(before)
	);
	assert_eq!(rows, 0, "refused producer batch wrote queue rows");
	assert_eq!(after, before, "refused producer batch consumed counter identities");
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_byte_limit_refuses_before_queue_and_counter_mutation() -> Result {
	let fixture = Fixture::new().await?;
	let destination = Destination::Appservice("admission-byte-limit".into());
	let event = edu(1536 * 1024);
	let before = fixture.services.globals.current_count();
	let result = fixture
		.services
		.sending
		.db
		.queue_requests(repeat_n((&event, &destination), 4))
		.await;
	let rows = queued(&fixture).await?;
	let after = fixture.services.globals.current_count();
	fixture.finish().await;
	assert!(
		result.is_err(),
		"native producer accepted {} rows ({} payload bytes) and advanced the counter by {}",
		rows,
		rows.saturating_mul(event.value_bytes().len()),
		after.saturating_sub(before)
	);
	assert_eq!(rows, 0, "refused producer batch wrote queue rows");
	assert_eq!(after, before, "refused producer batch consumed counter identities");
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_supported_batch_at_the_operation_limit_keeps_every_admission() -> Result {
	let fixture = Fixture::new().await?;
	let destination = Destination::Push(
		ruma::user_id!("@admission:localhost").to_owned(),
		"admission-supported-limit".into(),
	);
	let event = SendingEvent::BadgeRefresh;
	let keys = fixture
		.services
		.sending
		.db
		.queue_requests(repeat_n((&event, &destination), tuwunel_bridge::MAX_COMMIT_OPS))
		.await?;
	assert_eq!(keys.len(), tuwunel_bridge::MAX_COMMIT_OPS);
	assert_eq!(queued(&fixture).await?, tuwunel_bridge::MAX_COMMIT_OPS);
	for key in keys {
		assert_eq!(
			fixture.services.db["servernameevent_data"]
				.get(&key)
				.await?
				.as_ref(),
			event.value_bytes()
		);
	}
	fixture.finish().await;
	Ok(())
}

fn servers(count: usize) -> Vec<ruma::OwnedServerName> {
	(0..count)
		.map(|at| {
			ruma::OwnedServerName::try_from(format!("admission-{at}.localhost")).expect("server")
		})
		.collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn federation_operation_refusal_stops_the_source_before_any_mutation() -> Result {
	let fixture = Fixture::new().await?;
	let names = servers(tuwunel_bridge::MAX_COMMIT_OPS + 2);
	let polled = AtomicUsize::new(0);
	let source = iter(names.iter().map(AsRef::as_ref)).inspect(|_| {
		polled.fetch_add(1, Relaxed);
	});
	let mut raw = [0; 16];
	raw[7] = 1;
	raw[15] = 1;
	let id = crate::rooms::timeline::RawPduId::from_bytes(&raw)?;
	let before = fixture.services.globals.current_count();
	let result = fixture
		.services
		.sending
		.send_pdu_servers(source, &id)
		.await;
	let rows = queued(&fixture).await?;
	let after = fixture.services.globals.current_count();
	fixture.finish().await;
	assert!(result.is_err(), "oversized PDU fanout accepted");
	assert_eq!(polled.load(Relaxed), tuwunel_bridge::MAX_COMMIT_OPS + 1);
	assert_eq!(rows, 0);
	assert_eq!(after, before);
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn federation_byte_refusal_stops_the_source_before_any_mutation() -> Result {
	let fixture = Fixture::new().await?;
	let names = servers(4);
	let polled = AtomicUsize::new(0);
	let source = iter(names.iter().map(AsRef::as_ref)).inspect(|_| {
		polled.fetch_add(1, Relaxed);
	});
	let SendingEvent::Edu(value) = edu(1536 * 1024) else { unreachable!() };
	let before = fixture.services.globals.current_count();
	let result = fixture
		.services
		.sending
		.send_edu_servers(source, value)
		.await;
	let rows = queued(&fixture).await?;
	let after = fixture.services.globals.current_count();
	fixture.finish().await;
	assert!(result.is_err(), "oversized EDU fanout accepted");
	assert_eq!(polled.load(Relaxed), 3, "kept consuming after the first refusal");
	assert_eq!(rows, 0);
	assert_eq!(after, before);
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supported_federation_fanout_keeps_all_rows_and_only_payload_free_hints() -> Result {
	let fixture = Fixture::new().await?;
	let names = servers(tuwunel_bridge::MAX_COMMIT_OPS);
	let SendingEvent::Edu(value) = edu(4096) else { unreachable!() };
	fixture
		.services
		.sending
		.send_edu_servers(iter(names.iter().map(AsRef::as_ref)), value.clone())
		.await?;
	assert_eq!(queued(&fixture).await?, names.len());
	let mut rows = fixture.services.db["servernameevent_data"]
		.raw_stream()
		.boxed();
	while let Some((_, stored)) = rows.try_next().await? {
		assert_eq!(stored, value.as_slice());
	}
	drop(rows);
	for (_, receiver) in &fixture.services.sending.channels {
		while let Ok(hint) = receiver.try_recv() {
			assert!(hint.queue_id.is_empty());
			assert_eq!(hint.event, SendingEvent::BadgeRefresh);
		}
	}
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_admission_remains_a_noop_even_with_an_unusable_payload() -> Result {
	let fixture = Fixture::new().await?;
	let before = fixture.services.globals.current_count();
	let SendingEvent::Edu(value) = edu(tuwunel_bridge::MAX_VALUE_BYTES) else {
		unreachable!()
	};
	// No destinations means no value will need an active envelope or a lease.
	// An actual destination would refuse this width before any mutation.
	let fanout = fixture
		.services
		.sending
		.send_edu_servers(iter(std::iter::empty::<&ruma::ServerName>()), value)
		.await;
	let batch = fixture
		.services
		.sending
		.db
		.queue_requests(std::iter::empty())
		.await;
	let rows = queued(&fixture).await?;
	let after = fixture.services.globals.current_count();
	fixture.finish().await;
	fanout?;
	assert!(batch?.is_empty());
	assert_eq!(rows, 0);
	assert_eq!(after, before);
	Ok(())
}
