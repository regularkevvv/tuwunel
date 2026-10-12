//! Queue and owned-page input limits precede promotion and retained copies.
//! Each case owns a native reference database; no network delivery is used.
use std::iter::once;

use futures::TryStreamExt;
use tuwunel_core::Result;

use super::{
	CurTransactionStatus, Delivery, Destination, EduBuf, Msg, QueueRecovery, SendingEvent,
	SendingFutures, edu_tests::Fixture,
};

fn edu(bytes: usize) -> SendingEvent {
	let prefix = br#"{"type":"m.typing","content":{"room_id":"!budget:localhost","user_ids":[],"padding":""#;
	let suffix = br#""}}"#;
	let mut value = Vec::with_capacity(bytes);
	value.extend_from_slice(prefix);
	value.resize(
		bytes
			.checked_sub(suffix.len())
			.expect("fixture payload width"),
		b'a',
	);
	value.extend_from_slice(suffix);
	assert_eq!(value.len(), bytes);
	SendingEvent::Edu(EduBuf::from_slice(&value))
}

async fn counts(fixture: &Fixture) -> Result<(usize, usize)> {
	let queued = fixture.services.db["servernameevent_data"]
		.raw_keys()
		.try_fold(0_usize, |count, _| async move { Ok(count.saturating_add(1)) })
		.await?;
	let active = fixture.services.db["servercurrentevent_data"]
		.raw_keys()
		.try_fold(0_usize, |count, _| async move { Ok(count.saturating_add(1)) })
		.await?;
	Ok((queued, active))
}

async fn select(fixture: &Fixture, destination: Destination) -> Result<SendingFutures> {
	let mut futures = SendingFutures::new();
	fixture
		.services
		.sending
		.handle_request(
			Msg {
				dest: destination,
				event: SendingEvent::BadgeRefresh,
				queue_id: Vec::new(),
			},
			&mut futures,
			&mut CurTransactionStatus::new(),
		)
		.await?;
	Ok(futures)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pending_value_that_cannot_fit_its_active_envelope_is_not_promoted() -> Result {
	let fixture = Fixture::new().await?;
	let destination = Destination::Appservice("allocation-single".into());
	// This was a valid pending value under the old storage admission. Adding
	// its mandatory active identity would exceed the provider's value width.
	let event = edu(tuwunel_bridge::MAX_VALUE_BYTES);
	let count = fixture.services.globals.next_count().await?;
	let mut key = destination.get_prefix();
	key.extend_from_slice(&count.to_be_bytes());
	fixture.services.db["servernameevent_data"]
		.insert(&key, event.value_bytes())
		.await?;
	drop(count);
	let before = fixture.services.globals.current_count();
	let mut futures = select(&fixture, destination).await?;
	assert!(
		matches!(futures.next().await, Some(Ok(Delivery::Unprepared(_, error))) if error.status_code() == http::StatusCode::PAYLOAD_TOO_LARGE)
	);
	let retained = counts(&fixture).await?;
	let after = fixture.services.globals.current_count();
	futures.cancel_and_join().await;
	fixture.finish().await;
	assert_eq!(
		retained,
		(1, 0),
		"oversized pending admission was promoted before byte validation"
	);
	assert_eq!(after, before, "refused selection cannot consume a promotion identity");
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_byte_limited_prefix_resumes_without_losing_queued_successors() -> Result {
	let fixture = Fixture::new().await?;
	let destination = Destination::Appservice("allocation-resume".into());
	let event = edu(1024 * 1024);
	let data = &fixture.services.sending.db;
	for _ in 0..5 {
		data.queue_requests(once((&event, &destination)))
			.await?;
	}
	let mut futures = select(&fixture, destination.clone()).await?;
	assert_eq!(counts(&fixture).await?, (3, 2));
	futures.cancel_and_join().await;
	let (_, rows) = data.active_batch(&destination).await?;
	let mut statuses = CurTransactionStatus::new();
	fixture
		.services
		.sending
		.resume_queue(
			&destination,
			&mut futures,
			&mut statuses,
			&mut QueueRecovery::CleanupAcknowledged(rows),
		)
		.await?;
	assert_eq!(counts(&fixture).await?, (1, 2));
	futures.cancel_and_join().await;
	let (_, rows) = data.active_batch(&destination).await?;
	fixture
		.services
		.sending
		.resume_queue(
			&destination,
			&mut futures,
			&mut statuses,
			&mut QueueRecovery::CleanupAcknowledged(rows),
		)
		.await?;
	assert_eq!(counts(&fixture).await?, (0, 1));
	futures.cancel_and_join().await;
	let (_, rows) = data.active_batch(&destination).await?;
	data.acknowledge_active(&destination, &rows)
		.await?;
	assert_eq!(counts(&fixture).await?, (0, 0));
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_reserves_identity_width_before_counter_or_queue_mutation() -> Result {
	let fixture = Fixture::new().await?;
	let destination = Destination::Appservice("allocation-admission".into());
	let accepted = edu(tuwunel_bridge::MAX_VALUE_BYTES - 10);
	let refused = edu(tuwunel_bridge::MAX_VALUE_BYTES - 9);
	let data = &fixture.services.sending.db;
	let before = fixture.services.globals.current_count();
	let error = data
		.queue_requests([(&accepted, &destination), (&refused, &destination)].into_iter())
		.await
		.expect_err("entire producer batch is validated first");
	assert_eq!(error.status_code(), http::StatusCode::PAYLOAD_TOO_LARGE);
	assert_eq!(fixture.services.globals.current_count(), before);
	assert_eq!(counts(&fixture).await?, (0, 0));
	let keys = data
		.queue_requests(once((&accepted, &destination)))
		.await?;
	data.mark_as_active(once(&(keys[0].clone(), accepted)))
		.await?;
	let (_, rows) = data.active_batch(&destination).await?;
	assert_eq!(rows.selected_rows()[0].1.len(), tuwunel_bridge::MAX_VALUE_BYTES);
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_selected_suffix_preserves_rows_and_promotion_counter() -> Result {
	let fixture = Fixture::new().await?;
	let destination = Destination::Appservice("allocation-malformed".into());
	let event = edu(1024);
	fixture
		.services
		.sending
		.db
		.queue_requests(once((&event, &destination)))
		.await?;
	let count = fixture.services.globals.next_count().await?;
	let mut key = destination.get_prefix();
	key.extend_from_slice(&count.to_be_bytes());
	fixture.services.db["servernameevent_data"]
		.insert(&key, [0x05])
		.await?;
	drop(count);
	let before = fixture.services.globals.current_count();
	assert!(select(&fixture, destination).await.is_err());
	assert_eq!(counts(&fixture).await?, (2, 0));
	assert_eq!(fixture.services.globals.current_count(), before);
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_active_membership_precedes_an_unpromotable_queued_row() -> Result {
	let fixture = Fixture::new().await?;
	let destination = Destination::Appservice("allocation-active-first".into());
	let event = edu(1024);
	let data = &fixture.services.sending.db;
	let keys = data
		.queue_requests(once((&event, &destination)))
		.await?;
	data.mark_as_active(once(&(keys[0].clone(), event)))
		.await?;
	let count = fixture.services.globals.next_count().await?;
	let mut key = destination.get_prefix();
	key.extend_from_slice(&count.to_be_bytes());
	fixture.services.db["servernameevent_data"]
		.insert(&key, edu(tuwunel_bridge::MAX_VALUE_BYTES).value_bytes())
		.await?;
	drop(count);
	let mut futures = select(&fixture, destination).await?;
	assert_eq!(futures.len(), 1, "active attempt remains scheduled");
	assert_eq!(counts(&fixture).await?, (1, 1));
	futures.cancel_and_join().await;
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sixteen_unpromotable_destinations_do_not_block_healthy_startup_recovery() -> Result {
	use std::time::Duration;

	use tokio::time::timeout;
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let event = edu(tuwunel_bridge::MAX_VALUE_BYTES);
	for index in 0..16 {
		let destination = Destination::Appservice(format!("a-byte-refusal-{index:02}"));
		let count = services.globals.next_count().await?;
		let mut key = destination.get_prefix();
		key.extend_from_slice(&count.to_be_bytes());
		services.db["servernameevent_data"]
			.insert(&key, event.value_bytes())
			.await?;
	}
	let destination = super::inventory_tests::healthy(services).await?;
	let supported = SendingEvent::Edu(EduBuf::from_slice(
		br#"{"type":"m.typing","content":{"user_ids":[]}}"#,
	));
	services
		.sending
		.db
		.queue_requests(once((&supported, &destination)))
		.await?;
	let sending = services.sending.clone();
	let task = tokio::spawn(async move { sending.sender(0).await });
	let recovered = timeout(
		Duration::from_secs(8),
		super::inventory_tests::wait_empty(services, &destination),
	)
	.await;
	services.stop().await;
	timeout(Duration::from_secs(5), task)
		.await
		.expect("owned sender joined")
		.expect("sender task")?;
	assert_eq!(counts(&fixture).await?, (16, 0), "all refused legacy rows remain owed");
	fixture.finish().await;
	assert!(recovered.is_ok(), "oversized destinations blocked healthy work");
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_selection_stops_at_a_byte_budget_and_preserves_the_suffix() -> Result {
	let fixture = Fixture::new().await?;
	let destination = Destination::Appservice("allocation-prefix".into());
	let event = edu(1024 * 1024);
	for _ in 0..48 {
		fixture
			.services
			.sending
			.db
			.queue_requests(once((&event, &destination)))
			.await?;
	}
	let mut futures = select(&fixture, destination).await?;
	let (queued, active) = counts(&fixture).await?;
	futures.cancel_and_join().await;
	fixture.finish().await;
	assert_eq!(queued.saturating_add(active), 48, "bounded selection keeps every accepted row");
	assert!(active <= 3, "row-only selection promoted {active} MiB before byte admission");
	assert!(active > 0, "a supported prefix must make progress");
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_key_pages_refuse_native_keys_beyond_the_storage_width() -> Result {
	let fixture = Fixture::new().await?;
	let map = &fixture.services.db["servercurrentevent_data"];
	let key = vec![b'x'; tuwunel_bridge::MAX_KEY_BYTES.saturating_add(1)];
	map.insert(b"a", []).await?;
	map.insert(&key, []).await?;
	for page in [
		map.raw_keys_after(None, 4).await,
		map.raw_keys_capped(None, 4).await,
		map.raw_keys_prefix_after(b"x", None, 4).await,
		map.raw_keys_prefix_reverse(b"x", &key, 4).await,
	] {
		match page {
			| Err(_) => {},
			| Ok(_) => panic!("native owned key page copied an oversized storage key"),
		}
	}
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maximum_width_owned_key_and_exclusive_resume_remain_supported() -> Result {
	let fixture = Fixture::new().await?;
	let map = &fixture.services.db["servercurrentevent_data"];
	let key = vec![b'x'; tuwunel_bridge::MAX_KEY_BYTES];
	map.insert(&key, []).await?;
	map.insert(b"y", []).await?;
	assert_eq!(map.raw_keys_after(None, 1).await?, vec![key.clone()]);
	assert_eq!(map.raw_keys_after(Some(&key), 1).await?, vec![b"y".to_vec()]);
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_row_pages_refuse_values_beyond_the_storage_width() -> Result {
	let fixture = Fixture::new().await?;
	let map = &fixture.services.db["servercurrentevent_data"];
	let value = vec![0x61; tuwunel_bridge::MAX_VALUE_BYTES.saturating_add(1)];
	map.insert(b"a", &value).await?;
	assert!(
		map.raw_rows_after(None, 4).await.is_err(),
		"native owned row copied an oversized value"
	);
	assert!(
		map.raw_rows_prefix_after(b"a", None, 4)
			.await
			.is_err(),
		"native prefix row copied an oversized value"
	);
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_corrupt_pending_envelope_is_not_masked_as_a_width_refusal() -> Result {
	let fixture = Fixture::new().await?;
	let destination = Destination::Appservice("allocation-corrupt-envelope".into());
	let count = fixture.services.globals.next_count().await?;
	let mut key = destination.get_prefix();
	key.extend_from_slice(&count.to_be_bytes());
	let mut value = vec![0x61; tuwunel_bridge::MAX_VALUE_BYTES];
	value[0] = 0x05;
	value[1] = 1;
	fixture.services.db["servernameevent_data"]
		.insert(&key, &value)
		.await?;
	drop(count);
	let before = fixture.services.globals.current_count();
	let error = match select(&fixture, destination).await {
		| Err(error) => error,
		| Ok(mut futures) => {
			futures.cancel_and_join().await;
			panic!("corrupt pending envelope became a local width refusal");
		},
	};
	assert_ne!(error.status_code(), http::StatusCode::PAYLOAD_TOO_LARGE);
	assert_eq!(counts(&fixture).await?, (1, 0));
	assert_eq!(fixture.services.globals.current_count(), before);
	fixture.finish().await;
	Ok(())
}
