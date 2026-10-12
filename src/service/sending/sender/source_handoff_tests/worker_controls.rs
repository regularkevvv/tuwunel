//! Real source-worker capacity refusal, durable progress and fatal controls.
//! The busy case uses the actual remote snapshot registry and an owned bridge.
use std::{
	path::Path,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::Duration,
};

use futures::TryStreamExt;
use ruma::{
	api::error::{ErrorKind, LimitExceededErrorData, RetryAfter},
	events::room::message::RoomMessageEventContent,
	room_id, server_name, user_id,
};
use tokio::{task::JoinHandle, time::timeout};
use tuwunel_core::{
	Error, Result,
	http::StatusCode,
	matrix::{RawPduId, pdu::PduBuilder},
};

use super::{Destination, Fixture, SendingEvent};
use crate::{
	Services,
	bridge_fixture::{CommitStage, Fake},
};

pub(super) async fn child(root: &Path, phase: &str) -> Result {
	let fake = Fake::start().await?;
	let fixture = Fixture::open_remote(root, &fake.url).await?;
	let services = &fixture.services;
	let raw = pending_source(services).await?;
	match phase {
		| "worker-busy" | "worker-interrupt" =>
			held_snapshot(services, &fake, raw, phase).await?,
		| "worker-truncated" => truncated_snapshot(services, raw).await?,
		| "worker-corrupt" => corrupt_snapshot(services, raw).await?,
		| _ => unreachable!("owned source-worker phase"),
	}
	fixture.finish().await;
	Ok(())
}

async fn pending_source(services: &Services) -> Result<RawPduId> {
	super::setup(services).await?;
	tuwunel_database::refusal::refuse_next("servernameevent_data");
	let room = room_id!("!source-handoff:localhost");
	let lock = services.state.mutex.lock(room).await;
	let event = services
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(
				"snapshot capacity obligation",
			)),
			user_id!("@source:localhost"),
			room,
			&lock,
		)
		.await?;
	drop(lock);
	assert_eq!(tuwunel_database::refusal::pending(), 0, "queue refusal leaves a durable source");
	let raw = services.timeline.get_pdu_id(&event).await?;
	assert!(
		services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	assert_eq!(queued(services, raw).await?, 0);
	Ok(raw)
}

async fn held_snapshot(services: &Services, fake: &Fake, raw: RawPduId, phase: &str) -> Result {
	let global = tuwunel_bridge::catalog::map_id("global")
		.expect("global catalog")
		.0;
	let plan = tuwunel_bridge::catalog::map_id("pduid_federationplan")
		.expect("source catalog")
		.0;
	let before = fake.rows(plan);
	let gate = fake.pause_commit_on(global, CommitStage::BeforeApply);
	let db = services.db.clone();
	let commit = tokio::spawn(async move {
		let mut txn = db.txn();
		txn.insert_raw(&db["global"], b"source-worker-capacity-control", b"owned");
		txn.execute_flushed().await
	});
	timeout(Duration::from_secs(2), gate.entered.notified())
		.await
		.expect("owned write is dispatched");
	let refusal = services.db["global"]
		.raw_keys_prefix_after(&[0x08], None, 65)
		.await
		.expect_err("actual snapshot registry refuses admission during held write");
	assert_eq!(refusal.status_code(), StatusCode::TOO_MANY_REQUESTS);
	assert!(
		refusal
			.to_string()
			.contains("Snapshot admission is busy; retry the operation.")
	);
	let sending = services.sending.clone();
	let mut worker = tokio::spawn(async move { sending.federation_source_worker().await });
	timeout(Duration::from_millis(200), &mut worker)
		.await
		.expect_err("retryable snapshot admission must not terminate the worker");
	let guard =
		timeout(Duration::from_millis(200), services.sending.db.lock_federation_sources())
			.await
			.expect("source admission lock is released while waiting");
	drop(guard);
	assert_eq!(fake.rows(plan), before, "refused inventory preserves the exact durable source");
	if phase == "worker-interrupt" {
		stop(services, worker).await?;
		assert_eq!(fake.rows(plan), before, "interrupt does not discard owed work");
		gate.release.notify_one();
		timeout(Duration::from_secs(2), commit)
			.await
			.expect("owned commit finishes")??;
		assert!(
			services
				.sending
				.db
				.has_federation_plan(&raw)
				.await?
		);
	} else {
		gate.release.notify_one();
		timeout(Duration::from_secs(2), commit)
			.await
			.expect("owned commit finishes")??;
		progress(services, raw).await?;
		stop(services, worker).await?;
	}
	Ok(())
}

async fn truncated_snapshot(services: &Services, raw: RawPduId) -> Result {
	let calls = Arc::new(AtomicUsize::new(0));
	let observed = calls.clone();
	let sending = services.sending.clone();
	let worker = tokio::spawn(async move {
		sending
			.federation_source_worker_with_inventory(|| {
				let first = observed.fetch_add(1, Ordering::SeqCst) == 0;
				let sending = &sending;
				async move {
					if first {
						return Err(Error::Request(
							ErrorKind::LimitExceeded(LimitExceededErrorData {
								retry_after: Some(RetryAfter::Delay(Duration::from_millis(100))),
							}),
							tuwunel_database::TRUNCATED.into(),
							StatusCode::TOO_MANY_REQUESTS,
						));
					}
					sending.federation_source_inventory().await
				}
			})
			.await
	});
	timeout(Duration::from_secs(1), async {
		while calls.load(Ordering::SeqCst) == 0 {
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("actual worker reaches injected truncated inventory");
	for _ in 0..64 {
		services.sending.wake_federation_sources();
		tokio::task::yield_now().await;
	}
	tokio::time::sleep(Duration::from_millis(100)).await;
	assert_eq!(calls.load(Ordering::SeqCst), 1, "hints cannot bypass the capacity retry delay");
	assert!(
		services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	assert_eq!(queued(services, raw).await?, 0);
	progress(services, raw).await?;
	assert!(calls.load(Ordering::SeqCst) >= 2, "same worker retries its inventory");
	stop(services, worker).await
}

async fn corrupt_snapshot(services: &Services, raw: RawPduId) -> Result {
	let before = services.db["pduid_federationplan"]
		.get(raw.as_ref())
		.await?
		.to_vec();
	let mut key = vec![0x08];
	key.extend_from_slice(raw.as_ref());
	let mut txn = services.db.txn();
	txn.insert_raw(&services.db["global"], &key, b"invalid-witness");
	txn.execute_flushed().await?;
	let error = timeout(Duration::from_secs(2), services.sending.federation_source_worker())
		.await
		.expect("fatal corruption must terminate")
		.expect_err("corrupt source cannot be relabelled retryable capacity");
	assert_ne!(error.status_code(), StatusCode::TOO_MANY_REQUESTS);
	assert!(
		error
			.to_string()
			.contains("Invalid canonical federation plan")
	);
	assert_eq!(
		services.db["pduid_federationplan"]
			.get(raw.as_ref())
			.await?
			.as_ref(),
		before.as_slice()
	);
	assert_eq!(queued(services, raw).await?, 0);
	Ok(())
}

async fn queued(services: &Services, raw: RawPduId) -> Result<usize> {
	let destination = Destination::Federation(server_name!("handoff.invalid").to_owned());
	let rows = services
		.sending
		.db
		.queued_requests(&destination)
		.try_collect::<Vec<_>>()
		.await?;
	Ok(rows
		.iter()
		.filter(|(_, event)| *event == SendingEvent::Pdu(raw))
		.count())
}

async fn progress(services: &Services, raw: RawPduId) -> Result {
	timeout(Duration::from_secs(3), async {
		while services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
		{
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		Result::<()>::Ok(())
	})
	.await
	.expect("same live worker materializes the owed page after capacity returns")?;
	assert_eq!(queued(services, raw).await?, 1, "exact durable queue owns the obligation once");
	Ok(())
}

async fn stop(services: &Services, worker: JoinHandle<Result>) -> Result {
	crate::Service::interrupt(services.sending.as_ref()).await;
	timeout(Duration::from_millis(500), worker)
		.await
		.expect("capacity wait is promptly interruptible")??;
	Ok(())
}
