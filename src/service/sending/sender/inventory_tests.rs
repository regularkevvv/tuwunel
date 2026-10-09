//! Failed destinations must retain durable retry semantics without retaining
//! an unbounded process inventory or monopolizing transport admission.
use std::{iter::once, time::Duration};

use futures::TryStreamExt;
use tokio::time::timeout;
use tuwunel_core::{
	Error, Result,
	ruma::{
		api::appservice::{Namespaces, Registration, RegistrationInit},
		user_id,
	},
};

use super::{
	CurTransactionStatus, Delivery, Destination, EduBuf, QueueRecovery, QueueRetries,
	SendingEvent, SendingFutures, TransactionStatus, WakeQueue, arm_wake_in, edu_tests::Fixture,
};
use crate::Services;

fn push(index: usize) -> Destination {
	Destination::Push(user_id!("@backoff:localhost").to_owned(), format!("backoff-{index}"))
}

async fn fail_push(
	services: &Services,
	destination: &Destination,
	statuses: &mut CurTransactionStatus,
	wakes: &mut WakeQueue,
) -> Result {
	let pending = services
		.sending
		.db
		.queued_requests(destination)
		.try_collect::<Vec<_>>()
		.await?;
	if !pending.is_empty() {
		services
			.sending
			.db
			.mark_as_active(pending.iter())
			.await?;
	}
	let (_, rows) = services
		.sending
		.db
		.active_batch(destination)
		.await?;
	statuses.insert(destination.clone(), TransactionStatus::Running);
	services
		.sending
		.handle_response(
			Ok(Delivery::PushFailed(
				destination.clone(),
				rows,
				Box::new(Error::bad_database("controlled gateway failure")),
			)),
			&mut SendingFutures::new(),
			statuses,
			wakes,
			&mut QueueRecovery::ResumePending,
			&mut QueueRetries::new(),
		)
		.await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_push_destinations_do_not_accumulate_volatile_statuses() -> Result {
	let fixture = Fixture::new().await?;
	let mut statuses = CurTransactionStatus::new();
	let mut wakes = WakeQueue::new();
	for index in 0..256 {
		let destination = push(index);
		fixture
			.services
			.sending
			.db
			.queue_requests(once((&SendingEvent::BadgeRefresh, &destination)))
			.await?;
		fail_push(&fixture.services, &destination, &mut statuses, &mut wakes).await?;
		assert!(
			fixture
				.services
				.sending
				.db
				.push_backoff(&destination)
				.await?
				.is_some()
		);
	}
	let retained = statuses.len();
	fixture.finish().await;
	assert!(retained <= 16, "failed destinations retained {retained} statuses");
	Ok(())
}

#[test]
fn retry_wake_hints_are_bounded_and_coalesced() {
	let mut wakes = WakeQueue::new();
	for index in 0..256 {
		for _ in 0..3 {
			arm_wake_in(&mut wakes, push(index), Duration::from_secs(30));
		}
	}
	assert!(wakes.len() <= 128, "retry heap retained {} wake hints", wakes.len());
	let mut seen = std::collections::HashSet::new();
	for std::cmp::Reverse((_, destination)) in wakes {
		assert!(seen.insert(destination), "one hint per destination");
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_push_backoff_survives_sender_reconstruction() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let destination = push(0);
	services
		.sending
		.db
		.queue_requests(once((&SendingEvent::BadgeRefresh, &destination)))
		.await?;
	fail_push(services, &destination, &mut CurTransactionStatus::new(), &mut WakeQueue::new())
		.await?;
	let mut deliveries = SendingFutures::new();
	services
		.sending
		.startup_netburst(0, &mut deliveries, &mut CurTransactionStatus::new())
		.await?;
	let staged = deliveries.len();
	let pending = services
		.sending
		.db
		.queued_requests(&destination)
		.try_collect::<Vec<_>>()
		.await?
		.len();
	let active = services
		.sending
		.db
		.active_requests_for(&destination)
		.try_collect::<Vec<_>>()
		.await?
		.len();
	deliveries.cancel_and_join().await;
	fixture.finish().await;
	assert_eq!(pending + active, 1, "backoff preserves the accepted row");
	assert_eq!(staged, 0, "fresh sender bypassed the durable push backoff");
	Ok(())
}

async fn healthy(services: &Services) -> Result<Destination> {
	let registration: Registration = RegistrationInit {
		id: "z-healthy-localretry".into(),
		url: None,
		as_token: "disposable-healthy-localretry-as".into(),
		hs_token: "disposable-healthy-localretry-hs".into(),
		sender_localpart: "healthy-localretry".into(),
		namespaces: Namespaces::new(),
		rate_limited: None,
		protocols: None,
	}
	.into();
	services
		.appservice
		.load_appservice(registration)
		.await?;
	Ok(Destination::Appservice("z-healthy-localretry".into()))
}

async fn wait_empty(services: &Services, destination: &Destination) -> Result {
	loop {
		let pending = services
			.sending
			.db
			.queued_requests(destination)
			.try_collect::<Vec<_>>()
			.await?;
		let active = services
			.sending
			.db
			.active_requests_for(destination)
			.try_collect::<Vec<_>>()
			.await?;
		if pending.is_empty() && active.is_empty() {
			return Ok(());
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn permanent_local_failures_do_not_starve_a_healthy_queued_destination() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let event = SendingEvent::Edu(EduBuf::from_slice(
		br#"{"type":"m.typing","content":{"user_ids":[]}}"#,
	));
	let mut blocked = Vec::new();
	for index in 0..16 {
		// No registration: real composition returns a permanent local refusal.
		let destination = Destination::Appservice(format!("a-unprepared-{index:02}"));
		services
			.sending
			.db
			.queue_requests(once((&event, &destination)))
			.await?;
		blocked.push(destination);
	}
	let destination = healthy(services).await?;
	services
		.sending
		.db
		.queue_requests(once((&event, &destination)))
		.await?;
	let sending = services.sending.clone();
	let task = tokio::spawn(async move { sending.sender(0).await });
	let recovered = timeout(Duration::from_secs(8), wait_empty(services, &destination)).await;
	services.stop().await;
	timeout(Duration::from_secs(5), task)
		.await
		.expect("owned fairness sender joined")
		.expect("fairness sender task")?;
	let mut retained = 0_usize;
	for destination in &blocked {
		retained = retained.saturating_add(
			services
				.sending
				.db
				.queued_requests(destination)
				.try_collect::<Vec<_>>()
				.await?
				.len(),
		);
		retained = retained.saturating_add(
			services
				.sending
				.db
				.active_requests_for(destination)
				.try_collect::<Vec<_>>()
				.await?
				.len(),
		);
	}
	fixture.finish().await;
	assert_eq!(retained, 16, "fair scheduling never discards refused accepted rows");
	recovered.expect("permanent local failures occupied every retry slot")?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_push_failure_cannot_recreate_backoff_for_a_new_incarnation() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let data = &services.sending.db;
	let destination = push(0);
	let mut bytes = [0_u8; 16];
	bytes[7] = 1;
	bytes[15] = 1;
	let event = SendingEvent::Pdu(crate::rooms::timeline::RawPduId::from_bytes(&bytes)?);
	let keys = data
		.queue_requests(once((&event, &destination)))
		.await?;
	data.mark_as_active(once(&(keys[0].clone(), event.clone())))
		.await?;
	let (_, previous) = data.active_batch(&destination).await?;
	let backoff = super::PushBackoff::failed(2)?;
	assert!(
		data.persist_push_backoff(&destination, &previous, backoff)
			.await?
	);
	data.delete_all_requests_for(&destination).await?;
	assert!(
		data.push_backoff(&destination).await?.is_none(),
		"cancellation removes retry state"
	);
	assert!(
		!data
			.persist_push_backoff(&destination, &previous, backoff)
			.await?,
		"late failure cannot recreate an orphan record"
	);
	let keys = data
		.queue_requests(once((&event, &destination)))
		.await?;
	data.mark_as_active(once(&(keys[0].clone(), event)))
		.await?;
	let (_, current) = data.active_batch(&destination).await?;
	assert_eq!(
		previous.selected_rows()[0].0,
		current.selected_rows()[0].0,
		"same physical key was re-admitted"
	);
	assert_ne!(
		previous.selected_rows()[0].1,
		current.selected_rows()[0].1,
		"new identity owns the key"
	);
	assert!(
		!data
			.persist_push_backoff(&destination, &previous, backoff)
			.await?,
		"old failure cannot delay the new incarnation"
	);
	assert!(
		data.persist_push_backoff(&destination, &current, backoff)
			.await?
	);
	data.acknowledge_active(&destination, &previous)
		.await?;
	assert_eq!(
		data.push_backoff(&destination).await?,
		Some(backoff),
		"old ACK cannot clear the new backoff"
	);
	data.acknowledge_active(&destination, &current)
		.await?;
	assert!(
		data.push_backoff(&destination).await?.is_none(),
		"current ACK retires rows and backoff together"
	);
	fixture.finish().await;
	Ok(())
}

#[test]
fn advisory_retries_release_slots_without_discarding_exact_recovery_stages() -> Result {
	let mut retries = QueueRetries::new();
	let now = tokio::time::Instant::now();
	let backoff = super::PushBackoff::failed(1)?;
	retries.insert(push(0), (now, QueueRecovery::ResumePending));
	retries.insert(push(1), (now, QueueRecovery::CleanupAcknowledged(Default::default())));
	retries
		.insert(push(2), (now, QueueRecovery::PersistPushFailure(backoff, Default::default())));
	let mut statuses = CurTransactionStatus::new();
	statuses.insert(push(0), TransactionStatus::Running);
	let mut wakes = WakeQueue::new();
	super::release_advisory_retries(
		&mut retries,
		&SendingFutures::new(),
		&mut statuses,
		&mut wakes,
	);
	assert_eq!(retries.len(), 2);
	assert!(!retries.contains_key(&push(0)));
	assert!(statuses.is_empty());
	assert_eq!(wakes.len(), 1);
	assert!(matches!(retries[&push(1)].1, QueueRecovery::CleanupAcknowledged(_)));
	assert!(matches!(retries[&push(2)].1, QueueRecovery::PersistPushFailure(..)));
	Ok(())
}

const COLD_TEST: &str =
	"sending::sender::inventory_tests::push_backoff_survives_a_cold_database_reopen";
const COLD_PHASE: &str = "TUWUNEL_PUSH_BACKOFF_TEST_PHASE";
const COLD_DIRECTORY: &str = "TUWUNEL_PUSH_BACKOFF_TEST_DIRECTORY";

#[test]
fn push_backoff_survives_a_cold_database_reopen() -> Result {
	if let Ok(phase) = std::env::var(COLD_PHASE) {
		let root = std::path::PathBuf::from(
			std::env::var(COLD_DIRECTORY).expect("owned cold fixture root"),
		);
		return tokio::runtime::Builder::new_multi_thread()
			.worker_threads(2)
			.enable_all()
			.build()?
			.block_on(cold_child(&root, &phase));
	}
	let root = std::env::temp_dir()
		.join(format!("matrix-push-restart-{}", tuwunel_core::utils::rand::string(20)));
	let mut builder = std::fs::DirBuilder::new();
	#[cfg(unix)]
	{
		use std::os::unix::fs::DirBuilderExt;
		builder.mode(0o700)
	};
	builder.create(&root)?;
	for phase in ["fail", "reopen"] {
		let mut child = std::process::Command::new(std::env::current_exe()?)
			.args(["--exact", COLD_TEST, "--nocapture", "--test-threads=1"])
			.env(COLD_PHASE, phase)
			.env(COLD_DIRECTORY, &root)
			.spawn()?;
		let deadline = std::time::Instant::now()
			.checked_add(Duration::from_secs(45))
			.expect("bounded cold fixture deadline");
		loop {
			if let Some(status) = child.try_wait()? {
				assert!(status.success(), "cold push phase {phase}: {status}");
				break;
			}
			if std::time::Instant::now() >= deadline {
				child.kill().ok();
				child.wait().ok();
				panic!("cold push phase exceeded deadline");
			}
			std::thread::sleep(Duration::from_millis(20));
		}
	}
	std::fs::remove_dir_all(root)?;
	Ok(())
}

async fn cold_child(root: &std::path::Path, phase: &str) -> Result {
	let fixture = Fixture::open(root).await?;
	let services = &fixture.services;
	let destination = push(0);
	match phase {
		| "fail" => {
			services
				.sending
				.db
				.queue_requests(once((&SendingEvent::BadgeRefresh, &destination)))
				.await?;
			fail_push(
				services,
				&destination,
				&mut CurTransactionStatus::new(),
				&mut WakeQueue::new(),
			)
			.await?;
			assert!(
				services
					.sending
					.db
					.push_backoff(&destination)
					.await?
					.is_some()
			);
		},
		| "reopen" => {
			let backoff = services
				.sending
				.db
				.push_backoff(&destination)
				.await?
				.expect("durable retry record survived process exit");
			assert_eq!(backoff.tries, 1);
			let mut futures = SendingFutures::new();
			services
				.sending
				.startup_netburst(0, &mut futures, &mut CurTransactionStatus::new())
				.await?;
			assert!(futures.is_empty(), "cold sender cannot bypass committed push backoff");
			assert_eq!(
				services
					.sending
					.db
					.active_requests_for(&destination)
					.try_collect::<Vec<_>>()
					.await?
					.len(),
				1,
				"accepted delivery remains owed after cold reopen"
			);
			futures.cancel_and_join().await;
		},
		| _ => panic!("unknown cold fixture phase"),
	}
	fixture.finish().await;
	Ok(())
}

async fn admit_active(
	services: &Services,
	destination: &Destination,
) -> Result<super::ActiveAcknowledgement> {
	let event = SendingEvent::BadgeRefresh;
	let keys = services
		.sending
		.db
		.queue_requests(once((&event, destination)))
		.await?;
	services
		.sending
		.db
		.mark_as_active(once(&(keys[0].clone(), event)))
		.await?;
	let (_, rows) = services
		.sending
		.db
		.active_batch(destination)
		.await?;
	Ok(rows)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drain_persists_owned_push_failures() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let destination = push(0);
	admit_active(services, &destination).await?;
	let Destination::Push(user, pushkey) = &destination else { unreachable!() };
	// Actual delivery obtains a fallible stored pusher, then the dispatch
	// wrapper attaches physical membership to its error. No HTTP is attempted.
	let key = tuwunel_database::serialize_key((user, pushkey))?;
	services.db["senderkey_pusher"]
		.insert(&key, b"{invalid".as_slice())
		.await?;
	let mut futures = SendingFutures::new();
	futures.push(
		destination.clone(),
		services
			.sending
			.send_events(destination.clone(), vec![SendingEvent::Flush]),
		services.server.runtime(),
	);
	let mut statuses = CurTransactionStatus::new();
	statuses.insert(destination.clone(), TransactionStatus::Retrying(2));
	timeout(
		Duration::from_secs(5),
		services
			.sending
			.finish_responses(&mut futures, &mut statuses, &mut QueueRetries::new()),
	)
	.await
	.expect("owned shutdown failure finished")?;
	assert!(futures.is_empty());
	assert!(statuses.is_empty());
	let backoff = services
		.sending
		.db
		.push_backoff(&destination)
		.await?
		.expect("failed shutdown completion was persisted");
	assert_eq!(backoff.tries, 3, "one transport outcome increments one streak");
	assert_eq!(
		services
			.sending
			.db
			.active_requests_for(&destination)
			.try_collect::<Vec<_>>()
			.await?
			.len(),
		1
	);
	futures.cancel_and_join().await;
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drain_finishes_inherited_durable_stages_without_dispatching_successors()
-> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let failed = push(0);
	let acknowledged = push(1);
	let failed_rows = admit_active(services, &failed).await?;
	let acknowledged_rows = admit_active(services, &acknowledged).await?;
	let backoff = super::PushBackoff::failed(3)?;
	assert!(
		services
			.sending
			.db
			.persist_push_backoff(&acknowledged, &acknowledged_rows, backoff)
			.await?
	);
	services
		.sending
		.db
		.queue_requests(once((&SendingEvent::BadgeRefresh, &acknowledged)))
		.await?;
	let mut retries = QueueRetries::new();
	let now = tokio::time::Instant::now();
	retries
		.insert(failed.clone(), (now, QueueRecovery::PersistPushFailure(backoff, failed_rows)));
	retries.insert(
		acknowledged.clone(),
		(now, QueueRecovery::CleanupAcknowledged(acknowledged_rows)),
	);
	let mut futures = SendingFutures::new();
	let mut statuses = CurTransactionStatus::new();
	statuses.insert(failed.clone(), TransactionStatus::Retrying(2));
	statuses.insert(acknowledged.clone(), TransactionStatus::Running);
	timeout(
		Duration::from_secs(5),
		services
			.sending
			.finish_responses(&mut futures, &mut statuses, &mut retries),
	)
	.await
	.expect("inherited completion stages finished")?;
	assert!(futures.is_empty(), "draining completion never starts the queued successor");
	assert!(retries.is_empty());
	assert!(statuses.is_empty());
	assert_eq!(services.sending.db.push_backoff(&failed).await?, Some(backoff));
	assert!(
		services
			.sending
			.db
			.push_backoff(&acknowledged)
			.await?
			.is_none()
	);
	assert!(
		services
			.sending
			.db
			.active_requests_for(&acknowledged)
			.try_collect::<Vec<_>>()
			.await?
			.is_empty()
	);
	assert_eq!(
		services
			.sending
			.db
			.queued_requests(&acknowledged)
			.try_collect::<Vec<_>>()
			.await?
			.len(),
		1,
		"successor admission remains owed"
	);
	futures.cancel_and_join().await;
	fixture.finish().await;
	Ok(())
}
