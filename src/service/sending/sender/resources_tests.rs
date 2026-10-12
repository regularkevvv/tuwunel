//! Sender admission must leave accepted rows recoverable without keeping an
//! unbounded copy of their payloads or transport tasks in memory.
use std::{iter::once, time::Duration};

use futures::{FutureExt, TryStreamExt, poll};
use tokio::{runtime::Handle, time::timeout};
use tuwunel_core::{
	Result,
	ruma::{
		api::appservice::{Namespaces, Registration, RegistrationInit},
		room_id, server_name,
	},
};
use tuwunel_database::serialize_key;

use super::{
	CurTransactionStatus, Destination, EduBuf, SendingEvent, SendingFutures, edu_tests::Fixture,
};
use crate::{Services, sending::data::RecoverySource};

const HINT_LIMIT: usize = 128;
const DELIVERY_LIMIT: usize = 16;

fn event() -> SendingEvent {
	SendingEvent::Edu(EduBuf::from_slice(br#"{"type":"m.typing","content":{"user_ids":[]}}"#))
}

async fn register(services: &Services, name: &str) -> Result<Destination> {
	let registration: Registration = RegistrationInit {
		id: name.into(),
		url: None,
		as_token: format!("disposable-{name}-as-token"),
		hs_token: format!("disposable-{name}-hs-token"),
		sender_localpart: name.into(),
		namespaces: Namespaces::new(),
		rate_limited: None,
		protocols: None,
	}
	.into();
	services
		.appservice
		.load_appservice(registration)
		.await?;
	Ok(Destination::Appservice(name.into()))
}

async fn empty(services: &Services, destination: &Destination) -> Result<bool> {
	let queued = services
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
	Ok(queued.is_empty() && active.is_empty())
}

async fn wait_empty(services: &Services, destination: &Destination) -> Result {
	loop {
		if empty(services, destination).await? {
			return Ok(());
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_sender_hints_have_a_fixed_capacity() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let dest = Destination::Appservice("hint-capacity".into());
	for _ in 0..HINT_LIMIT * 3 {
		services
			.sending
			.queue_and_dispatch(dest.clone(), event())
			.await?;
	}
	let retained = services.sending.channels[0].1.len();
	let rows = services
		.sending
		.db
		.queued_requests(&dest)
		.try_collect::<Vec<_>>()
		.await?;
	assert_eq!(rows.len(), HINT_LIMIT * 3, "overflow may drop only a hint");
	fixture.finish().await;
	assert!(retained <= HINT_LIMIT, "sender retained {retained} hints");
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_hints_do_not_retain_durable_event_payloads() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let dest = Destination::Appservice("hint-payload".into());
	let payload = SendingEvent::Edu(EduBuf::from_slice(&[b'x'; 4096]));
	services
		.sending
		.queue_and_dispatch(dest.clone(), payload.clone())
		.await?;
	let hint = services.sending.channels[0]
		.1
		.try_recv()
		.expect("one dispatch hint");
	let lean = hint.queue_id.is_empty() && hint.event.value_bytes().len() <= 1;
	let rows = services
		.sending
		.db
		.queued_requests(&dest)
		.try_collect::<Vec<_>>()
		.await?;
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].1, payload, "canonical admission retains exact bytes");
	fixture.finish().await;
	assert!(lean, "dispatch retained a second event/key copy");
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_delivery_inventory_has_a_fixed_capacity() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	for index in 0..DELIVERY_LIMIT * 3 {
		let dest = Destination::Appservice(format!("startup-capacity-{index}"));
		services
			.sending
			.db
			.queue_requests(once((&event(), &dest)))
			.await?;
	}
	let mut deliveries = SendingFutures::new();
	let mut statuses = CurTransactionStatus::new();
	services
		.sending
		.startup_netburst(0, &mut deliveries, &mut statuses)
		.await?;
	let retained = deliveries.len();
	deliveries.cancel_and_join().await;
	fixture.finish().await;
	assert!(retained <= DELIVERY_LIMIT, "startup retained {retained} deliveries");
	Ok(())
}

#[tokio::test]
async fn staged_and_running_delivery_tasks_share_one_capacity() {
	let mut deliveries = SendingFutures::new();
	for index in 0..DELIVERY_LIMIT * 3 {
		deliveries.push(
			Destination::Appservice(format!("task-capacity-{index}")),
			std::future::pending().boxed(),
			&Handle::current(),
		);
	}
	let retained = deliveries.len();
	deliveries.cancel_and_join().await;
	assert!(retained <= DELIVERY_LIMIT, "staged deliveries grew to {retained}");
}

#[tokio::test]
async fn running_tasks_reserve_capacity_against_later_staging() {
	let mut deliveries = SendingFutures::new();
	for index in 0..DELIVERY_LIMIT / 2 {
		deliveries.push(
			Destination::Appservice(format!("running-{index}")),
			std::future::pending().boxed(),
			&Handle::current(),
		);
	}
	let pending = {
		let completion = deliveries.next();
		futures::pin_mut!(completion);
		poll!(&mut completion).is_pending()
	};
	assert!(pending);
	for index in 0..DELIVERY_LIMIT * 3 {
		deliveries.push(
			Destination::Appservice(format!("staged-{index}")),
			std::future::pending().boxed(),
			&Handle::current(),
		);
	}
	let retained = deliveries.len();
	deliveries.cancel_and_join().await;
	assert_eq!(retained, DELIVERY_LIMIT, "running tasks consume the same budget");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hint_overflow_and_startup_capacity_leave_every_destination_recoverable() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let mut destinations = Vec::new();
	for index in 0..HINT_LIMIT * 2 + DELIVERY_LIMIT {
		let destination = register(services, &format!("overflow-{index:03}")).await?;
		services
			.sending
			.queue_and_dispatch(destination.clone(), event())
			.await?;
		destinations.push(destination);
	}
	let hints = services.sending.channels[0].1.len();
	let sending = services.sending.clone();
	let task = tokio::spawn(async move { sending.sender(0).await });
	let recovered = timeout(Duration::from_secs(15), async {
		for destination in &destinations {
			wait_empty(services, destination).await?;
		}
		Result::<()>::Ok(())
	})
	.await;
	services.stop().await;
	timeout(Duration::from_secs(5), task)
		.await
		.expect("owned overflow sender joined")
		.expect("overflow sender task")?;
	fixture.finish().await;
	assert_eq!(hints, HINT_LIMIT, "overflow saturates the bounded hint channel");
	recovered.expect("all accepted destinations recover despite hint overflow")?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_sender_recovers_an_accepted_row_without_any_hint() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let sentinel = register(services, "resource-sentinel").await?;
	let destination = register(services, "resource-hintless").await?;
	services
		.sending
		.queue_and_dispatch(sentinel.clone(), event())
		.await?;
	let sending = services.sending.clone();
	let task = tokio::spawn(async move { sending.sender(0).await });
	timeout(Duration::from_secs(5), wait_empty(services, &sentinel))
		.await
		.expect("sender enters response loop")?;
	// A different destination avoids the sentinel ACK's successor scan.
	services
		.sending
		.db
		.queue_requests(once((&event(), &destination)))
		.await?;
	let recovered = timeout(Duration::from_secs(5), wait_empty(services, &destination))
		.await
		.is_ok();
	services.stop().await;
	timeout(Duration::from_secs(5), task)
		.await
		.expect("owned sender joined")
		.expect("sender task")?;
	fixture.finish().await;
	assert!(recovered, "accepted row was stranded until another dispatch or restart");
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_sender_recovers_a_peers_first_source_window_without_a_hint() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let sentinel = register(services, "first-window-sentinel").await?;
	services
		.sending
		.queue_and_dispatch(sentinel.clone(), event())
		.await?;
	let sending = services.sending.clone();
	let task = tokio::spawn(async move { sending.sender(0).await });
	timeout(Duration::from_secs(5), wait_empty(services, &sentinel))
		.await
		.expect("first-window sender entered response loop")?;
	let server = server_name!("first-window.example");
	let room = room_id!("!first-window:localhost");
	assert!(
		services.db["servername_educount"]
			.get(server)
			.await
			.expect_err("no first watermark yet")
			.is_not_found()
	);
	let membership = serialize_key((server, room))?;
	services.db["serverroomids"]
		.insert(&membership, [])
		.await?;
	let retired = services.globals.current_count();
	assert!(retired > 0, "sentinel generated a retired source window");
	// Canonical membership discovers this peer even when the first flush hint
	// is absent and no prior EDU watermark or delivery row exists.
	let recovered = timeout(Duration::from_secs(8), async {
		while services
			.sending
			.db
			.get_latest_educount(server)
			.await? < retired
		{
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		Result::<()>::Ok(())
	})
	.await;
	services.stop().await;
	timeout(Duration::from_secs(5), task)
		.await
		.expect("owned first-window sender joined")
		.expect("first-window sender task")?;
	fixture.finish().await;
	recovered.expect("first source window is recoverable without a watermark or hint")?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_keeps_a_stale_retired_cut_distinct_from_a_corrupt_watermark() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let server = server_name!("retired-cut.example");
	let old_cut = services.globals.current_count();
	drop(services.globals.next_count().await?);
	let current = services.globals.current_count();
	services
		.sending
		.db
		.persist_edus(server, &[], &[], current)
		.await?;
	let page = services
		.sending
		.db
		.recovery_page(RecoverySource::Edus, None, old_cut)
		.await?;
	assert_eq!(page.len(), 1);
	assert!(page[0].1.is_none(), "another sender already advanced beyond this cut");
	services
		.sending
		.db
		.persist_edus(server, &[], &[], current.saturating_add(1))
		.await?;
	let corrupt = services
		.sending
		.db
		.recovery_page(RecoverySource::Edus, None, old_cut)
		.await
		.is_err();
	fixture.finish().await;
	assert!(corrupt, "a watermark beyond current retired writes remains corrupt");
	Ok(())
}
