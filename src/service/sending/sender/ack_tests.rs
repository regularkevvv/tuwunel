//! Actual appservice responses must acknowledge only their selected durable
//! rows.

use std::{collections::BTreeMap, time::Duration};

use futures::{FutureExt, StreamExt, TryStreamExt, future::ready};
use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpListener,
	task::JoinHandle,
	time::timeout,
};
use tuwunel_core::{
	Result,
	ruma::api::appservice::{Namespaces, Registration, RegistrationInit},
};

use super::{
	CurTransactionStatus, Delivery, Destination, EduBuf, QueueRecovery, QueueRetries,
	SendingEvent, SendingFutures, WakeQueue, edu_tests::Fixture,
};
use crate::{Services, sending::data::QueueItem};

type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Clone, Copy)]
enum Mode {
	Response,
	Shutdown,
	Changed,
	Cancelled,
}

struct Endpoint {
	address: String,
	task: JoinHandle<Result<Value>>,
}
impl Drop for Endpoint {
	fn drop(&mut self) { self.task.abort(); }
}
impl Endpoint {
	async fn new() -> Result<Self> {
		let listener = TcpListener::bind("127.0.0.1:0").await?;
		let address = format!("http://{}", listener.local_addr()?);
		let task = tokio::spawn(async move {
			let (mut socket, _) = listener.accept().await?;
			let mut bytes = Vec::new();
			let mut buffer = [0_u8; 1024];
			let body = loop {
				let size = socket.read(&mut buffer).await?;
				assert!(size != 0, "request must finish");
				bytes.extend_from_slice(&buffer[..size]);
				assert!(bytes.len() <= 8192, "owned request bound");
				if let Some(start) = bytes
					.windows(4)
					.position(|part| part == b"\r\n\r\n")
				{
					let headers = std::str::from_utf8(&bytes[..start]).expect("HTTP headers");
					let length = headers
						.lines()
						.find_map(|line| {
							let (key, value) = line.split_once(':')?;
							key.eq_ignore_ascii_case("content-length")
								.then(|| {
									value
										.trim()
										.parse::<usize>()
										.expect("content length")
								})
						})
						.expect("request body length");
					let start = start.checked_add(4).expect("bounded header");
					let end = start
						.checked_add(length)
						.expect("bounded content length");
					if bytes.len() >= end {
						break serde_json::from_slice(&bytes[start..end])?;
					}
				}
			};
			socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await?;
			Ok(body)
		});
		Ok(Self { address, task })
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_transaction_preserves_unsent_active_and_pending_rows() -> Result {
	verify(Mode::Response).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_ack_preserves_unsent_active_and_pending_rows() -> Result {
	verify(Mode::Shutdown).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_selected_row_refuses_all_ack_removals_then_repairs() -> Result {
	verify(Mode::Changed).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicitly_cancelled_selected_row_does_not_expand_ack_membership() -> Result {
	verify(Mode::Cancelled).await
}

async fn rows(services: &Services, map: &str) -> Result<Rows> {
	services.db[map]
		.raw_stream()
		.map_ok(|(key, value)| (key.to_vec(), value.to_vec()))
		.try_collect()
		.await
}

async fn register(services: &Services, endpoint: &Endpoint) -> Result<Destination> {
	let mut registration: Registration = RegistrationInit {
		id: "ack-membership".into(),
		url: Some(endpoint.address.clone()),
		as_token: "disposable-ack-as-token".into(),
		hs_token: "disposable-ack-hs-token".into(),
		sender_localpart: "ack-bot".into(),
		namespaces: Namespaces::new(),
		rate_limited: None,
		protocols: None,
	}
	.into();
	registration.receive_ephemeral = true;
	services
		.appservice
		.load_appservice(registration)
		.await?;
	Ok(Destination::Appservice("ack-membership".into()))
}

async fn enqueue(services: &Services, destination: &Destination, n: usize) -> Result<QueueItem> {
	let event = SendingEvent::Edu(EduBuf::from_slice(&serde_json::to_vec(&json!({
		"type":"m.typing", "room_id":"!ack:localhost",
		"content":{"user_ids":[format!("@ack-{n}:localhost")]}
	}))?));
	let keys = services
		.sending
		.db
		.queue_requests(std::iter::once((&event, destination)))
		.await?;
	Ok((keys.into_iter().next().expect("one durable row"), event))
}

async fn verify(mode: Mode) -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let mut endpoint = Endpoint::new().await?;
	let destination = register(services, &endpoint).await?;
	let selected = [
		enqueue(services, &destination, 0).await?,
		enqueue(services, &destination, 1).await?,
	];
	services
		.sending
		.db
		.mark_as_active(selected.iter())
		.await?;
	let mut futures = SendingFutures::new();
	let mut statuses = CurTransactionStatus::new();
	services
		.sending
		.startup_netburst(services.sending.shard_id(&destination), &mut futures, &mut statuses)
		.await?;
	let response = timeout(Duration::from_secs(5), futures.next())
		.await
		.expect("actual appservice response")
		.expect("one attempt");
	assert!(
		matches!(&response, Ok(Delivery::Acknowledged(..))),
		"owned peer returned a valid ACK"
	);
	let body = timeout(Duration::from_secs(2), &mut endpoint.task)
		.await
		.expect("owned request capture")
		.expect("endpoint task")?;
	assert_eq!(
		body["ephemeral"]
			.as_array()
			.expect("selected EDUs")
			.len(),
		2
	);
	let tail = enqueue(services, &destination, 2).await?;
	services
		.sending
		.db
		.mark_as_active(std::iter::once(&tail))
		.await?;
	let pending = enqueue(services, &destination, 3).await?;
	let original = rows(services, "servercurrentevent_data").await?;
	assert_eq!(original.len(), 3);
	if matches!(mode, Mode::Cancelled) {
		services
			.sending
			.db
			.delete_active_request(&selected[0].0)
			.await?;
	}
	if matches!(mode, Mode::Changed) {
		services.db["servercurrentevent_data"]
			.insert(&selected[1].0, b"{changed".as_slice())
			.await?;
	}
	let before = rows(services, "servercurrentevent_data").await?;
	let queued = rows(services, "servernameevent_data").await?;
	assert_eq!(queued.len(), 1);
	assert!(queued.contains_key(&pending.0));
	let mut stage = QueueRecovery::ResumePending;
	if matches!(mode, Mode::Shutdown) {
		futures.push(ready(response).boxed());
		services
			.sending
			.finish_responses(&mut futures)
			.await?;
	} else {
		let result = services
			.sending
			.handle_response(
				response,
				&mut futures,
				&mut statuses,
				&mut WakeQueue::new(),
				&mut stage,
				&mut QueueRetries::new(),
			)
			.await;
		if matches!(mode, Mode::Changed) {
			result.expect_err("changed membership refuses the whole removal");
			assert_eq!(
				rows(services, "servercurrentevent_data").await?,
				before,
				"no earlier selected row removed"
			);
			services.db["servercurrentevent_data"]
				.insert(&selected[1].0, original[&selected[1].0].as_slice())
				.await?;
			services
				.sending
				.resume_queue(&destination, &mut futures, &mut statuses, &mut stage)
				.await?;
		} else {
			result?;
		}
	}
	let after = rows(services, "servercurrentevent_data").await?;
	eprintln!("ACK selected=2 unsent active before={} after={}", before.len(), after.len());
	assert_eq!(
		after,
		BTreeMap::from([(tail.0.clone(), tail.1.value_bytes().to_vec())]),
		"ACK must not delete unsent active successor"
	);
	assert_eq!(
		rows(services, "servernameevent_data").await?,
		queued,
		"pending successor untouched"
	);
	drop(futures);
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_delivery_values_require_exact_durable_membership() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let destination = Destination::Appservice("membership-only".into());
	let first = enqueue(services, &destination, 0).await?;
	let second = enqueue(services, &destination, 0).await?;
	assert_ne!(first.0, second.0);
	assert_eq!(first.1, second.1);
	services
		.sending
		.db
		.mark_as_active([&first, &second].into_iter())
		.await?;
	let before = rows(services, "servercurrentevent_data").await?;
	for count in [0, 1, 3] {
		services
			.sending
			.db
			.selected_acknowledgement(&destination, &vec![first.1.clone(); count])
			.await
			.expect_err("missing or extra duplicate cannot expand selected membership");
		assert_eq!(rows(services, "servercurrentevent_data").await?, before);
	}
	let acknowledgement = services
		.sending
		.db
		.selected_acknowledgement(&destination, &[first.1, second.1])
		.await?;
	services
		.sending
		.db
		.acknowledge_active(&Destination::Appservice("other".into()), &acknowledgement)
		.await
		.expect_err("wrong destination cannot delete selected rows");
	assert_eq!(rows(services, "servercurrentevent_data").await?, before);
	services
		.sending
		.db
		.acknowledge_active(&destination, &acknowledgement)
		.await?;
	assert!(
		rows(services, "servercurrentevent_data")
			.await?
			.is_empty()
	);
	fixture.finish().await;
	Ok(())
}

#[test]
fn refused_ack_cleanup_is_atomic_and_retry_keeps_selected_membership() -> Result {
	const CHILD: &str = "TUWUNEL_ACK_REFUSAL_TEST_CHILD";
	const TEST: &str = "sending::sender::ack_tests::refused_ack_cleanup_is_atomic_and_retry_keeps_selected_membership";
	if std::env::var(CHILD).as_deref() != Ok("refusal") {
		// The existing refusal control is process-wide. Isolate this case so
		// another fixture cannot consume its armed map.
		let status = std::process::Command::new(std::env::current_exe()?)
			.args(["--exact", TEST, "--nocapture", "--test-threads=1"])
			.env(CHILD, "refusal")
			.status()?;
		assert!(status.success(), "isolated actual-commit refusal case");
		return Ok(());
	}
	tokio::runtime::Builder::new_multi_thread()
		.worker_threads(2)
		.enable_all()
		.build()?
		.block_on(verify_refused_ack())
}

async fn verify_refused_ack() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let destination = Destination::Appservice("refused-ack".into());
	let selected = [
		enqueue(services, &destination, 0).await?,
		enqueue(services, &destination, 1).await?,
	];
	services
		.sending
		.db
		.mark_as_active(selected.iter())
		.await?;
	let acknowledgement = services
		.sending
		.db
		.selected_acknowledgement(&destination, &[selected[0].1.clone(), selected[1].1.clone()])
		.await?;
	let tail = enqueue(services, &destination, 2).await?;
	services
		.sending
		.db
		.mark_as_active(std::iter::once(&tail))
		.await?;
	enqueue(services, &destination, 3).await?;
	let active_before = rows(services, "servercurrentevent_data").await?;
	let pending_before = rows(services, "servernameevent_data").await?;
	assert_eq!(active_before.len(), 3);
	assert_eq!(pending_before.len(), 1);
	let mut futures = SendingFutures::new();
	let mut statuses = CurTransactionStatus::new();
	let mut stage = QueueRecovery::CleanupAcknowledged(acknowledgement);
	let owned_stage = stage.clone();
	tuwunel_database::refusal::refuse_next("servercurrentevent_data");
	services
		.sending
		.resume_queue(&destination, &mut futures, &mut statuses, &mut stage)
		.await
		.expect_err("actual atomic ACK commit refused before dispatch");
	assert_eq!(tuwunel_database::refusal::pending(), 0, "owned refusal consumed");
	assert_eq!(stage, owned_stage, "exact selected members survive cleanup failure");
	assert_eq!(rows(services, "servercurrentevent_data").await?, active_before);
	assert_eq!(rows(services, "servernameevent_data").await?, pending_before);
	services
		.sending
		.resume_queue(&destination, &mut futures, &mut statuses, &mut stage)
		.await?;
	assert_eq!(stage, QueueRecovery::ResumePending);
	assert_eq!(
		rows(services, "servercurrentevent_data").await?,
		BTreeMap::from([(tail.0, tail.1.value_bytes().to_vec())])
	);
	assert_eq!(rows(services, "servernameevent_data").await?, pending_before);
	assert_eq!(futures.len(), 1, "unsent tail now owns the next attempt");
	drop(futures);
	fixture.finish().await;
	Ok(())
}
