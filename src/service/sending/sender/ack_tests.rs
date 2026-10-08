//! Actual appservice responses must acknowledge only their selected durable
//! rows.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::{FutureExt, StreamExt, TryStreamExt, future::ready};
use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
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
	async fn new() -> Result<Self> { Self::new_requests(1).await }

	async fn new_requests(count: usize) -> Result<Self> {
		assert!((1..=11).contains(&count), "owned peer request bound");
		let listener = TcpListener::bind("127.0.0.1:0").await?;
		let address = format!("http://{}", listener.local_addr()?);
		let task = tokio::spawn(async move {
			let mut captured = Vec::new();
			for _ in 0..count {
				let (mut socket, _) = listener.accept().await?;
				let (path, wire_body, body) = capture_request(&mut socket).await?;
				socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await?;
				captured.push(if count == 1 {
					body
				} else if wire_body.len() <= 64 * 1024 {
					json!({"path": path, "wire_body": wire_body, "wire_len": wire_body.len(), "body": body})
				} else {
					json!({"path": path, "wire_hash": tuwunel_core::utils::hash::sha256::hash(&wire_body), "wire_len": wire_body.len(), "body": body})
				});
			}
			Ok(if count == 1 {
				captured.pop().expect("owned request")
			} else {
				Value::Array(captured)
			})
		});
		Ok(Self { address, task })
	}
}

async fn capture_request(socket: &mut TcpStream) -> Result<(String, Vec<u8>, Value)> {
	let mut bytes = Vec::new();
	let mut buffer = [0_u8; 1024];
	loop {
		let size = socket.read(&mut buffer).await?;
		assert!(size != 0, "request must finish");
		bytes.extend_from_slice(&buffer[..size]);
		assert!(bytes.len() <= 4 * 1024 * 1024, "owned request bound");
		if let Some(start) = bytes
			.windows(4)
			.position(|part| part == b"\r\n\r\n")
		{
			let headers = std::str::from_utf8(&bytes[..start]).expect("HTTP headers");
			let path = headers
				.lines()
				.next()
				.expect("request line")
				.split_whitespace()
				.nth(1)
				.expect("request URI")
				.split('?')
				.next()
				.expect("request path")
				.to_owned();
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
				return Ok((
					path,
					bytes[start..end].to_vec(),
					serde_json::from_slice(&bytes[start..end])?,
				));
			}
		}
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

pub(super) async fn rows(services: &Services, map: &str) -> Result<Rows> {
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

pub(super) async fn enqueue(
	services: &Services,
	destination: &Destination,
	n: usize,
) -> Result<QueueItem> {
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

#[test]
fn stopped_service_graph_releases_root_and_database_without_process_exit() -> Result {
	super::incarnation_tests::isolated(
		"sending::sender::ack_tests::stopped_service_graph_releases_root_and_database_without_process_exit",
		async {
			let fixture = Fixture::new().await?;
			let root = fixture.services.server.config.database_path.parent().expect("owned fixture directory").to_path_buf();
			let services = Arc::downgrade(&fixture.services);
			let database = Arc::downgrade(&fixture.services.db);
			let access = fixture.services.sending.services.clone();
			fixture.services.db["global"].insert(b"owned-service-lifecycle", b"durable").await?;
			fixture.finish().await;
			assert_eq!(services.strong_count(), 0, "stopped graph must release the root even with a retained service-access handle");
			assert_eq!(database.strong_count(), 0, "stopped graph must release its database");
			assert!(access.try_get().is_none(), "retained access handle no longer owns a stopped graph");
			let reopened = Fixture::open(&root).await?;
			assert_eq!(reopened.services.db["global"].get(b"owned-service-lifecycle").await?.as_ref(), b"durable");
			let reopened_services = Arc::downgrade(&reopened.services);
			let reopened_database = Arc::downgrade(&reopened.services.db);
			reopened.finish().await;
			assert_eq!(reopened_services.strong_count(), 0);
			assert_eq!(reopened_database.strong_count(), 0);
			Ok(())
		},
	)
}

#[test]
fn lazy_state_queries_retain_root_until_consumed_or_cancelled() -> Result {
	super::incarnation_tests::isolated(
		"sending::sender::ack_tests::lazy_state_queries_retain_root_until_consumed_or_cancelled",
		async {
			for poll_query in [false, true] {
				let fixture = Fixture::new().await?;
				let root = Arc::downgrade(&fixture.services);
				let database = Arc::downgrade(&fixture.services.db);
				let accessor = fixture.services.state_accessor.clone();
				let mut query = Box::pin(accessor.state_full_entries_strict(0));
				fixture.finish().await;
				assert!(root.upgrade().is_some(), "unpolled lazy query retains its owner");
				if poll_query {
					query
						.next()
						.await
						.expect("missing state yields an error")
						.expect_err("missing snapshot is refused");
				}
				drop(query);
				assert_eq!(
					root.strong_count(),
					0,
					"finished or cancelled query releases its root"
				);
				drop(accessor);
				assert_eq!(database.strong_count(), 0, "query and accessor release the database");
			}
			Ok(())
		},
	)
}

#[test]
fn splitting_membership_refuses_existing_journal_without_mutation() -> Result {
	super::incarnation_tests::isolated(
		"sending::sender::ack_tests::splitting_membership_refuses_existing_journal_without_mutation",
		async {
			let fixture = Fixture::new().await?;
			let services = &fixture.services;
			let data = &services.sending.db;
			let endpoint = Endpoint::new().await?;
			let destination = register(services, &endpoint).await?;
			for n in 0..2 {
				let item = enqueue(services, &destination, n).await?;
				data.mark_as_active(std::iter::once(&item)).await?;
			}
			let (_, rows) = data.active_batch(&destination).await?;
			let registration = services.appservice.get_registration("ack-membership").await.expect("owned registration");
			let attempt = data.persist_attempt(&destination, rows, serde_json::to_vec(&json!({"events":[]}))?, Some(super::super::data::appservice_owner(&registration)?)).await?;
			let before = task_failure_snapshot(services).await?;
			let mut selected = attempt.acknowledgement.clone();
			selected.retain_prefix(1).expect_err("persisted transaction may not split");
			assert_eq!(selected, attempt.acknowledgement);
			assert_eq!(task_failure_snapshot(services).await?, before);
			fixture.finish().await;
			Ok(())
		},
	)
}

#[test]
fn valid_active_values_split_when_the_http_envelope_exceeds_the_wire_limit() -> Result {
	super::incarnation_tests::isolated(
		"sending::sender::ack_tests::valid_active_values_split_when_the_http_envelope_exceeds_the_wire_limit",
		async {
			let fixture = Fixture::new().await?;
			let services = &fixture.services;
			let data = &services.sending.db;
			let mut endpoint = Endpoint::new_requests(2).await?;
			let destination = register(services, &endpoint).await?;
			let mut value = json!({"type":"example.large", "content":{"value":""}});
			let empty_len = serde_json::to_vec(&value)?.len();
			value["content"]["value"] = Value::String("x".repeat(1_572_854_usize.checked_sub(empty_len).expect("valid fixture width")));
			let event = SendingEvent::Edu(EduBuf::from_slice(&serde_json::to_vec(&value)?));
			for _ in 0..2 {
				let keys = data.queue_requests(std::iter::once((&event, &destination))).await?;
				data.mark_as_active(std::iter::once(&(keys[0].clone(), event.clone()))).await?;
			}
			let mut owed = rows(services, "servercurrentevent_data").await?;
			assert_eq!(owed.values().map(Vec::len).sum::<usize>(), 3 * 1024 * 1024, "both individually valid stored values fit the active budget exactly");
			let (events, selected) = data.active_batch(&destination).await?;
			assert_eq!(events.len(), 2);
			assert_eq!(selected.selected_rows().len(), 2);
			let mut futures = SendingFutures::new();
			let mut statuses = CurTransactionStatus::new();
			let mut wakes = WakeQueue::new();
			let mut retries = QueueRetries::new();
			let mut stage = QueueRecovery::ResumePending;
			services.sending.resume_queue(&destination, &mut futures, &mut statuses, &mut stage).await?;
			for _ in 0..2 {
				let response = timeout(Duration::from_secs(10), futures.next()).await.expect("bounded actual large HTTP delivery").expect("next accepted attempt");
				let Ok(Delivery::Acknowledged(owner, selected)) = &response else {
					panic!("valid accepted rows must reach the peer after splitting the envelope: {response:?}");
				};
				assert_eq!(owner, &destination);
				assert_eq!(selected.selected_rows().len(), 1, "each fitting attempt owns one physical admission");
				let (key, value) = &selected.selected_rows()[0];
				let original = owed.remove(key).expect("selected accepted key");
				assert_eq!(tuwunel_core::utils::hash::sha256::hash(value), tuwunel_core::utils::hash::sha256::hash(&original));
				services.sending.handle_response(response, &mut futures, &mut statuses, &mut wakes, &mut stage, &mut retries).await?;
				let retained = rows(services, "servercurrentevent_data").await?;
				assert_eq!(retained.keys().collect::<Vec<_>>(), owed.keys().collect::<Vec<_>>(), "unsent admission stays accepted");
				assert!(retries.is_empty());
			}
			assert!(futures.is_empty());
			assert!(data.load_attempt(&destination).await?.is_none());
			let received = timeout(Duration::from_secs(2), &mut endpoint.task).await.expect("owned peer capture").expect("owned peer finished")?;
			let received = received.as_array().expect("two actual large deliveries");
			assert_eq!(received.len(), 2);
			assert_ne!(received[0]["path"], received[1]["path"], "distinct durable transaction IDs");
			for request in received {
				assert!(request["wire_len"].as_u64().expect("actual body bytes") <= 3 * 1024 * 1024);
				assert_eq!(request["body"]["ephemeral"].as_array().expect("EDUs").len(), 1);
				assert_eq!(request["body"]["ephemeral"][0]["content"]["value"].as_str().expect("preserved value").len(), 1_572_854_usize.checked_sub(empty_len).expect("valid width"));
			}
			futures.cancel_and_join().await;
			fixture.finish().await;
			Ok(())
		},
	)
}

#[test]
fn active_backlog_drains_in_bounded_transactions_without_new_hint() -> Result {
	super::incarnation_tests::isolated(
		"sending::sender::ack_tests::active_backlog_drains_in_bounded_transactions_without_new_hint",
		verify_bounded_backlog(97),
	)
}

#[test]
fn active_backlog_larger_than_journal_limit_keeps_progress() -> Result {
	super::incarnation_tests::isolated(
		"sending::sender::ack_tests::active_backlog_larger_than_journal_limit_keeps_progress",
		verify_bounded_backlog(513),
	)
}

async fn verify_bounded_backlog(count: usize) -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let data = &services.sending.db;
	let mut endpoint = Endpoint::new_requests(count.div_ceil(48)).await?;
	let destination = register(services, &endpoint).await?;
	// Equal logical events have distinct physical admissions. A bounded
	// transaction must retain every independently admitted duplicate.
	for _ in 0..count {
		let item = enqueue(services, &destination, 0).await?;
		data.mark_as_active(std::iter::once(&item))
			.await?;
	}
	let mut owed = rows(services, "servercurrentevent_data").await?;
	assert_eq!(owed.len(), count);
	let mut futures = SendingFutures::new();
	let mut statuses = CurTransactionStatus::new();
	let mut wakes = WakeQueue::new();
	let mut retries = QueueRetries::new();
	let mut stage = QueueRecovery::ResumePending;
	services
		.sending
		.resume_queue(&destination, &mut futures, &mut statuses, &mut stage)
		.await?;
	let counts = (0..count)
		.step_by(48)
		.map(|offset| {
			count
				.checked_sub(offset)
				.expect("batch offset within accepted count")
				.min(48)
		})
		.collect::<Vec<_>>();
	for &expected_count in &counts {
		let response = timeout(Duration::from_secs(5), futures.next())
			.await
			.expect("bounded actual HTTP delivery")
			.expect("next accepted batch");
		let Ok(Delivery::Acknowledged(owner, selected)) = &response else {
			panic!("accepted active backlog must reach the owned HTTP peer: {response:?}");
		};
		assert_eq!(owner, &destination);
		assert!(
			selected.selected_rows().len() <= 48,
			"active backlog exceeds the transaction member budget: {}",
			selected.selected_rows().len()
		);
		assert_eq!(selected.selected_rows().len(), expected_count);
		let expected = owed
			.iter()
			.take(expected_count)
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect::<Vec<_>>();
		assert_eq!(
			selected.selected_rows(),
			expected,
			"ACK owns exact ordered physical rows, including duplicate values"
		);
		for (key, value) in selected.selected_rows() {
			assert_eq!(owed.remove(key).as_ref(), Some(value));
		}
		services
			.sending
			.handle_response(
				response,
				&mut futures,
				&mut statuses,
				&mut wakes,
				&mut stage,
				&mut retries,
			)
			.await?;
		assert_eq!(
			rows(services, "servercurrentevent_data").await?,
			owed,
			"unsent physical successors remain accepted"
		);
		assert!(
			rows(services, "servernameevent_data")
				.await?
				.is_empty()
		);
		assert!(retries.is_empty());
	}
	assert!(futures.is_empty());
	assert!(statuses.is_empty());
	assert!(data.load_attempt(&destination).await?.is_none());
	let captured = timeout(Duration::from_secs(2), &mut endpoint.task)
		.await
		.expect("owned peer capture")
		.expect("owned peer task")?;
	let captured = captured.as_array().expect("actual transactions");
	assert_eq!(
		captured
			.iter()
			.map(|request| request["body"]["ephemeral"]
				.as_array()
				.expect("EDUs")
				.len())
			.collect::<Vec<_>>(),
		counts
	);
	let paths = captured
		.iter()
		.map(|request| {
			request["path"]
				.as_str()
				.expect("transaction path")
		})
		.collect::<std::collections::BTreeSet<_>>();
	assert_eq!(
		paths.len(),
		count.div_ceil(48),
		"each batch owns a distinct durable transaction ID"
	);
	futures.cancel_and_join().await;
	fixture.finish().await;
	Ok(())
}

#[test]
fn active_batch_byte_budget_retains_large_successors() -> Result {
	super::incarnation_tests::isolated(
		"sending::sender::ack_tests::active_batch_byte_budget_retains_large_successors",
		async {
			let fixture = Fixture::new().await?;
			let services = &fixture.services;
			let destination = Destination::Appservice("large-active-values".into());
			let event = SendingEvent::Edu(EduBuf::from_slice(&serde_json::to_vec(
				&json!({"type":"example.large", "content":{"value":"x".repeat(1_600_000)}}),
			)?));
			for _ in 0..2 {
				let keys = services
					.sending
					.db
					.queue_requests(std::iter::once((&event, &destination)))
					.await?;
				services
					.sending
					.db
					.mark_as_active(std::iter::once(&(keys[0].clone(), event.clone())))
					.await?;
			}
			let before = rows(services, "servercurrentevent_data").await?;
			let (events, selected) = services
				.sending
				.db
				.active_batch(&destination)
				.await?;
			assert_eq!(events, [event]);
			assert_eq!(
				selected.selected_rows().len(),
				1,
				"a valid individual large row is selected while its successor exceeds the \
				 shared byte budget"
			);
			assert_eq!(
				rows(services, "servercurrentevent_data").await?,
				before,
				"selection does not mutate accepted work"
			);
			services
				.sending
				.db
				.acknowledge_active(&destination, &selected)
				.await?;
			let remaining = rows(services, "servercurrentevent_data").await?;
			assert_eq!(remaining.len(), 1);
			let (_, next) = services
				.sending
				.db
				.active_batch(&destination)
				.await?;
			assert_eq!(next.selected_rows(), &remaining.into_iter().collect::<Vec<_>>());
			fixture.finish().await;
			Ok(())
		},
	)
}

#[test]
fn active_batch_key_budget_retains_long_key_successors() -> Result {
	super::incarnation_tests::isolated(
		"sending::sender::ack_tests::active_batch_key_budget_retains_long_key_successors",
		async {
			let fixture = Fixture::new().await?;
			let services = &fixture.services;
			let destination = Destination::Appservice("x".repeat(14 * 1024));
			for _ in 0..10 {
				let item = enqueue(services, &destination, 0).await?;
				services
					.sending
					.db
					.mark_as_active(std::iter::once(&item))
					.await?;
			}
			let before = rows(services, "servercurrentevent_data").await?;
			let (_, selected) = services
				.sending
				.db
				.active_batch(&destination)
				.await?;
			assert_eq!(
				selected.selected_rows().len(),
				9,
				"valid long keys stay within the header byte budget"
			);
			assert_eq!(rows(services, "servercurrentevent_data").await?, before);
			services
				.sending
				.db
				.acknowledge_active(&destination, &selected)
				.await?;
			let remaining = rows(services, "servercurrentevent_data").await?;
			assert_eq!(remaining.len(), 1);
			let (_, next) = services
				.sending
				.db
				.active_batch(&destination)
				.await?;
			assert_eq!(next.selected_rows(), &remaining.into_iter().collect::<Vec<_>>());
			fixture.finish().await;
			Ok(())
		},
	)
}

#[test]
fn failed_task_preserves_attempt_and_recovers_without_new_hint() -> Result {
	super::incarnation_tests::isolated(
		"sending::sender::ack_tests::failed_task_preserves_attempt_and_recovers_without_new_hint",
		async {
			let fixture = Fixture::new().await?;
			let services = &fixture.services;
			let data = &services.sending.db;
			let mut endpoint = Endpoint::new_requests(2).await?;
			let destination = register(services, &endpoint).await?;
			let selected = enqueue(services, &destination, 0).await?;
			data.mark_as_active(std::iter::once(&selected))
				.await?;
			let acknowledgement = data
				.selected_acknowledgement(&destination, &[selected.1])
				.await?;
			let registration = services
				.appservice
				.get_registration("ack-membership")
				.await
				.expect("owned registration");
			let body = json!({"events": [], "ephemeral": [{"type": "m.typing", "room_id": "!ack:localhost", "content": {"user_ids": ["@ack-0:localhost"]}}]});
			let attempt = data
				.persist_attempt(
					&destination,
					acknowledgement,
					serde_json::to_vec(&body)?,
					Some(super::super::data::appservice_owner(&registration)?),
				)
				.await?;
			// Independently admitted active/pending successors are outside the
			// persisted attempt, even when a local task loses its completion.
			let tail = enqueue(services, &destination, 1).await?;
			data.mark_as_active(std::iter::once(&tail))
				.await?;
			enqueue(services, &destination, 2).await?;
			let before = task_failure_snapshot(services).await?;
			let mut futures = SendingFutures::new();
			let mut statuses = CurTransactionStatus::new();
			let mut wakes = WakeQueue::new();
			let mut retries = QueueRetries::new();
			let mut stage = QueueRecovery::ResumePending;
			for after_http in [false, true] {
				statuses.insert(destination.clone(), super::TransactionStatus::Running);
				let failed = if after_http {
					services
						.sending
						.send_events(destination.clone(), vec![SendingEvent::Flush])
						.then(|outcome| async move {
							assert!(
								matches!(outcome, Ok(Delivery::Acknowledged(..))),
								"owned peer accepted before task failure"
							);
							futures::future::poll_fn(
								|_| -> std::task::Poll<super::SendingResult> {
									panic!("owned task lost completion after HTTP success")
								},
							)
							.await
						})
						.boxed()
				} else {
					futures::future::poll_fn(|_| -> std::task::Poll<super::SendingResult> {
						panic!("owned delivery task failure")
					})
					.boxed()
				};
				futures.push(destination.clone(), failed, services.server.runtime());
				let response = timeout(Duration::from_secs(2), futures.next())
					.await
					.expect("bounded failed task completion")
					.expect("owned task outcome");
				services
					.sending
					.handle_response(
						response,
						&mut futures,
						&mut statuses,
						&mut wakes,
						&mut stage,
						&mut retries,
					)
					.await?;
				assert_eq!(
					task_failure_snapshot(services).await?,
					before,
					"a local task failure cannot retire or rewrite accepted work"
				);
				assert_eq!(
					retries.len(),
					1,
					"task failure must arm one local retry without a new dispatch hint"
				);
				assert_eq!(retries[&destination].1, QueueRecovery::ResumePending);
				assert!(!statuses.contains_key(&destination), "no remote failure/backoff status");
				assert!(wakes.is_empty(), "no remote retry timer");
			}
			retries
				.get_mut(&destination)
				.expect("owned local retry")
				.0 = tokio::time::Instant::now();
			services
				.sending
				.retry_queue(&mut futures, &mut statuses, &mut retries)
				.await?;
			let response = timeout(Duration::from_secs(5), futures.next())
				.await
				.expect("local timer recovers delivery")
				.expect("owned replay outcome");
			let Ok(Delivery::Acknowledged(owner, acknowledged)) = response else {
				panic!("local retry must reach the actual HTTP peer");
			};
			assert_eq!(owner, destination);
			assert_eq!(
				acknowledged, attempt.acknowledgement,
				"retry retains the original generation and membership"
			);
			let received = timeout(Duration::from_secs(2), &mut endpoint.task)
				.await
				.expect("owned HTTP capture")
				.expect("owned peer finished")?;
			assert_task_replay(&received, &body, &attempt.transaction_id());
			data.acknowledge_active(&destination, &acknowledged)
				.await?;
			let active = rows(services, "servercurrentevent_data").await?;
			assert_eq!(active.len(), 1, "unattempted active successor remains owed");
			assert_eq!(active.get(&tail.0), before["servercurrentevent_data"].get(&tail.0));
			assert_eq!(
				rows(services, "servernameevent_data").await?,
				before["servernameevent_data"]
			);
			assert!(
				data.load_attempt(&destination).await?.is_none(),
				"only the acknowledged journal retires"
			);
			futures.cancel_and_join().await;
			fixture.finish().await;
			Ok(())
		},
	)
}

fn assert_task_replay(received: &Value, body: &Value, transaction_id: &str) {
	let captured = received
		.as_array()
		.expect("two actual HTTP deliveries");
	assert_eq!(captured.len(), 2);
	assert_eq!(
		captured[0]["path"], captured[1]["path"],
		"retry retains transaction ID after lost task completion"
	);
	assert!(
		captured[0]["path"]
			.as_str()
			.expect("actual path")
			.ends_with(&format!("/transactions/{transaction_id}"))
	);
	assert_eq!(
		captured[0]["wire_body"], captured[1]["wire_body"],
		"retry retains exact wire bytes after HTTP success"
	);
	assert_eq!(&captured[1]["body"], body, "retry excludes successors");
}

async fn task_failure_snapshot(services: &Services) -> Result<BTreeMap<&'static str, Rows>> {
	let mut snapshot = BTreeMap::new();
	for map in [
		"global",
		"servercurrentevent_data",
		"servernameevent_data",
		"sendingtransaction_record",
		"servername_status",
	] {
		snapshot.insert(map, rows(services, map).await?);
	}
	Ok(snapshot)
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
		futures.push(destination.clone(), ready(response).boxed(), services.server.runtime());
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
		BTreeMap::from([(tail.0.clone(), original[&tail.0].clone())]),
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
		BTreeMap::from([(tail.0.clone(), active_before[&tail.0].clone())])
	);
	assert_eq!(rows(services, "servernameevent_data").await?, pending_before);
	assert_eq!(futures.len(), 1, "unsent tail now owns the next attempt");
	drop(futures);
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_ack_cannot_remove_same_key_and_bytes_readmitted_after_cancellation() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let destination = Destination::Appservice("readmitted-pdu".into());
	let mut bytes = [0_u8; 16];
	bytes[7] = 1;
	bytes[15] = 1;
	let event = SendingEvent::Pdu(crate::rooms::timeline::RawPduId::from_bytes(&bytes)?);
	let first = services
		.sending
		.db
		.queue_requests(std::iter::once((&event, &destination)))
		.await?;
	let original = (first[0].clone(), event.clone());
	services
		.sending
		.db
		.mark_as_active(std::iter::once(&original))
		.await?;
	let acknowledgement = services
		.sending
		.db
		.selected_acknowledgement(&destination, std::slice::from_ref(&event))
		.await?;
	services
		.sending
		.db
		.delete_all_requests_for(&destination)
		.await?;
	let readmitted = services
		.sending
		.db
		.queue_requests(std::iter::once((&event, &destination)))
		.await?;
	assert_eq!(readmitted, first, "actual PDU admission reuses the same queue key");
	services
		.sending
		.db
		.mark_as_active(std::iter::once(&original))
		.await?;
	let before = rows(services, "servercurrentevent_data").await?;
	assert_eq!(before.len(), 1);
	services
		.sending
		.db
		.acknowledge_active(&destination, &acknowledgement)
		.await?;
	assert_eq!(
		rows(services, "servercurrentevent_data").await?,
		before,
		"old acknowledgement must preserve the new same-key delivery"
	);
	let own_ack = services
		.sending
		.db
		.selected_acknowledgement(&destination, std::slice::from_ref(&event))
		.await?;
	services
		.sending
		.db
		.acknowledge_active(&destination, &own_ack)
		.await?;
	assert!(
		rows(services, "servercurrentevent_data")
			.await?
			.is_empty()
	);
	fixture.finish().await;
	Ok(())
}
