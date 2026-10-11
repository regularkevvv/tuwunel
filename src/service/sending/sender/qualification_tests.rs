//! Explicit sender resource/fault qualification. The loopback gateway and
//! bridge are owned test processes, never provider acceptance evidence.
use std::{
	collections::BTreeMap,
	fs,
	process::Command,
	sync::Arc,
	time::{Duration, Instant},
};

use futures::{FutureExt, TryStreamExt, poll};
use serde_json::json;
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	runtime::Handle,
	sync::{mpsc, watch},
	task::{JoinHandle, JoinSet},
	time::timeout,
};
use tracing::subscriber::NoSubscriber;
use tuwunel_core::{
	Result, Server,
	config::{Config, Figment, Sources},
	log::{LogLevelReloadHandles, Logging, capture::State},
	metrics::Metrics,
	ruma::{
		api::appservice::{Namespaces, Registration, RegistrationInit},
		user_id,
	},
	utils::{rand, sys},
};

use super::{
	CurTransactionStatus, Delivery, Destination, EduBuf, QueueRecovery, QueueRetries,
	SendingEvent, SendingFutures, edu_tests::Fixture,
};
use crate::{
	Services,
	bridge_fixture::{CommitStage, Fake, Faults},
};

const CHILD: &str = "TUWUNEL_SENDER_QUALIFICATION_CHILD";
const ROOT: &str = "TUWUNEL_SENDER_QUALIFICATION_ROOT";
const VERIFY: &str = "TUWUNEL_SENDER_QUALIFICATION_VERIFY_URL";
const VERIFY_EPOCH: &str = "TUWUNEL_SENDER_QUALIFICATION_VERIFY_EPOCH";

/// Separate processes avoid sharing backend globals and give VmHWM a useful
/// isolated denominator. Ignored stress must also execute in its child.
fn isolated(test: &str, ignored: bool, exercise: impl Future<Output = Result>) -> Result {
	if std::env::var(CHILD).as_deref() == Ok(test) {
		return tokio::runtime::Builder::new_multi_thread()
			.worker_threads(4)
			.enable_all()
			.build()?
			.block_on(exercise);
	}
	let root = std::env::temp_dir().join(format!("sender-qualification-{}", rand::string(20)));
	fs::DirBuilder::new().create(&root)?;
	let mut command = Command::new(std::env::current_exe()?);
	command
		.args(["--exact", test, "--nocapture", "--test-threads=1"])
		.env(CHILD, test)
		.env(ROOT, &root);
	if ignored {
		command.arg("--ignored");
	}
	let mut child = command.spawn()?;
	let deadline = Instant::now()
		.checked_add(Duration::from_mins(3))
		.expect("bounded child deadline");
	let status = loop {
		if let Some(status) = child.try_wait()? {
			break status;
		}
		if Instant::now() >= deadline {
			child.kill()?;
			child.wait()?;
			panic!("owned sender qualification exceeded its deadline");
		}
		std::thread::sleep(Duration::from_millis(20));
	};
	fs::remove_dir_all(root)?;
	assert!(status.success(), "sender qualification child failed: {status}");
	Ok(())
}

async fn fixture(remote: Option<&str>, delivery_timeout: u64) -> Result<Fixture> {
	sys::maximize_fd_limit()?;
	let root = std::path::PathBuf::from(std::env::var(ROOT).expect("owned scratch root"));
	let mut raw = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_backend", "rocksdb"))
		.merge(("database_path", root.join("database")))
		.merge(("sender_workers", 4_usize))
		.merge(("sender_timeout", delivery_timeout))
		.merge(("sender_shutdown_timeout", 1_u64))
		.merge(("startup_netburst", true))
		.merge(("startup_netburst_keep", -1))
		// Only this owned loopback fixture admits loopback push gateways.
		.merge(("ip_range_denylist", Vec::<String>::new()));
	if let Some(url) = remote {
		raw = raw
			.merge(("database_backend", "d1"))
			.merge(("d1_bridge_url", url))
			.merge(("d1_bridge_token", crate::bridge_fixture::TOKEN))
			.merge(("d1_read_cache_mb", 1))
			.merge(("d1_lease_ttl_ms", 15_000));
	}
	let runtime = Handle::current();
	let log = Logging {
		subscriber: Arc::new(NoSubscriber::new()),
		reload: LogLevelReloadHandles::default(),
		capture: Arc::new(State::new()),
	};
	let server = Arc::new(Server::new(
		Config::new(&raw)?,
		Sources::default(),
		Some(&runtime),
		log,
		Metrics::new(Some(&runtime)),
	));
	let services = Services::build(server).await?;
	services
		.globals
		.db
		.bump_database_version(crate::migrations::DATABASE_VERSION)
		.await?;
	Ok(Fixture { services })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Capture {
	path: String,
	bytes: usize,
	digest: u64,
}

/// Streaming capture avoids retaining another large body per held request.
/// The digest is a fixture equality checksum, not a security primitive.
async fn capture(socket: &mut TcpStream) -> Result<Capture> {
	let mut header = Vec::new();
	let mut byte = [0_u8; 1];
	while !header.ends_with(b"\r\n\r\n") {
		assert!(header.len() < 8192, "owned header limit");
		socket.read_exact(&mut byte).await?;
		header.push(byte[0]);
	}
	let header = std::str::from_utf8(&header).expect("HTTP header");
	let path = header
		.lines()
		.next()
		.expect("request line")
		.split_whitespace()
		.nth(1)
		.expect("request path")
		.split('?')
		.next()
		.expect("path")
		.to_owned();
	let bytes = header
		.lines()
		.find_map(|line| {
			let (name, value) = line.split_once(':')?;
			name.eq_ignore_ascii_case("content-length")
				.then(|| {
					value
						.trim()
						.parse::<usize>()
						.expect("body length")
				})
		})
		.expect("bounded request body");
	assert!(bytes <= 3 * 1024 * 1024, "owned wire body limit");
	let mut left = bytes;
	let mut buffer = vec![0_u8; 8192].into_boxed_slice();
	let mut digest = 0xCBF2_9CE4_8422_2325_u64;
	while left > 0 {
		let size = left.min(buffer.len());
		socket.read_exact(&mut buffer[..size]).await?;
		for byte in &buffer[..size] {
			digest = (digest ^ u64::from(*byte)).wrapping_mul(0x100_0000_01B3);
		}
		left = left.saturating_sub(size);
	}
	Ok(Capture { path, bytes, digest })
}

struct Gateway {
	url: String,
	captures: mpsc::Receiver<Capture>,
	release: watch::Sender<bool>,
	task: JoinHandle<Result>,
}
impl Drop for Gateway {
	fn drop(&mut self) { self.task.abort(); }
}
impl Gateway {
	async fn new(count: usize, held: usize, first_failure: bool) -> Result<Self> {
		assert!((1..=128).contains(&count) && held <= count);
		let listener = TcpListener::bind("127.0.0.1:0").await?;
		let url = format!("http://{}", listener.local_addr()?);
		let (send, captures) = mpsc::channel(count);
		let (release, released) = watch::channel(false);
		let task = tokio::spawn(async move {
			let mut requests = JoinSet::new();
			for index in 0..count {
				let (mut socket, _) = listener.accept().await?;
				let send = send.clone();
				let mut released = released.clone();
				requests.spawn(async move {
					let request = capture(&mut socket).await?;
					send.send(request).await.expect("owned capture receiver");
					if index < held { released.wait_for(|ready| *ready).await.expect("owned release"); }
					let response: &[u8] = if first_failure && index == 0 {
						b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
					} else {
						b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"rejected\":[]}"
					};
					// The held first wave intentionally loses its HTTP callers.
					if index < held { socket.write_all(response).await.ok(); }
					else { socket.write_all(response).await?; }
					Result::<()>::Ok(())
				});
			}
			drop(send);
			while let Some(result) = requests.join_next().await {
				result.expect("owned gateway task")?;
			}
			Ok(())
		});
		Ok(Self { url, captures, release, task })
	}

	async fn next(&mut self) -> Capture {
		timeout(Duration::from_secs(30), self.captures.recv())
			.await
			.expect("gateway request deadline")
			.expect("actual gateway request")
	}
}

async fn admit(services: &Services, destination: &Destination, event: &SendingEvent) -> Result {
	let keys = services
		.sending
		.db
		.queue_requests(std::iter::once((event, destination)))
		.await?;
	services
		.sending
		.db
		.mark_as_active(std::iter::once(&(keys[0].clone(), event.clone())))
		.await
}

async fn count(services: &Services, map: &str) -> Result<usize> {
	services.db[map]
		.raw_keys()
		.try_fold(0_usize, |count, _| async move { Ok(count.saturating_add(1)) })
		.await
}

fn report(case: &str, started: Instant, values: &serde_json::Value) {
	let high_water = fs::read_to_string("/proc/self/status")
		.ok()
		.and_then(|status| {
			status.lines().find_map(|line| {
				line.strip_prefix("VmHWM:")?
					.split_whitespace()
					.next()?
					.parse::<u64>()
					.ok()
			})
		});
	println!(
		"\n{}",
		json!({"phase":"sender-qualification", "case":case,
		"source_base":"d9aab89d9dddce75f2470c5229871af780114a1b",
		"source_revision":std::env::var("TUWUNEL_QUALIFICATION_REVISION").ok(),
		"profile":if cfg!(debug_assertions) {"debug"} else {"release"},
		"os":std::env::consts::OS,"arch":std::env::consts::ARCH,
		"elapsed_ms":started.elapsed().as_millis(),"linux_vm_hwm_kib":high_water,
		"memory_scope":"entire isolated test process, including native database and loopback peer; not Container/provider usage",
		"provider_acceptance":false,"measurements":values})
	);
}

#[test]
#[ignore = "explicit ~180MB durable-body sender inventory stress; run alone with --ignored \
            --nocapture"]
fn maximum_sender_inventories_recover_large_held_http_after_bounded_shutdown() -> Result {
	isolated("sending::sender::qualification_tests::maximum_sender_inventories_recover_large_held_http_after_bounded_shutdown", true, stress())
}

#[expect(
	clippy::too_many_lines,
	reason = "one bounded admission/cancellation/replay qualification sequence"
)]
async fn stress() -> Result {
	let started = Instant::now();
	let fixture = fixture(None, 120).await?;
	let services = &fixture.services;
	let workers = crate::sending::sender_count(usize::MAX, usize::MAX, usize::MAX);
	let slots = super::deliveries::DELIVERY_LIMIT;
	assert_eq!((workers, slots), (4, 16));
	let deliveries = workers
		.checked_mul(slots)
		.expect("bounded inventory count");
	let rows_expected = deliveries
		.checked_mul(2)
		.expect("bounded physical row count");
	let mut gateway = Gateway::new(rows_expected, deliveries, false).await?;
	let mut body = json!({"type":"example.large", "content":{"value":""}});
	body["content"]["value"] = "x".repeat(1_400_000).into();
	let event = SendingEvent::Edu(EduBuf::from_slice(&serde_json::to_vec(&body)?));
	let payload_bytes_per_row = event.value_bytes().len();
	drop(body);
	let mut destinations = Vec::new();
	let mut inventories = Vec::new();
	for worker in 0..workers {
		let mut futures = SendingFutures::new();
		let mut statuses = CurTransactionStatus::new();
		for slot in 0..slots {
			let id = format!("qualification-{worker}-{slot}");
			let mut registration: Registration = RegistrationInit {
				id: id.clone(),
				url: Some(gateway.url.clone()),
				as_token: format!("owned-as-token-{id}"),
				hs_token: format!("owned-hs-token-{id}"),
				sender_localpart: id.clone(),
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
			let destination = Destination::Appservice(id);
			for _ in 0..2 {
				admit(services, &destination, &event).await?;
			}
			services
				.sending
				.resume_queue(
					&destination,
					&mut futures,
					&mut statuses,
					&mut QueueRecovery::ResumePending,
				)
				.await?;
			destinations.push(destination);
		}
		assert_eq!(futures.len(), slots);
		assert!(!futures.push(
			Destination::Appservice("over-capacity".into()),
			std::future::pending().boxed(),
			&Handle::current()
		));
		let pending = {
			let completion = futures.next();
			futures::pin_mut!(completion);
			poll!(&mut completion).is_pending()
		};
		assert!(pending, "owned gateway holds the full first wave");
		inventories.push((futures, statuses));
	}
	drop(event);
	let mut first = BTreeMap::new();
	for _ in 0..deliveries {
		let captured = gateway.next().await;
		assert!(captured.bytes > 2_800_000);
		assert!(
			first
				.insert(captured.path.clone(), captured)
				.is_none()
		);
	}
	assert_eq!(count(services, "servercurrentevent_data").await?, rows_expected);
	let drain = Instant::now();
	for (futures, statuses) in &mut inventories {
		timeout(
			Duration::from_secs(3),
			services
				.sending
				.finish_responses(futures, statuses, &mut QueueRetries::new()),
		)
		.await
		.expect("configured one-second shutdown drain")?;
		timeout(Duration::from_secs(2), futures.cancel_and_join())
			.await
			.expect("joined held HTTP tasks");
		assert!(futures.is_empty());
		statuses.clear();
	}
	let shutdown_ms = drain.elapsed().as_millis();
	assert_eq!(
		count(services, "servercurrentevent_data").await?,
		rows_expected,
		"unacknowledged rows survive shutdown"
	);
	gateway
		.release
		.send(true)
		.expect("release held peer");
	for (worker, (futures, statuses)) in inventories.iter_mut().enumerate() {
		let start = worker
			.checked_mul(slots)
			.expect("bounded inventory offset");
		let end = start
			.checked_add(slots)
			.expect("bounded inventory end");
		for destination in &destinations[start..end] {
			services
				.sending
				.resume_queue(destination, futures, statuses, &mut QueueRecovery::ResumePending)
				.await?;
		}
		timeout(Duration::from_secs(30), async {
			while !futures.is_empty() {
				services
					.sending
					.finish_responses(futures, statuses, &mut QueueRetries::new())
					.await?;
			}
			Result::<()>::Ok(())
		})
		.await
		.expect("bounded recovered actual HTTP acknowledgements")?;
	}
	for _ in 0..deliveries {
		let replay = gateway.next().await;
		assert_eq!(
			first
				.remove(&replay.path)
				.expect("same durable transaction URI"),
			replay,
			"same persisted wire bytes after cancellation"
		);
	}
	assert!(first.is_empty());
	assert_eq!(count(services, "servercurrentevent_data").await?, 0);
	assert_eq!(count(services, "servernameevent_data").await?, 0);
	for destination in &destinations {
		assert!(
			services
				.sending
				.db
				.load_attempt(destination)
				.await?
				.is_none()
		);
	}
	report(
		"maximum_sender_inventory",
		started,
		&json!({"inventory_workers":workers,"slots_per_worker":slots,
		"actual_service_channels":services.sending.channels.len(),"deliveries":deliveries,"active_rows":rows_expected,
		"payload_bytes_per_row":payload_bytes_per_row,"shutdown_ms":shutdown_ms,"replayed_identical_transactions":deliveries,"remaining_rows":0,
		"scope":"four upper-envelope production inventories; configured service worker count remains runtime/core-clamped"}),
	);
	fixture.finish().await;
	Ok(())
}

async fn push(services: &Services, gateway: &Gateway) -> Result<Destination> {
	let user = user_id!("@qualification:localhost");
	let pushkey = "qualification-push";
	let pusher = json!({"pushkey":pushkey,"app_id":"qualification","kind":"http",
		"app_display_name":"owned","device_display_name":"owned","lang":"en",
		"data":{"url":gateway.url}});
	let key = tuwunel_database::serialize_key((user, pushkey))?;
	services.db["senderkey_pusher"]
		.insert(&key, &serde_json::to_vec(&pusher)?)
		.await?;
	let destination = Destination::Push(user.to_owned(), pushkey.into());
	admit(services, &destination, &SendingEvent::BadgeRefresh).await?;
	Ok(destination)
}

#[test]
fn actual_push_failure_retains_rows_when_shutdown_retry_persistence_is_refused() -> Result {
	isolated("sending::sender::qualification_tests::actual_push_failure_retains_rows_when_shutdown_retry_persistence_is_refused", false, refused_failure())
}

async fn recover_refused_failure(url: &str) -> Result {
	let fixture = fixture(Some(url), 10).await?;
	let services = &fixture.services;
	let previous = std::env::var(VERIFY_EPOCH)
		.expect("prior owned writer epoch")
		.parse::<u64>()
		.expect("numeric prior epoch");
	let lease = services
		.db
		.lease_status()
		.expect("cold remote lease");
	assert!(lease.held && lease.epoch > previous, "cold writer must acquire a newer epoch");
	assert_eq!(count(services, "servercurrentevent_data").await?, 1);
	assert_eq!(count(services, "servernameevent_data").await?, 1);
	let destination = Destination::Push(
		user_id!("@qualification:localhost").to_owned(),
		"qualification-push".into(),
	);
	let mut futures = SendingFutures::new();
	let mut statuses = CurTransactionStatus::new();
	let mut retries = QueueRetries::new();
	// Reconstruct from durable ownership in a fresh backend, never from the
	// refused writer's dropped in-memory completion stage.
	services
		.sending
		.resume_queue(
			&destination,
			&mut futures,
			&mut statuses,
			&mut QueueRecovery::ResumePending,
		)
		.await?;
	timeout(
		Duration::from_secs(30),
		services
			.sending
			.finish_responses(&mut futures, &mut statuses, &mut retries),
	)
	.await
	.expect("bounded cold active ACK")?;
	assert!(futures.is_empty() && retries.is_empty());
	assert_eq!(count(services, "servercurrentevent_data").await?, 0);
	assert_eq!(
		count(services, "servernameevent_data").await?,
		1,
		"shutdown ACK must preserve the unsent successor"
	);
	services
		.sending
		.resume_queue(
			&destination,
			&mut futures,
			&mut statuses,
			&mut QueueRecovery::ResumePending,
		)
		.await?;
	timeout(
		Duration::from_secs(30),
		services
			.sending
			.finish_responses(&mut futures, &mut statuses, &mut retries),
	)
	.await
	.expect("bounded cold successor ACK")?;
	assert!(futures.is_empty() && retries.is_empty());
	assert_eq!(count(services, "servercurrentevent_data").await?, 0);
	assert_eq!(count(services, "servernameevent_data").await?, 0);
	assert!(
		services
			.sending
			.db
			.push_backoff(&destination)
			.await?
			.is_none()
	);
	fixture.finish().await;
	Ok(())
}

async fn refused_failure() -> Result {
	if let Ok(url) = std::env::var(VERIFY) {
		return recover_refused_failure(&url).await;
	}
	let started = Instant::now();
	let fake = Fake::start().await?;
	let fixture = fixture(Some(&fake.url), 10).await?;
	let services = &fixture.services;
	let mut gateway = Gateway::new(3, 0, true).await?;
	let destination = push(services, &gateway).await?;
	let response = services
		.sending
		.send_events(destination.clone(), vec![SendingEvent::BadgeRefresh])
		.await;
	assert!(
		matches!(&response, Ok(Delivery::PushFailed(..))),
		"actual HTTP500 retains physical membership"
	);
	let original = gateway.next().await;
	services
		.sending
		.db
		.queue_requests(std::iter::once((&SendingEvent::BadgeRefresh, &destination)))
		.await?;
	let active_map = tuwunel_bridge::catalog::map_id("servercurrentevent_data")
		.expect("active catalog")
		.0;
	let pending_map = tuwunel_bridge::catalog::map_id("servernameevent_data")
		.expect("pending catalog")
		.0;
	let active = fake.rows(active_map);
	let pending = fake.rows(pending_map);
	assert_eq!(active.len(), 1);
	assert_eq!(pending.len(), 1);
	let previous_epoch = services
		.db
		.lease_status()
		.expect("owned remote lease")
		.epoch;
	let map = tuwunel_bridge::catalog::map_id("global")
		.expect("global catalog")
		.0;
	fake.faults(Faults {
		reject_map: Some(map),
		..Faults::default()
	});
	let mut futures = SendingFutures::new();
	futures.push(
		destination.clone(),
		futures::future::ready(response).boxed(),
		&Handle::current(),
	);
	let mut statuses = CurTransactionStatus::new();
	let mut retries = QueueRetries::new();
	let global = fake.rows(map);
	let applied = fake.applied();
	let drain = Instant::now();
	let error = timeout(
		Duration::from_secs(3),
		services
			.sending
			.finish_responses(&mut futures, &mut statuses, &mut retries),
	)
	.await
	.expect("bounded refused persistence drain")
	.expect_err("known non-backpressure refusal must propagate and stop the sender");
	assert!(matches!(&error, tuwunel_core::Error::Database(..)));
	assert!(
		error
			.to_string()
			.contains("invalid request: test refusal"),
		"assert the actual known bridge refusal"
	);
	assert!(
		futures.is_empty() && retries.is_empty(),
		"known refusal must not masquerade as deferred 429 recovery"
	);
	assert!(!services.db.is_read_only());
	assert_eq!(fake.applied(), applied, "refused mutation applies no batch");
	assert_eq!(fake.rows(active_map), active, "exact active ownership survives refusal");
	assert_eq!(fake.rows(pending_map), pending, "unsent successor survives refusal");
	assert_eq!(fake.rows(map), global, "retry metadata mutation was wholly refused");
	assert_eq!(count(services, "servercurrentevent_data").await?, 1);
	assert!(
		services
			.sending
			.db
			.push_backoff(&destination)
			.await?
			.is_none()
	);
	let refused_ms = drain.elapsed().as_millis();
	fake.faults(Faults::default());
	futures.cancel_and_join().await;
	fixture.finish().await;
	fake.expire_lease();
	let test = "sending::sender::qualification_tests::actual_push_failure_retains_rows_when_shutdown_retry_persistence_is_refused";
	let status = timeout(
		Duration::from_secs(70),
		tokio::process::Command::new(std::env::current_exe()?)
			.args(["--exact", test, "--nocapture", "--test-threads=1"])
			.env(VERIFY, &fake.url)
			.env(VERIFY_EPOCH, previous_epoch.to_string())
			.kill_on_drop(true)
			.status(),
	)
	.await
	.expect("bounded cold refused-failure recovery")?;
	assert!(status.success(), "fresh writer verifies known-refusal recovery and actual ACKs");
	assert_eq!(gateway.next().await, original, "cold replay preserves exact gateway wire bytes");
	gateway.next().await;
	assert!(fake.rows(active_map).is_empty());
	assert!(fake.rows(pending_map).is_empty());
	report(
		"gateway_failure_refused_persistence",
		started,
		&json!({"gateway_requests":3,"refused_shutdown_ms":refused_ms,"retained_rows_during_refusal":1,
		"retained_successors_during_refusal":1,"known_refusal_propagated":true,"deferred_429_recovery":false,
		"cold_newer_writer":true,"cold_replayed_identical_gateway_body":true,"remaining_rows":0}),
	);
	Ok(())
}

#[test]
fn held_remote_ack_shutdown_stops_uncertain_writer_and_recovers_exact_rows() -> Result {
	isolated("sending::sender::qualification_tests::held_remote_ack_shutdown_stops_uncertain_writer_and_recovers_exact_rows", false, held_ack())
}

async fn held_ack() -> Result {
	if let Ok(url) = std::env::var(VERIFY) {
		let fixture = fixture(Some(&url), 10).await?;
		assert_eq!(count(&fixture.services, "servercurrentevent_data").await?, 0);
		assert_eq!(
			count(&fixture.services, "servernameevent_data").await?,
			1,
			"unsent successor survives cold recovery"
		);
		fixture.finish().await;
		return Ok(());
	}
	let started = Instant::now();
	let fake = Fake::start().await?;
	let fixture = fixture(Some(&fake.url), 10).await?;
	let services = &fixture.services;
	let mut gateway = Gateway::new(1, 0, false).await?;
	let destination = push(services, &gateway).await?;
	let response = services
		.sending
		.send_events(destination.clone(), vec![SendingEvent::BadgeRefresh])
		.await;
	assert!(matches!(&response, Ok(Delivery::Acknowledged(..))));
	gateway.next().await;
	services
		.sending
		.db
		.queue_requests(std::iter::once((&SendingEvent::BadgeRefresh, &destination)))
		.await?;
	let map = tuwunel_bridge::catalog::map_id("servercurrentevent_data")
		.expect("active catalog")
		.0;
	let gate = fake.pause_commit_on(map, CommitStage::BeforeApply);
	let mut futures = SendingFutures::new();
	futures.push(destination, futures::future::ready(response).boxed(), &Handle::current());
	let mut statuses = CurTransactionStatus::new();
	let mut retries = QueueRetries::new();
	let ack_applied_once = {
		let drain = services
			.sending
			.finish_responses(&mut futures, &mut statuses, &mut retries);
		tokio::pin!(drain);
		timeout(Duration::from_secs(2), async {
			tokio::select! {
				() = gate.entered.notified() => (),
				result = &mut drain => panic!("shutdown finished before its held ACK commit: {result:?}"),
			}
		})
		.await
		.expect("ACK cleanup reached provider boundary");
		assert_eq!(fake.rows(map).len(), 1, "held ACK has not deleted its row");
		timeout(Duration::from_secs(3), &mut drain)
			.await
			.expect("shutdown deadline cancels held cleanup")?;
		assert!(
			services.db.is_read_only(),
			"cancelled dispatched mutation stops uncertain writer"
		);
		gate.release.notify_one();
		timeout(Duration::from_secs(2), gate.finished.notified())
			.await
			.expect("owned accepted commit finishes");
		fake.rows(map).is_empty()
	};
	assert!(ack_applied_once, "accepted exact ACK applies once after cancellation");
	futures.cancel_and_join().await;
	fixture.finish().await;
	fake.expire_lease();
	let test = "sending::sender::qualification_tests::held_remote_ack_shutdown_stops_uncertain_writer_and_recovers_exact_rows";
	let status = timeout(
		Duration::from_secs(30),
		tokio::process::Command::new(std::env::current_exe()?)
			.args(["--exact", test, "--nocapture", "--test-threads=1"])
			.env(VERIFY, &fake.url)
			.kill_on_drop(true)
			.status(),
	)
	.await
	.expect("bounded cold sender verification")?;
	assert!(status.success(), "fresh backend verifies exact durable recovery");
	report(
		"held_ack_shutdown_cold_recovery",
		started,
		&json!({"held_ack_rows":1,"remaining_ack_rows":0,"unsent_successors":1,"uncertain_writer_stopped":true}),
	);
	Ok(())
}
