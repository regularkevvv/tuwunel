#![cfg(test)]

//! Real accepted PDUs are queued through service APIs in a process with no
//! sender workers. SIGKILL removes its in-memory wakes. Fresh starts must
//! drain the durable push/appservice rows, including a destination beyond
//! the first inventory page, without another message or explicit dispatch.
//! A native WAL barrier makes the queue durable before the kill; this tests
//! startup reconstruction, not every acknowledgement boundary or remote D1.

mod client;

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::{
	collections::{BTreeMap, BTreeSet},
	env::{current_exe, temp_dir, var},
	fs::{DirBuilder, read, remove_dir_all, write},
	net::TcpListener,
	path::{Path, PathBuf},
	process::{Child, Command, id},
	thread,
	time::{Duration, Instant},
};

use futures::{TryStreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpListener as StubListener,
	spawn,
	sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
	task::JoinHandle,
	time::{sleep, timeout},
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{
		OwnedEventId, OwnedRoomId, OwnedUserId,
		api::{
			appservice::{Namespaces, Registration, RegistrationInit},
			client::push::{
				PusherIds, PusherInit, PusherKind, set_pusher::v3::Request as SetPusherRequest,
			},
		},
		device_id,
		push::HttpPusherData,
	},
};
use tuwunel_service::{Services, sending::EduBuf};

use self::client::{Client, register, wait_until_ready};

const PHASE: &str = "TUWUNEL_QUEUE_RESTART_PHASE";
const DIRECTORY: &str = "TUWUNEL_QUEUE_RESTART_DIRECTORY";
const ALICE: &str = "disposable-queue-restart-alice-token";
const BOB: &str = "disposable-queue-restart-bob-token";
const AS_FIRST: &str = "queued-a";
const AS_LAST: &str = "queued-z";
const PUSHKEY: &str = "queued-message-only";
const EDUS: usize = 273;

type Rows = BTreeMap<Vec<u8>, Vec<u8>>;
type Captured = (&'static str, String, Value);

#[derive(Serialize, Deserialize)]
struct Manifest {
	room: OwnedRoomId,
	recipient: OwnedUserId,
	events: Vec<OwnedEventId>,
}

struct OwnedDirectory(PathBuf);
impl Drop for OwnedDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

struct OwnedChild(Child);
impl Drop for OwnedChild {
	fn drop(&mut self) {
		self.0.kill().ok();
		self.0.wait().ok();
	}
}

struct Stub {
	receiver: UnboundedReceiver<Captured>,
	tasks: Vec<JoinHandle<()>>,
}
impl Drop for Stub {
	fn drop(&mut self) {
		for task in &self.tasks {
			task.abort();
		}
	}
}

#[test]
fn durable_pending_deliveries_resume_after_kill_without_a_new_wake() -> Result {
	if let Ok(phase) = var(PHASE) {
		return child(&PathBuf::from(var(DIRECTORY).expect("owned child directory")), &phase);
	}
	let directory = temp_dir().join(format!("sending-queue-restart-{}", id()));
	DirBuilder::new().create(&directory)?; // Never adopt a pre-existing directory.
	let directory = OwnedDirectory(directory);
	run_child(&directory.0, "prepare")?;
	let mut queued = OwnedChild(command(&directory.0, "queue")?.spawn()?);
	let deadline = Instant::now() + Duration::from_secs(30);
	while !directory.0.join("queued-ready").exists() {
		assert!(queued.0.try_wait()?.is_none(), "queue child exited before its durable barrier");
		assert!(Instant::now() < deadline, "queue child never reached its durable barrier");
		thread::sleep(Duration::from_millis(20));
	}
	assert!(queued.0.try_wait()?.is_none(), "barrier child must still be alive");
	queued.0.kill()?;
	let status = queued.0.wait()?;
	assert!(!status.success(), "the acknowledged queue must survive an actual process kill");
	#[cfg(unix)]
	assert_eq!(status.signal(), Some(9), "exercise SIGKILL rather than graceful cleanup");
	for phase in ["disabled", "resume", "again"] {
		run_child(&directory.0, phase)?;
	}
	Ok(())
}

fn command(directory: &Path, phase: &str) -> Result<Command> {
	let mut command = Command::new(current_exe()?);
	command
		.env(PHASE, phase)
		.env(DIRECTORY, directory);
	Ok(command)
}

fn run_child(directory: &Path, phase: &str) -> Result {
	assert!(
		command(directory, phase)?.status()?.success(),
		"queue restart child {phase} failed"
	);
	Ok(())
}

fn child(directory: &Path, phase: &str) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let modes: &[&str] = if phase == "prepare" { &["fresh"] } else { &[] };
	let mut args = Args::default_test(modes);
	args.option.extend([
		format!("database_path={:?}", directory.join("database")),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		format!("listening={}", phase == "prepare"),
		format!("startup_netburst={}", phase != "disabled"),
		"ip_range_denylist=[]".into(),
		"suppress_push_when_active=false".into(),
		"log=\"error\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	drop(listener);
	let result = runtime.block_on(async {
		if phase == "queue" {
			let services = Services::build(server.server.clone()).await?;
			queue(&services, directory).await?;
			// The parent kills this process. No destructors, flush-on-drop,
			// sender workers or stale in-memory wake can supply the recovery.
			loop {
				sleep(Duration::from_secs(1)).await;
			}
		}
		let (services, mut stub) = if phase == "prepare" {
			(async_start(&server).await?, None)
		} else {
			let services = Services::build(server.server.clone()).await?;
			let manifest = manifest(directory)?;
			let stub = prepare_stub(&services, &manifest).await?;
			let services = services.start().await?;
			*server.services.lock().await = Some(services.clone());
			(services, Some(stub))
		};
		let exercise = async {
			let outcome = match phase {
				| "prepare" =>
					prepare(&services, &format!("http://127.0.0.1:{port}"), directory).await,
				| "disabled" | "resume" | "again" =>
					verify(&services, stub.as_mut().expect("restart stub"), directory, phase)
						.await,
				| _ => panic!("unexpected queue fixture phase"),
			};
			let shutdown = server.server.shutdown();
			outcome.and(shutdown)
		};
		let (run, outcome) = tokio::join!(async_run(&server), exercise);
		drop(stub);
		drop(services);
		outcome.and(run).and(async_stop(&server).await)
	});
	drop(server);
	drop(runtime);
	result
}

fn manifest(directory: &Path) -> Result<Manifest> {
	Ok(serde_json::from_slice(&read(directory.join("manifest.json"))?)?)
}

async fn prepare(services: &Services, base: &str, directory: &Path) -> Result {
	wait_until_ready(services, base).await?;
	register(services, "queued-alice", ALICE).await?;
	let recipient = register(services, "queued-bob", BOB).await?;
	let alice = Client { services, base, token: ALICE };
	let room = alice
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/rooms/{room}/join"))
		.bearer_auth(BOB)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;
	let mut events = Vec::new();
	for n in 0..3 {
		let response: Value = services
			.client
			.clients
			.default
			.put(format!("{base}/_matrix/client/v3/rooms/{room}/send/m.room.message/queue-{n}"))
			.bearer_auth(ALICE)
			.json(&json!({"msgtype":"m.text","body":format!("durable delivery {n}")}))
			.send()
			.await?
			.error_for_status()?
			.json()
			.await?;
		events.push(
			response["event_id"]
				.as_str()
				.expect("actual accepted event")
				.try_into()?,
		);
	}
	write(
		directory.join("manifest.json"),
		serde_json::to_vec(&Manifest { room, recipient, events })?,
	)?;
	assert!(
		rows(services, "servernameevent_data")
			.await?
			.is_empty()
	);
	Ok(())
}

async fn rows(services: &Services, map: &str) -> Result<Rows> {
	let stream = services.db[map].raw_stream();
	pin_mut!(stream);
	let mut out = BTreeMap::new();
	while let Some((key, value)) = stream.try_next().await? {
		out.insert(key.to_vec(), value.to_vec());
	}
	Ok(out)
}

async fn queue(services: &Services, directory: &Path) -> Result {
	let manifest = manifest(directory)?;
	for appservice in [AS_FIRST, AS_LAST] {
		persist_appservice(services, appservice, "http://127.0.0.1:9".into()).await?;
	}
	register_pusher(services, &manifest, "http://127.0.0.1:9").await?;
	for event in &manifest.events {
		let pdu = services.timeline.get_pdu_id(event).await?;
		for appservice in [AS_FIRST, AS_LAST] {
			services
				.sending
				.send_pdu_appservice(appservice.into(), pdu)
				.await?;
		}
		services
			.sending
			.send_pdu_push(&pdu, &manifest.recipient, PUSHKEY.into())
			.await?;
	}
	for n in 0..EDUS {
		let edu = serde_json::to_vec(&json!({
			"type":"m.typing", "room_id":manifest.room,
			"content":{"user_ids":[format!("@queued-typing-{n}:localhost")]}
		}))?;
		services
			.sending
			.send_edu_appservice(AS_FIRST.into(), EduBuf::from_slice(&edu))
			.await?;
	}
	let pending = rows(services, "servernameevent_data").await?;
	assert_eq!(pending.len(), EDUS + 9);
	assert!(pending.len() > 256, "last appservice must lie beyond the first startup page");
	assert!(
		pending
			.keys()
			.last()
			.expect("pending rows")
			.starts_with(b"+queued-z\xff")
	);
	assert!(
		pending
			.values()
			.all(|value| value.as_slice() != [3]),
		"no badge wake masks lost PDUs"
	);
	assert!(
		rows(services, "servercurrentevent_data")
			.await?
			.is_empty()
	);
	assert!(
		rows(services, "servername_educount")
			.await?
			.is_empty()
	);
	write(
		directory.join("pending.json"),
		serde_json::to_vec(&pending.into_iter().collect::<Vec<_>>())?,
	)?;
	// Queue APIs cork native writes; without workers there is no periodic
	// flush. Establish the on-disk prerequisite explicitly before SIGKILL.
	services.db.engine()?.sync()?;
	write(directory.join("queued-ready"), b"queue acknowledged and native WAL synced")?;
	Ok(())
}

async fn prepare_stub(services: &Services, manifest: &Manifest) -> Result<Stub> {
	let (sender, receiver) = unbounded_channel();
	let mut stub = Stub { receiver, tasks: Vec::new() };
	for appservice in [AS_FIRST, AS_LAST] {
		let listener = StubListener::bind(("127.0.0.1", 0)).await?;
		let base = format!("http://{}", listener.local_addr()?);
		stub.tasks
			.push(spawn(serve(listener, sender.clone(), appservice)));
		persist_appservice(services, appservice, base).await?;
	}
	let listener = StubListener::bind(("127.0.0.1", 0)).await?;
	let base = format!("http://{}", listener.local_addr()?);
	stub.tasks
		.push(spawn(serve(listener, sender, "push")));
	register_pusher(services, manifest, &base).await?;
	Ok(stub)
}

async fn persist_appservice(services: &Services, appservice: &str, base: String) -> Result {
	let mut registration: Registration = RegistrationInit {
		id: appservice.into(),
		url: Some(base),
		as_token: format!("{appservice}-disposable-as-token"),
		hs_token: format!("{appservice}-disposable-hs-token"),
		sender_localpart: format!("{appservice}-bot"),
		namespaces: Namespaces::new(),
		rate_limited: None,
		protocols: None,
	}
	.into();
	registration.receive_ephemeral = true;
	// Seed the persisted registration before any workers start. The normal
	// appservice worker loads it on restart; register_appservice waits for
	// that worker and therefore cannot be called in this pre-start phase.
	services.db["id_appserviceregistrations"]
		.insert(appservice, serde_json::to_string(&registration)?)
		.await?;
	Ok(())
}

async fn register_pusher(services: &Services, manifest: &Manifest, base: &str) -> Result {
	let pusher = PusherInit {
		ids: PusherIds::new(PUSHKEY.into(), "disposable.queue.test".into()),
		kind: PusherKind::Http(HttpPusherData::new(format!("{base}/_matrix/push/v1/notify"))),
		app_display_name: "Queue restart fixture".into(),
		device_display_name: "Disposable device".into(),
		profile_tag: None,
		lang: "en".into(),
	}
	.into();
	services
		.pusher
		.set_pusher(
			&manifest.recipient,
			device_id!("QUEUERESTART"),
			&SetPusherRequest::post(pusher).action,
		)
		.await?;
	Ok(())
}

async fn verify(services: &Services, stub: &mut Stub, directory: &Path, phase: &str) -> Result {
	let manifest = manifest(directory)?;
	if phase != "resume" {
		assert!(
			timeout(Duration::from_millis(300), stub.receiver.recv())
				.await
				.is_err(),
			"disabled recovery and an empty later start must emit no fabricated delivery"
		);
		assert!(
			rows(services, "servercurrentevent_data")
				.await?
				.is_empty()
		);
		let expected = if phase == "disabled" {
			serde_json::from_slice::<Vec<(Vec<u8>, Vec<u8>)>>(&read(
				directory.join("pending.json"),
			)?)?
			.into_iter()
			.collect()
		} else {
			BTreeMap::new()
		};
		assert_eq!(rows(services, "servernameevent_data").await?, expected);
		return Ok(());
	}
	let mut received = BTreeMap::<String, Vec<String>>::new();
	let mut edus = BTreeSet::new();
	timeout(Duration::from_secs(15), async {
		loop {
			let (owner, path, body) = stub
				.receiver
				.recv()
				.await
				.expect("live HTTP stub");
			if path.starts_with("/_matrix/push/") {
				let event = body["notification"]["event_id"]
					.as_str()
					.expect("actual PDU push, not a badge");
				received
					.entry("push".into())
					.or_default()
					.push(event.into());
			} else {
				assert!(
					path.contains("/transactions/"),
					"appservice transaction reached its real route"
				);
				let service = body["events"]
					.as_array()
					.expect("appservice PDU array");
				let received = received.entry(owner.into()).or_default();
				for event in service {
					received.push(
						event["event_id"]
							.as_str()
							.expect("event ID")
							.into(),
					);
				}
				if let Some(events) = body["ephemeral"].as_array() {
					for event in events {
						assert_eq!(event["type"], "m.typing");
						assert_eq!(event["room_id"], manifest.room.as_str());
						let users = event["content"]["user_ids"]
							.as_array()
							.expect("typing users");
						assert_eq!(users.len(), 1);
						assert!(
							edus.insert(
								users[0]
									.as_str()
									.expect("typing user ID")
									.to_owned()
							),
							"no duplicate temporary event"
						);
					}
				}
			}
			if received.values().map(Vec::len).sum::<usize>() == 9 && edus.len() == EDUS {
				break;
			}
		}
		Ok::<(), tuwunel_core::Error>(())
	})
	.await
	.map_err(|_| {
		err!("durable queue recovery timed out: received {received:?}; EDUs {}", edus.len())
	})??;
	assert_eq!(
		edus,
		(0..EDUS)
			.map(|n| format!("@queued-typing-{n}:localhost"))
			.collect()
	);
	let mut expected: Vec<_> = manifest
		.events
		.iter()
		.map(ToString::to_string)
		.collect();
	expected.sort();
	for owner in [AS_FIRST, AS_LAST, "push"] {
		let values = received
			.get_mut(owner)
			.expect("every destination resumed");
		values.sort();
		assert_eq!(values, &expected, "no missing or duplicate accepted PDU");
	}
	timeout(Duration::from_secs(10), async {
		while !rows(services, "servernameevent_data")
			.await?
			.is_empty()
			|| !rows(services, "servercurrentevent_data")
				.await?
				.is_empty()
		{
			sleep(Duration::from_millis(20)).await;
		}
		Ok::<(), tuwunel_core::Error>(())
	})
	.await
	.map_err(|_| err!("acknowledged delivery rows did not drain"))??;
	assert!(
		timeout(Duration::from_millis(300), stub.receiver.recv())
			.await
			.is_err(),
		"no late duplicate delivery"
	);
	Ok(())
}

async fn serve(listener: StubListener, sender: UnboundedSender<Captured>, owner: &'static str) {
	while let Ok((mut socket, _)) = listener.accept().await {
		let result = timeout(Duration::from_secs(10), async {
			let mut bytes = Vec::new();
			let mut chunk = [0; 4096];
			loop {
				let n = socket.read(&mut chunk).await.ok()?;
				if n == 0 || bytes.len().checked_add(n)? > 256 * 1024 {
					return None;
				}
				bytes.extend_from_slice(&chunk[..n]);
				let Some(end) = bytes
					.windows(4)
					.position(|part| part == b"\r\n\r\n")
				else {
					continue;
				};
				let headers = std::str::from_utf8(&bytes[..end]).ok()?;
				let length: usize = headers.lines().find_map(|line| {
					line.to_ascii_lowercase()
						.strip_prefix("content-length:")
						.map(|value| value.trim().parse().ok())
				})??;
				let body_start = end.checked_add(4)?;
				let body_end = body_start.checked_add(length)?;
				if body_end > 256 * 1024 {
					return None;
				}
				if bytes.len() < body_end {
					continue;
				}
				let path = headers
					.lines()
					.next()?
					.split_whitespace()
					.nth(1)?
					.to_owned();
				let body: Value =
					serde_json::from_slice(bytes.get(body_start..body_end)?).ok()?;
				return Some((path, body));
			}
		})
		.await;
		if let Ok(Some((path, body))) = result {
			sender.send((owner, path, body)).ok();
		}
		let body = if owner == "push" { "{\"rejected\":[]}" } else { "{}" };
		let response = format!(
			"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
			 {}\r\nConnection: close\r\n\r\n{body}",
			body.len()
		);
		socket.write_all(response.as_bytes()).await.ok();
	}
}
