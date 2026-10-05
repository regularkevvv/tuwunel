#![cfg(test)]
//! Durable legacy/frozen push deferral, native kill and scoped read
//! cancellation.
mod client;

#[cfg(unix)]
use std::os::unix::{fs::DirBuilderExt, process::ExitStatusExt};
use std::{
	collections::BTreeSet,
	env::{current_exe, temp_dir, var},
	fs::{DirBuilder, read, remove_dir_all, write},
	future::pending,
	net::TcpListener,
	path::{Path, PathBuf},
	process::{Child, Command, id},
	thread,
	time::{Duration, Instant},
};

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpListener as Gateway,
	sync::mpsc::{UnboundedReceiver, unbounded_channel},
	task::JoinHandle,
	time::timeout,
};
use tuwunel::{Args, Runtime, Server, async_run, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, presence::PresenceState},
};
use tuwunel_database::{Database, serialize_key};
use tuwunel_service::{
	Services,
	presence::Ping,
	sending::{Destination, SendingEvent},
};

use self::client::{Client, field, poll_until, register, wait_until_ready};

const PHASE: &str = "TUWUNEL_PUSH_SUPPRESSION_PHASE";
const DIRECTORY: &str = "TUWUNEL_PUSH_SUPPRESSION_DIRECTORY";
const OWNER: &str = "disposable-push-suppression-owner";
const WRITER: &str = "disposable-push-suppression-writer";
const PUSHKEY: &str = "durable-suppression";

#[derive(Deserialize, Serialize)]
struct Manifest {
	user: OwnedUserId,
	room: OwnedRoomId,
	gateway: u16,
	legacy: OwnedEventId,
	legacy_root: OwnedEventId,
	root: Option<OwnedEventId>,
	read: Option<OwnedEventId>,
	unread: Option<OwnedEventId>,
	main: Option<OwnedEventId>,
}
struct Directory(PathBuf);
impl Drop for Directory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}
struct OwnedChild(Child);
impl Drop for OwnedChild {
	fn drop(&mut self) {
		self.0.kill().ok();
		self.0.wait().ok();
	}
}
struct GatewayTask(JoinHandle<()>);
impl Drop for GatewayTask {
	fn drop(&mut self) { self.0.abort(); }
}

#[test]
fn deferred_pushes_survive_kill_and_cancel_only_read_scopes() -> Result {
	if let Ok(phase) = var(PHASE) {
		return child(&PathBuf::from(var(DIRECTORY).expect("owned path")), &phase);
	}
	let path = temp_dir().join(format!("tuwunel-push-suppression-{}", id()));
	let mut builder = DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&path)?;
	let directory = Directory(path);
	for phase in ["prepare", "accept", "refuse", "repair", "recover", "again"] {
		let mut child = OwnedChild(
			Command::new(current_exe()?)
				.env(PHASE, phase)
				.env(DIRECTORY, &directory.0)
				.spawn()?,
		);
		let started = Instant::now();
		loop {
			if phase == "accept" && directory.0.join("accepted.ready").exists() {
				assert!(child.0.try_wait()?.is_none(), "accepted process still live");
				child.0.kill()?;
				let status = child.0.wait()?;
				#[cfg(unix)]
				assert_eq!(status.signal(), Some(9));
				break;
			}
			if let Some(status) = child.0.try_wait()? {
				assert!(phase != "accept" && status.success(), "phase {phase} failed: {status}");
				break;
			}
			assert!(started.elapsed() < Duration::from_mins(2), "phase {phase} deadline");
			thread::sleep(Duration::from_millis(20));
		}
	}
	Ok(())
}

fn manifest(path: &Path) -> Result<Manifest> {
	Ok(serde_json::from_slice(&read(path.join("manifest.json"))?)?)
}

fn child(path: &Path, phase: &str) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let modes: &[&str] = if phase == "prepare" { &["fresh"] } else { &[] };
	let mut args = Args::default_test(modes);
	args.option.extend([
		format!("database_path={:?}", path.join("database")),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		"listening=true".into(),
		"ip_range_denylist=[]".into(),
		"suppress_push_when_active=true".into(),
		"startup_netburst=true".into(),
		"startup_netburst_keep=-1".into(),
		"sender_retry_backoff_limit=1".into(),
		"log=\"warn\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	drop(listener);
	let result = runtime.block_on(async {
		if phase == "repair" {
			let m = manifest(path)?;
			let db = Database::open(&server.server).await?;
			let key = serialize_key((&m.room, &m.user, ""))?;
			assert_eq!(
				db["roomuserid_notificationcutoff"]
					.get(&key)
					.await?
					.as_ref(),
				b"broken-cutoff"
			);
			let saved: Option<Vec<u8>> =
				serde_json::from_slice(&read(path.join("cutoff.saved"))?)?;
			let result = match saved {
				| Some(saved) =>
					db["roomuserid_notificationcutoff"]
						.insert(&key, &saved)
						.await,
				| None =>
					db["roomuserid_notificationcutoff"]
						.remove(&key)
						.await,
			};
			db.close().await;
			return result;
		}
		let services = Services::build(server.server.clone()).await?;
		let (_gateway, mut rx) = if phase == "prepare" {
			(None, unbounded_channel().1)
		} else {
			let m = manifest(path)?;
			if phase == "refuse" {
				let key = serialize_key((&m.room, &m.user, ""))?;
				let map = &services.db["roomuserid_notificationcutoff"];
				let saved = match map.get(&key).await {
					| Ok(value) => Some(value.to_vec()),
					| Err(error) if error.is_not_found() => None,
					| Err(error) => return Err(error),
				};
				write(path.join("cutoff.saved"), serde_json::to_vec(&saved)?)?;
				map.insert(&key, b"broken-cutoff").await?;
			}
			let (gateway, rx) = gateway(m.gateway).await?;
			(Some(gateway), rx)
		};
		let services = services.start().await?;
		_ = server
			.services
			.lock()
			.await
			.insert(services.clone());
		let base = format!("http://127.0.0.1:{port}");
		let exercise = async {
			wait_until_ready(&services, &base).await?;
			let outcome = match phase {
				| "prepare" => prepare(&services, &base, path).await,
				| "accept" => accept(&services, &base, path, &mut rx).await,
				| "refuse" => refuse(&services, path, &mut rx).await,
				| "recover" => recover(&services, path, &mut rx).await,
				| "again" => again(&services, path, &mut rx).await,
				| _ => Err(err!("unknown owned fixture phase")),
			};
			let shutdown = server.server.shutdown();
			outcome.and(shutdown)
		};
		let (run, outcome) = tokio::join!(async_run(&server), exercise);
		drop(services);
		outcome.and(run).and(async_stop(&server).await)
	});
	drop(server);
	drop(runtime);
	result
}

async fn prepare(services: &Services, base: &str, path: &Path) -> Result {
	let user = register(services, "push-suppression-owner", OWNER).await?;
	let writer_user = register(services, "push-suppression-writer", WRITER).await?;
	let owner = Client { services, base, token: OWNER };
	let writer = Client { services, base, token: WRITER };
	let room = owner
		.create_room(&json!({"preset":"private_chat"}))
		.await?;
	services
		.client
		.clients
		.default
		.post(owner.url(&format!("rooms/{room}/invite")))
		.bearer_auth(OWNER)
		.json(&json!({"user_id":writer_user}))
		.send()
		.await?
		.error_for_status()?;
	services
		.client
		.clients
		.default
		.post(writer.url(&format!("join/{room}")))
		.bearer_auth(WRITER)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;
	services.client.clients.default.put(owner.url("pushrules/global/override/suppression-control"))
		.bearer_auth(OWNER).json(&json!({"conditions":[{"kind":"event_match","key":"type","pattern":"m.room.message"}],"actions":["notify"]}))
		.send().await?.error_for_status()?;
	let root = send(&writer, &room, "legacy-root", None).await?;
	let legacy = send(&writer, &room, "legacy-reply", Some(&root)).await?;
	assert!(
		poll_until(Duration::from_secs(10), async || {
			services
				.pusher
				.notification_state(&user, &room)
				.await
				.is_ok_and(|state| {
					state
						.threads
						.get(&root)
						.is_some_and(|pair| pair.0 == 1)
				})
		})
		.await,
		"legacy thread counts finish before the preparation shutdown"
	);
	let gateway = TcpListener::bind(("127.0.0.1", 0))?
		.local_addr()?
		.port();
	let m = Manifest {
		user,
		room,
		gateway,
		legacy,
		legacy_root: root,
		root: None,
		read: None,
		unread: None,
		main: None,
	};
	write(path.join("manifest.json"), serde_json::to_vec(&m)?)?;
	Ok(())
}

async fn accept(
	services: &Services,
	base: &str,
	path: &Path,
	rx: &mut UnboundedReceiver<Value>,
) -> Result {
	let mut m = manifest(path)?;
	// Prove the active heuristic is driven before any sender can attempt work.
	services
		.presence
		.maybe_ping_presence(&m.user, Ping::default())
		.await?;
	services.presence.note_sync(&m.user, None).await;
	let presence = services.presence.get_presence(&m.user).await?;
	assert_eq!(presence.content.presence, PresenceState::Online);
	assert!(
		presence
			.content
			.last_active_ago
			.is_some_and(|age| u64::from(age) < 65_000)
	);
	assert!(
		services
			.presence
			.last_sync_gap_ms(&m.user)
			.await
			.is_some_and(|gap| gap < 32_000)
	);
	let owner = Client { services, base, token: OWNER };
	let writer = Client { services, base, token: WRITER };
	services.client.clients.default.post(owner.url("pushers/set"))
		.bearer_auth(OWNER).json(&json!({"pushkey":PUSHKEY,"app_id":"suppression-fixture","kind":"http","app_display_name":"Disposable fixture","device_display_name":"Native test","lang":"en","data":{"url":format!("http://127.0.0.1:{}/_matrix/push/v1/notify",m.gateway),"disable_badge_count":true}}))
		.send().await?.error_for_status()?;
	let legacy = services.timeline.get_pdu_id(&m.legacy).await?;
	let legacy_root = &m.legacy_root;
	assert_eq!(
		services
			.pusher
			.notification_state(&m.user, &m.room)
			.await?
			.threads
			.get(legacy_root),
		Some(&(1, 0)),
		"legacy thread counts survive restart"
	);
	services
		.sending
		.send_pdu_push(&legacy, &m.user, PUSHKEY.into())
		.await?;
	let root = send(&writer, &m.room, "frozen-root", None).await?;
	let read = send(&writer, &m.room, "frozen-read", Some(&root)).await?;
	let unread = send(&writer, &m.room, "frozen-unread", Some(&root)).await?;
	let main = send(&writer, &m.room, "frozen-main", None).await?;
	m.root = Some(root.clone());
	m.read = Some(read.clone());
	m.unread = Some(unread);
	m.main = Some(main.clone());
	assert!(
		poll_until(Duration::from_secs(10), async || {
			owed(services, &m)
				.await
				.is_ok_and(|events| events.len() == 5)
		})
		.await,
		"all accepted events remain in durable queues"
	);
	receipt(&owner, &m.room, &read, root.as_str()).await?;
	assert_eq!(
		services
			.pusher
			.notification_state(&m.user, &m.room)
			.await?
			.threads
			.get(legacy_root),
		Some(&(1, 0)),
		"another thread's receipt preserves legacy unread"
	);
	receipt(&owner, &m.room, &root, "main").await?;
	assert_eq!(
		services
			.pusher
			.notification_state(&m.user, &m.room)
			.await?
			.notifications,
		1,
		"an older main receipt preserves the newer main notification"
	);
	receipt(&owner, &m.room, &main, "main").await?;
	let state = services
		.pusher
		.notification_state(&m.user, &m.room)
		.await?;
	assert_eq!(state.notifications, 0, "main reset clears only main counts");
	assert_eq!(u64::from(state.totals()?.0), 2, "independent unread threads remain");
	for _ in 0..10_000 {
		services
			.sending
			.schedule_resume_pushes_for_user(m.user.clone(), "duplicate hint fixture");
	}
	tokio::task::yield_now().await;
	let events = owed(services, &m).await?;
	assert_eq!(events.len(), 5, "read cutoffs do not prematurely remove queue ownership");
	assert_eq!(
		events
			.iter()
			.filter(|(_, event)| matches!(event, SendingEvent::Pdu(_)))
			.count(),
		1
	);
	assert_eq!(
		events
			.iter()
			.filter(|(_, event)| matches!(event, SendingEvent::FrozenPush(_)))
			.count(),
		4
	);
	quiet(rx).await?;
	write(path.join("manifest.json"), serde_json::to_vec(&m)?)?;
	write(path.join("accepted.ready"), b"five durable push rows; native kill boundary")?;
	pending().await
}

async fn refuse(services: &Services, path: &Path, rx: &mut UnboundedReceiver<Value>) -> Result {
	let m = manifest(path)?;
	let raw = services.timeline.get_pdu_id(&m.legacy).await?;
	let pdu = services.timeline.get_pdu_from_id(&raw).await?;
	services
		.pusher
		.notification_is_read(&m.user, &pdu, raw.pdu_count().into_unsigned())
		.await
		.expect_err("corrupt read state refuses legacy delivery");
	for _ in 0..6 {
		quiet(rx).await?;
		assert_eq!(
			owed(services, &m).await?.len(),
			5,
			"refused reads retain every durable obligation"
		);
	}
	Ok(())
}

async fn recover(services: &Services, path: &Path, rx: &mut UnboundedReceiver<Value>) -> Result {
	let m = manifest(path)?;
	let expected = BTreeSet::from([
		m.legacy.to_string(),
		m.unread
			.as_ref()
			.expect("accepted unread")
			.to_string(),
	]);
	let mut delivered = BTreeSet::new();
	timeout(Duration::from_secs(20), async {
		while delivered.len() < expected.len() {
			let body = rx.recv().await.expect("gateway live");
			if let Some(event) = body["notification"]["event_id"].as_str() {
				assert!(
					expected.contains(event),
					"read main/thread event must not be delivered: {event}"
				);
				assert!(
					delivered.insert(event.to_owned()),
					"no duplicate event in successful recovery"
				);
			}
		}
	})
	.await
	.map_err(|_| err!("deferred recovery deadline"))?;
	assert_eq!(delivered, expected);
	assert!(
		poll_until(Duration::from_secs(10), async || owed(services, &m)
			.await
			.is_ok_and(|rows| rows.is_empty()))
		.await,
		"acknowledged queue drains"
	);
	quiet(rx).await
}

async fn again(services: &Services, path: &Path, rx: &mut UnboundedReceiver<Value>) -> Result {
	assert!(
		owed(services, &manifest(path)?).await?.is_empty(),
		"successful delivery stays acknowledged after restart"
	);
	quiet(rx).await
}

async fn owed(services: &Services, m: &Manifest) -> Result<Vec<(Vec<u8>, SendingEvent)>> {
	let destination = Destination::Push(m.user.clone(), PUSHKEY.into());
	let mut events: Vec<_> = services
		.sending
		.db
		.active_requests_for(&destination)
		.try_collect()
		.await?;
	events.extend(
		services
			.sending
			.db
			.queued_requests(&destination)
			.try_collect::<Vec<_>>()
			.await?,
	);
	events
		.retain(|(_, event)| matches!(event, SendingEvent::Pdu(_) | SendingEvent::FrozenPush(_)));
	Ok(events)
}

async fn send(
	client: &Client<'_>,
	room: &RoomId,
	txn: &str,
	root: Option<&EventId>,
) -> Result<OwnedEventId> {
	let mut content = json!({"msgtype":"m.text","body":txn});
	if let Some(root) = root {
		content["m.relates_to"] = json!({"rel_type":"m.thread","event_id":root});
	}
	let body: Value = client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/{txn}")))
		.bearer_auth(client.token)
		.json(&content)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	Ok(field(&body, "event_id")?.try_into()?)
}

async fn receipt(client: &Client<'_>, room: &RoomId, event: &EventId, thread: &str) -> Result {
	client
		.services
		.client
		.clients
		.default
		.post(client.url(&format!("rooms/{room}/receipt/m.read.private/{event}")))
		.bearer_auth(client.token)
		.json(&json!({"thread_id":thread}))
		.send()
		.await?
		.error_for_status()?;
	Ok(())
}

async fn quiet(rx: &mut UnboundedReceiver<Value>) -> Result {
	match timeout(Duration::from_millis(500), rx.recv()).await {
		| Err(_) => Ok(()),
		| Ok(Some(body)) => Err(err!("unexpected gateway delivery: {body}")),
		| Ok(None) => Err(err!("owned gateway stopped")),
	}
}

async fn gateway(port: u16) -> Result<(GatewayTask, UnboundedReceiver<Value>)> {
	let listener = Gateway::bind(("127.0.0.1", port)).await?;
	let (tx, rx) = unbounded_channel();
	let task = tokio::spawn(async move {
		loop {
			let (mut socket, _) = listener.accept().await.expect("gateway listener");
			let mut bytes = Vec::new();
			let (start, length) = loop {
				let mut buf = [0; 4096];
				let n = socket.read(&mut buf).await.expect("headers");
				assert_ne!(n, 0);
				bytes.extend_from_slice(&buf[..n]);
				assert!(bytes.len() <= 65536, "bounded fixture request");
				if let Some(end) = bytes
					.windows(4)
					.position(|part| part == b"\r\n\r\n")
				{
					let headers = std::str::from_utf8(&bytes[..end]).expect("HTTP headers");
					let length = headers
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
						.expect("content length");
					assert!(length <= 65536);
					break (end.checked_add(4).expect("header bound"), length);
				}
			};
			let end = start.checked_add(length).expect("request bound");
			assert!(end <= 65536, "bounded fixture request");
			while bytes.len() < end {
				let mut buf = [0; 4096];
				let n = socket.read(&mut buf).await.expect("body");
				assert_ne!(n, 0);
				bytes.extend_from_slice(&buf[..n]);
			}
			let body = serde_json::from_slice(&bytes[start..end]).expect("notification JSON");
			tx.send(body).expect("owned capture receiver");
			socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"rejected\":[]}").await.expect("gateway response");
		}
	});
	Ok((GatewayTask(task), rx))
}
