#![cfg(test)]
//! Real API acceptance, native SIGKILL, frozen decisions, coupled recipient
//! progress and read/erasure controls. Real-D1 ack boundaries remain separate.
mod client;

#[cfg(unix)]
use std::os::unix::{fs::DirBuilderExt, process::ExitStatusExt};
use std::{
	env::{current_exe, temp_dir, var},
	fs::{DirBuilder, read, remove_dir_all, write},
	future::pending,
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
	net::TcpListener as Gateway,
	sync::mpsc::{UnboundedReceiver, unbounded_channel},
	time::timeout,
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, UserId},
	utils::hash::sha256,
};
use tuwunel_database::{Database, refusal, serialize_key};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

const PHASE: &str = "TUWUNEL_NOTIFICATION_RECOVERY_PHASE";
const DIRECTORY: &str = "TUWUNEL_NOTIFICATION_RECOVERY_DIRECTORY";
const ALICE: &str = "notification-recovery-alice-token-001";
const BOB: &str = "notification-recovery-bob-token-00001";
const CAROL: &str = "notification-recovery-carol-token-01";
const PLAN: &str = "pduid_notificationplan";
const RECEIPT: &str = "notificationreceiptid_record";
const TABLES: &[&str] = &[
	"global",
	PLAN,
	RECEIPT,
	"notificationid_index",
	"pduid_pdu",
	"eventid_pduid",
	"eventid_outlierpdu",
	"roomid_tscount_pducount",
	"userdevicetxnid_response",
	"roomid_pduleaves",
	"userroomid_notificationcount",
	"userroomid_highlightcount",
	"useridcount_notification",
	"roomuserid_lastnotificationread",
	"roomuserid_notificationcutoff",
	"roomuserid_privateread",
	"roomuserid_lastprivatereadupdate",
	"roomuserid_privatereadsync",
	"servernameevent_data",
	"servercurrentevent_data",
	"adminjobid_record",
];
type Snapshot = Vec<(String, Vec<u8>, sha256::Digest)>;

#[derive(Deserialize, Serialize)]
struct Manifest {
	boundary: String,
	room: OwnedRoomId,
	bob: OwnedUserId,
	carol: OwnedUserId,
	gateway: u16,
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

#[test]
fn notification_intents_resume_without_double_counts_or_changed_rules() -> Result {
	if let Ok(phase) = var(PHASE) {
		return child(&PathBuf::from(var(DIRECTORY).expect("owned directory")), &phase);
	}
	for boundary in ["zero", "partial"] {
		let directory = temp_dir().join(format!("notification-recovery-{}-{boundary}", id()));
		let mut builder = DirBuilder::new();
		#[cfg(unix)]
		builder.mode(0o700);
		builder.create(&directory)?;
		let directory = OwnedDirectory(directory);
		write(directory.0.join("boundary"), boundary)?;
		run_child(&directory.0, "prepare", false)?;
		run_child(&directory.0, "accept", true)?;
		for phase in ["corrupt", "snapshot", "reject", "verify", "repair", "recover", "again"] {
			run_child(&directory.0, phase, false)?;
		}
	}
	Ok(())
}

fn run_child(directory: &Path, phase: &str, kill: bool) -> Result {
	let mut child = OwnedChild(
		Command::new(current_exe()?)
			.env(PHASE, phase)
			.env(DIRECTORY, directory)
			.spawn()?,
	);
	let deadline = Instant::now()
		.checked_add(Duration::from_mins(2))
		.expect("valid deadline");
	loop {
		if kill && directory.join("accept.ready").exists() {
			assert!(child.0.try_wait()?.is_none(), "accepted child still running");
			child.0.kill()?;
			let status = child.0.wait()?;
			#[cfg(unix)]
			assert_eq!(status.signal(), Some(9));
			return Ok(());
		}
		if let Some(status) = child.0.try_wait()? {
			assert!(!kill && status.success(), "notification phase {phase} failed: {status}");
			return Ok(());
		}
		assert!(Instant::now() < deadline, "notification phase {phase} exceeded deadline");
		thread::sleep(Duration::from_millis(20));
	}
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
		format!("listening={}", matches!(phase, "prepare" | "accept" | "recover")),
		"grant_admin_to_first_user=false".into(),
		"ip_range_denylist=[]".into(),
		"suppress_push_when_active=false".into(),
		"startup_netburst=true".into(),
		"sender_retry_backoff_limit=1".into(),
		"log=\"error\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	drop(listener);
	let result = runtime.block_on(async {
        if matches!(phase, "corrupt" | "snapshot" | "verify" | "repair") {
            let db = Database::open(&server.server).await?;
            let result = raw_phase(&db, directory, phase).await;
            db.close().await;
            return result;
        }
        let gateway = if phase == "recover" {
            Some(Gateway::bind(("127.0.0.1", manifest(directory)?.gateway)).await?)
        } else { None };
        let (tx, mut rx) = unbounded_channel();
        let stub = gateway.map(|gateway| tokio::spawn(async move {
            loop {
                let (mut socket, _) = gateway.accept().await.expect("owned gateway");
                let mut bytes = Vec::new();
                let (body, length) = loop {
                    let mut buffer = [0_u8; 4096];
                    let count = socket.read(&mut buffer).await.expect("owned request");
                    assert!(count != 0, "complete gateway headers");
                    bytes.extend_from_slice(&buffer[..count]);
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&bytes[..end]).expect("HTTP headers");
                        let length = headers.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse::<usize>().expect("HTTP body length"))
                        }).expect("request length");
                        break (end.saturating_add(4), length);
                    }
                };
                while bytes.len() < body.saturating_add(length) {
                    let mut buffer = [0_u8; 4096];
                    let count = socket.read(&mut buffer).await.expect("owned body");
                    assert!(count != 0, "complete gateway body"); bytes.extend_from_slice(&buffer[..count]);
                }
                let notification: Value = serde_json::from_slice(&bytes[body..body.saturating_add(length)]).expect("gateway JSON");
                tx.send(notification).expect("owned receiver");
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"rejected\":[]}").await.expect("gateway response");
            }
        }));
        let start = async_start(&server).await;
        if phase == "reject" {
            assert!(start.is_err(), "corrupt later plan must refuse before readiness");
            assert!(server.services.lock().await.is_none());
            return Ok(());
        }
        let services = start?;
        services.pusher.pause_notification_retry_for_test(true);
        let base = format!("http://127.0.0.1:{port}");
        let exercise = async {
            let result = match phase {
                | "prepare" => prepare(&services, &base, directory).await,
                | "accept" => accept(&services, &base, directory).await,
                | "recover" => recover(&services, &base, directory, &mut rx).await,
                | "again" => {
                    let current = completed_snapshot(&services.db).await?;
                    assert_eq!(current, serde_json::from_slice::<Snapshot>(&read(directory.join("completed.snapshot"))?)?, "restart changes no completed notification rows");
                    assert!(services.db[PLAN].raw_keys_prefix_after(&[], None, 1).await?.is_empty());
                    Ok(())
                },
                | _ => panic!("owned phase"),
            };
            let shutdown = server.server.shutdown();
            result.and(shutdown)
        };
        let (run, outcome) = tokio::join!(async_run(&server), exercise);
        if let Some(stub) = stub { stub.abort(); }
        drop(services);
        outcome.and(run).and(async_stop(&server).await)
    });
	drop(runtime);
	result
}

async fn prepare(services: &Services, base: &str, directory: &Path) -> Result {
	wait_until_ready(services, base).await?;
	register(services, "notificationalice", ALICE).await?;
	let bob = register(services, "notificationbob", BOB).await?;
	let carol = register(services, "notificationcarol", CAROL).await?;
	let client = Client { services, base, token: ALICE };
	let room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	for token in [BOB, CAROL] {
		services
			.client
			.clients
			.default
			.post(client.url(&format!("join/{room}")))
			.bearer_auth(token)
			.json(&json!({}))
			.send()
			.await?
			.error_for_status()?;
	}
	for user in [&bob, &carol] {
		services
			.pusher
			.reset_notification_counts(user, &room)
			.await?;
	}
	rules(services, base, BOB, json!(["notify", {"set_tweak":"sound","value":"original"}]))
		.await?;
	rules(services, base, CAROL, json!(["notify", {"set_tweak":"highlight","value":true}]))
		.await?;
	let gateway = TcpListener::bind(("127.0.0.1", 0))?
		.local_addr()?
		.port();
	for (token, key) in [(BOB, "bob-original"), (CAROL, "carol-original")] {
		pusher(services, base, token, key, gateway).await?;
	}
	let manifest = Manifest {
		boundary: String::from_utf8(read(directory.join("boundary"))?)?,
		room,
		bob,
		carol,
		gateway,
		events: Vec::new(),
	};
	write(directory.join("manifest.json"), serde_json::to_vec(&manifest)?)?;
	Ok(())
}

async fn accept(services: &Services, base: &str, directory: &Path) -> Result {
	wait_until_ready(services, base).await?;
	let mut manifest = manifest(directory)?;
	for index in 0..2 {
		// Sender read/reset state is part of PDU acceptance. Target the later
		// recipient commits: Bob completes, then Carol's whole batch refuses.
		refusal::refuse_after(RECEIPT, usize::from(manifest.boundary == "partial"));
		let event =
			message(services, base, &manifest.room, &format!("accepted-{index}"), None).await?;
		assert_eq!(refusal::pending(), 0);
		let raw = services.timeline.get_pdu_id(&event).await?;
		let plan: Value = serde_json::from_slice(&services.db[PLAN].get(&raw).await?)?;
		assert_eq!(plan["next"], usize::from(manifest.boundary == "partial"));
		assert_eq!(
			plan["recipients"]
				.as_array()
				.expect("recipients")
				.len(),
			2
		);
		manifest.events.push(event);
	}
	assert_counts(
		services,
		&manifest.bob,
		&manifest.room,
		if manifest.boundary == "partial" { 2 } else { 0 },
		0,
	)
	.await?;
	assert_counts(services, &manifest.carol, &manifest.room, 0, 0).await?;
	// These edits must not affect previously accepted actions or destinations.
	rules(services, base, BOB, json!(["dont_notify"])).await?;
	rules(services, base, CAROL, json!(["dont_notify"])).await?;
	pusher(services, base, CAROL, "later", manifest.gateway).await?;
	write(directory.join("manifest.json"), serde_json::to_vec(&manifest)?)?;
	write(directory.join("accept.ready"), b"accepted; no fixture WAL barrier")?;
	pending().await
}

async fn recover(
	services: &Services,
	base: &str,
	directory: &Path,
	rx: &mut UnboundedReceiver<Value>,
) -> Result {
	let manifest = manifest(directory)?;
	assert!(
		services.db[PLAN]
			.raw_keys_prefix_after(&[], None, 1)
			.await?
			.is_empty()
	);
	assert_counts(services, &manifest.bob, &manifest.room, 2, 0).await?;
	assert_counts(services, &manifest.carol, &manifest.room, 2, 2).await?;
	let mut notices = std::collections::BTreeSet::new();
	timeout(Duration::from_secs(20), async {
		while notices.len() < 4 {
			let body = rx.recv().await.expect("owned gateway running");
			let notification = &body["notification"];
			let Some(event) = notification["event_id"].as_str() else {
				continue;
			};
			assert!(
				manifest
					.events
					.iter()
					.any(|expected| expected.as_str() == event)
			);
			let device = &notification["devices"][0];
			let key = device["pushkey"].as_str().expect("pushkey");
			let tweaks = &device["tweaks"];
			let recipient = if tweaks["sound"] == "original" {
				"bob"
			} else {
				assert_eq!(tweaks["highlight"], true);
				"carol"
			};
			assert_eq!(
				key,
				format!("{recipient}-original"),
				"newly registered pusher was not in frozen manifest"
			);
			assert!(
				notices.insert((event.to_owned(), recipient)),
				"no duplicate native delivery after successful receipt"
			);
		}
	})
	.await
	.map_err(|_| err!("frozen original notifications were not delivered"))?;
	services
		.pusher
		.retry_notifications_for_test()
		.await?;
	assert_counts(services, &manifest.bob, &manifest.room, 2, 0).await?;
	assert_counts(services, &manifest.carol, &manifest.room, 2, 2).await?;
	wait_until_ready(services, base).await?;
	if manifest.boundary == "zero" {
		live_controls(services, base, &manifest).await?;
	}
	write(
		directory.join("completed.snapshot"),
		serde_json::to_vec(&completed_snapshot(&services.db).await?)?,
	)?;
	Ok(())
}

async fn live_controls(services: &Services, base: &str, manifest: &Manifest) -> Result {
	for token in [BOB, CAROL] {
		rules(services, base, token, json!(["notify", {"set_tweak":"highlight","value":true}]))
			.await?;
	}
	// A read that advances beyond unfinished work cancels every older alert.
	refusal::refuse_next(RECEIPT);
	let root = message(services, base, &manifest.room, "late-read", None).await?;
	private_read(services, base, BOB, &manifest.room, &root, None).await?;
	private_read(services, base, CAROL, &manifest.room, &root, None).await?;
	services
		.pusher
		.retry_notifications_for_test()
		.await?;
	for user in [&manifest.bob, &manifest.carol] {
		assert_counts(services, user, &manifest.room, 0, 0).await?;
	}
	// Main reads do not consume a deferred thread notification.
	refusal::refuse_next(RECEIPT);
	let reply = message(services, base, &manifest.room, "thread-after-main", Some(&root)).await?;
	for user in [&manifest.bob, &manifest.carol] {
		services
			.pusher
			.reset_notification_counts(user, &manifest.room)
			.await?;
	}
	services
		.pusher
		.retry_notifications_for_test()
		.await?;
	for user in [&manifest.bob, &manifest.carol] {
		let key = serialize_key((user, &manifest.room, &root))?;
		for map in ["userroomid_notificationcount", "userroomid_highlightcount"] {
			assert_eq!(services.db[map].get(&key).await?.as_ref(), 1_u64.to_be_bytes());
		}
	}
	private_read(services, base, BOB, &manifest.room, &reply, Some(&root)).await?;
	private_read(services, base, CAROL, &manifest.room, &reply, Some(&root)).await?;
	// A deactivated/erased recipient never has its counters recreated.
	refusal::refuse_next(RECEIPT);
	let erased = message(services, base, &manifest.room, "erasure-before-replay", None).await?;
	services.users.set_erased(&manifest.bob).await?;
	services
		.pusher
		.retry_notifications_for_test()
		.await?;
	assert_counts(services, &manifest.bob, &manifest.room, 0, 0).await?;
	assert_counts(services, &manifest.carol, &manifest.room, 1, 1).await?;
	let raw = services.timeline.get_pdu_id(&erased).await?;
	let mut key = raw.as_ref().to_vec();
	key.push(0xFF);
	key.extend_from_slice(manifest.bob.as_bytes());
	let receipt: Value = serde_json::from_slice(&services.db[RECEIPT].get(&key).await?)?;
	assert_eq!(receipt["canceled"], true);
	services.users.clear_erased(&manifest.bob).await?;
	// Completed replay writes nothing; an accepted-message retry adds no PDU.
	let before = completed_snapshot(&services.db).await?;
	assert_eq!(
		message(services, base, &manifest.room, "erasure-before-replay", None).await?,
		erased
	);
	services
		.pusher
		.retry_notifications_for_test()
		.await?;
	assert_eq!(completed_snapshot(&services.db).await?, before);
	// Plan capacity refuses before accepting another event. There is no
	// partial 65th PDU, transaction receipt or recipient state.
	for index in 0..64 {
		refusal::refuse_next(RECEIPT);
		message(services, base, &manifest.room, &format!("capacity-{index}"), None).await?;
		assert_eq!(refusal::pending(), 0);
	}
	assert_eq!(
		services.db[PLAN]
			.raw_keys_prefix_after(&[], None, 65)
			.await?
			.len(),
		64
	);
	let before = completed_snapshot(&services.db).await?;
	let response = services
		.client
		.clients
		.default
		.put(format!(
			"{base}/_matrix/client/v3/rooms/{}/send/m.room.message/capacity-overflow",
			manifest.room
		))
		.bearer_auth(ALICE)
		.json(&json!({"msgtype":"m.text","body":"must not be accepted"}))
		.send()
		.await?;
	assert_eq!(response.status(), 429);
	assert_eq!(completed_snapshot(&services.db).await?, before);
	services
		.pusher
		.retry_notifications_for_test()
		.await?;
	assert!(
		services.db[PLAN]
			.raw_keys_prefix_after(&[], None, 1)
			.await?
			.is_empty()
	);
	assert_counts(services, &manifest.bob, &manifest.room, 64, 64).await?;
	assert_counts(services, &manifest.carol, &manifest.room, 65, 65).await?;
	let before = completed_snapshot(&services.db).await?;
	services
		.pusher
		.retry_notifications_for_test()
		.await?;
	assert_eq!(completed_snapshot(&services.db).await?, before);
	Ok(())
}

async fn raw_phase(db: &Database, directory: &Path, phase: &str) -> Result {
	if phase == "snapshot" {
		write(
			directory.join("refusal.snapshot"),
			serde_json::to_vec(&snapshot(db, TABLES).await?)?,
		)?;
		return Ok(());
	}
	if phase == "verify" {
		assert_eq!(
			snapshot(db, TABLES).await?,
			serde_json::from_slice::<Snapshot>(&read(directory.join("refusal.snapshot"))?)?,
			"refused startup changed inspected storage, including global sequence"
		);
		return Ok(());
	}
	let manifest = manifest(directory)?;
	let raw = db["eventid_pduid"]
		.get(manifest.events.last().expect("second event"))
		.await?
		.to_vec();
	let file = directory.join("original.plan");
	let bytes = if phase == "repair" {
		read(file)?
	} else {
		let original = db[PLAN].get(&raw).await?.to_vec();
		write(file, &original)?;
		let mut plan: Value = serde_json::from_slice(&original)?;
		plan["next"] = json!(2);
		serde_json::to_vec(&plan)?
	};
	db[PLAN].insert(&raw, bytes).await
}

async fn snapshot(db: &Database, tables: &[&str]) -> Result<Snapshot> {
	let mut rows = Vec::new();
	for &name in tables {
		let stream = db[name].raw_stream();
		pin_mut!(stream);
		while let Some((key, value)) = stream.try_next().await? {
			rows.push((name.to_owned(), key.to_vec(), sha256::hash(value)));
		}
	}
	Ok(rows)
}
async fn completed_snapshot(db: &Database) -> Result<Snapshot> {
	snapshot(db, &[
		PLAN,
		RECEIPT,
		"pduid_pdu",
		"eventid_pduid",
		"userdevicetxnid_response",
		"userroomid_notificationcount",
		"userroomid_highlightcount",
		"useridcount_notification",
		"roomuserid_notificationcutoff",
		"roomuserid_lastnotificationread",
	])
	.await
}
fn manifest(directory: &Path) -> Result<Manifest> {
	Ok(serde_json::from_slice(&read(directory.join("manifest.json"))?)?)
}

async fn rules(services: &Services, base: &str, token: &str, actions: Value) -> Result {
	services
		.client
		.clients
		.default
		.put(format!("{base}/_matrix/client/v3/pushrules/global/override/frozen"))
		.bearer_auth(token)
		.json(&json!({"conditions":[{"kind":"room_member_count","is":"3"}],"actions":actions}))
		.send()
		.await?
		.error_for_status()?;
	Ok(())
}
async fn pusher(services: &Services, base: &str, token: &str, key: &str, port: u16) -> Result {
	services.client.clients.default.post(format!("{base}/_matrix/client/v3/pushers/set")).bearer_auth(token).json(&json!({
        "pushkey":key,"app_id":"notification-recovery","kind":"http","app_display_name":"Native recovery fixture","device_display_name":"Disposable fixture","lang":"en",
        "data":{"url":format!("http://127.0.0.1:{port}/_matrix/push/v1/notify"),"disable_badge_count":true}
    })).send().await?.error_for_status()?;
	Ok(())
}
async fn message(
	services: &Services,
	base: &str,
	room: &OwnedRoomId,
	txn: &str,
	root: Option<&OwnedEventId>,
) -> Result<OwnedEventId> {
	let mut body = json!({"msgtype":"m.text","body":txn});
	if let Some(root) = root {
		body["m.relates_to"] = json!({"rel_type":"m.thread","event_id":root});
	}
	let response: Value = services
		.client
		.clients
		.default
		.put(format!("{base}/_matrix/client/v3/rooms/{room}/send/m.room.message/{txn}"))
		.bearer_auth(ALICE)
		.json(&body)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	Ok(response["event_id"]
		.as_str()
		.expect("accepted event id")
		.try_into()?)
}
async fn private_read(
	services: &Services,
	base: &str,
	token: &str,
	room: &OwnedRoomId,
	event: &OwnedEventId,
	thread: Option<&OwnedEventId>,
) -> Result {
	let mut body = json!({});
	if let Some(thread) = thread {
		body["thread_id"] = json!(thread);
	}
	services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/rooms/{room}/receipt/m.read.private/{event}"))
		.bearer_auth(token)
		.json(&body)
		.send()
		.await?
		.error_for_status()?;
	Ok(())
}
async fn assert_counts(
	services: &Services,
	user: &UserId,
	room: &OwnedRoomId,
	notify: u64,
	highlight: u64,
) -> Result {
	for (map, value) in [
		("userroomid_notificationcount", notify),
		("userroomid_highlightcount", highlight),
	] {
		assert_eq!(
			services.db[map]
				.qry(&(user, room))
				.await?
				.as_ref(),
			value.to_be_bytes(),
			"{map} for {user}"
		);
	}
	Ok(())
}
