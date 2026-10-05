#![cfg(test)]
#![cfg(debug_assertions)]
//! Actual canonical erasure and receipt requests must not recreate erased room
//! receipt data or accept an event from another room. No provider inputs.
mod client;

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, temp_dir, var},
	fs::{DirBuilder, remove_dir_all},
	net::TcpListener,
	path::{Path, PathBuf},
	process::{Child, Command, id},
	sync::Arc,
	thread::sleep,
	time::{Duration, Instant},
};

use futures::TryStreamExt;
use serde_json::{Value, json};
use tokio::{task::JoinHandle, time::timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err, http,
	ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, RoomId},
	utils::hash::sha256,
};
use tuwunel_database::{Interfix, refusal, serialize_key};
use tuwunel_service::Services;

use self::client::{Client, field, register, wait_until_ready};

const PHASE: &str = "TUWUNEL_RECEIPT_ERASURE_CASE";
const DIRECTORY: &str = "TUWUNEL_RECEIPT_ERASURE_DIRECTORY";
const ALICE: &str = "receipt-erasure-alice-owned-token-001";
const BOB: &str = "receipt-erasure-bob-owned-token-00001";
const MAPS: &[&str] = &[
	"roomuserid_privateread",
	"roomuserid_lastprivatereadupdate",
	"roomuserid_privatereadsync",
	"roomuserid_notificationcutoff",
	"roomuserid_lastnotificationread",
	"userroomid_notificationcount",
	"userroomid_highlightcount",
	"readreceiptid_readreceipt",
	"notificationid_index",
	"useridcount_notification",
];

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
struct OwnedTask<T>(JoinHandle<T>);
impl<T> Drop for OwnedTask<T> {
	fn drop(&mut self) { self.0.abort(); }
}

#[test]
fn receipts_preserve_canonical_room_ownership_during_erasure() -> Result {
	if let Ok(case) = var(PHASE) {
		return child(&PathBuf::from(var(DIRECTORY).expect("owned fixture path")), &case);
	}
	let mut results = Vec::new();
	for case in ["cross-room", "history-private", "room-private", "room-public"] {
		let path = temp_dir().join(format!("receipt-erasure-{}-{case}", id()));
		let mut builder = DirBuilder::new();
		#[cfg(unix)]
		builder.mode(0o700);
		builder.create(&path)?;
		let directory = OwnedDirectory(path);
		let mut child = OwnedChild(
			Command::new(current_exe()?)
				.env(PHASE, case)
				.env(DIRECTORY, &directory.0)
				.spawn()?,
		);
		let started = Instant::now();
		loop {
			if let Some(status) = child.0.try_wait()? {
				results.push((case, status.success()));
				break;
			}
			assert!(started.elapsed() < Duration::from_mins(2), "receipt case {case} deadline");
			sleep(Duration::from_millis(20));
		}
	}
	assert!(
		results.iter().all(|(_, passed)| *passed),
		"actual receipt controls: {results:?}"
	);
	Ok(())
}

fn child(directory: &Path, case: &str) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let database = directory.join("database");
	let mut args = Args::default_test(&["fresh"]);
	args.option.extend([
		format!("database_path={database:?}"),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		"listening=true".into(),
		"grant_admin_to_first_user=false".into(),
		"delete_rooms_after_leave=false".into(),
		"allow_local_presence=false".into(),
		"allow_outgoing_presence=false".into(),
		"suppress_push_when_active=false".into(),
		"log=\"error\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	drop(listener);
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");
		let exercise = async {
			let outcome = timeout(Duration::from_mins(1), exercise(&services, &base, case))
				.await
				.map_err(|_| err!("receipt erasure exercise deadline"))
				.and_then(|result| result);
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

async fn room(writer: &Client<'_>, owner: &Client<'_>, bob: &OwnedUserId) -> Result<OwnedRoomId> {
	let room = writer
		.create_room(&json!({"preset":"private_chat", "invite":[bob]}))
		.await?;
	owner
		.services
		.client
		.clients
		.default
		.post(owner.url(&format!("join/{room}")))
		.bearer_auth(BOB)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;
	Ok(room)
}

async fn send(writer: &Client<'_>, room: &RoomId, txn: &str) -> Result<OwnedEventId> {
	let body: Value = writer
		.services
		.client
		.clients
		.default
		.put(writer.url(&format!("rooms/{room}/send/m.room.message/{txn}")))
		.bearer_auth(ALICE)
		.json(&json!({"msgtype":"m.text", "body":txn}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	Ok(field(&body, "event_id")?.try_into()?)
}

async fn request(
	owner: &Client<'_>,
	room: &RoomId,
	event: &OwnedEventId,
	public: bool,
) -> Result<http::StatusCode> {
	let kind = if public { "m.read" } else { "m.read.private" };
	let response = owner
		.services
		.client
		.clients
		.default
		.post(owner.url(&format!("rooms/{room}/receipt/{kind}/{event}")))
		.bearer_auth(BOB)
		.json(&json!({"thread_id":"main"}))
		.send()
		.await?;
	Ok(response.status())
}

async fn exercise(services: &Arc<Services>, base: &str, case: &str) -> Result {
	wait_until_ready(services, base).await?;
	let alice = register(services, "receipt-erasure-alice", ALICE).await?;
	services.admin.make_user_admin(&alice).await?;
	let bob = register(services, "receipt-erasure-bob", BOB).await?;
	let writer = Client { services, base, token: ALICE };
	let owner = Client { services, base, token: BOB };
	services.client.clients.default.put(owner.url("pushrules/global/override/receipt-erasure-notify"))
		.bearer_auth(BOB).json(&json!({"conditions":[{"kind":"event_match", "key":"type", "pattern":"m.room.message"}], "actions":["notify"]}))
		.send().await?.error_for_status()?;
	let target = room(&writer, &owner, &bob).await?;
	let early = send(&writer, &target, "early").await?;
	let later = send(&writer, &target, "later").await?;
	if case == "cross-room" {
		let other = room(&writer, &owner, &bob).await?;
		let event = send(&writer, &other, "other").await?;
		let before = snapshot(services).await?;
		let status = request(&owner, &target, &event, false).await?;
		assert_eq!(
			status,
			http::StatusCode::BAD_REQUEST,
			"private receipt rejects an actual event from another room"
		);
		assert_eq!(
			snapshot(services).await?,
			before,
			"wrong-room receipt changes no receipt or notification rows"
		);
		return Ok(());
	}
	let whole_room = case.starts_with("room-");
	if whole_room {
		for client in [&owner, &writer] {
			services
				.client
				.clients
				.default
				.post(client.url(&format!("rooms/{target}/leave")))
				.bearer_auth(client.token)
				.json(&json!({}))
				.send()
				.await?
				.error_for_status()?;
		}
	}
	let mut pause = refusal::pause_next("pduid_pdu");
	let cleanup_services = services.clone();
	let cleanup_room = target.clone();
	let boundary = services
		.timeline
		.get_pdu_id(&later)
		.await?
		.pdu_count()
		.into_unsigned()
		.saturating_add(1)
		.into();
	let mut cleanup = OwnedTask(tokio::spawn(async move {
		if whole_room {
			let guard = cleanup_services
				.state
				.mutex
				.lock(&cleanup_room)
				.await;
			cleanup_services
				.delete
				.delete_room(&cleanup_room, false, guard)
				.await?;
		} else {
			assert_eq!(
				cleanup_services
					.timeline
					.purge_history(&cleanup_room, boundary, true)
					.await?,
				2,
				"controlled history operation erases exactly the two message PDUs"
			);
		}
		Ok::<_, tuwunel_core::Error>(())
	}));
	timeout(Duration::from_secs(10), pause.entered())
		.await
		.map_err(|_| err!("actual erasure did not reach its pre-dispatch boundary"))??;
	let receipt_services = services.clone();
	let receipt_base = base.to_owned();
	let receipt_room = target.clone();
	let public = case == "room-public";
	let mut receipt = OwnedTask(tokio::spawn(async move {
		let owner = Client {
			services: &receipt_services,
			base: &receipt_base,
			token: BOB,
		};
		request(&owner, &receipt_room, &early, public).await
	}));
	let early_result = timeout(Duration::from_millis(400), &mut receipt.0)
		.await
		.ok();
	let blocked = early_result.is_none();
	drop(pause);
	timeout(Duration::from_secs(10), &mut cleanup.0)
		.await
		.map_err(|_| err!("canonical erasure failed to finish"))???;
	let status = if let Some(result) = early_result {
		result??
	} else {
		timeout(Duration::from_secs(10), &mut receipt.0)
			.await
			.map_err(|_| err!("receipt failed to finish after erasure"))???
	};
	let private_key = serialize_key((&target, &bob, "main"))?;
	let ghost = services.db["roomuserid_privateread"]
		.get(&private_key)
		.await
		.is_ok();
	eprintln!(
		"controlled receipt after {case}: status={status}, blocked={blocked}, \
		 recreated_private_main={ghost}"
	);
	if whole_room {
		let prefix = serialize_key((&target, Interfix))?;
		for map in [
			"roomuserid_privateread",
			"roomuserid_notificationcutoff",
			"readreceiptid_readreceipt",
		] {
			let empty = services.db[map]
				.raw_keys_prefix_after(&prefix, None, 1)
				.await?
				.is_empty();
			eprintln!("controlled erased room {case}: {map} empty={empty}");
			assert!(empty, "a delayed receipt cannot recreate erased room rows in {map}");
		}
	}
	assert!(blocked, "receipt waits while actual canonical erasure owns the room");
	assert_eq!(
		status,
		if public {
			http::StatusCode::OK
		} else {
			http::StatusCode::NOT_FOUND
		},
		"erased private target is refused; a public update for a gone room is ignored"
	);
	assert!(!ghost, "receipt cannot recreate erased private main data");
	Ok(())
}

async fn snapshot(services: &Services) -> Result<Vec<(String, Vec<u8>, sha256::Digest)>> {
	let mut result = Vec::new();
	for map in MAPS {
		let rows: Vec<_> = services.db[map]
			.raw_stream()
			.map_ok(|(key, value)| ((*map).to_owned(), key.to_vec(), sha256::hash(value)))
			.try_collect()
			.await?;
		result.extend(rows);
	}
	Ok(result)
}
