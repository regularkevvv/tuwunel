#![cfg(test)]
#![cfg(debug_assertions)]
//! Real completed notifications in unrelated rooms must not consume a quiet
//! room's receipt budget. Native upgrades resume after refusal and an actual
//! after-page-commit SIGKILL; provider acknowledgement faults remain separate.
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
	thread::sleep,
	time::{Duration, Instant},
};

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::timeout;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err, http,
	matrix::pdu::RawPduId,
	ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, RoomId},
	utils::hash::sha256,
};
use tuwunel_database::{Database, Interfix, refusal, serialize_key};
use tuwunel_service::Services;

use self::client::{Client, field, poll_until, register, wait_until_ready};

const PHASE: &str = "TUWUNEL_NOTIFICATION_INDEX_PHASE";
const DIRECTORY: &str = "TUWUNEL_NOTIFICATION_INDEX_DIRECTORY";
const ALICE: &str = "notification-index-alice-owned-token-001";
const BOB: &str = "notification-index-bob-owned-token-00001";
const INDEX: &str = "notificationid_index";
const DONE: &[u8] = b"notification_index_v1";
const CURSOR: &[u8] = b"notification_index_cursor_v1";
const ROOMS: u64 = 17;
const MESSAGES: u64 = 256;
const ORIGINALS: &[&str] = &[
	"useridcount_notification",
	"userroomid_notificationcount",
	"userroomid_highlightcount",
	"roomuserid_notificationcutoff",
	"roomuserid_lastnotificationread",
	"roomuserid_privateread",
	"roomuserid_lastprivatereadupdate",
	"roomuserid_privatereadsync",
];
type Snapshot = Vec<(String, Vec<u8>, sha256::Digest)>;

#[derive(Deserialize, Serialize)]
struct Manifest {
	bob: OwnedUserId,
	quiet: OwnedRoomId,
	busy: Vec<OwnedRoomId>,
	early: OwnedEventId,
	later: OwnedEventId,
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
fn scoped_receipts_and_index_upgrade_survive_refusal_and_kill() -> Result {
	if let Ok(phase) = var(PHASE) {
		return child(&PathBuf::from(var(DIRECTORY).expect("owned fixture path")), &phase);
	}
	let directory = temp_dir().join(format!("notification-scope-index-{}", id()));
	let mut builder = DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&directory)?;
	let directory = OwnedDirectory(directory);
	for phase in [
		"fresh",
		"legacy",
		"refuse",
		"inspect-refused",
		"kill",
		"inspect-killed",
		"recover",
		"again",
	] {
		run_child(&directory.0, phase)?;
	}
	Ok(())
}

fn run_child(directory: &Path, phase: &str) -> Result {
	let mut child = OwnedChild(
		Command::new(current_exe()?)
			.env(PHASE, phase)
			.env(DIRECTORY, directory)
			.spawn()?,
	);
	let start = Instant::now();
	loop {
		if phase == "kill" && directory.join("migration.ready").exists() {
			assert!(child.0.try_wait()?.is_none(), "migration child is actually paused");
			child.0.kill()?;
			let status = child.0.wait()?;
			#[cfg(unix)]
			assert_eq!(status.signal(), Some(9), "actual migration SIGKILL");
			return Ok(());
		}
		if let Some(status) = child.0.try_wait()? {
			assert!(phase != "kill" && status.success(), "index phase {phase}: {status}");
			return Ok(());
		}
		assert!(start.elapsed() < Duration::from_mins(5), "index phase {phase} deadline");
		sleep(Duration::from_millis(20));
	}
}

fn child(directory: &Path, phase: &str) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let modes: &[&str] = if phase == "fresh" { &["fresh"] } else { &[] };
	let mut args = Args::default_test(modes);
	let database = directory.join("database");
	args.option.extend([
		format!("database_path={database:?}"),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		format!("listening={}", matches!(phase, "fresh" | "recover")),
		"grant_admin_to_first_user=false".into(),
		"allow_local_presence=false".into(),
		"allow_outgoing_presence=false".into(),
		"suppress_push_when_active=false".into(),
		"client_sync_timeout_min=0".into(),
		"delete_rooms_after_leave=false".into(),
		"log=\"error\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	drop(listener);
	let result = runtime.block_on(async {
		if phase == "legacy" || phase.starts_with("inspect-") {
			let db = Database::open(&server.server).await?;
			let outcome = raw_phase(&db, directory, phase).await;
			db.close().await;
			return outcome;
		}
		if phase == "refuse" {
			refusal::refuse_next(INDEX);
			assert!(async_start(&server).await.is_err(), "index page refusal blocks readiness");
			assert_eq!(refusal::pending(), 0, "the actual migration commit was refused");
			assert!(
				server.services.lock().await.is_none(),
				"refused migration publishes no services"
			);
			return Ok(());
		}
		if phase == "kill" {
			let mut pause = Services::pause_notification_index_migration_for_test();
			let observe = async {
				let after = pause.entered().await?;
				write(directory.join("migration.after"), &after)?;
				write(directory.join("migration.ready"), b"page-committed")?;
				pending::<Result>().await
			};
			let (start, observed) = tokio::join!(async_start(&server), observe);
			drop(start);
			return observed;
		}
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");
		let exercise = async {
			let outcome = timeout(Duration::from_mins(4), async {
				match phase {
					| "fresh" => prepare(&services, &base, directory).await,
					| "recover" => controls(&services, &base, directory).await,
					| "again" => {
						assert_completed(&services.db).await?;
						assert_saved(&services.db, directory, "final", ORIGINALS).await?;
						assert_saved(&services.db, directory, "final-index", &[INDEX]).await
					},
					| _ => panic!("owned fixture phase"),
				}
			})
			.await
			.map_err(|_| err!("notification index exercise deadline"))
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

async fn room(alice: &Client<'_>, bob: &Client<'_>, user: &OwnedUserId) -> Result<OwnedRoomId> {
	let room = alice
		.create_room(&json!({"preset":"private_chat", "invite":[user]}))
		.await?;
	bob.services
		.client
		.clients
		.default
		.post(bob.url(&format!("join/{room}")))
		.bearer_auth(BOB)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;
	Ok(room)
}

async fn send(alice: &Client<'_>, room: &RoomId, txn: &str) -> Result<OwnedEventId> {
	let response: Value = alice
		.services
		.client
		.clients
		.default
		.put(alice.url(&format!("rooms/{room}/send/m.room.message/{txn}")))
		.bearer_auth(ALICE)
		.json(&json!({"msgtype":"m.text", "body":txn}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	Ok(field(&response, "event_id")?.try_into()?)
}

async fn prepare(services: &Services, base: &str, directory: &Path) -> Result {
	wait_until_ready(services, base).await?;
	let alice = register(services, "notification-index-alice", ALICE).await?;
	services.admin.make_user_admin(&alice).await?;
	let bob = register(services, "notification-index-bob", BOB).await?;
	let writer = Client { services, base, token: ALICE };
	let owner = Client { services, base, token: BOB };
	services.client.clients.default.put(owner.url("pushrules/global/override/index-notify"))
		.bearer_auth(BOB).json(&json!({"conditions":[{"kind":"event_match", "key":"type", "pattern":"m.room.message"}], "actions":["notify"]}))
		.send().await?.error_for_status()?;
	// Keep invitation/state notifications out of this controlled inventory.
	// The real message rule precedes this user rule and server defaults.
	services
		.client
		.clients
		.default
		.put(owner.url("pushrules/global/override/index-quiet-default"))
		.bearer_auth(BOB)
		.query(&[("after", "index-notify")])
		.json(&json!({"conditions":[], "actions":["dont_notify"]}))
		.send()
		.await?
		.error_for_status()?;
	let quiet = room(&writer, &owner, &bob).await?;
	let early = send(&writer, &quiet, "early").await?;
	let later = send(&writer, &quiet, "later").await?;
	let mut busy = Vec::new();
	for room_number in 0..ROOMS {
		let room = room(&writer, &owner, &bob).await?;
		for message in 0..MESSAGES {
			send(&writer, &room, &format!("busy-{room_number}-{message}")).await?;
		}
		assert_eq!(
			services
				.pusher
				.notification_count(&bob, &room)
				.await?,
			MESSAGES,
			"each busy room has all accepted notifications"
		);
		busy.push(room);
	}
	assert_eq!(
		services
			.pusher
			.notification_count(&bob, &quiet)
			.await?,
		2,
		"both quiet messages completed before the upgrade"
	);
	assert!(
		poll_until(Duration::from_secs(10), async || {
			services.db["pduid_notificationplan"]
				.raw_keys_after(None, 1)
				.await
				.is_ok_and(|keys| keys.is_empty())
		})
		.await,
		"all real notification completions finished"
	);
	let prefix = serialize_key((&bob, Interfix))?;
	assert_eq!(
		services.db["useridcount_notification"]
			.raw_keys_prefix_after(&prefix, None, 5000)
			.await?
			.len(),
		4354,
		"accepted real API notifications exceed the old user-wide 4096-row budget"
	);
	write(
		directory.join("manifest.json"),
		serde_json::to_vec(&Manifest { bob, quiet, busy, early, later })?,
	)?;
	save(&services.db, directory, "original", ORIGINALS).await?;
	save(&services.db, directory, "original-index", &[INDEX]).await
}

async fn raw_phase(db: &Database, directory: &Path, phase: &str) -> Result {
	if phase == "legacy" {
		loop {
			let keys = db[INDEX].raw_keys_after(None, 64).await?;
			if keys.is_empty() {
				break;
			}
			let mut txn = db.txn();
			for key in keys {
				txn.del_raw(&db[INDEX], key);
			}
			txn.execute().await?;
		}
		let mut txn = db.txn();
		txn.insert_raw(&db["global"], b"version", 19_u64.to_be_bytes());
		txn.del_raw(&db["global"], DONE);
		txn.del_raw(&db["global"], CURSOR);
		txn.execute().await?;
	} else {
		assert_eq!(
			db["global"].get(b"version").await?.as_ref(),
			19_u64.to_be_bytes(),
			"an incomplete migration cannot stamp version 20"
		);
		assert!(
			db["global"]
				.get(DONE)
				.await
				.expect_err("unfinished migration has no completion marker")
				.is_not_found(),
			"completion marker is genuinely missing"
		);
		if phase == "inspect-refused" {
			assert!(
				db[INDEX]
					.raw_keys_after(None, 1)
					.await?
					.is_empty(),
				"refused first page writes no index rows"
			);
			assert!(
				db["global"]
					.get(CURSOR)
					.await
					.expect_err("refused first page has no cursor")
					.is_not_found(),
				"refused cursor is genuinely missing"
			);
		} else {
			let cursor: Value = serde_json::from_slice(&db["global"].get(CURSOR).await?)?;
			assert_eq!(cursor["version"], 1, "cursor uses the supported format");
			let after: Vec<u8> = serde_json::from_value(cursor["after"].clone())?;
			assert_eq!(
				after,
				read(directory.join("migration.after"))?,
				"persisted cursor matches the acknowledged page"
			);
			let source = db["useridcount_notification"]
				.raw_keys_after(None, 64)
				.await?;
			assert_eq!(source.last(), Some(&after), "actual first page cursor committed");
			assert_eq!(
				db[INDEX].raw_keys_after(None, 129).await?.len(),
				128,
				"64 real completed records produced both index directions before kill"
			);
		}
	}
	assert_saved(db, directory, "original", ORIGINALS).await
}

async fn receipt(owner: &Client<'_>, manifest: &Manifest, expected: http::StatusCode) -> Result {
	let response =
		owner
			.services
			.client
			.clients
			.default
			.post(owner.url(&format!(
				"rooms/{}/receipt/m.read.private/{}",
				manifest.quiet, manifest.early
			)))
			.bearer_auth(BOB)
			.json(&json!({"thread_id":"main"}))
			.send()
			.await?;
	let status = response.status();
	let body: Value = response.json().await?;
	assert_eq!(status, expected, "quiet-room receipt: {body}");
	Ok(())
}

fn reverse(raw: RawPduId, bob: &OwnedUserId) -> Vec<u8> {
	let mut key = vec![1];
	key.extend_from_slice(raw.as_ref());
	key.push(tuwunel_database::SEP);
	key.extend_from_slice(bob.as_bytes());
	key
}

async fn controls(services: &Services, base: &str, directory: &Path) -> Result {
	wait_until_ready(services, base).await?;
	assert_completed(&services.db).await?;
	assert_saved(&services.db, directory, "original", ORIGINALS).await?;
	assert_saved(&services.db, directory, "original-index", &[INDEX]).await?;
	let manifest: Manifest = serde_json::from_slice(&read(directory.join("manifest.json"))?)?;
	let owner = Client { services, base, token: BOB };
	let early = services
		.timeline
		.get_pdu_id(&manifest.early)
		.await?;
	let later = services
		.timeline
		.get_pdu_id(&manifest.later)
		.await?;
	receipt_controls(services, &owner, &manifest, later).await?;
	erasure_controls(services, &manifest, early, later).await?;
	room_erasure_controls(services, base, &manifest.bob).await?;
	save(&services.db, directory, "final", ORIGINALS).await?;
	save(&services.db, directory, "final-index", &[INDEX]).await
}

async fn receipt_controls(
	services: &Services,
	owner: &Client<'_>,
	manifest: &Manifest,
	later: RawPduId,
) -> Result {
	let key = reverse(later, &manifest.bob);
	let saved = services.db[INDEX].get(&key).await?.to_vec();
	services.db[INDEX]
		.insert(&key, b"corrupt-reciprocal-index")
		.await?;
	let before = snapshot(&services.db, ORIGINALS).await?;
	let index_before = snapshot(&services.db, &[INDEX]).await?;
	receipt(owner, manifest, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	assert_eq!(
		snapshot(&services.db, ORIGINALS).await?,
		before,
		"corruption refuses all receipt/count writes"
	);
	assert_eq!(
		snapshot(&services.db, &[INDEX]).await?,
		index_before,
		"receipt refusal repairs no corrupt index rows"
	);
	services.db[INDEX].insert(&key, &saved).await?;
	refusal::refuse_next("roomuserid_notificationcutoff");
	receipt(owner, manifest, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	assert_eq!(refusal::pending(), 0, "actual receipt commit consumed the refusal");
	assert_eq!(
		snapshot(&services.db, ORIGINALS).await?,
		before,
		"refused receipt commit is atomic"
	);
	receipt(owner, manifest, http::StatusCode::OK).await?;
	assert_eq!(
		services
			.pusher
			.notification_count(&manifest.bob, &manifest.quiet)
			.await?,
		1,
		"quiet-room older receipt preserves the later accepted unread message"
	);
	for room in &manifest.busy {
		assert_eq!(
			services
				.pusher
				.notification_count(&manifest.bob, room)
				.await?,
			MESSAGES,
			"quiet-room receipt leaves every unrelated room unread"
		);
	}
	assert_eq!(
		services
			.pusher
			.global_notification_count(&manifest.bob)
			.await?,
		4353,
		"account badge retains the later quiet message and all busy messages"
	);
	Ok(())
}

async fn erasure_controls(
	services: &Services,
	manifest: &Manifest,
	early: RawPduId,
	later: RawPduId,
) -> Result {
	let saved = services.db[INDEX]
		.get(&reverse(later, &manifest.bob))
		.await?
		.to_vec();
	let boundary = later
		.pdu_count()
		.into_unsigned()
		.saturating_add(1)
		.into();
	let early_key = reverse(early, &manifest.bob);
	let early_saved = services.db[INDEX].get(&early_key).await?.to_vec();
	services.db[INDEX]
		.insert(&early_key, b"corrupt-erasure-index")
		.await?;
	let before = snapshot(&services.db, &[
		INDEX,
		"useridcount_notification",
		"pduid_pdu",
		"eventid_pduid",
	])
	.await?;
	services
		.timeline
		.purge_history(&manifest.quiet, boundary, true)
		.await
		.expect_err("corrupt first target refuses history deletion");
	assert_eq!(
		snapshot(&services.db, &[
			INDEX,
			"useridcount_notification",
			"pduid_pdu",
			"eventid_pduid"
		])
		.await?,
		before,
		"first-target corruption refuses erasure without deleting canonical or index data"
	);
	services.db[INDEX]
		.insert(&early_key, &early_saved)
		.await?;
	let before = snapshot(&services.db, &[
		INDEX,
		"useridcount_notification",
		"pduid_pdu",
		"eventid_pduid",
	])
	.await?;
	refusal::refuse_next(INDEX);
	services
		.timeline
		.purge_history(&manifest.quiet, boundary, true)
		.await
		.expect_err("armed index commit refuses canonical deletion");
	assert_eq!(refusal::pending(), 0, "actual deletion commit consumed the refusal");
	assert_eq!(
		snapshot(&services.db, &[
			INDEX,
			"useridcount_notification",
			"pduid_pdu",
			"eventid_pduid"
		])
		.await?,
		before,
		"refused coupled erasure retains canonical PDU, metadata and both directions"
	);
	assert_eq!(
		services
			.timeline
			.purge_history(&manifest.quiet, boundary, true)
			.await?,
		2,
		"successful retry erases exactly the two quiet messages"
	);
	for raw in [early, later] {
		assert!(
			services.db[INDEX]
				.get(&reverse(raw, &manifest.bob))
				.await
				.expect_err("erasure removes the event-index direction")
				.is_not_found(),
			"event-index erasure is genuinely absent"
		);
		assert!(
			services.db["useridcount_notification"]
				.get(&serialize_key((&manifest.bob, raw.pdu_count().into_unsigned()))?)
				.await
				.expect_err("erasure removes original notification metadata")
				.is_not_found(),
			"notification metadata is genuinely absent"
		);
		assert!(
			services
				.timeline
				.get_pdu_from_id(&raw)
				.await
				.expect_err("erasure removes canonical PDU")
				.is_not_found(),
			"canonical PDU is genuinely absent"
		);
	}
	let mut prefix = vec![0];
	prefix.extend(serialize_key((&manifest.quiet, Interfix))?);
	assert!(
		services.db[INDEX]
			.raw_keys_prefix_after(&prefix, None, 1)
			.await?
			.is_empty(),
		"both quiet-room forward index rows were erased"
	);
	let after_index = snapshot(&services.db, &[INDEX]).await?;
	assert_eq!(
		after_index,
		before
			.into_iter()
			.filter(|(map, key, _)| map == INDEX
				&& key != &early_key
				&& key != &early_saved
				&& key != &reverse(later, &manifest.bob)
				&& key != &saved)
			.collect::<Vec<_>>(),
		"all unrelated index rows are preserved exactly"
	);
	Ok(())
}

async fn room_erasure_controls(services: &Services, base: &str, bob: &OwnedUserId) -> Result {
	let writer = Client { services, base, token: ALICE };
	let owner = Client { services, base, token: BOB };
	let room = room(&writer, &owner, bob).await?;
	let event = send(&writer, &room, "room-erasure-index-control").await?;
	let raw = services.timeline.get_pdu_id(&event).await?;
	let reverse = reverse(raw, bob);
	let scope = services.db[INDEX].get(&reverse).await?.to_vec();
	let primary = serialize_key((bob, raw.pdu_count().into_unsigned()))?;
	for client in [&owner, &writer] {
		services
			.client
			.clients
			.default
			.post(client.url(&format!("rooms/{room}/leave")))
			.bearer_auth(client.token)
			.json(&json!({}))
			.send()
			.await?
			.error_for_status()?;
	}
	let maps = [
		INDEX,
		"useridcount_notification",
		"pduid_pdu",
		"eventid_pduid",
		"roomid_shortroomid",
		"roomid_shortstatehash",
	];
	let before = snapshot(&services.db, &maps).await?;
	refusal::refuse_next(INDEX);
	let guard = services.state.mutex.lock(&room).await;
	services
		.delete
		.delete_room(&room, false, guard)
		.await
		.expect_err("actual whole-room index transaction is refused");
	assert_eq!(refusal::pending(), 0, "whole-room erasure attempted the index commit");
	assert_eq!(
		snapshot(&services.db, &maps).await?,
		before,
		"refused storage erase preserves the canonical PDU and both index directions"
	);
	let guard = services.state.mutex.lock(&room).await;
	services
		.delete
		.delete_room(&room, false, guard)
		.await?;
	for key in [&scope, &reverse] {
		assert!(
			services.db[INDEX]
				.get(key)
				.await
				.expect_err("whole-room index row erased")
				.is_not_found(),
			"the index direction is genuinely absent"
		);
	}
	assert!(
		services.db["useridcount_notification"]
			.get(&primary)
			.await
			.expect_err("whole-room original notification erased")
			.is_not_found(),
		"the original metadata is genuinely absent"
	);
	assert!(
		services
			.timeline
			.get_pdu_from_id(&raw)
			.await
			.expect_err("whole-room canonical PDU erased")
			.is_not_found(),
		"the erased PDU is genuinely absent"
	);
	let after = snapshot(&services.db, &[INDEX]).await?;
	let expected = before
		.into_iter()
		.filter(|(map, key, _)| map == INDEX && key != &scope && key != &reverse)
		.collect::<Vec<_>>();
	assert_eq!(
		after, expected,
		"whole-room erasure preserves every unrelated notification index row"
	);
	Ok(())
}

async fn assert_completed(db: &Database) -> Result {
	assert_eq!(
		db["global"].get(b"version").await?.as_ref(),
		24_u64.to_be_bytes(),
		"only the completed startup stamps the current schema 24"
	);
	assert!(db["global"].get(DONE).await?.is_empty(), "completed migration marker is empty");
	assert!(
		db["global"]
			.get(CURSOR)
			.await
			.expect_err("completed migration removes its cursor")
			.is_not_found(),
		"completed cursor is genuinely absent"
	);
	Ok(())
}

async fn snapshot(db: &Database, maps: &[&str]) -> Result<Snapshot> {
	let mut result = Vec::new();
	for map in maps {
		let rows: Vec<_> = db[map]
			.raw_stream()
			.map_ok(|(key, value)| ((*map).to_owned(), key.to_vec(), sha256::hash(value)))
			.try_collect()
			.await?;
		result.extend(rows);
	}
	Ok(result)
}

async fn save(db: &Database, directory: &Path, name: &str, maps: &[&str]) -> Result {
	write(
		directory.join(format!("{name}.snapshot")),
		serde_json::to_vec(&snapshot(db, maps).await?)?,
	)?;
	Ok(())
}

async fn assert_saved(db: &Database, directory: &Path, name: &str, maps: &[&str]) -> Result {
	let saved: Snapshot =
		serde_json::from_slice(&read(directory.join(format!("{name}.snapshot")))?)?;
	assert_eq!(snapshot(db, maps).await?, saved, "saved {name} metadata is unchanged");
	Ok(())
}
