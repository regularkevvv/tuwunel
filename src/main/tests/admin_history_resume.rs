#![cfg(test)]

//! Actual API admission, four SIGKILL boundaries, multi-batch cleanup and
//! restart totals. These owned native fixtures do not qualify remote acks.
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
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{OwnedEventId, OwnedRoomId},
	utils::hash::sha256,
};
use tuwunel_database::{Database, refusal};
use tuwunel_service::{Services, tasks::Status};

use self::client::{Client, register, wait_until_ready};

const PHASE: &str = "TUWUNEL_HISTORY_RESUME_PHASE";
const DIRECTORY: &str = "TUWUNEL_HISTORY_RESUME_DIRECTORY";
const OLDER_BINARY: &str = "TUWUNEL_HISTORY_OLDER_JOURNAL_BINARY";
const TOKEN: &str = "disposable-history-resume-admin-token";
const JOURNAL: &str = "adminjobid_record";
const LEGACY: &str = "0000000000000000";
const TABLES: &[&str] = &[
	JOURNAL,
	"global",
	"pduid_notificationplan",
	"notificationreceiptid_record",
	"notificationid_index",
	"roomuserid_notificationcutoff",
	"pduid_pdu",
	"eventid_pduid",
	"eventid_outlierpdu",
	"roomid_tscount_pducount",
	"tokenids",
	"tofrom_relation",
	"relatesto_typed",
	"referencedevents",
	"eventid_policysigstate",
	"softfailedeventids",
	"eventid_originalpdu",
	"timeredacted_eventid",
];
type Snapshot = Vec<(String, Vec<u8>, sha256::Digest)>;

#[derive(Deserialize, Serialize)]
struct Manifest {
	room: OwnedRoomId,
	small: OwnedEventId,
	large: OwnedEventId,
	boundary: OwnedEventId,
	foreign: OwnedEventId,
	task: Option<String>,
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
fn history_progress_resumes_each_kill_and_never_double_counts() -> Result {
	if let Ok(phase) = var(PHASE) {
		return child(&PathBuf::from(var(DIRECTORY).expect("owned directory")), &phase);
	}
	let directory = prepare_interrupted_history()?;
	for corrupt in ["corrupt", "corrupt-terminal", "corrupt-version"] {
		run_child(&directory.0, corrupt)?;
		for phase in ["snapshot-corrupt", "reject", "verify-corrupt", "repair"] {
			run_child(&directory.0, phase)?;
		}
	}
	for phase in ["relations", "notifications", "final"] {
		kill_child(&directory.0, phase)?;
		run_child(&directory.0, &format!("inspect-{phase}"))?;
	}
	run_child(&directory.0, "recover")?;
	run_child(&directory.0, "again")?;
	Ok(())
}

#[test]
#[ignore = "requires the pinned predecessor journal test executable; mandatory in native CI"]
fn older_journal_refuses_schema_and_record_changes_without_mutation() -> Result {
	let binary =
		var(OLDER_BINARY).expect("pinned predecessor journal test executable is required");
	let directory = prepare_interrupted_history()?;
	for phase in ["schema-gate", "record-gate"] {
		if phase == "record-gate" {
			// Isolate the record-version refusal from the schema-version refusal.
			// This deliberate downgrade is confined to the disposable database.
			run_child(&directory.0, phase)?;
		}
		run_child(&directory.0, "snapshot-compat")?;
		older_refuses(&directory.0, &binary)?;
		run_child(&directory.0, "verify-compat")?;
	}
	Ok(())
}

fn prepare_interrupted_history() -> Result<OwnedDirectory> {
	let directory = temp_dir().join(format!("history-resume-{}", id()));
	let mut builder = DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&directory)?;
	let directory = OwnedDirectory(directory);
	run_child(&directory.0, "prepare")?;
	run_child(&directory.0, "import")?;
	kill_child(&directory.0, "accept")?;
	run_child(&directory.0, "inspect-accept")?;
	Ok(directory)
}

fn command(directory: &Path, phase: &str) -> Result<Command> {
	let mut command = Command::new(current_exe()?);
	command
		.env(PHASE, phase)
		.env(DIRECTORY, directory);
	Ok(command)
}

fn kill_child(directory: &Path, phase: &str) -> Result {
	let mut child = OwnedChild(command(directory, phase)?.spawn()?);
	let deadline = Instant::now()
		.checked_add(Duration::from_mins(2))
		.expect("valid deadline");
	while !directory.join(format!("{phase}.ready")).exists() {
		assert!(child.0.try_wait()?.is_none(), "{phase} exited before kill boundary");
		assert!(Instant::now() < deadline, "{phase} did not reach deadline");
		thread::sleep(Duration::from_millis(20));
	}
	assert!(child.0.try_wait()?.is_none());
	child.0.kill()?;
	let status = child.0.wait()?;
	#[cfg(unix)]
	assert_eq!(status.signal(), Some(9));
	Ok(())
}

fn run_child(directory: &Path, phase: &str) -> Result {
	let mut child = OwnedChild(command(directory, phase)?.spawn()?);
	wait_child(&mut child, phase)
}

fn older_refuses(directory: &Path, binary: &str) -> Result {
	let mut child = OwnedChild(
		Command::new(binary)
			.env("TUWUNEL_ADMIN_JOURNAL_PHASE", "reject")
			.env("TUWUNEL_ADMIN_JOURNAL_DIRECTORY", directory)
			.spawn()?,
	);
	wait_child(&mut child, "older binary rejects before readiness")
}

fn wait_child(child: &mut OwnedChild, phase: &str) -> Result {
	let deadline = Instant::now()
		.checked_add(Duration::from_mins(2))
		.expect("valid deadline");
	loop {
		if let Some(status) = child.0.try_wait()? {
			assert!(status.success(), "{phase} failed");
			return Ok(());
		}
		assert!(Instant::now() < deadline, "{phase} exceeded deadline");
		thread::sleep(Duration::from_millis(20));
	}
}

fn child(directory: &Path, phase: &str) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let modes: &[&str] = if phase == "prepare" { &["fresh"] } else { &[] };
	let mut args = Args::default_test(modes);
	args.maintenance = phase.starts_with("inspect")
		|| phase.starts_with("corrupt")
		|| matches!(phase, "repair" | "record-gate");
	let retention = u8::from(!matches!(phase, "prepare" | "import" | "accept"));
	args.option.extend([
		format!("database_path={:?}", directory.join("database")),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		format!("listening={}", !args.maintenance && phase != "reject"),
		"save_unredacted_events=true".into(),
		format!("redaction_retention_seconds={retention}"),
		"log=\"error\"".into(),
	]);
	if phase == "relations" {
		refusal::refuse_next("relatesto_typed");
	}
	if phase == "notifications" {
		refusal::refuse_after("notificationreceiptid_record", 1);
	}
	if phase == "final" {
		refusal::refuse_next("pduid_pdu");
	}
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	drop(listener);
	let result = runtime.block_on(async {
		if phase.starts_with("snapshot-") || phase.starts_with("verify-") {
			// Inspect a closed database without migrations or service workers.
			// Shutdown may legitimately advance the sequence counter; it is
			// captured before and after the old/refused startup, never omitted.
			let db = Database::open(&server.server).await?;
			let rows = database_snapshot(&db).await?;
			let file = directory.join(if phase.ends_with("compat") {
				"compat.snapshot"
			} else {
				"corrupt.snapshot"
			});
			if phase.starts_with("snapshot-") {
				write(file, serde_json::to_vec(&rows)?)?;
			} else {
				assert_snapshot(&rows, &serde_json::from_slice(&read(file)?)?);
			}
			db.close().await;
			return Ok(());
		}
		let start = async_start(&server).await;
		if phase == "reject" {
			assert!(start.is_err(), "changed frozen target must refuse startup");
			assert!(server.services.lock().await.is_none());
			return Ok(());
		}
		let services = start?;
		let base = format!("http://127.0.0.1:{port}");
		let exercise = async {
			let outcome = match phase {
				| "prepare" => prepare(&services, &base, directory).await,
				| "import" => {
					assert_eq!(services.globals.db.database_version().await, 23);
					assert!(
						services.db["global"]
							.get(b"populate_userroomid_leftstate_table")
							.await
							.expect_err("completed import claims native lineage")
							.is_not_found()
					);
					// Acceptance must also perform a native 17 -> 23 upgrade.
					services
						.globals
						.db
						.bump_database_version(17)
						.await
				},
				| "record-gate" =>
					services
						.globals
						.db
						.bump_database_version(17)
						.await,
				| "accept" | "relations" | "notifications" | "final" =>
					paused(&services, &base, directory, phase).await,
				| "recover" | "again" => complete(&services, directory, phase).await,
				| "corrupt" | "corrupt-terminal" | "corrupt-version" | "repair" =>
					change_record(&services, directory, phase).await,
				| _ if phase.starts_with("inspect-") =>
					inspect(&services, directory, phase.trim_start_matches("inspect-")).await,
				| _ => panic!("unknown owned phase"),
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

fn manifest(directory: &Path) -> Result<Manifest> {
	Ok(serde_json::from_slice(&read(directory.join("manifest.json"))?)?)
}
fn save(directory: &Path, manifest: &Manifest) -> Result {
	Ok(write(directory.join("manifest.json"), serde_json::to_vec(manifest)?)?)
}

async fn send(
	client: &Client<'_>,
	room: &OwnedRoomId,
	tx: &str,
	body: &str,
) -> Result<OwnedEventId> {
	let response: Value = client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/{tx}")))
		.bearer_auth(TOKEN)
		.json(&json!({"msgtype":"m.text", "body":body}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	Ok(OwnedEventId::try_from(response["event_id"].as_str().expect("event"))?)
}

async fn prepare(services: &Services, base: &str, directory: &Path) -> Result {
	wait_until_ready(services, base).await?;
	let user = register(services, "history-admin", TOKEN).await?;
	services.admin.make_user_admin(&user).await?;
	let client = Client { services, base, token: TOKEN };
	let room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	let small = send(&client, &room, "small", "first event").await?;
	let large = send(&client, &room, "large", &format!("large {}", "İ".repeat(25))).await?;
	let boundary = send(&client, &room, "boundary", "retained boundary").await?;
	let foreign_room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	let foreign = send(&client, &foreign_room, "foreign", "foreign sentinel").await?;
	let manifest = Manifest {
		room,
		small,
		large,
		boundary,
		foreign,
		task: None,
	};
	seed_cleanup(services, &manifest).await?;
	save(directory, &manifest)?;
	// A completed foreign import must stop older native builds from bypassing
	// their version gate. All actual fixture data belongs to this server.
	let mut txn = services.db.txn();
	txn.raw_put(&services.db["global"], b"version", 42_u64);
	txn.insert_raw(&services.db["global"], b"populate_userroomid_leftstate_table", b"");
	txn.execute().await
}

async fn seed_cleanup(services: &Services, manifest: &Manifest) -> Result {
	let raw = services
		.timeline
		.get_pdu_id(&manifest.large)
		.await?;
	let short = services
		.short
		.get_shortroomid(&manifest.room)
		.await?;
	let original_body = (0..1000)
		.map(|n| format!("originalword{n}"))
		.collect::<Vec<_>>()
		.join(" ");
	let mut original = services
		.timeline
		.get_pdu_json(&manifest.large)
		.await?;
	original
		.get_mut("content")
		.expect("content")
		.as_object_mut()
		.expect("object")
		.insert(
			"body".into(),
			tuwunel_core::ruma::CanonicalJsonValue::String(original_body.clone()),
		);
	let state = services.state.mutex.lock(&manifest.room).await;
	services
		.retention
		.save_original_pdu(&manifest.large, &original, &state)
		.await?;
	drop(state);
	services
		.search
		.index_pdu(short, &raw, &original_body)
		.await?;
	// Make expiry due before restarts. Acceptance runs with expiry disabled;
	// resumed workers must honor the canonical room pin even before this event
	// becomes the current target. No fixture-side WAL barrier is added.
	let times = services.db["timeredacted_eventid"]
		.raw_rows_after(None, 64)
		.await?;
	let mut txn = services.db.txn();
	for (key, _) in times {
		txn.del_raw(&services.db["timeredacted_eventid"], key);
	}
	txn.put_raw(&services.db["timeredacted_eventid"], (1_600_000_000_u64, &manifest.large), []);
	txn.execute().await?;
	for start in (1..=1300_u64).step_by(128) {
		let mut txn = services.db.txn();
		for n in start..=(start.saturating_add(127)).min(1300) {
			let mut legacy = raw.count().to_vec();
			legacy.extend_from_slice(&n.to_be_bytes());
			txn.insert_raw(&services.db["tofrom_relation"], legacy, b"");
			let mut typed = short.to_be_bytes().to_vec();
			typed.extend_from_slice(&raw.count());
			typed.push(1);
			typed.extend_from_slice(&n.to_be_bytes());
			typed.extend_from_slice(&n.to_be_bytes());
			txn.insert_raw(&services.db["relatesto_typed"], typed, n.to_be_bytes());
		}
		txn.execute().await?;
	}
	// Completed decision records are seeded directly to exercise cleanup
	// beyond one transaction's budget, without claiming 1,024 real clients.
	for start in (0..1024_usize).step_by(128) {
		let mut txn = services.db.txn();
		for index in start..start.saturating_add(128) {
			let mut key = raw.as_ref().to_vec();
			key.push(0xFF);
			key.extend_from_slice(format!("@historyreceipt{index:04}:localhost").as_bytes());
			txn.insert_raw(
				&services.db["notificationreceiptid_record"],
				key,
				serde_json::to_vec(
					&json!({"format":1,"room":manifest.room,"event":manifest.large,
                    "thread":null,"actions":["notify"],"push_everything":false,"canceled":false}),
				)?,
			);
		}
		txn.execute().await?;
	}
	let foreign = services
		.timeline
		.get_pdu_id(&manifest.foreign)
		.await?;
	let foreign_pdu = services
		.timeline
		.get_pdu(&manifest.foreign)
		.await?;
	let mut key = foreign.as_ref().to_vec();
	key.push(0xFF);
	key.extend_from_slice(b"@historyreceiptforeign:localhost");
	services.db["notificationreceiptid_record"]
		.insert(
			&key,
			serde_json::to_vec(
				&json!({"format":1,"room":foreign_pdu.room_id,"event":manifest.foreign,
            "thread":null,"actions":["notify"],"push_everything":false,"canceled":false}),
			)?,
		)
		.await?;
	Ok(())
}

async fn record(services: &Services, id: &str) -> Result<Value> {
	Ok(serde_json::from_slice(&services.db[JOURNAL].get(id.as_bytes()).await?)?)
}

async fn boundary_refusals(services: &Services, base: &str, manifest: &Manifest) -> Result {
	let raw = services
		.timeline
		.get_pdu_id(&manifest.boundary)
		.await?;
	for (map, key) in [
		("pduid_pdu", raw.as_ref().to_vec()),
		("eventid_pduid", manifest.boundary.as_bytes().to_vec()),
	] {
		let original = services.db[map].get(&key).await?.to_vec();
		services.db[map]
			.insert(&key, b"{invalid}")
			.await?;
		let before = snapshot(services).await?;
		let response = services
			.client
			.clients
			.default
			.post(format!(
				"{base}/_synapse/admin/v1/purge_history/{}/{}",
				manifest.room, manifest.boundary
			))
			.bearer_auth(TOKEN)
			.json(&json!({"delete_local_events":true}))
			.send()
			.await?;
		assert!(response.status().is_server_error(), "corrupt boundary is not a missing event");
		assert_eq!(snapshot(services).await?, before, "refusal starts no purge");
		services.db[map].insert(&key, original).await?;
	}
	for (event, status) in [(manifest.foreign.as_str(), 400), ("$absent:localhost", 404)] {
		let before = snapshot(services).await?;
		let response = services
			.client
			.clients
			.default
			.post(format!("{base}/_synapse/admin/v1/purge_history/{}/{event}", manifest.room))
			.bearer_auth(TOKEN)
			.json(&json!({"delete_local_events":true}))
			.send()
			.await?;
		assert_eq!(response.status().as_u16(), status);
		assert_eq!(snapshot(services).await?, before);
	}
	Ok(())
}

async fn paused(services: &Services, base: &str, directory: &Path, phase: &str) -> Result {
	let _cork = services.db.cork();
	let mut manifest = manifest(directory)?;
	if phase == "accept" {
		wait_until_ready(services, base).await?;
		boundary_refusals(services, base, &manifest).await?;
		refusal::refuse_next("tokenids");
		let response: Value = services
			.client
			.clients
			.default
			.post(format!(
				"{base}/_synapse/admin/v1/purge_history/{}/{}",
				manifest.room, manifest.boundary
			))
			.bearer_auth(TOKEN)
			.json(&json!({"delete_local_events":true}))
			.send()
			.await?
			.error_for_status()?
			.json()
			.await?;
		manifest.task = Some(
			response["purge_id"]
				.as_str()
				.expect("accepted id")
				.to_owned(),
		);
		save(directory, &manifest)?;
	}
	let task = manifest.task.as_deref().expect("accepted task");
	timeout(Duration::from_secs(90), async {
		loop {
			if refusal::pending() == 0 {
				break;
			}
			sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.map_err(|_| err!("owned refusal not reached"))?;
	inspect(services, directory, phase).await?;
	// Duplicate/cross-action requests refuse while the interrupted request owns
	// its room; refused admission starts no additional destructive work.
	let rows = snapshot(services).await?;
	let boundary = services
		.timeline
		.get_pdu_count(&manifest.boundary)
		.await?;
	services
		.tasks
		.spawn_history(manifest.room.clone(), boundary, true)
		.await
		.expect_err("active history owns the room");
	assert_eq!(snapshot(services).await?, rows);
	assert_eq!(services.retention.expire_originals().await?, 0);
	assert!(
		services.db["eventid_originalpdu"]
			.get(&manifest.large)
			.await
			.is_ok(),
		"due original retained while request is active"
	);
	assert_eq!(
		services
			.tasks
			.get(task)
			.await?
			.expect("durable task")
			.status,
		Status::Active
	);
	write(directory.join(format!("{phase}.ready")), b"durable progress")?;
	pending::<Result<()>>().await
}

async fn inspect(services: &Services, directory: &Path, phase: &str) -> Result {
	let manifest = manifest(directory)?;
	let task = record(services, manifest.task.as_deref().expect("task")).await?;
	assert_eq!(task["status"], "active");
	assert_eq!(task["version"], 2);
	assert!(task["result"].is_null());
	let progress = &task["parameters"];
	let expected = match phase {
		| "accept" => "search_current",
		| "relations" => "typed_relations",
		| "notifications" => "notifications",
		| "final" => "final",
		| _ => panic!("owned inspection phase"),
	};
	assert_eq!(progress["current"]["phase"], expected);
	assert_eq!(progress["purged"], u8::from(phase != "accept"));
	services.timeline.get_pdu(&manifest.large).await?;
	services
		.timeline
		.get_pdu(&manifest.boundary)
		.await?;
	services
		.timeline
		.get_pdu(&manifest.foreign)
		.await?;
	services.db["eventid_originalpdu"]
		.get(&manifest.large)
		.await?;
	let raw = services
		.timeline
		.get_pdu_id(&manifest.large)
		.await?;
	let short = services
		.short
		.get_shortroomid(&manifest.room)
		.await?;
	let legacy = services.db["tofrom_relation"]
		.raw_keys_prefix_after(&raw.count(), None, 1)
		.await?;
	let mut typed_prefix = short.to_be_bytes().to_vec();
	typed_prefix.extend_from_slice(&raw.count());
	let typed = services.db["relatesto_typed"]
		.raw_keys_prefix_after(&typed_prefix, None, 1)
		.await?;
	if phase == "accept" {
		assert_eq!(services.globals.db.database_version().await, 23);
		services.timeline.get_pdu(&manifest.small).await?;
		assert!(!legacy.is_empty());
	} else {
		assert!(
			services
				.timeline
				.get_pdu(&manifest.small)
				.await
				.expect_err("first event already removed")
				.is_not_found()
		);
		assert!(legacy.is_empty());
	}
	assert_eq!(typed.is_empty(), matches!(phase, "notifications" | "final"));
	let mut prefix = raw.as_ref().to_vec();
	prefix.push(0xFF);
	let receipts = services.db["notificationreceiptid_record"]
		.raw_keys_prefix_after(&prefix, None, 1025)
		.await?;
	assert_eq!(receipts.len(), match phase {
		| "notifications" => 960,
		| "final" => 0,
		| _ => 1024,
	});
	if phase == "notifications" {
		let mut cursor = prefix;
		cursor.extend_from_slice(b"@historyreceipt0063:localhost");
		assert_eq!(
			progress["current"]["after"],
			json!(cursor),
			"first receipt page and exclusive cursor committed together"
		);
	}
	Ok(())
}

async fn complete(services: &Services, directory: &Path, phase: &str) -> Result {
	let manifest = manifest(directory)?;
	let id = manifest.task.as_deref().expect("task");
	timeout(Duration::from_secs(90), async {
		loop {
			if services
				.tasks
				.get(id)
				.await?
				.is_some_and(|task| task.status == Status::Complete)
			{
				break;
			}
			sleep(Duration::from_millis(10)).await;
		}
		Ok::<(), tuwunel_core::Error>(())
	})
	.await
	.map_err(|_| err!("history resume exceeded deadline"))??;
	let task = record(services, id).await?;
	assert_eq!(task["result"], json!({"purged":2}));
	assert_eq!(task["parameters"]["purged"], 2);
	assert!(task["parameters"]["current"].is_null());
	assert_eq!(task["parameters"]["done"], true);
	for event in [&manifest.small, &manifest.large] {
		assert!(
			services
				.timeline
				.get_pdu(event)
				.await
				.expect_err("selected event removed")
				.is_not_found()
		);
	}
	services
		.timeline
		.get_pdu(&manifest.boundary)
		.await?;
	services
		.timeline
		.get_pdu(&manifest.foreign)
		.await?;
	assert!(
		services.db["eventid_originalpdu"]
			.get(&manifest.large)
			.await
			.expect_err("original removed")
			.is_not_found()
	);
	let prefix = services
		.timeline
		.get_pdu_id(&manifest.boundary)
		.await?
		.shortroomid();
	assert!(
		services.db["notificationreceiptid_record"]
			.raw_keys_prefix_after(&prefix, None, 1)
			.await?
			.is_empty(),
		"selected room's completed receipts were removed"
	);
	let foreign_raw = services
		.timeline
		.get_pdu_id(&manifest.foreign)
		.await?;
	let mut foreign_key = foreign_raw.as_ref().to_vec();
	foreign_key.push(0xFF);
	foreign_key.extend_from_slice(b"@historyreceiptforeign:localhost");
	services.db["notificationreceiptid_record"]
		.get(&foreign_key)
		.await?;
	let short = services
		.short
		.get_shortroomid(&manifest.room)
		.await?;
	for map in ["tofrom_relation", "relatesto_typed"] {
		let prefix = if map == "relatesto_typed" {
			short.to_be_bytes().to_vec()
		} else {
			Vec::new()
		};
		assert!(
			services.db[map]
				.raw_keys_prefix_after(&prefix, None, 1)
				.await?
				.is_empty()
		);
	}
	let target_tokens = services.db["tokenids"]
		.raw_keys_prefix_after(&short.to_be_bytes(), None, 2000)
		.await?;
	// Only the boundary body remains in this room's token index.
	let boundary_raw = services
		.timeline
		.get_pdu_id(&manifest.boundary)
		.await?;
	assert!(
		target_tokens
			.iter()
			.all(|key| key.ends_with(boundary_raw.as_ref()))
	);
	let record = services.db[JOURNAL]
		.get(id.as_bytes())
		.await?
		.to_vec();
	if phase == "recover" {
		write(directory.join("completed.record"), record)?;
	} else {
		assert_eq!(
			record,
			read(directory.join("completed.record"))?,
			"later startup must not re-count or rewrite completion"
		);
	}
	Ok(())
}

async fn snapshot(services: &Services) -> Result<Snapshot> {
	database_snapshot(&services.db).await
}

fn assert_snapshot(actual: &Snapshot, expected: &Snapshot) {
	assert_eq!(actual.len(), expected.len(), "inspected row count is unchanged");
	for (actual, expected) in actual.iter().zip(expected) {
		assert_eq!(actual, expected, "inspected row is unchanged");
	}
}

async fn database_snapshot(db: &Database) -> Result<Snapshot> {
	let mut rows = Vec::new();
	for &map in TABLES {
		let stream = db[map].raw_stream();
		pin_mut!(stream);
		while let Some((key, value)) = stream.try_next().await? {
			rows.push((map.to_owned(), key.to_vec(), sha256::hash(value)));
		}
	}
	Ok(rows)
}

async fn change_record(services: &Services, directory: &Path, phase: &str) -> Result {
	let manifest = manifest(directory)?;
	let id = manifest.task.as_deref().expect("task");
	if phase == "repair" {
		services.db[JOURNAL]
			.insert(id.as_bytes(), read(directory.join("original.record"))?)
			.await?;
		services.db[JOURNAL]
			.remove(LEGACY.as_bytes())
			.await?;
		return Ok(());
	}
	let original = services.db[JOURNAL]
		.get(id.as_bytes())
		.await?
		.to_vec();
	write(directory.join("original.record"), &original)?;
	let mut record: Value = serde_json::from_slice(&original)?;
	match phase {
		| "corrupt" => {
			let byte = record["parameters"]["current"]["canonical"][0]
				.as_u64()
				.expect("digest");
			record["parameters"]["current"]["canonical"][0] = json!(byte ^ 1);
		},
		| "corrupt-terminal" => {
			record["status"] = json!("failed");
			record["error"] = json!("invalid terminal interruption");
		},
		| "corrupt-version" => record["version"] = json!(1),
		| _ => panic!("unknown corruption phase"),
	}
	let mut txn = services.db.txn();
	txn.insert_raw(&services.db[JOURNAL], id.as_bytes(), serde_json::to_vec(&record)?);
	let legacy = json!({"version":1,"id":LEGACY,"action":"purge_history","resource_id":"!legacy:localhost","parameters":{},"status":"scheduled","timestamp_ms":1,"result":null,"error":null});
	txn.insert_raw(&services.db[JOURNAL], LEGACY.as_bytes(), serde_json::to_vec(&legacy)?);
	txn.execute().await
}
