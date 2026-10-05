#![cfg(test)]

//! Journal admission, status and refusal checks on the real native database.
//! SIGKILL verifies accepted records without an extra test-side WAL barrier.
//! Interrupted destructive work is reported explicitly, not automatically
//! resumed: operation-level replay receipts are a separate completion gate.

mod client;

#[cfg(unix)]
use std::os::unix::{fs::DirBuilderExt, process::ExitStatusExt};
use std::{
	collections::BTreeMap,
	env::{current_exe, temp_dir, var},
	fs::{DirBuilder, read, remove_dir_all, write},
	future::pending,
	net::TcpListener,
	path::{Path, PathBuf},
	process::{Child, Command, id},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

use futures::{TryStreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
	sync::oneshot,
	time::{sleep, timeout},
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Result, err, ruma::OwnedRoomId};
use tuwunel_database::refusal;
use tuwunel_service::{Services, tasks::Status};

use self::client::{Client, register, wait_until_ready};

const PHASE: &str = "TUWUNEL_ADMIN_JOURNAL_PHASE";
const DIRECTORY: &str = "TUWUNEL_ADMIN_JOURNAL_DIRECTORY";
const TOKEN: &str = "disposable-admin-task-journal-token";
const MAP: &str = "adminjobid_record";
const ACTION: &str = "purge_history";

type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Serialize, Deserialize)]
struct Manifest {
	room: OwnedRoomId,
	complete: String,
	failed: String,
	interrupted: Vec<String>,
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
fn accepted_admin_tasks_and_status_survive_kill_with_honest_refusals() -> Result {
	if let Ok(phase) = var(PHASE) {
		return child(&PathBuf::from(var(DIRECTORY).expect("owned child directory")), &phase);
	}
	let directory = temp_dir().join(format!("admin-task-journal-{}", id()));
	let mut builder = DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&directory)?;
	let directory = OwnedDirectory(directory);
	run_child(&directory.0, "prepare")?;
	let mut accepted = OwnedChild(command(&directory.0, "accept")?.spawn()?);
	let deadline = Instant::now()
		.checked_add(Duration::from_secs(30))
		.expect("valid deadline");
	while !directory.0.join("accepted-ready").exists() {
		assert!(accepted.0.try_wait()?.is_none(), "child exited before durable acceptance");
		assert!(Instant::now() < deadline, "admission did not reach its deadline");
		thread::sleep(Duration::from_millis(20));
	}
	assert!(accepted.0.try_wait()?.is_none());
	accepted.0.kill()?;
	let status = accepted.0.wait()?;
	assert!(!status.success());
	#[cfg(unix)]
	assert_eq!(status.signal(), Some(9));
	for phase in ["inspect", "recover", "again", "corrupt", "reject", "repair", "again"] {
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
	let mut child = OwnedChild(command(directory, phase)?.spawn()?);
	let deadline = Instant::now()
		.checked_add(Duration::from_secs(90))
		.expect("valid deadline");
	loop {
		if let Some(status) = child.0.try_wait()? {
			assert!(status.success(), "admin journal phase {phase} failed");
			return Ok(());
		}
		if Instant::now() >= deadline {
			return Err(err!("Admin journal phase {phase} exceeded its deadline"));
		}
		thread::sleep(Duration::from_millis(20));
	}
}

fn child(directory: &Path, phase: &str) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let modes: &[&str] = if phase == "prepare" { &["fresh"] } else { &[] };
	let mut args = Args::default_test(modes);
	args.maintenance = matches!(phase, "inspect" | "repair");
	args.option.extend([
		format!("database_path={:?}", directory.join("database")),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		format!("listening={}", !args.maintenance && phase != "reject"),
		"log=\"error\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	drop(listener);
	let result = runtime.block_on(async {
		let start = async_start(&server).await;
		if phase == "reject" {
			assert!(start.is_err(), "corrupt full inventory must refuse startup");
			assert!(server.services.lock().await.is_none());
			return Ok(());
		}
		let services = start?;
		let base = format!("http://127.0.0.1:{port}");
		let exercise = async {
			let outcome = match phase {
				| "prepare" => prepare(&services, &base, directory).await,
				| "accept" => accept(&services, directory).await,
				| "inspect" | "repair" => inspect(&services, directory, phase).await,
				| "recover" | "again" => recovered(&services, &base, directory, phase).await,
				| "corrupt" => corrupt(&services, directory).await,
				| _ => panic!("unknown owned phase"),
			};
			let shutdown = server.server.shutdown();
			outcome.and(shutdown)
		};
		let (run, outcome) = tokio::join!(async_run(&server), exercise);
		drop(services);
		let stop = async_stop(&server).await;
		outcome.and(run).and(stop)
	});
	drop(server);
	drop(runtime);
	result
}

fn manifest(directory: &Path) -> Result<Manifest> {
	Ok(serde_json::from_slice(&read(directory.join("manifest.json"))?)?)
}

async fn rows(services: &Services) -> Result<Rows> {
	let stream = services.db[MAP].raw_stream();
	pin_mut!(stream);
	let mut out = BTreeMap::new();
	while let Some((key, value)) = stream.try_next().await? {
		out.insert(key.to_vec(), value.to_vec());
	}
	Ok(out)
}

async fn wait_status(services: &Services, id: &str, status: Status) -> Result {
	timeout(Duration::from_secs(10), async {
		loop {
			if services
				.tasks
				.get(id)
				.await?
				.is_some_and(|task| task.status == status)
			{
				return Ok::<_, tuwunel_core::Error>(());
			}
			sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.map_err(|_| err!("Admin task did not reach expected status"))?
}

async fn prepare(services: &Services, base: &str, directory: &Path) -> Result {
	wait_until_ready(services, base).await?;
	let user = register(services, "journal-admin", TOKEN).await?;
	services.admin.make_user_admin(&user).await?;
	let client = Client { services, base, token: TOKEN };
	let room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	let parameters = json!({"boundary": 1, "delete_local_events": false});

	let executed = Arc::new(AtomicBool::new(false));
	let work_flag = executed.clone();
	refusal::refuse_next(MAP);
	let admission = services
		.tasks
		.spawn(ACTION, room.to_string(), parameters.clone(), async move {
			work_flag.store(true, Ordering::SeqCst);
			Ok(json!({"purged": 999}))
		})
		.await;
	assert!(admission.is_err(), "refused admission must return no id");
	assert_eq!(refusal::pending(), 0);
	assert!(!executed.load(Ordering::SeqCst));
	assert!(rows(services).await?.is_empty());

	let too_large = services
		.tasks
		.spawn(ACTION, room.to_string(), json!({"padding":"x".repeat(16 * 1024)}), pending())
		.await;
	assert_eq!(
		too_large
			.expect_err("oversized request must refuse")
			.status_code(),
		reqwest::StatusCode::TOO_MANY_REQUESTS
	);
	assert!(rows(services).await?.is_empty());

	let complete = services
		.tasks
		.spawn(ACTION, room.to_string(), parameters.clone(), async { Ok(json!({"purged": 7})) })
		.await?
		.to_string();
	wait_status(services, &complete, Status::Complete).await?;
	status_read_refusals(services, base, &complete).await?;
	let failed = services
		.tasks
		.spawn(ACTION, room.to_string(), parameters, async {
			Err(err!("Owned fixture failure"))
		})
		.await?
		.to_string();
	wait_status(services, &failed, Status::Failed).await?;
	write(
		directory.join("manifest.json"),
		serde_json::to_vec(&Manifest {
			room,
			complete,
			failed,
			interrupted: Vec::new(),
		})?,
	)?;
	Ok(())
}

async fn accept(services: &Services, directory: &Path) -> Result {
	let _cork = services.db.cork();
	let mut manifest = manifest(directory)?;
	let parameters = json!({"boundary": 2, "delete_local_events": true});
	let (left, right) = tokio::join!(
		services
			.tasks
			.spawn(ACTION, manifest.room.to_string(), parameters.clone(), pending()),
		services
			.tasks
			.spawn(ACTION, manifest.room.to_string(), parameters.clone(), pending()),
	);
	let first = match (left, right) {
		| (Ok(id), Err(_)) | (Err(_), Ok(id)) => id.to_string(),
		| _ => panic!("exactly one concurrent admission must succeed"),
	};
	wait_status(services, &first, Status::Active).await?;
	let before = rows(services).await?;
	let duplicate = services
		.tasks
		.spawn(ACTION, manifest.room.to_string(), parameters.clone(), pending())
		.await;
	duplicate.expect_err("duplicate admission must refuse");
	assert_eq!(rows(services).await?, before, "duplicate admission has no journal mutation");

	let (release, wait) = oneshot::channel();
	let second = services
		.tasks
		.spawn(
			"shutdown_and_purge_room",
			manifest.room.to_string(),
			json!({"sender":"@journal-admin:example.com", "block":false,"purge":true}),
			async {
				wait.await
					.map_err(|_| err!("owned completion channel closed"))?;
				Ok(
					json!({"kicked_users":[],"failed_to_kick_users":[],"local_aliases":[],"new_room_id":null}),
				)
			},
		)
		.await?
		.to_string();
	wait_status(services, &second, Status::Active).await?;
	refusal::refuse_next(MAP);
	release.send(()).expect("live owned task");
	timeout(Duration::from_secs(10), async {
		while refusal::pending() != 0 {
			sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.map_err(|_| err!("Completion refusal did not fire"))?;
	assert_eq!(
		services
			.tasks
			.get(&second)
			.await?
			.expect("retained task")
			.status,
		Status::Active
	);
	manifest.interrupted = vec![first, second];
	for n in 0..14 {
		let id = services
			.tasks
			.spawn(ACTION, format!("!queued{n}:example.com"), parameters.clone(), pending())
			.await?
			.to_string();
		wait_status(services, &id, Status::Active).await?;
		manifest.interrupted.push(id);
	}
	let baseline = rows(services).await?;
	let overflow = services
		.tasks
		.spawn(ACTION, "!overflow:example.com".into(), parameters, pending())
		.await;
	assert_eq!(
		overflow
			.expect_err("running capacity must refuse")
			.status_code(),
		reqwest::StatusCode::TOO_MANY_REQUESTS
	);
	assert_eq!(rows(services).await?, baseline);
	write(directory.join("manifest.json"), serde_json::to_vec(&manifest)?)?;
	write(
		directory.join("before-kill.json"),
		serde_json::to_vec(
			&rows(services)
				.await?
				.into_iter()
				.collect::<Vec<_>>(),
		)?,
	)?;
	// There is deliberately no fixture-side flush: production admission and
	// activation are responsible for their acknowledged process-kill boundary.
	write(directory.join("accepted-ready"), b"accepted")?;
	pending::<Result>().await
}

async fn inspect(services: &Services, directory: &Path, phase: &str) -> Result {
	let snapshot = if phase == "inspect" {
		"before-kill.json"
	} else {
		"before-rejection.json"
	};
	let expected: Vec<(Vec<u8>, Vec<u8>)> =
		serde_json::from_slice(&read(directory.join(snapshot))?)?;
	assert_eq!(
		rows(services).await?,
		expected.into_iter().collect(),
		"maintenance and failed startup preserve every physical journal row"
	);
	if phase == "repair" {
		let recovered: Vec<(Vec<u8>, Vec<u8>)> =
			serde_json::from_slice(&read(directory.join("recovered.json"))?)?;
		let mut txn = services.db.txn();
		txn.del_raw(&services.db[MAP], b"zzzzzzzzzzzzzzzz");
		for (key, value) in recovered {
			txn.insert_raw(&services.db[MAP], &key, &value);
		}
		txn.execute().await?;
	}
	Ok(())
}

async fn recovered(services: &Services, base: &str, directory: &Path, phase: &str) -> Result {
	wait_until_ready(services, base).await?;
	let manifest = manifest(directory)?;
	let complete = services
		.tasks
		.get(&manifest.complete)
		.await?
		.expect("completed id survives");
	assert_eq!(complete.status, Status::Complete);
	assert_eq!(complete.result, Some(json!({"purged":7})));
	let failed = services
		.tasks
		.get(&manifest.failed)
		.await?
		.expect("failed id survives");
	assert_eq!(failed.status, Status::Failed);
	assert!(
		failed
			.error
			.expect("recorded error")
			.contains("Owned fixture failure")
	);
	for id in &manifest.interrupted {
		let task = services
			.tasks
			.get(id)
			.await?
			.expect("accepted id survives");
		assert_eq!(task.status, Status::Failed);
		assert!(
			task.error
				.expect("explicit interruption")
				.contains("partial changes may exist")
		);
		assert!(task.result.is_none(), "interruption fabricates no result");
	}
	let response = services
		.client
		.clients
		.default
		.get(format!("{base}/_synapse/admin/v1/scheduled_tasks"))
		.bearer_auth(TOKEN)
		.send()
		.await?
		.error_for_status()?
		.json::<Value>()
		.await?;
	assert_eq!(
		response["scheduled_tasks"]
			.as_array()
			.expect("task list")
			.len(),
		18
	);
	let query = services
		.client
		.clients
		.default
		.get(format!(
			"{base}/_synapse/admin/v1/purge_history_status/{}",
			manifest.interrupted[0]
		))
		.bearer_auth(TOKEN)
		.send()
		.await?
		.error_for_status()?
		.json::<Value>()
		.await?;
	assert_eq!(query["status"], "failed");
	assert!(
		query["error"]
			.as_str()
			.expect("interruption")
			.contains("partial changes")
	);
	let delete = services
		.client
		.clients
		.default
		.get(format!(
			"{base}/_synapse/admin/v2/rooms/delete_status/{}",
			manifest.interrupted[1]
		))
		.bearer_auth(TOKEN)
		.send()
		.await?
		.error_for_status()?
		.json::<Value>()
		.await?;
	assert_eq!(delete["status"], "failed");
	assert!(delete["shutdown_room"].is_null());
	let current = rows(services).await?;
	if phase == "recover" {
		write(
			directory.join("recovered.json"),
			serde_json::to_vec(&current.into_iter().collect::<Vec<_>>())?,
		)?;
	} else {
		let expected: Vec<(Vec<u8>, Vec<u8>)> =
			serde_json::from_slice(&read(directory.join("recovered.json"))?)?;
		assert_eq!(
			current,
			expected.into_iter().collect(),
			"another restart changes no completed/interrupted outcome"
		);
	}
	Ok(())
}

async fn corrupt(services: &Services, directory: &Path) -> Result {
	// The lexically last bad record must be validated before rewriting an
	// earlier valid Active record. Startup refusal must preserve both bytes.
	let mut current = rows(services).await?;
	let (key, value) = current.first_key_value().expect("owned journal");
	let mut record: Value = serde_json::from_slice(value)?;
	record["status"] = json!("active");
	record["result"] = Value::Null;
	record["error"] = Value::Null;
	let mut txn = services.db.txn();
	txn.insert_raw(&services.db[MAP], key, &serde_json::to_vec(&record)?);
	txn.insert_raw(&services.db[MAP], b"zzzzzzzzzzzzzzzz", b"{invalid}");
	txn.execute().await?;
	services.db.engine()?.flush()?;
	current = rows(services).await?;
	write(
		directory.join("before-rejection.json"),
		serde_json::to_vec(&current.into_iter().collect::<Vec<_>>())?,
	)?;
	Ok(())
}

async fn status_read_refusals(services: &Services, base: &str, id: &str) -> Result {
	let baseline = services.db[MAP]
		.get(id.as_bytes())
		.await?
		.to_vec();
	let mut bad_outcome: Value = serde_json::from_slice(&baseline)?;
	bad_outcome["result"] = json!({"purged":"seven"});
	for corrupt in [b"{invalid}".to_vec(), serde_json::to_vec(&bad_outcome)?] {
		let mut txn = services.db.txn();
		txn.insert_raw(&services.db[MAP], id.as_bytes(), &corrupt);
		txn.execute().await?;
		services
			.tasks
			.get(id)
			.await
			.expect_err("corrupt status must refuse");
		for path in [format!("purge_history_status/{id}"), "scheduled_tasks".into()] {
			let response = services
				.client
				.clients
				.default
				.get(format!("{base}/_synapse/admin/v1/{path}"))
				.bearer_auth(TOKEN)
				.send()
				.await?;
			assert_eq!(response.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
			let body = response.json::<Value>().await?;
			assert!(body.get("status").is_none());
			assert!(body.get("scheduled_tasks").is_none());
		}
	}
	let mut txn = services.db.txn();
	txn.insert_raw(&services.db[MAP], id.as_bytes(), &baseline);
	txn.execute().await?;
	assert_eq!(
		services
			.tasks
			.get(id)
			.await?
			.expect("restored task")
			.status,
		Status::Complete
	);
	let absent = services
		.client
		.clients
		.default
		.get(format!("{base}/_synapse/admin/v1/purge_history_status/0000000000000000"))
		.bearer_auth(TOKEN)
		.send()
		.await?;
	assert_eq!(absent.status(), reqwest::StatusCode::NOT_FOUND);
	Ok(())
}
