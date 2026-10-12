#![cfg(test)]

//! Real rooms recover deferred counts before startup returns, without a count
//! read or another event. Refusals preserve storage; maintenance can repair it.

mod client;

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, temp_dir, var},
	fs::{DirBuilder, read, remove_dir_all, write},
	net::TcpListener,
	path::{Path, PathBuf},
	process::{Command, id as process_id},
	thread::sleep,
	time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Result, err, ruma::OwnedRoomId};
use tuwunel_database::{refusal, serialize_key};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

const PHASE: &str = "TUWUNEL_RECOUNT_STARTUP_PHASE";
const SCENARIO: &str = "TUWUNEL_RECOUNT_STARTUP_SCENARIO";
const DIRECTORY: &str = "TUWUNEL_RECOUNT_STARTUP_DIRECTORY";
const PENDING: &str = "membership_recount_pending";
const GENERATION: &str = "membership_recount_generation_v1";
const OWNER_TOKEN: &str = "disposable-startup-recount-owner-token";
const JOIN_TOKEN: &str = "disposable-startup-recount-joining-token";

struct OwnedDirectory(PathBuf);
impl Drop for OwnedDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
	room: OwnedRoomId,
	state: Vec<u8>,
	generation: Vec<u8>,
}

#[test]
fn pending_counts_recover_before_readiness_and_erasure_stays_erased() -> Result {
	if let Ok(phase) = var(PHASE) {
		return run_server(
			&PathBuf::from(var(DIRECTORY).expect("owned child directory")),
			&var(SCENARIO).expect("child scenario"),
			&phase,
		);
	}
	let root = temp_dir().join(format!("membership-recount-startup-{}", process_id()));
	let mut builder = DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&root)?;
	let root = OwnedDirectory(root);
	for scenario in [
		"valid",
		"marker",
		"generation",
		"key",
		"rows",
		"bytes",
		"unknown",
		"refused",
		"erasure",
	] {
		let directory = root.0.join(scenario);
		builder.create(&directory)?;
		let phases: &[&str] = match scenario {
			| "valid" => &["seed", "resume", "again"],
			| "erasure" => &["seed", "resume", "erase", "again"],
			| _ => &["seed", "reject", "inspect", "resume", "again"],
		};
		for phase in phases {
			child(&directory, scenario, phase)?;
		}
	}
	Ok(())
}

fn child(directory: &Path, scenario: &str, phase: &str) -> Result {
	let mut child = Command::new(current_exe()?)
		.env(PHASE, phase)
		.env(SCENARIO, scenario)
		.env(DIRECTORY, directory)
		.spawn()?;
	let started = Instant::now();
	loop {
		if let Some(status) = child.try_wait()? {
			assert!(status.success(), "startup recount {scenario}/{phase} failed");
			return Ok(());
		}
		if started.elapsed() > Duration::from_secs(90) {
			child.kill()?;
			child.wait()?;
			return Err(err!("startup recount {scenario}/{phase} exceeded its deadline"));
		}
		sleep(Duration::from_millis(20));
	}
}

fn run_server(directory: &Path, scenario: &str, phase: &str) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let modes: &[&str] = if phase == "seed" { &["fresh"] } else { &[] };
	let mut args = Args::default_test(modes);
	args.maintenance = matches!(phase, "inspect" | "erase");
	args.option.extend([
		format!("database_path={:?}", directory.join("database")),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		format!("listening={}", phase == "seed"),
		"log=\"error\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	drop(listener);
	let result = runtime.block_on(async {
		if phase == "reject" && scenario == "refused" {
			refusal::refuse_next("roomid_joinedcount");
		}
		let start = async_start(&server).await;
		if phase == "reject" {
			let Err(error) = start else {
				panic!("invalid startup unexpectedly accepted {scenario}");
			};
			assert!(
				server.services.lock().await.is_none(),
				"failed startup installs no services"
			);
			if scenario == "refused" {
				assert_eq!(refusal::pending(), 0, "startup reached the refused atomic repair");
				assert!(error.to_string().contains("armed to refuse"));
			}
			if matches!(scenario, "rows" | "bytes") {
				assert!(
					error
						.to_string()
						.contains("Pending recount inventory limit reached")
				);
			}
			return Ok(());
		}
		let services = start?;
		let base = format!("http://127.0.0.1:{port}");
		let exercise = async {
			let outcome = match phase {
				| "seed" => seed(&services, &base, directory, scenario).await,
				| "inspect" => inspect(&services, directory, scenario).await,
				| "resume" | "again" => resumed(&services, directory, scenario, phase).await,
				| "erase" => erased(&services, directory).await,
				| _ => panic!("unexpected startup fixture phase"),
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

fn snapshots(directory: &Path) -> Result<Vec<Snapshot>> {
	Ok(serde_json::from_slice(&read(directory.join("rooms.json"))?)?)
}

async fn seed(services: &Services, base: &str, directory: &Path, scenario: &str) -> Result {
	wait_until_ready(services, base).await?;
	register(services, "startup-owner", OWNER_TOKEN).await?;
	register(services, "startup-joining", JOIN_TOKEN).await?;
	let client = Client { services, base, token: OWNER_TOKEN };
	let mut rooms = Vec::new();
	for _ in 0..2 {
		let room = client
			.create_room(&json!({"preset": "public_chat"}))
			.await?;
		refusal::refuse_next("roomid_joinedcount");
		refusal::refuse_next("roomid_joinedcount");
		services
			.client
			.clients
			.default
			.post(client.url(&format!("join/{room}")))
			.bearer_auth(JOIN_TOKEN)
			.json(&json!({}))
			.send()
			.await?
			.error_for_status()?;
		assert_eq!(refusal::pending(), 0);
		assert_raw(services, &room, 1, true).await?;
		rooms.push(Snapshot {
			state: services.db["roomid_shortstatehash"]
				.get(&room)
				.await?
				.to_vec(),
			generation: services.db["global"]
				.qry(&(GENERATION, &room))
				.await?
				.to_vec(),
			room,
		});
	}
	rooms.sort_by(|a, b| a.room.cmp(&b.room));
	write(directory.join("rooms.json"), serde_json::to_vec(&rooms)?)?;
	let global = &services.db["global"];
	let last = &rooms[1].room;
	match scenario {
		| "marker" =>
			global
				.put((PENDING, last), b"invalid".as_slice())
				.await?,
		| "generation" =>
			global
				.put((GENERATION, last), b"invalid".as_slice())
				.await?,
		| "key" =>
			global
				.put((PENDING, "invalid-room-id"), b"".as_slice())
				.await?,
		| "rows" | "bytes" | "unknown" =>
			for key in extra_keys(scenario)? {
				global.insert(&key, &[]).await?;
			},
		| _ => {},
	}
	// A different namespace sharing the text prefix must never be examined.
	global
		.put(("membership_recount_pending_extra", "invalid-room-id"), b"retained".as_slice())
		.await?;
	Ok(())
}

fn extra_keys(scenario: &str) -> Result<Vec<Vec<u8>>> {
	let count = match scenario {
		| "rows" => 1023,
		| "bytes" => 600,
		| _ => 1,
	};
	(0..count)
		.map(|n| {
			let name = if scenario == "bytes" {
				format!("!byte-{n:04}{}:localhost", "x".repeat(230))
			} else {
				format!("!startup-orphan-{n:04}:localhost")
			};
			// Validate the ID: byte overflow cannot be a hidden malformed-key case.
			let room = OwnedRoomId::try_from(name)?;
			Ok(serialize_key((PENDING, room))?.to_vec())
		})
		.collect()
}

async fn assert_raw(
	services: &Services,
	room: &OwnedRoomId,
	count: u64,
	pending: bool,
) -> Result {
	let raw = services.db["roomid_joinedcount"]
		.get(room)
		.await?;
	assert_eq!(raw.as_ref(), count.to_be_bytes());
	let marker = services.db["global"].qry(&(PENDING, room)).await;
	if pending {
		assert!(marker?.is_empty());
	} else {
		assert!(
			marker
				.expect_err("completed marker absent")
				.is_not_found()
		);
	}
	Ok(())
}

async fn inspect(services: &Services, directory: &Path, scenario: &str) -> Result {
	let rooms = snapshots(directory)?;
	let global = &services.db["global"];
	for snapshot in &rooms {
		let raw = services.db["roomid_joinedcount"]
			.get(&snapshot.room)
			.await?;
		assert_eq!(raw.as_ref(), 1_u64.to_be_bytes(), "failed startup preserves every count");
		assert_eq!(
			services.db["roomid_shortstatehash"]
				.get(&snapshot.room)
				.await?
				.as_ref(),
			snapshot.state
		);
	}
	let last = &rooms[1].room;
	match scenario {
		| "marker" => {
			assert_eq!(global.qry(&(PENDING, last)).await?.as_ref(), b"invalid");
			global
				.put((PENDING, last), b"".as_slice())
				.await?;
		},
		| "generation" => {
			assert_eq!(global.qry(&(GENERATION, last)).await?.as_ref(), b"invalid");
			global
				.put((GENERATION, last), rooms[1].generation.as_slice())
				.await?;
		},
		| "key" => global.del((PENDING, "invalid-room-id")).await?,
		| "rows" | "bytes" | "unknown" =>
			for key in extra_keys(scenario)? {
				global.remove(&key).await?;
			},
		| _ => {},
	}
	for snapshot in &rooms {
		assert_raw(services, &snapshot.room, 1, true).await?;
	}
	Ok(())
}

async fn resumed(services: &Services, directory: &Path, scenario: &str, phase: &str) -> Result {
	for (n, snapshot) in snapshots(directory)?.iter().enumerate() {
		if scenario == "erasure" && phase == "again" && n == 0 {
			assert!(
				services.db["roomid_joinedcount"]
					.get(&snapshot.room)
					.await
					.expect_err("erased count absent")
					.is_not_found()
			);
			for namespace in [PENDING, GENERATION] {
				assert!(
					services.db["global"]
						.qry(&(namespace, &snapshot.room))
						.await
						.expect_err("erased metadata absent")
						.is_not_found()
				);
			}
			continue;
		}
		// These are physical reads: no count helper or new event repairs them.
		assert_raw(services, &snapshot.room, 2, false).await?;
		assert_eq!(
			services.db["roomid_shortstatehash"]
				.get(&snapshot.room)
				.await?
				.as_ref(),
			snapshot.state,
			"repair emits no event or room state"
		);
	}
	assert_eq!(
		services.db["global"]
			.qry(&("membership_recount_pending_extra", "invalid-room-id"))
			.await?
			.as_ref(),
		b"retained"
	);
	Ok(())
}

async fn erased(services: &Services, directory: &Path) -> Result {
	let rooms = snapshots(directory)?;
	let room = &rooms[0].room;
	let global = &services.db["global"];
	global
		.put((PENDING, room), b"".as_slice())
		.await?;
	let generation = global.qry(&(GENERATION, room)).await?.to_vec();
	refusal::refuse_next("roomid_joinedcount");
	services
		.state_cache
		.delete_room_join_counts(room, false)
		.await
		.expect_err("counter erasure refuses atomically");
	assert_eq!(refusal::pending(), 0);
	assert_raw(services, room, 2, true).await?;
	assert_eq!(global.qry(&(GENERATION, room)).await?.as_ref(), generation);
	services
		.state_cache
		.delete_room_join_counts(room, false)
		.await?;
	for map in ["roomid_joinedcount", "roomid_invitedcount", "roomid_knockedcount"] {
		assert!(
			services.db[map]
				.get(room)
				.await
				.expect_err("counter erased")
				.is_not_found()
		);
	}
	for namespace in [PENDING, GENERATION] {
		assert!(
			global
				.qry(&(namespace, room))
				.await
				.expect_err("repair metadata erased")
				.is_not_found()
		);
	}
	let guard = services.state.mutex.lock(room).await;
	services
		.delete
		.delete_room(room, false, guard)
		.await?;
	assert!(
		services.db["roomid_shortroomid"]
			.get(room)
			.await
			.expect_err("room purged")
			.is_not_found()
	);
	Ok(())
}
