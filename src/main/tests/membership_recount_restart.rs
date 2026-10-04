#![cfg(test)]

//! Durable recount obligations are created with membership and cleared with
//! aggregates. Separate processes verify refused and deferred recounts retain
//! their obligation across restart; rejected membership creates neither.

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, var},
	fs::{DirBuilder, remove_dir_all},
	path::{Path, PathBuf},
	process::{Command, id as process_id},
};

use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Error, Result,
	matrix::PduCount,
	ruma::{
		OwnedRoomId, RoomId, UserId,
		events::room::member::{MembershipState, RoomMemberEventContent},
	},
};
use tuwunel_database::refusal;
use tuwunel_service::{Services, rooms::state_cache::MembershipUpdate};

const PHASE: &str = "TUWUNEL_RECOUNT_RESTART_PHASE";
const DIRECTORY: &str = "TUWUNEL_RECOUNT_RESTART_DIRECTORY";
const PENDING: &str = "membership_recount_pending";

struct OwnedDirectory(PathBuf);

impl Drop for OwnedDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn durable_recount_markers_survive_process_restart() -> Result {
	if let Ok(phase) = var(PHASE) {
		return run_server(&PathBuf::from(var(DIRECTORY).expect("child directory")), &phase);
	}
	let root = PathBuf::from(var("TMPDIR").unwrap_or_else(|_| "/tmp".into()));
	let directory = root.join(format!("membership-recount-restart-{}", process_id()));
	let mut builder = DirBuilder::new();
	builder.recursive(false);
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&directory)?;
	let directory = OwnedDirectory(directory);
	for phase in ["seed", "resume", "again"] {
		let status = Command::new(current_exe()?)
			.env(PHASE, phase)
			.env(DIRECTORY, &directory.0)
			.status()?;
		assert!(status.success(), "durable membership recount child {phase} failed");
	}
	Ok(())
}

fn run_server(directory: &Path, phase: &str) -> Result {
	let modes: &[&str] = if phase == "seed" { &["fresh"] } else { &[] };
	let mut args = Args::default_test(modes);
	args.maintenance = true;
	args.option
		.push(format!("database_path={:?}", directory.join("database")));
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let outcome = match phase {
			| "seed" => seed(&services).await,
			| "resume" => resume(&services).await,
			| "again" => again(&services).await,
			| _ => panic!("unexpected recount child phase"),
		};
		let shutdown = server.server.shutdown();
		drop(services);
		let run = async_run(&server).await;
		let stop = async_stop(&server).await;
		outcome.and(shutdown).and(run).and(stop)
	});
	drop(runtime);
	result
}

fn room(name: &str) -> Result<OwnedRoomId> {
	Ok(format!("!recount-{name}:localhost").try_into()?)
}

async fn membership(services: &Services, room: &RoomId, recount: bool) -> Result {
	let user = UserId::parse("@recount-restart:localhost")?;
	services
		.state_cache
		.update_membership(MembershipUpdate {
			room_id: room,
			user_id: &user,
			membership_event: RoomMemberEventContent::new(MembershipState::Join),
			sender: &user,
			last_state: None,
			invite_via: None,
			update_joined_count: recount,
			count: PduCount::Normal(42),
		})
		.await
}

async fn seed(services: &Services) -> Result {
	for name in ["refused", "deferred", "rejected"] {
		let room = room(name)?;
		services.db["roomid_joinedcount"]
			.raw_put(room.as_bytes(), 0_u64.to_be_bytes().as_slice())
			.await?;
		if name == "refused" {
			refusal::refuse_next("roomid_joinedcount");
			membership(services, &room, true)
				.await
				.expect_err("aggregate commit is refused");
		} else if name == "rejected" {
			// The global map also carries unrelated counters and startup writes.
			// Target the joined index in the same atomic membership/marker batch.
			refusal::refuse_next("roomuserid_joined");
			membership(services, &room, false)
				.await
				.expect_err("membership and marker commit is refused");
		} else {
			membership(services, &room, false).await?;
		}
		assert_eq!(refusal::pending(), 0, "seed reached the intended refused commit");
		assert_state(services, &room, name != "rejected", 0).await?;
	}
	Ok(())
}

async fn assert_state(services: &Services, room: &RoomId, pending: bool, count: u64) -> Result {
	assert_eq!(
		services
			.state_cache
			.room_joined_count(room)
			.await?,
		count,
		"aggregate count for {room}"
	);
	let marker = services.db["global"].qry(&(PENDING, room)).await;
	if pending {
		assert!(marker?.is_empty(), "durable marker remains intact for {room}");
	} else {
		assert!(
			marker.expect_err("marker absent").is_not_found(),
			"only missing marker means no pending recount"
		);
	}
	let user = UserId::parse("@recount-restart:localhost")?;
	assert_eq!(
		services.state_cache.is_joined(&user, room).await,
		room.as_str() != "!recount-rejected:localhost",
		"membership commit for {room}"
	);
	Ok(())
}

async fn resume(services: &Services) -> Result {
	let refused = room("refused")?;
	let deferred = room("deferred")?;
	assert_state(services, &refused, true, 0).await?;
	assert_state(services, &deferred, true, 0).await?;
	assert_state(services, &room("rejected")?, false, 0).await?;
	refusal::refuse_next("roomid_joinedcount");
	services
		.state_cache
		.repair_joined_count(&refused)
		.await
		.expect_err("retry aggregate commit refused");
	assert_eq!(refusal::pending(), 0, "repair reached the refused aggregate commit");
	assert_state(services, &refused, true, 0).await?;
	let global = &services.db["global"];
	global
		.put((PENDING, &*refused), b"corrupt-marker".as_slice())
		.await?;
	let error = services
		.state_cache
		.repair_joined_count(&refused)
		.await
		.expect_err("corrupt pending marker refuses repair");
	assert!(
		matches!(error, Error::Database(message) if message.as_ref() == "Invalid membership recount marker"),
		"marker corruption is classified exactly"
	);
	assert_eq!(
		global.qry(&(PENDING, &*refused)).await?.as_ref(),
		b"corrupt-marker",
		"refusal preserves corrupt marker"
	);
	assert_eq!(
		services
			.state_cache
			.room_joined_count(&refused)
			.await?,
		0,
		"corrupt marker cannot publish aggregate counts"
	);
	global
		.put((PENDING, &*refused), &[0_u8; 0][..])
		.await?;
	for room in [&refused, &deferred] {
		services
			.state_cache
			.repair_joined_count(room)
			.await?;
		assert_state(services, room, false, 1).await?;
	}
	Ok(())
}

async fn again(services: &Services) -> Result {
	for name in ["refused", "deferred"] {
		let room = room(name)?;
		assert_state(services, &room, false, 1).await?;
		services
			.state_cache
			.repair_joined_count(&room)
			.await?;
		assert_state(services, &room, false, 1).await?;
	}
	assert_state(services, &room("rejected")?, false, 0).await
}
