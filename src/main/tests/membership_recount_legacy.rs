#![cfg(test)]

//! Separate processes simulate unmarked legacy writes and rollback after a
//! successful reconciliation. This is a RocksDB service proof, not execution
//! of an old deployed artifact or real-provider recovery evidence.

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
	Error, Result, err,
	ruma::{OwnedRoomId, RoomId, UserId},
};
use tuwunel_database::refusal;
use tuwunel_service::Services;

const PHASE: &str = "TUWUNEL_LEGACY_RECOUNT_PHASE";
const DIRECTORY: &str = "TUWUNEL_LEGACY_RECOUNT_DIRECTORY";
const PENDING: &str = "membership_recount_pending";
const GENERATION: &str = "membership_recount_generation_v1";
const SAVED: &str = "legacy_recount_fixture_previous_generation";
const COUNTS: [&str; 3] = ["roomid_joinedcount", "roomid_invitedcount", "roomid_knockedcount"];
const ROOMS: [&str; 6] = ["joined", "invited", "knocked", "refused", "inventory", "corrupt"];

struct OwnedDirectory(PathBuf);

impl Drop for OwnedDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn unmarked_legacy_counts_reconcile_after_process_replacement() -> Result {
	if let Ok(phase) = var(PHASE) {
		return run_server(&PathBuf::from(var(DIRECTORY).expect("child directory")), &phase);
	}
	let root = PathBuf::from(var("TMPDIR").unwrap_or_else(|_| "/tmp".into()));
	let directory = root.join(format!("legacy-membership-recount-{}", process_id()));
	let mut builder = DirBuilder::new();
	builder.recursive(false);
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&directory)?;
	let directory = OwnedDirectory(directory);
	for phase in ["seed", "resume", "rollback", "again"] {
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
		let outcome = tokio::time::timeout(std::time::Duration::from_mins(2), async {
			match phase {
				| "seed" => seed(&services).await,
				| "resume" => resume(&services).await,
				| "rollback" => rollback(&services).await,
				| "again" => again(&services).await,
				| _ => panic!("unexpected recount child phase"),
			}
		})
		.await
		.map_err(|_| err!("legacy recount fixture exceeded its deadline"))
		.and_then(|result| result);
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
	Ok(format!("!legacy-recount-{name}:localhost").try_into()?)
}

async fn raw_counts(services: &Services, room: &RoomId, expected: [u64; 3]) -> Result {
	for (map, count) in COUNTS.into_iter().zip(expected) {
		assert_eq!(services.db[map].get(room).await?.as_ref(), count.to_be_bytes());
	}
	Ok(())
}

async fn marker(services: &Services, room: &RoomId, pending: bool) -> Result {
	let marker = services.db["global"].qry(&(PENDING, room)).await;
	if pending {
		assert!(marker?.is_empty(), "failed reconciliation retains its obligation");
	} else {
		assert!(
			marker
				.expect_err("no pending obligation")
				.is_not_found()
		);
	}
	Ok(())
}

async fn generation(services: &Services, room: &RoomId) -> Result<Vec<u8>> {
	Ok(services.db["global"]
		.qry(&(GENERATION, room))
		.await?
		.as_ref()
		.to_vec())
}

async fn read(services: &Services, room: &RoomId, kind: usize) -> Result<u64> {
	match kind {
		| 0 => services.state_cache.room_joined_count(room).await,
		| 1 =>
			services
				.state_cache
				.room_invited_count(room)
				.await,
		| 2 =>
			services
				.state_cache
				.room_knocked_count(room)
				.await,
		| _ => unreachable!("owned counter kind"),
	}
}

async fn seed(services: &Services) -> Result {
	let user = UserId::parse("@legacy-recount:localhost")?;
	for name in ROOMS {
		let room = room(name)?;
		// Simulate an older writer: raw index/count commits without a marker.
		for map in ["roomuserid_joined", "roomuserid_invitecount", "roomuserid_knockedcount"] {
			services.db[map]
				.put_raw((&*room, &*user), 1_u64.to_be_bytes())
				.await?;
		}
		services.db["userroomid_joined"]
			.put_raw((&*user, &*room), 1_u64.to_be_bytes())
			.await?;
		for map in COUNTS {
			services.db[map]
				.insert(room.as_bytes(), 99_u64.to_be_bytes())
				.await?;
		}
		if name != "joined" && name != "corrupt" {
			services.db["global"]
				.put_raw((GENERATION, &*room), [b'X'; 32])
				.await?;
		}
		raw_counts(services, &room, [99; 3]).await?;
		marker(services, &room, false).await?;
	}
	Ok(())
}

async fn resume(services: &Services) -> Result {
	for (kind, name) in ["joined", "invited", "knocked"]
		.into_iter()
		.enumerate()
	{
		let room = room(name)?;
		raw_counts(services, &room, [99; 3]).await?;
		marker(services, &room, false).await?;
		assert_eq!(read(services, &room, kind).await?, 1, "each reader reconciles legacy counts");
		raw_counts(services, &room, [1; 3]).await?;
		marker(services, &room, false).await?;
		assert_ne!(generation(services, &room).await?, [b'X'; 32]);
	}
	refused(services).await?;
	incomplete_inventory(services).await?;
	corrupt(services).await
}

async fn refused(services: &Services) -> Result {
	let room = room("refused")?;
	refusal::refuse_next("roomid_joinedcount");
	read(services, &room, 0)
		.await
		.expect_err("aggregate commit refused");
	assert_eq!(refusal::pending(), 0, "reconciliation reached the aggregate commit");
	raw_counts(services, &room, [99; 3]).await?;
	assert_eq!(generation(services, &room).await?, [b'X'; 32], "stamp commits with aggregates");
	marker(services, &room, true).await?;
	assert_eq!(read(services, &room, 1).await?, 1, "pending retry repairs legacy counts");
	raw_counts(services, &room, [1; 3]).await?;
	marker(services, &room, false).await?;
	refusal::refuse_next("roomid_joinedcount");
	for kind in 0..3 {
		assert_eq!(read(services, &room, kind).await?, 1, "current stamps avoid repeat rebuilds");
	}
	assert_eq!(refusal::pending(), 1, "healthy reads did not write aggregates");
	services
		.state_cache
		.update_joined_count(&room)
		.await
		.expect_err("explicit recount still writes");
	assert_eq!(refusal::pending(), 0);
	marker(services, &room, true).await?;
	assert_eq!(read(services, &room, 2).await?, 1);
	marker(services, &room, false).await
}

async fn incomplete_inventory(services: &Services) -> Result {
	let room = room("inventory")?;
	let key = tuwunel_database::serialize_key((&*room, "not-a-user"))?;
	let map = &services.db["roomuserid_invitecount"];
	map.insert(key.as_slice(), 1_u64.to_be_bytes())
		.await?;
	assert!(matches!(
		read(services, &room, 0)
			.await
			.expect_err("invalid inventory"),
		Error::Database(_)
	));
	raw_counts(services, &room, [99; 3]).await?;
	assert_eq!(generation(services, &room).await?, [b'X'; 32]);
	marker(services, &room, true).await?;
	map.remove(key.as_slice()).await?;
	assert_eq!(read(services, &room, 0).await?, 1);
	raw_counts(services, &room, [1; 3]).await?;
	marker(services, &room, false).await
}

async fn corrupt(services: &Services) -> Result {
	let room = room("corrupt")?;
	let global = &services.db["global"];
	let missing = self::room("missing")?;
	for kind in 0..3 {
		assert!(
			read(services, &missing, kind)
				.await
				.expect_err("missing count")
				.is_not_found()
		);
	}
	marker(services, &missing, false).await?;
	assert!(
		global
			.qry(&(GENERATION, &*missing))
			.await
			.expect_err("missing room has no stamp")
			.is_not_found()
	);
	for bytes in [vec![], vec![b'X'; 31], vec![b'X'; 33], vec![0xFF; 32]] {
		global
			.put_raw((GENERATION, &*room), bytes.as_slice())
			.await?;
		assert!(matches!(
			read(services, &room, 0)
				.await
				.expect_err("invalid stamp"),
			Error::Database(_)
		));
		assert_eq!(generation(services, &room).await?, bytes, "corruption is preserved");
		raw_counts(services, &room, [99; 3]).await?;
		marker(services, &room, false).await?;
	}
	global
		.put_raw((GENERATION, &*room), [b'X'; 32])
		.await?;
	for length in [0_usize, 7, 9] {
		let bytes = vec![0_u8; length];
		services.db[COUNTS[0]]
			.insert(room.as_bytes(), bytes.as_slice())
			.await?;
		assert!(matches!(
			read(services, &room, 0)
				.await
				.expect_err("invalid legacy counter"),
			Error::Database(_)
		));
		assert_eq!(
			services.db[COUNTS[0]]
				.get(room.as_bytes())
				.await?
				.as_ref(),
			bytes
		);
		assert_eq!(generation(services, &room).await?, [b'X'; 32]);
		marker(services, &room, false).await?;
	}
	services.db[COUNTS[0]]
		.insert(room.as_bytes(), 99_u64.to_be_bytes())
		.await?;
	assert_eq!(read(services, &room, 0).await?, 1);
	raw_counts(services, &room, [1; 3]).await?;
	marker(services, &room, false).await
}

async fn save_generation(services: &Services, room: &RoomId) -> Result {
	let generation = generation(services, room).await?;
	assert_eq!(generation.len(), 32);
	assert!(generation.iter().all(u8::is_ascii_alphanumeric));
	services.db["global"]
		.put_raw((SAVED, room), generation.as_slice())
		.await
}

async fn rollback(services: &Services) -> Result {
	let added = UserId::parse("@legacy-added-after-rollback:localhost")?;
	for name in ROOMS {
		let room = room(name)?;
		save_generation(services, &room).await?;
		services.db["roomuserid_joined"]
			.put_raw((&*room, &*added), 2_u64.to_be_bytes())
			.await?;
		services.db["userroomid_joined"]
			.put_raw((&*added, &*room), 2_u64.to_be_bytes())
			.await?;
		for map in COUNTS {
			services.db[map]
				.insert(room.as_bytes(), 99_u64.to_be_bytes())
				.await?;
		}
		marker(services, &room, false).await?;
	}
	Ok(())
}

async fn again(services: &Services) -> Result {
	for (index, name) in ROOMS.into_iter().enumerate() {
		let room = room(name)?;
		raw_counts(services, &room, [99; 3]).await?;
		marker(services, &room, false).await?;
		let kind = index % 3;
		assert_eq!(read(services, &room, kind).await?, [2, 1, 1][kind]);
		raw_counts(services, &room, [2, 1, 1]).await?;
		marker(services, &room, false).await?;
		let previous = services.db["global"]
			.qry(&(SAVED, &*room))
			.await?;
		assert_ne!(
			generation(services, &room).await?,
			previous.as_ref(),
			"replacement reconciles prior stamps"
		);
	}
	Ok(())
}
