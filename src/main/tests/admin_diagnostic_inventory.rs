#![cfg(test)]

//! Dispatch the installed administrative root against a real native store.
//! Inventory refusal must precede query output or the first device-key marker.
use std::{env::temp_dir, fs::remove_dir_all, path::PathBuf, process::id, time::SystemTime};

use futures::{StreamExt, lock::Mutex};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_admin::{fini, init};
use tuwunel_core::{
	Result, http,
	ruma::{RoomId, UserId, user_id},
};
use tuwunel_database::{Row, refusal};
use tuwunel_service::{Services, admin::Context};

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn administrative_inventories_refuse_before_output_or_key_markers() -> Result {
	let path =
		DatabasePath(temp_dir().join(format!("tuwunel-admin-diagnostic-inventory-{}", id())));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.push(format!("database_path={:?}", path.0));
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		init(&services.admin);
		let outcome = exercise(&services).await;
		fini(&services.admin);
		let shutdown = server.server.shutdown();
		drop(services);
		let run = async_run(&server).await;
		let stop = async_stop(&server).await;
		outcome.and(shutdown).and(run).and(stop)
	});
	drop(runtime);
	result
}

async fn dispatch(services: &Services, argv: &[&str]) -> (Result, String) {
	let root = services
		.admin
		.command
		.read()
		.expect("command lock")
		.as_ref()
		.expect("installed admin root")
		.clone();
	let matches = root
		.clap()
		.try_get_matches_from(argv)
		.expect("valid command");
	let context = Context {
		services,
		body: &[],
		timer: SystemTime::now(),
		output: Mutex::new(String::new()),
	};
	let result = root.dispatch(matches, &context).await;
	let output = context.output.lock().await.clone();
	(result, output)
}

const SHARED: &[&str] = &[
	"tuwunel",
	"query",
	"users",
	"get-shared-rooms",
	"@alice:localhost",
	"@bob:localhost",
];
const UPDATE: &[&str] = &["tuwunel", "debug", "force-device-list-updates"];

type Snapshot = (Vec<Row>, Vec<Row>, u64);
async fn snapshot(services: &Services) -> Result<Snapshot> {
	Ok((
		services.db["keychangeid_userid"]
			.raw_rows_after(None, 2_000)
			.await?,
		services.db["global"]
			.raw_rows_after(None, 2_000)
			.await?,
		services.globals.pending_count().end,
	))
}

async fn refused(services: &Services, argv: &[&str], status: http::StatusCode) -> Result {
	let before = snapshot(services).await?;
	let (result, output) = dispatch(services, argv).await;
	assert_eq!(
		result
			.expect_err("complete source must refuse")
			.status_code(),
		status
	);
	assert!(output.is_empty(), "a failed inventory cannot render partial success: {output}");
	assert_eq!(
		snapshot(services).await?,
		before,
		"preflight precedes every marker/counter write"
	);
	Ok(())
}

async fn exercise(services: &Services) -> Result {
	shared_rooms(services).await?;
	registered_users(services).await
}

async fn joined(services: &Services, user: &UserId, room: &RoomId) -> Result {
	services.db["userroomid_joined"]
		.put_raw((user, room), 1_u64.to_be_bytes())
		.await
}

async fn shared_rooms(services: &Services) -> Result {
	let map = &services.db["userroomid_joined"];
	map.clear().await?;
	let alice = user_id!("@alice:localhost");
	let bob = user_id!("@bob:localhost");
	let shared = RoomId::parse("!shared:localhost")?;
	joined(services, alice, &shared).await?;
	joined(services, bob, &shared).await?;
	joined(services, alice, &RoomId::parse("!alice-only:localhost")?).await?;
	// Exact row limit still succeeds, including every nonmatching Bob room.
	for index in 0..1023 {
		joined(services, bob, &RoomId::parse(format!("!b-{index:04}:localhost"))?).await?;
	}
	let (result, output) = dispatch(services, SHARED).await;
	result?;
	assert!(output.contains(shared.as_str()));
	assert!(!output.contains("alice-only"));
	assert!(!output.contains("!b-"));
	joined(services, bob, &RoomId::parse("!overflow:localhost")?).await?;
	refused(services, SHARED, http::StatusCode::TOO_MANY_REQUESTS).await?;
	map.clear().await?;
	joined(services, alice, &shared).await?;
	joined(services, bob, &shared).await?;
	// Alice is valid and already shares a room; corruption in the second
	// inventory must still prevent even that valid prefix being printed.
	map.put_raw((bob, "not-a-room-id"), 1_u64.to_be_bytes())
		.await?;
	refused(services, SHARED, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	map.clear().await?;
	joined(services, alice, &shared).await?;
	for index in 0..700 {
		let room = RoomId::parse(format!("!{}-{index:03}:localhost", "x".repeat(190)))?;
		joined(services, bob, &room).await?;
	}
	refused(services, SHARED, http::StatusCode::TOO_MANY_REQUESTS).await?;
	// The first inventory must be complete too, even with an empty second.
	map.clear().await?;
	map.put_raw((alice, "not-a-room-id"), 1_u64.to_be_bytes())
		.await?;
	refused(services, SHARED, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	map.clear().await?;
	Ok(())
}

async fn registered_users(services: &Services) -> Result {
	let users = &services.db["userid_password"];
	let changes = &services.db["keychangeid_userid"];
	users.clear().await?;
	changes.clear().await?;
	let alice = user_id!("@alice:localhost");
	let disabled = user_id!("@disabled:localhost");
	users.insert(alice, "*").await?;
	users.insert(disabled, "").await?;
	let (result, output) = dispatch(services, UPDATE).await;
	result?;
	assert!(output.contains("Marked all devices for all users"));
	for user in [alice, disabled] {
		assert_eq!(
			services
				.users
				.keys_changed(user, 0, None)
				.collect::<Vec<_>>()
				.await,
			vec![user]
		);
	}
	// The exact raw source-byte limit remains usable.
	let key_bytes = alice
		.as_bytes()
		.len()
		.saturating_add(disabled.as_bytes().len());
	users
		.insert(alice, vec![b'a'; (128_usize * 1024).saturating_sub(key_bytes)])
		.await?;
	let (result, output) = dispatch(services, UPDATE).await;
	result?;
	assert!(output.contains("Marked all devices for all users"));
	users.insert(alice, "*").await?;
	// A late corrupt source row must not allow earlier valid users to be
	// marked. Keep existing markers as canaries in every refusal snapshot.
	users.insert(&[0xFF], "*").await?;
	refused(services, UPDATE, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	users.remove(&[0xFF]).await?;
	users.clear().await?;
	for index in 0..1025 {
		users
			.insert(&format!("@user-{index:04}:localhost"), "*")
			.await?;
	}
	refused(services, UPDATE, http::StatusCode::TOO_MANY_REQUESTS).await?;
	// Raw values consume the complete source budget, even though no password
	// is retained in the command's owned user list.
	users.clear().await?;
	users.insert(alice, vec![b'a'; 90_000]).await?;
	users.insert(disabled, vec![b'b'; 50_000]).await?;
	refused(services, UPDATE, http::StatusCode::TOO_MANY_REQUESTS).await?;
	users.clear().await?;
	// Disabled rows also consume raw ID bytes before local-user filtering.
	for index in 0..700 {
		users
			.insert(&format!("@{}-{index:03}:localhost", "x".repeat(190)), "")
			.await?;
	}
	refused(services, UPDATE, http::StatusCode::TOO_MANY_REQUESTS).await?;
	assert_eq!(
		services
			.users
			.bounded_local_users()
			.await
			.expect_err("disabled source bytes must refuse before local-user filtering")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	users.clear().await?;
	changes.clear().await?;
	let bob = user_id!("@bob:localhost");
	users.insert(alice, "*").await?;
	users.insert(bob, "*").await?;
	// Keep the established individually atomic semantics: the first user
	// commits, a refused second user propagates, and success is not printed.
	refusal::refuse_after("keychangeid_userid", 1);
	let (result, output) = dispatch(services, UPDATE).await;
	result.expect_err("individual user commit failure propagates");
	assert_eq!(refusal::pending(), 0);
	assert!(output.is_empty(), "failed update cannot claim all users were marked");
	assert_eq!(
		services
			.users
			.keys_changed(alice, 0, None)
			.collect::<Vec<_>>()
			.await,
		vec![alice]
	);
	assert!(
		services
			.users
			.keys_changed(bob, 0, None)
			.collect::<Vec<_>>()
			.await
			.is_empty()
	);
	Ok(())
}
