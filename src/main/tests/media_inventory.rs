#![cfg(test)]

use std::{env::temp_dir, fs::remove_dir_all, path::PathBuf, process::id as process_id};

use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_admin::{fini, init};
use tuwunel_core::{
	Result, http,
	matrix::PduBuilder,
	ruma::{
		Mxc, OwnedMxcUri, UserId, api::error::ErrorKind,
		events::room::avatar::RoomAvatarEventContent,
	},
};
use tuwunel_database::Json;
use tuwunel_service::{Services, admin::create_admin_room, profile::Propagation};

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn media_inventory_is_complete_or_refused_before_bulk_deletion() -> Result {
	let db_path =
		DatabasePath(temp_dir().join(format!("tuwunel-media-inventory-{}", process_id())));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.extend([format!("database_path={:?}", db_path.0), "log=\"warn\"".into()]);
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
	drop(server);
	drop(runtime);
	result
}

async fn exercise(services: &Services) -> Result {
	let map = &services.db["mediaid_file"];
	map.clear().await?;
	assert!(services.media.get_all_mxcs().await?.is_empty());
	let anchor = Mxc {
		server_name: services.globals.server_name(),
		media_id: "anchor",
	};
	let bytes: &[u8] = b"disposable media inventory anchor";
	services
		.media
		.create(&anchor, None, None, None, bytes)
		.await?;
	assert_eq!(services.media.get_all_mxcs().await?.len(), 1);
	for index in 0..4095 {
		map.insert(&format!("mxc://localhost/record-{index:04}"), 0_u64.to_be_bytes())
			.await?;
	}
	assert_eq!(services.media.get_all_mxcs().await?.len(), 4096);
	map.insert("mxc://localhost/row-overflow", 0_u64.to_be_bytes())
		.await?;
	let error = services
		.media
		.get_all_mxcs()
		.await
		.expect_err("an extra row cannot produce a short inventory");
	assert_eq!(error.status_code(), http::StatusCode::TOO_MANY_REQUESTS);
	assert!(matches!(error.kind(), ErrorKind::LimitExceeded(_)));
	assert_eq!(
		services
			.media
			.delete_by_date_size(u64::MAX, 0, false)
			.await
			.expect_err("inventory refusal must precede deletions")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	let command = format!(
		"media delete-all-from-server {} --yes-i-want-to-delete-local-media",
		services.globals.server_name()
	);
	assert!(
		services
			.admin
			.command_in_place(command, None)
			.await
			.is_err()
	);
	assert_eq!(services.media.get(&anchor, None).await?.content, bytes);
	map.remove("mxc://localhost/row-overflow").await?;
	assert_eq!(services.media.get_all_mxcs().await?.len(), 4096);
	corruption_and_byte_budget(services).await?;
	avatar_protection(services).await
}

async fn avatar_protection(services: &Services) -> Result {
	services.db["mediaid_file"].clear().await?;
	services.db["roomid_shortroomid"].clear().await?;
	let users = &services.db["userid_password"];
	users.clear().await?;
	let user = services.globals.server_user.as_ref();
	users.insert(user, "*").await?;
	let anchor = Mxc {
		server_name: services.globals.server_name(),
		media_id: "avatar-anchor",
	};
	let bytes: &[u8] = b"disposable protected avatar";
	services
		.media
		.create(&anchor, None, None, None, bytes)
		.await?;
	let uri: OwnedMxcUri = anchor.to_string().into();
	services
		.profile
		.set_avatar_url(user, Some(&uri), Some(Propagation::None))
		.await?;
	assert!(
		services
			.media
			.delete_by_date_size(u64::MAX, 0, true)
			.await?
			.is_empty()
	);
	// Disabled accounts still own protected profile media.
	users.insert(user, "").await?;
	assert!(
		services
			.media
			.delete_by_date_size(u64::MAX, 0, true)
			.await?
			.is_empty()
	);
	let profiles = &services.db["useridprofilekey_value"];
	profiles
		.put((user, "avatar_url"), b"not-json")
		.await?;
	services
		.media
		.delete_by_date_size(u64::MAX, 0, true)
		.await
		.expect_err("profile corruption cannot shorten the protected set");
	profiles
		.put((user, "avatar_url"), Json("not-an-mxc"))
		.await?;
	services
		.media
		.delete_by_date_size(u64::MAX, 0, true)
		.await
		.expect_err("malformed stored avatar must refuse deletion");
	profiles.del((user, "avatar_url")).await?;
	users.insert(user, "*").await?;
	create_admin_room(services).await?;
	let room = services.admin.get_admin_room().await?;
	let mut content = RoomAvatarEventContent::new();
	content.url = Some(uri);
	{
		let guard = services.state.mutex.lock(&room).await;
		services
			.timeline
			.build_and_append_pdu(PduBuilder::state(String::new(), &content), user, &room, &guard)
			.await?;
	}
	assert!(
		services
			.media
			.delete_by_date_size(u64::MAX, 0, true)
			.await?
			.is_empty()
	);
	assert_eq!(services.media.get(&anchor, None).await?.content, bytes);
	avatar_refusal_budgets(services, &anchor, bytes).await
}

async fn avatar_refusal_budgets(services: &Services, anchor: &Mxc<'_>, bytes: &[u8]) -> Result {
	let rooms = &services.db["roomid_shortroomid"];
	rooms.clear().await?;
	rooms
		.insert("!missing-state:localhost", 0_u64.to_be_bytes())
		.await?;
	services
		.media
		.delete_by_date_size(u64::MAX, 0, true)
		.await
		.expect_err("missing room state is not proof that its avatar is absent");
	rooms.clear().await?;
	rooms
		.insert("not-a-room", 0_u64.to_be_bytes())
		.await?;
	services
		.media
		.delete_by_date_size(u64::MAX, 0, true)
		.await
		.expect_err("malformed room IDs cannot disappear from the protected set");
	rooms.clear().await?;
	for index in 0..1025 {
		rooms
			.insert(&format!("!budget-{index:04}:localhost"), 0_u64.to_be_bytes())
			.await?;
	}
	assert_eq!(
		services
			.media
			.delete_by_date_size(u64::MAX, 0, true)
			.await
			.expect_err("room row overflow refuses before avatar reads or deletion")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	rooms.clear().await?;
	for index in 0..600 {
		rooms
			.insert(&format!("!{}-{index:04}:localhost", "r".repeat(220)), 0_u64.to_be_bytes())
			.await?;
	}
	assert_eq!(
		services
			.media
			.delete_by_date_size(u64::MAX, 0, true)
			.await
			.expect_err("room ID bytes have an independent budget")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	rooms.clear().await?;
	let users = &services.db["userid_password"];
	users.clear().await?;
	let profiles = &services.db["useridprofilekey_value"];
	let repeated_uri = format!("mxc://localhost/{}", "a".repeat(1024));
	for index in 0..128 {
		let user = UserId::parse(format!("@avatar-budget-{index:03}:localhost"))?;
		users.insert(&user, "").await?;
		profiles
			.put((&user, "avatar_url"), Json(&repeated_uri))
			.await?;
	}
	assert_eq!(
		services
			.media
			.delete_by_date_size(u64::MAX, 0, true)
			.await
			.expect_err("duplicate avatar references still consume the byte budget")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	assert_eq!(services.media.get(anchor, None).await?.content, bytes);
	Ok(())
}

async fn corruption_and_byte_budget(services: &Services) -> Result {
	let map = &services.db["mediaid_file"];
	map.clear().await?;
	map.insert("not-an-mxc", 0_u64.to_be_bytes())
		.await?;
	services
		.media
		.get_all_mxcs()
		.await
		.expect_err("malformed stored URI cannot disappear from the inventory");
	services
		.media
		.delete_by_date_size(u64::MAX, 0, false)
		.await
		.expect_err("corruption must refuse before deletion");
	assert!(map.get("not-an-mxc").await.is_ok());
	map.clear().await?;
	map.insert(&[0xFE], 0_u64.to_be_bytes()).await?;
	services
		.media
		.get_all_mxcs()
		.await
		.expect_err("invalid UTF-8 must propagate");
	map.clear().await?;
	for index in 0..180 {
		let mut key = format!("mxc://localhost/bytes-{index:04}").into_bytes();
		key.push(0xFF);
		key.extend_from_slice(&vec![b'x'; 6000]);
		map.insert(&key, 0_u64.to_be_bytes()).await?;
	}
	assert_eq!(
		services
			.media
			.get_all_mxcs()
			.await
			.expect_err("key bytes have an independent budget below the row limit")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	assert_eq!(
		services
			.media
			.delete_by_date_size(u64::MAX, 0, false)
			.await
			.expect_err("byte overflow cannot produce partial deletions")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	Ok(())
}
