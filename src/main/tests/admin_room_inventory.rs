#![cfg(test)]

use std::{
	env::temp_dir, fs::remove_dir_all, net::TcpListener, path::PathBuf,
	process::id as process_id, time::Duration,
};

use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_admin::{fini, init};
use tuwunel_core::{
	Result, err, http,
	ruma::{OwnedRoomId, RoomId, UserId},
};
use tuwunel_service::Services;

const TOKEN: &str = "disposable-admin-room-inventory-token";

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

struct Endpoint<'a> {
	services: &'a Services,
	base: &'a str,
}

#[test]
fn admin_room_pages_preserve_totals_or_refuse_incomplete_inventories() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path =
		DatabasePath(temp_dir().join(format!("tuwunel-admin-room-inventory-{}", process_id())));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={:?}", db_path.0),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		"listening=true".into(),
		"log=\"warn\"".into(),
		"allow_local_presence=false".into(),
		"allow_outgoing_presence=false".into(),
		"delete_rooms_after_leave=true".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		init(&services.admin);
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);
		let exercise = async {
			let outcome = exercise(&Endpoint { services: &services, base: &base }).await;
			fini(&services.admin);
			let shutdown = server.server.shutdown();
			outcome.and(shutdown)
		};
		let (run, outcome) = tokio::join!(async_run(&server), exercise);
		drop(services);
		let stop = async_stop(&server).await;
		outcome.and(run).and(stop)
	});
	drop(runtime);
	result
}

impl Endpoint<'_> {
	async fn members(&self, room: &RoomId) -> Result<(http::StatusCode, Value)> {
		let response = self
			.services
			.client
			.clients
			.default
			.get(format!("{}/_synapse/admin/v1/rooms/{room}/members", self.base))
			.bearer_auth(TOKEN)
			.send()
			.await?;
		let status = response.status();
		Ok((status, response.json().await?))
	}

	async fn member_page(&self, room: &RoomId, total: usize) -> Result<Value> {
		let (status, body) = self.members(room).await?;
		assert_eq!(status, http::StatusCode::OK, "{body}");
		assert_eq!(body["total"], total);
		assert_eq!(body["members"].as_array().expect("members").len(), total);
		Ok(body)
	}

	async fn members_refused(&self, room: &RoomId, expected: http::StatusCode) -> Result {
		let (status, body) = self.members(room).await?;
		assert_eq!(status, expected, "{body}");
		assert!(body.get("members").is_none());
		assert!(body.get("total").is_none());
		if expected == http::StatusCode::TOO_MANY_REQUESTS {
			assert_eq!(body["errcode"], "M_LIMIT_EXCEEDED");
		}
		Ok(())
	}

	async fn request(&self, query: &str) -> Result<(http::StatusCode, Value)> {
		let response = self
			.services
			.client
			.clients
			.default
			.get(format!("{}/_synapse/admin/v1/rooms?{query}", self.base))
			.bearer_auth(TOKEN)
			.send()
			.await?;
		let status = response.status();
		Ok((status, response.json().await?))
	}

	async fn page(&self, query: &str, total: usize) -> Result<Value> {
		let (status, body) = self.request(query).await?;
		assert_eq!(status, http::StatusCode::OK, "{body}");
		assert_eq!(body["total_rooms"], total);
		assert!(body["rooms"].is_array());
		Ok(body)
	}

	async fn refused(&self, query: &str, expected: http::StatusCode) -> Result {
		let (status, body) = self.request(query).await?;
		assert_eq!(status, expected, "{body}");
		assert!(body.get("rooms").is_none());
		assert!(body.get("total_rooms").is_none());
		if expected == http::StatusCode::TOO_MANY_REQUESTS {
			assert_eq!(body["errcode"], "M_LIMIT_EXCEEDED");
		}
		Ok(())
	}

	async fn create(&self, name: &str) -> Result<OwnedRoomId> {
		let body: Value = self
			.services
			.client
			.clients
			.default
			.post(format!("{}/_matrix/client/v3/createRoom", self.base))
			.bearer_auth(TOKEN)
			.json(&json!({"name":name, "preset":"private_chat", "room_version":"11"}))
			.send()
			.await?
			.error_for_status()?
			.json()
			.await?;
		Ok(RoomId::parse(body["room_id"].as_str().expect("created room ID"))?)
	}
}

async fn exercise(endpoint: &Endpoint<'_>) -> Result {
	let services = endpoint.services;
	timeout(Duration::from_secs(10), async {
		loop {
			if services
				.client
				.clients
				.default
				.get(format!("{}/_matrix/client/versions", endpoint.base))
				.send()
				.await
				.is_ok_and(|response| response.status().is_success())
			{
				break;
			}
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| err!("disposable room API listener did not become ready"))?;
	assert_eq!(
		services
			.client
			.clients
			.default
			.get(format!("{}/_synapse/admin/v1/rooms", endpoint.base))
			.send()
			.await?
			.status(),
		http::StatusCode::UNAUTHORIZED
	);
	let admin = &services.globals.server_user;
	services.db["userid_password"]
		.insert(admin, "*")
		.await?;
	services
		.users
		.create_device(admin, None, (Some(TOKEN), None), None, None, None)
		.await?;
	services.db["roomid_shortroomid"].clear().await?;
	endpoint.page("", 0).await?;
	let alpha = endpoint.create("Alpha lounge").await?;
	let zeta = endpoint.create("Zeta lounge").await?;
	services.directory.set_public(&zeta, None).await?;
	let first = endpoint.page("limit=1", 2).await?;
	assert_eq!(first["rooms"][0]["room_id"], alpha.as_str());
	assert_eq!(first["rooms"][0]["joined_local_members"], 1);
	assert_eq!(first["next_batch"], 1);
	let second = endpoint.page("from=1&limit=1", 2).await?;
	assert_eq!(second["rooms"][0]["room_id"], zeta.as_str());
	assert_eq!(second["prev_batch"], 0);
	assert!(second["next_batch"].is_null());
	assert_eq!(endpoint.page("dir=b&limit=1", 2).await?["rooms"][0]["room_id"], zeta.as_str());
	assert_eq!(
		endpoint.page("public_rooms=true", 1).await?["rooms"][0]["room_id"],
		zeta.as_str()
	);
	assert_eq!(
		endpoint.page("search_term=ALPHA", 1).await?["rooms"][0]["room_id"],
		alpha.as_str()
	);
	assert_eq!(
		endpoint
			.page(&format!("search_term={alpha}"), 1)
			.await?["rooms"][0]["room_id"],
		alpha.as_str()
	);
	assert!(
		endpoint.page("from=9&limit=1", 2).await?["rooms"]
			.as_array()
			.expect("rooms")
			.is_empty()
	);
	endpoint.page("empty_rooms=true", 0).await?;
	checked_room_members(endpoint, &alpha).await?;
	empty_deletion_inventory(endpoint, &alpha).await?;
	complete_empty_deletion(endpoint).await?;
	complete_room_pruning(endpoint).await?;
	corrupt_inputs(endpoint, &alpha).await?;
	member_budgets(endpoint, &alpha).await?;
	aggregate_members(endpoint, &[alpha, zeta]).await
}

async fn checked_room_members(endpoint: &Endpoint<'_>, room: &RoomId) -> Result {
	let services = endpoint.services;
	let admin = &services.globals.server_user;
	assert_eq!(
		services
			.client
			.clients
			.default
			.get(format!("{}/_synapse/admin/v1/rooms/{room}/members", endpoint.base))
			.send()
			.await?
			.status(),
		http::StatusCode::UNAUTHORIZED
	);
	assert_eq!(endpoint.member_page(room, 1).await?["members"], json!([admin]));
	endpoint
		.members_refused(&RoomId::parse("!missing:localhost")?, http::StatusCode::NOT_FOUND)
		.await?;
	let rooms = &services.db["roomid_shortroomid"];
	let prefix = rooms.get(room).await?.to_vec();
	rooms.remove(room).await?;
	endpoint
		.members_refused(room, http::StatusCode::NOT_FOUND)
		.await?;
	rooms.insert(room, &prefix).await?;
	// A corrupt first timeline key must refuse instead of proving existence
	// or panicking in the historical infallible RawPduId decoder.
	services.db["pduid_pdu"]
		.insert(&prefix, b"")
		.await?;
	endpoint
		.members_refused(room, http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	services.db["pduid_pdu"].remove(&prefix).await?;
	let joined = &services.db["roomuserid_joined"];
	let original = joined.qry(&(room, admin)).await?.to_vec();
	joined.del((room, admin)).await?;
	endpoint.member_page(room, 0).await?;
	joined.put_raw((room, admin), original).await?;
	endpoint.member_page(room, 1).await?;
	Ok(())
}

async fn empty_deletion_inventory(endpoint: &Endpoint<'_>, room: &RoomId) -> Result {
	let services = endpoint.services;
	let joined = &services.db["roomuserid_joined"];
	let admin = &services.globals.server_user;
	let original = joined.qry(&(room, admin)).await?.to_vec();
	joined.del((room, admin)).await?;
	assert_eq!(
		services
			.delete
			.bounded_empty_local_rooms()
			.await?,
		[room.to_owned()]
	);
	assert!(
		!services
			.state_cache
			.has_local_membership_checked(room)
			.await?
	);
	for name in ["roomuserid_joined", "roomuserid_invitecount"] {
		let members = &services.db[name];
		members.put((room, "not-a-user"), b"").await?;
		assert_eq!(
			services
				.delete
				.bounded_empty_local_rooms()
				.await
				.expect_err("prune cannot return a partial candidate list")
				.status_code(),
			http::StatusCode::INTERNAL_SERVER_ERROR
		);
		assert_eq!(
			services
				.state_cache
				.has_local_membership_checked(room)
				.await
				.expect_err("corrupt membership cannot prove an empty room")
				.status_code(),
			http::StatusCode::INTERNAL_SERVER_ERROR
		);
		empty_deletion_preserves_room(services, room).await?;
		prune_refuses_without_deletion(services, room).await?;
		members.del((room, "not-a-user")).await?;
		for index in 0..1025 {
			members
				.put((room, &format!("@guard-{index:04}:remote.test")), b"")
				.await?;
		}
		assert_eq!(
			services
				.delete
				.bounded_empty_local_rooms()
				.await
				.expect_err("prune cannot accept a member row overflow")
				.status_code(),
			http::StatusCode::TOO_MANY_REQUESTS
		);
		assert_eq!(
			services
				.state_cache
				.has_local_membership_checked(room)
				.await
				.expect_err("incomplete membership cannot prove an empty room")
				.status_code(),
			http::StatusCode::TOO_MANY_REQUESTS
		);
		empty_deletion_preserves_room(services, room).await?;
		prune_refuses_without_deletion(services, room).await?;
		for index in 0..1025 {
			members
				.del((room, &format!("@guard-{index:04}:remote.test")))
				.await?;
		}
		for index in 0..600 {
			members
				.put((room, &format!("@{}-{index:04}:remote.test", "x".repeat(220))), b"")
				.await?;
		}
		assert_eq!(
			services
				.delete
				.bounded_empty_local_rooms()
				.await
				.expect_err("prune cannot accept a member byte overflow")
				.status_code(),
			http::StatusCode::TOO_MANY_REQUESTS
		);
		assert_eq!(
			services
				.state_cache
				.has_local_membership_checked(room)
				.await
				.expect_err("membership byte overflow cannot prove an empty room")
				.status_code(),
			http::StatusCode::TOO_MANY_REQUESTS
		);
		empty_deletion_preserves_room(services, room).await?;
		prune_refuses_without_deletion(services, room).await?;
		for index in 0..600 {
			members
				.del((room, &format!("@{}-{index:04}:remote.test", "x".repeat(220))))
				.await?;
		}
	}
	let invited = &services.db["roomuserid_invitecount"];
	invited
		.put((room, "@disabled:localhost"), b"")
		.await?;
	assert!(
		services
			.delete
			.bounded_empty_local_rooms()
			.await?
			.is_empty()
	);
	assert!(
		services
			.state_cache
			.has_local_membership_checked(room)
			.await?
	);
	empty_deletion_preserves_room(services, room).await?;
	invited.del((room, "@disabled:localhost")).await?;
	joined.put_raw((room, admin), original).await?;
	assert!(
		services
			.delete
			.bounded_empty_local_rooms()
			.await?
			.is_empty()
	);
	assert!(
		services
			.state_cache
			.has_local_membership_checked(room)
			.await?
	);
	Ok(())
}

async fn complete_empty_deletion(endpoint: &Endpoint<'_>) -> Result {
	let services = endpoint.services;
	let room = endpoint.create("Empty deletion").await?;
	services.db["roomuserid_joined"]
		.del((&room, &services.globals.server_user))
		.await?;
	assert_eq!(
		services
			.delete
			.bounded_empty_local_rooms()
			.await?,
		std::slice::from_ref(&room)
	);
	assert!(
		!services
			.state_cache
			.has_local_membership_checked(&room)
			.await?
	);
	let state_lock = services.state.mutex.lock(&room).await;
	services
		.delete
		.delete_if_empty_local(&room, state_lock)
		.await;
	assert!(
		services.db["roomid_shortroomid"]
			.get(&room)
			.await
			.expect_err("a proved empty room must be deleted")
			.is_not_found()
	);
	assert!(
		services.db["roomid_shortstatehash"]
			.get(&room)
			.await
			.expect_err("a proved empty room must lose its current state")
			.is_not_found()
	);
	endpoint.page("", 2).await?;
	Ok(())
}

async fn empty_deletion_preserves_room(services: &Services, room: &RoomId) -> Result {
	let rooms = &services.db["roomid_shortroomid"];
	let states = &services.db["roomid_shortstatehash"];
	let original_room = rooms.get(room).await?.to_vec();
	let original_state = states.get(room).await?.to_vec();
	let state_lock = services.state.mutex.lock(room).await;
	services
		.delete
		.delete_if_empty_local(room, state_lock)
		.await;
	assert_eq!(rooms.get(room).await?.to_vec(), original_room);
	assert_eq!(states.get(room).await?.to_vec(), original_state);
	Ok(())
}

async fn prune_refuses_without_deletion(services: &Services, room: &RoomId) -> Result {
	assert!(
		services
			.admin
			.command_in_place("rooms list".into(), None)
			.await
			.is_err(),
		"administrative pagination must not hide an incomplete room inventory"
	);
	let published = &services.db["publicroomids"];
	for index in 0..1025 {
		published
			.insert(&format!("!directory-boundary-{index:04}:localhost"), "")
			.await?;
	}
	assert!(
		services
			.admin
			.command_in_place("rooms directory list".into(), None)
			.await
			.is_err(),
		"directory pagination must not hide an incomplete publication inventory"
	);
	for index in 0..1025 {
		published
			.remove(&format!("!directory-boundary-{index:04}:localhost"))
			.await?;
	}
	let rooms = &services.db["roomid_shortroomid"];
	let states = &services.db["roomid_shortstatehash"];
	let original_room = rooms.get(room).await?.to_vec();
	let original_state = states.get(room).await?.to_vec();
	match services
		.admin
		.command_in_place("rooms prune-empty".into(), None)
		.await
	{
		| Err(output) => assert!(
			output.as_str().contains("Command failed"),
			"prune must reach the handler and refuse the inventory: {}",
			output.as_str()
		),
		| Ok(Some(output)) =>
			panic!("prune succeeded over an incomplete inventory: {}", output.as_str()),
		| Ok(None) => panic!("prune succeeded over an incomplete inventory without output"),
	}
	assert_eq!(rooms.get(room).await?.to_vec(), original_room);
	assert_eq!(states.get(room).await?.to_vec(), original_state);
	Ok(())
}

async fn complete_room_pruning(endpoint: &Endpoint<'_>) -> Result {
	let services = endpoint.services;
	let room = endpoint.create("Complete prune").await?;
	services.db["roomuserid_joined"]
		.del((&room, &services.globals.server_user))
		.await?;
	match services
		.admin
		.command_in_place("rooms prune-empty".into(), None)
		.await
	{
		| Ok(Some(output)) => assert!(
			output
				.as_str()
				.contains("Successfully deleted 1 rooms")
		),
		| Err(output) => panic!("prune refused complete empty inventory: {}", output.as_str()),
		| Ok(None) => panic!("prune omitted successful deletion count"),
	}
	assert!(
		services.db["roomid_shortroomid"]
			.get(&room)
			.await
			.expect_err("prune must delete a proved empty room")
			.is_not_found()
	);
	assert!(
		services.db["roomid_shortstatehash"]
			.get(&room)
			.await
			.expect_err("prune must delete its current state")
			.is_not_found()
	);
	endpoint.page("", 2).await?;
	Ok(())
}

async fn corrupt_inputs(endpoint: &Endpoint<'_>, room: &RoomId) -> Result {
	let services = endpoint.services;
	let rooms = &services.db["roomid_shortroomid"];
	rooms
		.insert("not-a-room", 0_u64.to_be_bytes())
		.await?;
	endpoint
		.refused("search_term=unmatched&limit=1", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	rooms.remove("not-a-room").await?;
	let counts = &services.db["roomid_joinedcount"];
	let count = counts.get(room).await?.to_vec();
	counts.remove(room).await?;
	endpoint
		.refused("limit=1", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	let malformed: [&[u8]; 4] = [b"", b"short", b"123456789", b"invalid-count"];
	for value in malformed {
		counts.insert(room, value).await?;
		endpoint
			.refused("limit=1", http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
	}
	counts
		.insert(room, u64::MAX.to_be_bytes())
		.await?;
	endpoint
		.refused("limit=1", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	counts.insert(room, count).await?;
	let members = &services.db["roomuserid_joined"];
	members.put((room, "not-a-user"), b"").await?;
	endpoint
		.members_refused(room, http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	endpoint
		.refused("limit=1", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	members.del((room, "not-a-user")).await?;
	let states = &services.db["roomid_shortstatehash"];
	let state = states.get(room).await?.to_vec();
	states.remove(room).await?;
	endpoint
		.refused("limit=1", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	states.insert(room, state).await?;
	let room_key = rooms.get(room).await?.to_vec();
	for value in malformed {
		rooms.insert(room, value).await?;
		endpoint
			.members_refused(room, http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		endpoint
			.refused("limit=1", http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
	}
	rooms.insert(room, room_key).await?;
	endpoint.page("", 2).await?;
	Ok(())
}

async fn member_budgets(endpoint: &Endpoint<'_>, room: &RoomId) -> Result {
	let services = endpoint.services;
	let members = &services.db["roomuserid_joined"];
	// The key-only count includes disabled local accounts and remote rows;
	// corrupt values do not change membership-presence semantics.
	let disabled = UserId::parse("@disabled:localhost")?;
	services.db["userid_password"]
		.insert(&disabled, "")
		.await?;
	members
		.put((room, &disabled), b"irrelevant-to-key-count")
		.await?;
	let page = endpoint
		.page(&format!("search_term={room}"), 1)
		.await?;
	assert_eq!(page["rooms"][0]["joined_local_members"], 2);
	assert!(
		endpoint.member_page(room, 2).await?["members"]
			.as_array()
			.expect("members")
			.contains(&json!(disabled))
	);
	members.del((room, &disabled)).await?;
	for index in 0..1023 {
		members
			.put((room, &format!("@row-{index:04}:remote.test")), b"")
			.await?;
	}
	endpoint.page("limit=1", 2).await?;
	let page = endpoint.member_page(room, 1024).await?;
	let mut expected = vec![services.globals.server_user.to_string()];
	expected.extend((0..1023).map(|index| format!("@row-{index:04}:remote.test")));
	expected.sort();
	assert_eq!(page["members"], json!(expected));
	members
		.put((room, "@overflow:remote.test"), b"")
		.await?;
	endpoint
		.members_refused(room, http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	endpoint
		.refused("limit=1", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	members
		.del((room, "@overflow:remote.test"))
		.await?;
	for index in 0..1023 {
		members
			.del((room, &format!("@row-{index:04}:remote.test")))
			.await?;
	}
	for index in 0..600 {
		members
			.put((room, &format!("@{}-{index:04}:remote.test", "x".repeat(220))), b"")
			.await?;
	}
	endpoint
		.members_refused(room, http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	endpoint
		.refused("limit=1", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	for index in 0..600 {
		members
			.del((room, &format!("@{}-{index:04}:remote.test", "x".repeat(220))))
			.await?;
	}
	Ok(())
}

async fn aggregate_members(endpoint: &Endpoint<'_>, first_rooms: &[OwnedRoomId]) -> Result {
	let mut rooms = first_rooms.to_vec();
	for index in 0..3 {
		rooms.push(endpoint.create(&format!("Extra {index}")).await?);
	}
	let members = &endpoint.services.db["roomuserid_joined"];
	for room in &rooms {
		for index in 0..1023 {
			members
				.put((room, &format!("@aggregate-{index:04}:remote.test")), b"")
				.await?;
		}
	}
	assert_eq!(
		endpoint
			.services
			.delete
			.bounded_empty_local_rooms()
			.await
			.expect_err("protected rooms still consume the shared prune budget")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	endpoint
		.refused("search_term=unmatched&limit=1", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	room_source_budgets(endpoint).await
}

async fn room_source_budgets(endpoint: &Endpoint<'_>) -> Result {
	let rooms = &endpoint.services.db["roomid_shortroomid"];
	rooms.clear().await?;
	for index in 0..1025 {
		rooms
			.insert(&format!("!budget-{index:04}:localhost"), 0_u64.to_be_bytes())
			.await?;
	}
	endpoint
		.refused("search_term=unmatched&limit=1", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	rooms.clear().await?;
	for index in 0..600 {
		rooms
			.insert(&format!("!{}-{index:04}:localhost", "x".repeat(220)), 0_u64.to_be_bytes())
			.await?;
	}
	endpoint
		.refused("search_term=unmatched&limit=1", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	Ok(())
}
