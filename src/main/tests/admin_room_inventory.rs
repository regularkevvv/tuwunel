#![cfg(test)]

use std::{
	env::temp_dir, fs::remove_dir_all, net::TcpListener, path::PathBuf,
	process::id as process_id, time::Duration,
};

use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
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
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);
		let exercise = async {
			let outcome = exercise(&Endpoint { services: &services, base: &base }).await;
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
	corrupt_inputs(endpoint, &alpha).await?;
	member_budgets(endpoint, &alpha).await?;
	aggregate_members(endpoint, &[alpha, zeta]).await
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
	counts.insert(room, "invalid-count").await?;
	endpoint
		.refused("limit=1", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	counts.insert(room, count).await?;
	let members = &services.db["roomuserid_joinedcount"];
	members.put((room, "not-a-user"), b"").await?;
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
	rooms.insert(room, "invalid-short-id").await?;
	endpoint
		.refused("limit=1", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	rooms.insert(room, room_key).await?;
	endpoint.page("", 2).await?;
	Ok(())
}

async fn member_budgets(endpoint: &Endpoint<'_>, room: &RoomId) -> Result {
	let services = endpoint.services;
	let members = &services.db["roomuserid_joinedcount"];
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
	members.del((room, &disabled)).await?;
	for index in 0..1023 {
		members
			.put((room, &format!("@row-{index:04}:remote.test")), b"")
			.await?;
	}
	endpoint.page("limit=1", 2).await?;
	members
		.put((room, "@overflow:remote.test"), b"")
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
	let members = &endpoint.services.db["roomuserid_joinedcount"];
	for room in &rooms {
		for index in 0..1023 {
			members
				.put((room, &format!("@aggregate-{index:04}:remote.test")), b"")
				.await?;
		}
	}
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
