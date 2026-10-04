#![cfg(test)]

mod client;

use std::{
	env::temp_dir, fs::remove_dir_all, net::TcpListener, path::PathBuf,
	process::id as process_id, time::Duration,
};

use serde_json::{Value, json};
use tokio::time::timeout;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err, http,
	ruma::{OwnedUserId, RoomId, UserId, events::StateEventType},
};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

const TOKEN: &str = "disposable-client-membership-owner-token";
const OUTSIDER_TOKEN: &str = "disposable-client-membership-outsider-token";
const JOINED: usize = 5;
const MEMBERS: usize = 6;

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

struct Endpoint<'a> {
	client: Client<'a>,
	room: &'a RoomId,
}

struct SavedPdu {
	key: Vec<u8>,
	value: Vec<u8>,
}

#[test]
fn client_membership_responses_are_complete_filtered_and_bounded() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = DatabasePath(
		temp_dir().join(format!("tuwunel-client-membership-bounds-{}-{port}", process_id())),
	);
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
			let outcome = timeout(Duration::from_mins(2), exercise(&services, &base))
				.await
				.map_err(|_| err!("client membership fixture exceeded its deadline"))
				.and_then(|result| result);
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

impl Endpoint<'_> {
	async fn request(
		&self,
		route: &str,
		token: Option<&str>,
	) -> Result<(http::StatusCode, Value)> {
		let request = self.client.services.client.clients.default.get(
			self.client
				.url(&format!("rooms/{}/{route}", self.room)),
		);
		let request = if let Some(token) = token {
			request.bearer_auth(token)
		} else {
			request
		};
		let response = request.send().await?;
		let status = response.status();
		Ok((status, response.json().await?))
	}

	async fn members(&self, query: &str, count: usize) -> Result<Value> {
		let (status, body) = self
			.request(&format!("members{query}"), Some(self.client.token))
			.await?;
		assert_eq!(status, http::StatusCode::OK, "{body}");
		assert_eq!(
			body["chunk"]
				.as_array()
				.expect("complete chunk")
				.len(),
			count
		);
		Ok(body)
	}

	async fn joined(&self) -> Result<Value> {
		let (status, body) = self
			.request("joined_members", Some(self.client.token))
			.await?;
		assert_eq!(status, http::StatusCode::OK, "{body}");
		assert_eq!(
			body["joined"]
				.as_object()
				.expect("joined map")
				.len(),
			JOINED
		);
		Ok(body)
	}

	async fn healthy(&self) -> Result {
		self.members("", MEMBERS).await?;
		self.joined().await?;
		Ok(())
	}

	async fn refused(&self, route: &str, expected: http::StatusCode) -> Result {
		let (status, body) = self
			.request(route, Some(self.client.token))
			.await?;
		assert_eq!(status, expected, "{route}: {body}");
		assert!(body.get("chunk").is_none(), "refusal cannot include a partial chunk");
		assert!(body.get("joined").is_none(), "refusal cannot include a partial map");
		if expected == http::StatusCode::TOO_MANY_REQUESTS {
			assert_eq!(body["errcode"], "M_LIMIT_EXCEEDED");
		}
		Ok(())
	}

	async fn post_as(&self, route: &str, token: &str, body: &Value) -> Result {
		self.client
			.services
			.client
			.clients
			.default
			.post(
				self.client
					.url(&format!("rooms/{}/{route}", self.room)),
			)
			.bearer_auth(token)
			.json(body)
			.send()
			.await?
			.error_for_status()?;
		Ok(())
	}
}

impl SavedPdu {
	async fn load(
		services: &Services,
		room: &RoomId,
		kind: &StateEventType,
		key: &str,
	) -> Result<Self> {
		let event = services
			.state_accessor
			.room_state_get_id(room, kind, key)
			.await?;
		let pdu_id = services.timeline.get_pdu_id(&event).await?;
		let key = pdu_id.as_ref().to_vec();
		let value = services.db["pduid_pdu"].get(&key).await?.to_vec();
		Ok(Self { key, value })
	}

	fn json(&self) -> Result<Value> { Ok(serde_json::from_slice(&self.value)?) }

	async fn write(&self, services: &Services, value: &Value) -> Result {
		services.db["pduid_pdu"]
			.raw_put(&self.key, serde_json::to_vec(value)?)
			.await?;
		services.clear_cache().await;
		Ok(())
	}

	async fn restore(&self, services: &Services) -> Result {
		services.db["pduid_pdu"]
			.raw_put(&self.key, &self.value)
			.await?;
		services.clear_cache().await;
		Ok(())
	}
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	let owner = register(services, "members-owner", TOKEN).await?;
	register(services, "members-outsider", OUTSIDER_TOKEN).await?;
	let owner_client = Client { services, base, token: TOKEN };
	let room = owner_client
		.create_room(&json!({"preset": "public_chat", "name": "membership bounds"}))
		.await?;
	let endpoint = Endpoint { client: owner_client, room: &room };
	let mut users = vec![owner];
	for index in 0..5 {
		let token = format!("disposable-client-membership-peer-{index}-token");
		let user = register(services, &format!("members-peer{index}"), &token).await?;
		if index < 4 {
			endpoint
				.post_as("join", &token, &json!({}))
				.await?;
		} else {
			endpoint
				.post_as("invite", TOKEN, &json!({"user_id": user}))
				.await?;
		}
		users.push(user);
	}
	services
		.client
		.clients
		.default
		.put(
			endpoint
				.client
				.url(&format!("rooms/{room}/state/m.room.topic")),
		)
		.bearer_auth(TOKEN)
		.json(&json!({"topic": "healthy non-member state"}))
		.send()
		.await?
		.error_for_status()?;
	endpoint.healthy().await?;
	endpoint
		.members("?membership=join", JOINED)
		.await?;
	endpoint
		.members("?not_membership=join", 1)
		.await?;
	// Matrix combines both filters with OR; identical filters include everyone.
	endpoint
		.members("?membership=join&not_membership=join", MEMBERS)
		.await?;
	endpoint
		.members("?membership=join&not_membership=invite", JOINED)
		.await?;
	endpoint.members("?membership=ban", 0).await?;
	for route in ["members", "joined_members"] {
		let (status, body) = endpoint.request(route, None).await?;
		assert_eq!(status, http::StatusCode::UNAUTHORIZED, "{body}");
		let (status, body) = endpoint
			.request(route, Some(OUTSIDER_TOKEN))
			.await?;
		assert_eq!(status, http::StatusCode::FORBIDDEN, "{body}");
	}
	let unknown = RoomId::parse("!members-unknown:localhost")?;
	let unknown_endpoint = Endpoint {
		client: Client { services, base, token: TOKEN },
		room: &unknown,
	};
	for route in ["members", "joined_members"] {
		unknown_endpoint
			.refused(route, http::StatusCode::FORBIDDEN)
			.await?;
	}
	appservice_access(&endpoint).await?;
	stored_members(&endpoint, &users[1], &users[0]).await?;
	response_budget(&endpoint, &users[..JOINED]).await?;
	source_budget_and_missing_state(&endpoint).await?;
	dictionary_and_snapshot_budgets(&endpoint).await
}

async fn appservice_access(endpoint: &Endpoint<'_>) -> Result {
	let services = endpoint.client.services;
	for (id, namespace, allowed) in [
		("members-matching", "^@members-peer0:localhost$", true),
		("members-unjoined", "^@members-neverjoined:localhost$", false),
	] {
		let token = format!("disposable-{id}-as-token");
		services
			.appservice
			.register_appservice(serde_json::from_value(json!({
				"id": id, "url": null, "as_token": token,
				"hs_token": format!("disposable-{id}-hs-token"),
				"sender_localpart": id,
				"namespaces": {"users": [{"exclusive": false, "regex": namespace}],
					"aliases": [], "rooms": []}, "rate_limited": false
			}))?)
			.await?;
		let (status, body) = endpoint
			.request("joined_members", Some(&token))
			.await?;
		if allowed {
			assert_eq!(status, http::StatusCode::OK, "{body}");
			assert_eq!(
				body["joined"]
					.as_object()
					.expect("complete map")
					.len(),
				JOINED
			);
		} else {
			assert_eq!(status, http::StatusCode::FORBIDDEN, "{body}");
		}
	}
	// World-readable state does not grant joined_members access to an outsider.
	services
		.client
		.clients
		.default
		.put(
			endpoint
				.client
				.url(&format!("rooms/{}/state/m.room.history_visibility", endpoint.room)),
		)
		.bearer_auth(TOKEN)
		.json(&json!({"history_visibility": "world_readable"}))
		.send()
		.await?
		.error_for_status()?;
	let (status, body) = endpoint
		.request("joined_members", Some(OUTSIDER_TOKEN))
		.await?;
	assert_eq!(status, http::StatusCode::FORBIDDEN, "{body}");
	let (status, body) = endpoint
		.request("members", Some(OUTSIDER_TOKEN))
		.await?;
	assert_eq!(status, http::StatusCode::OK, "{body}");
	assert_eq!(
		body["chunk"]
			.as_array()
			.expect("world-readable chunk")
			.len(),
		MEMBERS
	);
	Ok(())
}

async fn stored_members(endpoint: &Endpoint<'_>, target: &UserId, sender: &UserId) -> Result {
	let services = endpoint.client.services;
	let saved =
		SavedPdu::load(services, endpoint.room, &StateEventType::RoomMember, target.as_str())
			.await?;
	let mut value = saved.json()?;
	value["sender"] = json!(sender);
	value["content"]["displayname"] = json!("target profile");
	saved.write(services, &value).await?;
	let joined = endpoint.joined().await?;
	assert_eq!(joined["joined"][target.as_str()]["display_name"], "target profile");
	assert_ne!(joined["joined"][sender.as_str()]["display_name"], "target profile");
	saved.restore(services).await?;
	endpoint.healthy().await?;

	value = saved.json()?;
	value["content"] = json!({});
	saved.write(services, &value).await?;
	for route in ["members?membership=ban", "joined_members"] {
		endpoint
			.refused(route, http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
	}
	saved.restore(services).await?;
	endpoint.healthy().await?;

	let short = services
		.short
		.get_shortstatekey(&StateEventType::RoomMember, target.as_str())
		.await?;
	let mapping = &services.db["shortstatekey_statekey"];
	let saved_mapping = mapping.get(&short.to_be_bytes()).await?.to_vec();
	mapping
		.put(short, (&StateEventType::RoomMember, "not-a-user-id"))
		.await?;
	value = saved.json()?;
	value["state_key"] = json!("not-a-user-id");
	saved.write(services, &value).await?;
	for route in ["members?membership=ban", "joined_members"] {
		endpoint
			.refused(route, http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
	}
	mapping
		.raw_put(&short.to_be_bytes(), &saved_mapping)
		.await?;
	saved.restore(services).await?;
	endpoint.healthy().await
}

async fn response_budget(endpoint: &Endpoint<'_>, joined: &[OwnedUserId]) -> Result {
	let services = endpoint.client.services;
	let mut saved = Vec::new();
	for user in joined {
		let row =
			SavedPdu::load(services, endpoint.room, &StateEventType::RoomMember, user.as_str())
				.await?;
		let mut value = row.json()?;
		value["content"]["displayname"] = json!("x".repeat(50_000));
		row.write(services, &value).await?;
		saved.push(row);
	}
	endpoint.healthy().await?;
	for row in &saved {
		let mut value = row.json()?;
		value["content"]["displayname"] = json!("x".repeat(60_000));
		row.write(services, &value).await?;
	}
	for route in ["members", "joined_members"] {
		endpoint
			.refused(route, http::StatusCode::TOO_MANY_REQUESTS)
			.await?;
	}
	endpoint.members("?membership=ban", 0).await?;
	for row in saved {
		row.restore(services).await?;
	}
	endpoint.healthy().await
}

async fn source_budget_and_missing_state(endpoint: &Endpoint<'_>) -> Result {
	let services = endpoint.client.services;
	let saved = SavedPdu::load(services, endpoint.room, &StateEventType::RoomTopic, "").await?;
	services.db["pduid_pdu"]
		.remove(&saved.key)
		.await?;
	services.clear_cache().await;
	for route in ["members?membership=ban", "joined_members"] {
		endpoint
			.refused(route, http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
	}
	saved.restore(services).await?;
	endpoint.healthy().await?;
	let mut value = saved.json()?;
	value["content"]["topic"] = json!("x".repeat(400 * 1024));
	saved.write(services, &value).await?;
	endpoint.healthy().await?;
	value["content"]["topic"] = json!("x".repeat(512 * 1024));
	saved.write(services, &value).await?;
	for route in ["members?membership=ban", "joined_members"] {
		endpoint
			.refused(route, http::StatusCode::TOO_MANY_REQUESTS)
			.await?;
	}
	saved.restore(services).await?;
	endpoint.healthy().await
}

async fn dictionary_and_snapshot_budgets(endpoint: &Endpoint<'_>) -> Result {
	let services = endpoint.client.services;
	let room_state = &services.db["roomid_shortstatehash"];
	let saved_hash = room_state.get(endpoint.room).await?.to_vec();
	let parent = services
		.state
		.get_room_shortstatehash(endpoint.room)
		.await?;
	let event = services
		.state_accessor
		.room_state_get_id(endpoint.room, &StateEventType::RoomTopic, "")
		.await?;
	let short_event = services.short.get_shorteventid(&event).await?;
	let hash = *services.globals.next_count().await?;
	let mut encoded = parent.to_be_bytes().to_vec();
	for index in 0..7 {
		let key = format!("{index}{}", "x".repeat(80_000));
		let short_key = services
			.short
			.get_or_create_shortstatekey(&StateEventType::RoomTopic, &key)
			.await?;
		encoded.extend_from_slice(&short_key.to_be_bytes());
		encoded.extend_from_slice(&short_event.to_be_bytes());
	}
	services.db["shortstatehash_statediff"]
		.raw_put(&hash.to_be_bytes(), &encoded)
		.await?;
	room_state.raw_put(endpoint.room, hash).await?;
	assert_eq!(
		room_state.get(endpoint.room).await?.as_ref(),
		hash.to_be_bytes(),
		"synthetic current state must retain an exact u64 encoding"
	);
	services.clear_cache().await;
	for route in ["members?membership=ban", "joined_members"] {
		endpoint
			.refused(route, http::StatusCode::TOO_MANY_REQUESTS)
			.await?;
	}
	room_state
		.raw_put(endpoint.room, &saved_hash)
		.await?;
	services.clear_cache().await;
	endpoint.healthy().await?;

	let hash = *services.globals.next_count().await?;
	let mut encoded = 0_u64.to_be_bytes().to_vec();
	for key in 1..=4097_u64 {
		encoded.extend_from_slice(&key.to_be_bytes());
		encoded.extend_from_slice(&key.to_be_bytes());
	}
	services.db["shortstatehash_statediff"]
		.raw_put(&hash.to_be_bytes(), &encoded)
		.await?;
	room_state.raw_put(endpoint.room, hash).await?;
	assert_eq!(
		room_state.get(endpoint.room).await?.as_ref(),
		hash.to_be_bytes(),
		"synthetic current state must retain an exact u64 encoding"
	);
	services.clear_cache().await;
	for route in ["members", "joined_members"] {
		endpoint
			.refused(route, http::StatusCode::TOO_MANY_REQUESTS)
			.await?;
	}
	room_state
		.raw_put(endpoint.room, &saved_hash)
		.await?;
	services.clear_cache().await;
	endpoint.healthy().await?;

	room_state
		.raw_put(endpoint.room, b"invalid-hash")
		.await?;
	services.clear_cache().await;
	for route in ["members", "joined_members"] {
		endpoint
			.refused(route, http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
	}
	let (status, body) = endpoint
		.request("members", Some(OUTSIDER_TOKEN))
		.await?;
	assert_eq!(
		status,
		http::StatusCode::INTERNAL_SERVER_ERROR,
		"checked visibility must preserve errors: {body}"
	);
	room_state
		.raw_put(endpoint.room, &saved_hash)
		.await?;
	services.clear_cache().await;
	endpoint.healthy().await
}
