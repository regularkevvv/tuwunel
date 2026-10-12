#![cfg(test)]

//! Actual response and push-rule consumers must refuse an unreadable count.
//! Raw corruption is preserved; restoring storage permits the same request.

mod client;

use std::{
	env::temp_dir, fs::remove_dir_all, net::TcpListener, path::PathBuf,
	process::id as process_id, time::Duration,
};

use reqwest::RequestBuilder;
use serde_json::{Value, json};
use tokio::time::timeout;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_admin::{fini, init};
use tuwunel_core::{
	Result, err, http,
	ruma::{
		RoomId, UserId,
		events::AnySyncTimelineEvent,
		push::{Action, Ruleset},
		serde::Raw,
	},
};
use tuwunel_database::{refusal, serialize_key};
use tuwunel_service::{Services, pusher::Evaluate};

use self::client::{Client, register, wait_until_ready};

const TOKEN: &str = "disposable-member-count-consumer-token";

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn summaries_directory_hierarchy_and_push_rules_refuse_failed_member_counts() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = DatabasePath(
		temp_dir().join(format!("tuwunel-member-count-consumers-{}-{port}", process_id())),
	);
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={:?}", db_path.0),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		"listening=true".into(),
		"log=\"warn\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		init(&services.admin);
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);
		let exercise = async {
			let outcome = timeout(Duration::from_mins(2), exercise(&services, &base))
				.await
				.map_err(|_| err!("member count consumer fixture exceeded its deadline"))
				.and_then(|result| result);
			fini(&services.admin);
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

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	let user = register(services, "member-count-owner", TOKEN).await?;
	let client = Client { services, base, token: TOKEN };
	let child = client
		.create_room(&json!({"preset": "public_chat", "visibility": "public"}))
		.await?;
	let space = client
		.create_room(&json!({
			"preset": "public_chat", "visibility": "public",
			"creation_content": {"type": "m.space"},
		}))
		.await?;
	services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{space}/state/m.space.child/{child}")))
		.bearer_auth(TOKEN)
		.json(&json!({"via": ["localhost"]}))
		.send()
		.await?
		.error_for_status()?;

	for room in [&space, &child] {
		views(&client, &user, room, &space, http::StatusCode::OK).await?;
		let counts = &services.db["roomid_joinedcount"];
		let saved = counts.get(room).await?.to_vec();
		for corrupt in [vec![], vec![0_u8; 7], vec![0_u8; 9], u64::MAX.to_be_bytes().to_vec()] {
			counts
				.insert(room.as_bytes(), corrupt.as_slice())
				.await?;
			// The preceding healthy request populated both hierarchy caches.
			hierarchy(&client, &space, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
			views(&client, &user, room, &space, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
			assert_eq!(counts.get(room).await?.as_ref(), corrupt, "refusal preserves raw count");
			counts
				.insert(room.as_bytes(), saved.as_slice())
				.await?;
			views(&client, &user, room, &space, http::StatusCode::OK).await?;
		}
		counts.remove(room).await?;
		views(&client, &user, room, &space, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
		assert!(
			counts
				.get(room)
				.await
				.expect_err("missing count remains absent")
				.is_not_found()
		);
		counts
			.insert(room.as_bytes(), saved.as_slice())
			.await?;
		views(&client, &user, room, &space, http::StatusCode::OK).await?;

		let global = &services.db["global"];
		let generation = serialize_key(("membership_recount_generation_v1", room))?;
		let saved_generation = global.get(&generation).await?.to_vec();
		global
			.insert(&generation, b"invalid-generation")
			.await?;
		views(&client, &user, room, &space, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
		assert_eq!(global.get(&generation).await?.as_ref(), b"invalid-generation");
		global
			.insert(&generation, saved_generation.as_slice())
			.await?;
		views(&client, &user, room, &space, http::StatusCode::OK).await?;

		let pending = serialize_key(("membership_recount_pending", room))?;
		global.insert(&pending, b"invalid-marker").await?;
		views(&client, &user, room, &space, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
		assert_eq!(global.get(&pending).await?.as_ref(), b"invalid-marker");
		global.remove(&pending).await?;
		views(&client, &user, room, &space, http::StatusCode::OK).await?;

		global.insert(&pending, &[]).await?;
		// Each actual consumer attempts the same pending repair, which refuses.
		for _ in 0..5 {
			refusal::refuse_next("roomid_joinedcount");
		}
		views(&client, &user, room, &space, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
		assert_eq!(refusal::pending(), 0, "all five consumers attempted repair");
		assert!(global.get(&pending).await?.is_empty(), "refused repair remains owed");
		assert_eq!(counts.get(room).await?.as_ref(), saved, "refused repair is atomic");
		views(&client, &user, room, &space, http::StatusCode::OK).await?;
		assert!(
			global
				.get(&pending)
				.await
				.expect_err("healthy retry repaired count")
				.is_not_found()
		);
	}
	purge_controls(&client).await
}

async fn response(request: RequestBuilder, expected: http::StatusCode) -> Result<Value> {
	let response = request.send().await?;
	let status = response.status();
	let body: Value = response.json().await?;
	assert_eq!(status, expected, "member count consumer response: {body}");
	if !expected.is_success() {
		assert_eq!(body["errcode"], "M_UNKNOWN", "sanitized storage refusal");
		for field in ["num_joined_members", "chunk", "rooms", "next_batch"] {
			assert!(body.get(field).is_none(), "refused response cannot publish {field}");
		}
	}
	Ok(body)
}

async fn views(
	client: &Client<'_>,
	user: &UserId,
	room: &RoomId,
	space: &RoomId,
	expected: http::StatusCode,
) -> Result {
	client.services.clear_cache().await;
	let http = &client.services.client.clients.default;
	let summary = response(
		http.get(format!(
			"{}/_matrix/client/unstable/im.nheko.summary/rooms/{room}/summary",
			client.base,
		))
		.bearer_auth(TOKEN),
		expected,
	)
	.await?;
	let directory = response(
		http.post(client.url("publicRooms"))
			.bearer_auth(TOKEN)
			.json(&json!({})),
		expected,
	)
	.await?;
	let hierarchy = hierarchy(client, space, expected).await?;
	if expected.is_success() {
		assert_eq!(summary["num_joined_members"], 1, "summary serves actual count");
		for (body, field) in [(&directory, "chunk"), (&hierarchy, "rooms")] {
			let row = body[field]
				.as_array()
				.expect("healthy room inventory")
				.iter()
				.find(|row| row["room_id"].as_str() == Some(room.as_str()))
				.expect("healthy inventory includes requested room");
			assert_eq!(row["num_joined_members"], 1, "inventory serves actual count");
		}
	}
	push_actions(client, user, room, expected.is_success()).await?;
	let listing = admin(client, "rooms list", expected.is_success()).await?;
	if let Some(listing) = listing {
		assert!(listing.contains(&format!("{room}\tMembers: 1")), "admin serves actual count");
	}
	Ok(())
}

async fn hierarchy(
	client: &Client<'_>,
	space: &RoomId,
	expected: http::StatusCode,
) -> Result<Value> {
	response(
		client
			.services
			.client
			.clients
			.default
			.get(format!("{}/_matrix/client/v1/rooms/{space}/hierarchy", client.base))
			.bearer_auth(TOKEN),
		expected,
	)
	.await
}

async fn push_actions(
	client: &Client<'_>,
	user: &UserId,
	room: &RoomId,
	healthy: bool,
) -> Result {
	let rules: Ruleset = serde_json::from_value(json!({
		"override": [{
			"rule_id": "one-member-only", "default": false, "enabled": true,
			"conditions": [{"kind": "room_member_count", "is": "1"}],
			"actions": ["notify"],
		}],
		"content": [], "room": [], "sender": [], "underride": [],
	}))?;
	let pdu: Raw<AnySyncTimelineEvent> =
		Raw::from_json(serde_json::value::to_raw_value(&json!({
			"type": "m.room.message", "event_id": "$member-count-consumer:localhost",
			"room_id": room, "sender": "@other:localhost", "origin_server_ts": 1,
			"content": {"msgtype": "m.text", "body": "count-based notification"},
		}))?);
	let actions = client
		.services
		.pusher
		.get_actions(Evaluate {
			user,
			ruleset: &rules,
			power_levels: None,
			pdu: &pdu,
			room_id: room,
			related_events: None,
		})
		.await;
	if healthy {
		assert!(actions?.iter().any(Action::should_notify), "actual count matches rule");
	} else {
		assert!(actions.is_err(), "failed count cannot fabricate a push-rule context");
	}
	Ok(())
}

async fn admin(client: &Client<'_>, command: &str, healthy: bool) -> Result<Option<String>> {
	match client
		.services
		.admin
		.command_in_place(command.to_owned(), None)
		.await
	{
		| Ok(Some(output)) if healthy => Ok(Some(output.as_str().to_owned())),
		| Err(output) if !healthy => {
			assert!(
				output.as_str().contains("Command failed"),
				"handler refusal: {}",
				output.as_str()
			);
			Ok(None)
		},
		| Ok(Some(output)) | Err(output) =>
			panic!("unexpected admin command result: {}", output.as_str()),
		| Ok(None) => panic!("admin command returned no result"),
	}
}

async fn purge_controls(client: &Client<'_>) -> Result {
	let services = client.services;
	let user =
		register(services, "purge-count-owner", "disposable-member-purge-count-fixture-token")
			.await?;
	let purge_client = Client {
		services,
		base: client.base,
		token: "disposable-member-purge-count-fixture-token",
	};
	let mut rooms = vec![
		purge_client
			.create_room(&json!({"preset": "public_chat"}))
			.await?,
		purge_client
			.create_room(&json!({"preset": "public_chat"}))
			.await?,
	];
	rooms.sort();
	let damaged = &rooms[1];
	exact_membership_budgets(services, &user, damaged).await?;
	let counts = &services.db["roomid_joinedcount"];
	let saved = counts.get(damaged).await?.to_vec();
	let indexed = &services.db["roomid_shortroomid"];
	let states = &services.db["roomid_shortstatehash"];
	let mut snapshots = Vec::new();
	for room in &rooms {
		snapshots.push((indexed.get(room).await?.to_vec(), states.get(room).await?.to_vec()));
	}
	counts
		.insert(damaged.as_bytes(), &[0_u8; 7])
		.await?;
	for regex in ["", " --regex"] {
		admin(client, &format!("rooms purge-user {user} --sole-member{regex}"), false).await?;
		for (room, (index, state)) in rooms.iter().zip(&snapshots) {
			assert_eq!(
				indexed.get(room).await?.as_ref(),
				index,
				"preflight refuses before any deletion"
			);
			assert_eq!(
				states.get(room).await?.as_ref(),
				state,
				"preflight preserves all room state"
			);
		}
	}
	counts
		.insert(damaged.as_bytes(), saved.as_slice())
		.await?;

	// A malformed room key must not disappear from the exact user's source.
	let joins = &services.db["userroomid_joined"];
	let bad_key = serialize_key((&user, "invalid-room-id"))?;
	joins
		.insert(&bad_key, &0_u64.to_be_bytes())
		.await?;
	admin(client, &format!("rooms purge-user {user} --sole-member"), false).await?;
	joins.remove(&bad_key).await?;
	for room in &rooms {
		assert!(indexed.get(room).await.is_ok(), "invalid source preserves every candidate");
	}

	// Source overflow is rejected before the handler inspects any candidate.
	let mut extra = Vec::new();
	for n in 0..1023 {
		let room = format!("!purge-overflow-{n:04}:localhost");
		let key = serialize_key((&user, room.as_str()))?;
		joins.insert(&key, &0_u64.to_be_bytes()).await?;
		extra.push(key);
	}
	assert!(
		services
			.state_cache
			.bounded_rooms_joined(&user)
			.await
			.is_err(),
		"complete source exceeds 1024 IDs"
	);
	admin(client, &format!("rooms purge-user {user} --sole-member"), false).await?;
	for key in &extra {
		joins.remove(key).await?;
	}
	for room in &rooms {
		assert!(indexed.get(room).await.is_ok(), "overflow preserves every candidate");
	}

	// A valid-width stale count cannot turn a two-member room into a sole one.
	services
		.client
		.clients
		.default
		.post(client.url(&format!("join/{damaged}")))
		.bearer_auth(TOKEN)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;
	let actual = counts.get(damaged).await?.to_vec();
	assert_eq!(
		u64::from(
			services
				.state_cache
				.room_joined_count_uint(damaged)
				.await?
		),
		2
	);
	counts
		.insert(damaged.as_bytes(), &1_u64.to_be_bytes())
		.await?;
	admin(client, &format!("rooms purge-user {user} --sole-member"), false).await?;
	for room in &rooms {
		assert!(indexed.get(room).await.is_ok(), "mismatched sole-member count preserves rooms");
	}
	counts
		.insert(damaged.as_bytes(), actual.as_slice())
		.await?;
	let output = admin(client, &format!("rooms purge-user {user} --sole-member --dry-run"), true)
		.await?
		.expect("dry run output");
	assert!(output.contains("Matched 1 rooms."));
	for room in &rooms {
		assert!(indexed.get(room).await.is_ok(), "dry run never deletes");
	}
	let output = admin(client, &format!("rooms purge-user {user} --sole-member"), true)
		.await?
		.expect("deletion output");
	assert!(output.contains("Deleted 1 rooms"));
	assert!(
		indexed
			.get(&rooms[0])
			.await
			.expect_err("sole-member candidate deleted")
			.is_not_found()
	);
	assert!(indexed.get(damaged).await.is_ok(), "two-member room preserved");
	assert!(
		indexed
			.get(&services.admin.get_admin_room().await?)
			.await
			.is_ok(),
		"admin room protected"
	);
	Ok(())
}

async fn exact_membership_budgets(services: &Services, user: &UserId, room: &RoomId) -> Result {
	let member_bytes = user.as_bytes().len();
	assert_eq!(
		services
			.state_cache
			.bounded_room_members_with_budget(room, 1, member_bytes)
			.await?,
		vec![user.to_owned()],
		"exact caller row and byte budgets are accepted"
	);
	for (rows, bytes) in [(0, member_bytes), (1, member_bytes.saturating_sub(1))] {
		assert!(
			services
				.state_cache
				.bounded_room_members_with_budget(room, rows, bytes)
				.await
				.is_err(),
			"smaller shared budget refuses instead of returning a partial inventory"
		);
	}
	Ok(())
}
