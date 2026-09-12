#![cfg(test)]

use std::{env::var, net::TcpListener, path::PathBuf, process::id as process_id};

use futures::future::join;
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{RoomId, events::StateEventType},
};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

mod client;

const OWNER_TOKEN: &str = "sync-state-completeness-owner-token";
const PEER_TOKEN: &str = "sync-state-completeness-peer-token";

#[test]
fn corrupt_full_sync_state_withholds_the_room() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path = PathBuf::from(root).join(format!("tuwunel-sync-state-{}", process_id()));

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={db_path:?}"),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"log=\"warn\"".to_owned(),
	]);

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);

		let exercise = async {
			let outcome = exercise(&services, &base).await;
			let shutdown = server.server.shutdown();

			outcome.and(shutdown)
		};
		let (run_result, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run_result?;

		outcome
	});

	drop(runtime);

	result
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let _owner = register(services, "syncstateowner", OWNER_TOKEN).await?;
	let owner = Client { services, base, token: OWNER_TOKEN };
	let room = owner
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;
	set_topic(&owner, &room, "sync completeness anchor").await?;
	send_message(&owner, &room, "sync-state-anchor").await?;
	let topic = services
		.state_accessor
		.room_state_get_id(&room, &StateEventType::RoomTopic, "")
		.await?;

	assert_full_state_contains(&owner, &room, topic.as_str(), "healthy").await?;

	let pdu_id = services.timeline.get_pdu_id(&topic).await?;
	let pdus = &services.db["pduid_pdu"];
	let saved = pdus.get(&pdu_id).await?.to_vec();
	pdus.remove(&pdu_id).await?;
	services.clear_cache().await;

	assert_room_withheld(&owner, &room, topic.as_str(), "corrupt state").await?;

	pdus.raw_put(&pdu_id, &saved).await?;
	services.clear_cache().await;
	assert_full_state_contains(&owner, &room, topic.as_str(), "restored PDU").await?;

	let shortstatekey = services
		.short
		.get_shortstatekey(&StateEventType::RoomTopic, "")
		.await?;
	let statekeys = &services.db["shortstatekey_statekey"];
	let key = shortstatekey.to_be_bytes();
	let saved = statekeys.get(&key).await?.to_vec();
	statekeys.remove(&key).await?;
	services.clear_cache().await;

	assert_room_withheld(&owner, &room, topic.as_str(), "corrupt state key").await?;

	statekeys.raw_put(&key, &saved).await?;
	services.clear_cache().await;
	assert_full_state_contains(&owner, &room, topic.as_str(), "restored state key").await?;

	let baseline = full_state_response(&owner).await?;
	let since = next_batch(&baseline)?.to_owned();
	drop(baseline);

	set_topic(&owner, &room, "sync completeness delta").await?;
	send_message(&owner, &room, "sync-state-delta-message").await?;
	let delta_topic = services
		.state_accessor
		.room_state_get_id(&room, &StateEventType::RoomTopic, "")
		.await?;
	assert_incremental_state_contains(
		&owner,
		&room,
		&since,
		delta_topic.as_str(),
		"healthy delta",
	)
	.await?;

	let shortstatehash = services
		.state
		.get_room_shortstatehash(&room)
		.await?;
	let statediffs = &services.db["shortstatehash_statediff"];
	let key = shortstatehash.to_be_bytes();
	let saved = statediffs.get(&key).await?.to_vec();
	statediffs.remove(&key).await?;
	services.clear_cache().await;

	assert_incremental_room_withheld(
		&owner,
		&room,
		&since,
		delta_topic.as_str(),
		"corrupt delta",
	)
	.await?;

	statediffs.raw_put(&key, &saved).await?;
	services.clear_cache().await;
	assert_incremental_state_contains(
		&owner,
		&room,
		&since,
		delta_topic.as_str(),
		"restored delta",
	)
	.await?;

	exercise_lazy_member_forward_mapping(services, &owner, &room).await
}

async fn exercise_lazy_member_forward_mapping(
	services: &Services,
	owner: &Client<'_>,
	room: &RoomId,
) -> Result {
	let peer_user = register(services, "syncstatepeer", PEER_TOKEN).await?;
	let peer = Client {
		services,
		base: owner.base,
		token: PEER_TOKEN,
	};
	join_room(&peer, room).await?;

	let baseline = lazy_timeline_response(owner, None).await?;
	let since = next_batch(&baseline)?.to_owned();
	drop(baseline);

	send_message(&peer, room, "sync-state-lazy-member-message").await?;
	let member = services
		.state_accessor
		.room_state_get_id(room, &StateEventType::RoomMember, peer_user.as_str())
		.await?;
	assert_lazy_member_contains(owner, room, &since, member.as_str(), "healthy lazy member")
		.await?;

	let key = tuwunel_database::serialize_key((&StateEventType::RoomMember, peer_user.as_str()))?;
	let forward = &services.db["statekey_shortstatekey"];
	let saved = forward.get(&key).await?.to_vec();
	forward.remove(&key).await?;
	services.clear_cache().await;

	assert_lazy_member_contains(owner, room, &since, member.as_str(), "missing lazy forward key")
		.await?;

	forward.raw_put(&key, &saved).await?;
	services.clear_cache().await;
	assert_lazy_member_contains(owner, room, &since, member.as_str(), "restored lazy forward key")
		.await
}

async fn set_topic(client: &Client<'_>, room: &RoomId, topic: &str) -> Result {
	client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/state/m.room.topic")))
		.bearer_auth(client.token)
		.json(&json!({ "topic": topic }))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn send_message(client: &Client<'_>, room: &RoomId, transaction_id: &str) -> Result {
	client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/{transaction_id}")))
		.bearer_auth(client.token)
		.json(&json!({ "msgtype": "m.text", "body": "sync state anchor" }))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn join_room(client: &Client<'_>, room: &RoomId) -> Result {
	client
		.services
		.client
		.clients
		.default
		.post(client.url(&format!("rooms/{room}/join")))
		.bearer_auth(client.token)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn assert_full_state_contains(
	client: &Client<'_>,
	room: &RoomId,
	state_event: &str,
	phase: &str,
) -> Result {
	let response = full_state_response(client).await?;
	let state = joined_room(&response, room, phase)?
		.get("state")
		.and_then(|state| state.get("events"))
		.and_then(Value::as_array)
		.expect("full sync response must contain state events");

	assert!(
		state
			.iter()
			.any(|entry| { entry.get("event_id").and_then(Value::as_str) == Some(state_event) }),
		"{phase}: full sync state omitted {state_event}: {response}"
	);

	Ok(())
}

async fn assert_room_withheld(
	client: &Client<'_>,
	room: &RoomId,
	state_event: &str,
	phase: &str,
) -> Result {
	let response = full_state_response(client).await?;

	if let Some(rooms) = response
		.get("rooms")
		.and_then(|rooms| rooms.get("join"))
		.and_then(Value::as_object)
	{
		assert!(
			!rooms.contains_key(room.as_str()),
			"{phase}: returned a partial room payload: {response}"
		);
	}
	assert!(
		!response.to_string().contains(state_event),
		"{phase}: response exposed the corrupt state event: {response}"
	);

	Ok(())
}

async fn assert_incremental_state_contains(
	client: &Client<'_>,
	room: &RoomId,
	since: &str,
	state_event: &str,
	phase: &str,
) -> Result {
	let response = incremental_response(client, since).await?;
	let state = joined_room(&response, room, phase)?
		.get("state")
		.and_then(|state| state.get("events"))
		.and_then(Value::as_array)
		.ok_or_else(|| {
			err!("{phase}: incremental sync response omitted state events: {response}")
		})?;

	assert!(
		state
			.iter()
			.any(|entry| { entry.get("event_id").and_then(Value::as_str) == Some(state_event) }),
		"{phase}: incremental sync state omitted {state_event}: {response}"
	);

	Ok(())
}

async fn assert_incremental_room_withheld(
	client: &Client<'_>,
	room: &RoomId,
	since: &str,
	state_event: &str,
	phase: &str,
) -> Result {
	let response = incremental_response(client, since).await?;

	if let Some(rooms) = response
		.get("rooms")
		.and_then(|rooms| rooms.get("join"))
		.and_then(Value::as_object)
	{
		assert!(
			!rooms.contains_key(room.as_str()),
			"{phase}: returned a partial room payload: {response}"
		);
	}
	assert!(
		!response.to_string().contains(state_event),
		"{phase}: response exposed the corrupt state event: {response}"
	);

	Ok(())
}

async fn assert_lazy_member_contains(
	client: &Client<'_>,
	room: &RoomId,
	since: &str,
	member_event: &str,
	phase: &str,
) -> Result {
	let response = lazy_timeline_response(client, Some(since)).await?;
	let state = joined_room(&response, room, phase)?
		.get("state")
		.and_then(|state| state.get("events"))
		.and_then(Value::as_array)
		.ok_or_else(|| err!("{phase}: sync response omitted lazy member state: {response}"))?;

	assert!(
		state
			.iter()
			.any(|entry| { entry.get("event_id").and_then(Value::as_str) == Some(member_event) }),
		"{phase}: sync response omitted lazy member {member_event}: {response}"
	);

	Ok(())
}

async fn full_state_response(client: &Client<'_>) -> Result<Value> {
	client
		.services
		.client
		.clients
		.default
		.get(client.url("sync"))
		.bearer_auth(client.token)
		.query(&[
			("timeout", "0"),
			("full_state", "true"),
			(
				"filter",
				r#"{"room":{"state":{"lazy_load_members":true},"timeline":{"limit":0}}}"#,
			),
		])
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}

async fn incremental_response(client: &Client<'_>, since: &str) -> Result<Value> {
	client
		.services
		.client
		.clients
		.default
		.get(client.url("sync"))
		.bearer_auth(client.token)
		.query(&[
			("timeout", "0"),
			("since", since),
			(
				"filter",
				r#"{"room":{"state":{"lazy_load_members":true},"timeline":{"limit":0}}}"#,
			),
		])
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}

async fn lazy_timeline_response(client: &Client<'_>, since: Option<&str>) -> Result<Value> {
	let request = client
		.services
		.client
		.clients
		.default
		.get(client.url("sync"))
		.bearer_auth(client.token);

	let response = match since {
		| Some(since) =>
			request
				.query(&[
					("timeout", "0"),
					("since", since),
					(
						"filter",
						r#"{"room":{"state":{"lazy_load_members":true},"timeline":{"limit":1}}}"#,
					),
				])
				.send()
				.await?,
		| None =>
			request
				.query(&[
					("timeout", "0"),
					(
						"filter",
						r#"{"room":{"state":{"lazy_load_members":true},"timeline":{"limit":1}}}"#,
					),
				])
				.send()
				.await?,
	};

	response
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}

fn next_batch(response: &Value) -> Result<&str> {
	response
		.get("next_batch")
		.and_then(Value::as_str)
		.ok_or_else(|| err!("sync response omitted next_batch"))
}

fn joined_room<'a>(response: &'a Value, room: &RoomId, phase: &str) -> Result<&'a Value> {
	response
		.get("rooms")
		.and_then(|rooms| rooms.get("join"))
		.and_then(|rooms| rooms.get(room.as_str()))
		.ok_or_else(|| err!("{phase}: response omitted {room}: {response}"))
}
