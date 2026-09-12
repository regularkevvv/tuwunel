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

const OWNER_TOKEN: &str = "sync-v5-state-completeness-owner-token";

#[test]
fn corrupt_required_state_withholds_the_sliding_sync_room() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path = PathBuf::from(root).join(format!("tuwunel-sync-v5-state-{}", process_id()));

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

	let _owner = register(services, "syncv5stateowner", OWNER_TOKEN).await?;
	let owner = Client { services, base, token: OWNER_TOKEN };
	let room = owner
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;
	set_topic(&owner, &room).await?;
	let topic = services
		.state_accessor
		.room_state_get_id(&room, &StateEventType::RoomTopic, "")
		.await?;

	assert_required_state_contains(&owner, &room, topic.as_str(), "healthy").await?;
	assert_absent_required_state_keeps_room(&owner, &room).await?;

	let pdu_id = services.timeline.get_pdu_id(&topic).await?;
	let pdus = &services.db["pduid_pdu"];
	let saved = pdus.get(&pdu_id).await?.to_vec();
	pdus.remove(&pdu_id).await?;
	services.clear_cache().await;

	assert_room_withheld(&owner, &room, topic.as_str(), "corrupt state").await?;

	pdus.raw_put(&pdu_id, &saved).await?;
	services.clear_cache().await;
	assert_required_state_contains(&owner, &room, topic.as_str(), "restored PDU").await
}

async fn set_topic(client: &Client<'_>, room: &RoomId) -> Result {
	client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/state/m.room.topic")))
		.bearer_auth(client.token)
		.json(&json!({ "topic": "sliding sync completeness anchor" }))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn assert_required_state_contains(
	client: &Client<'_>,
	room: &RoomId,
	state_event: &str,
	phase: &str,
) -> Result {
	let response = required_state_response(client, "m.room.topic").await?;
	let state = room_payload(&response, room, phase)?
		.get("required_state")
		.and_then(Value::as_array)
		.expect("room payload must contain required_state");

	assert!(
		state
			.iter()
			.any(|entry| { entry.get("event_id").and_then(Value::as_str) == Some(state_event) }),
		"{phase}: required_state omitted {state_event}: {response}"
	);

	Ok(())
}

async fn assert_absent_required_state_keeps_room(client: &Client<'_>, room: &RoomId) -> Result {
	let response = required_state_response(client, "m.room.avatar").await?;
	let state = room_payload(&response, room, "absent state")?
		.get("required_state")
		.and_then(Value::as_array)
		.cloned()
		.unwrap_or_default();

	assert!(state.is_empty(), "an absent state cell must not withhold the room: {response}");

	Ok(())
}

async fn assert_room_withheld(
	client: &Client<'_>,
	room: &RoomId,
	state_event: &str,
	phase: &str,
) -> Result {
	let response = required_state_response(client, "m.room.topic").await?;

	if let Some(rooms) = response.get("rooms").and_then(Value::as_object) {
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

async fn required_state_response(client: &Client<'_>, event_type: &str) -> Result<Value> {
	let url =
		format!("{}/_matrix/client/unstable/org.matrix.simplified_msc3575/sync", client.base);
	let response = client
		.services
		.client
		.clients
		.default
		.post(url)
		.bearer_auth(client.token)
		.json(&json!({
			"lists": {
				"all": {
					"ranges": [[0, 99]],
					"required_state": [[event_type, "*"]],
					"timeline_limit": 0,
				},
			},
		}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;

	Ok(response)
}

fn room_payload<'a>(response: &'a Value, room: &RoomId, phase: &str) -> Result<&'a Value> {
	response
		.get("rooms")
		.and_then(|rooms| rooms.get(room.as_str()))
		.ok_or_else(|| err!("{phase}: response omitted {room}: {response}"))
}
