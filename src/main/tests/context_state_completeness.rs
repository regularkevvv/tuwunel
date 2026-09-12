#![cfg(test)]

use std::{env::var, net::TcpListener, path::PathBuf, process::id as process_id};

use futures::future::join;
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result,
	ruma::{OwnedEventId, RoomId, events::StateEventType},
};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

mod client;

const OWNER_TOKEN: &str = "context-state-completeness-owner-token";

#[test]
fn corrupt_context_state_never_returns_partial_success() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path = PathBuf::from(root).join(format!("tuwunel-context-state-{}", process_id()));

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

	let _owner = register(services, "contextstateowner", OWNER_TOKEN).await?;
	let owner = Client { services, base, token: OWNER_TOKEN };
	let room = owner
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;
	set_topic(&owner, &room).await?;
	let event = send_message(&owner, &room).await?;
	let topic = services
		.state_accessor
		.room_state_get_id(&room, &StateEventType::RoomTopic, "")
		.await?;

	assert_context_state_contains(&owner, &room, &event, &topic, "healthy").await?;

	let pdu_id = services.timeline.get_pdu_id(&topic).await?;
	let pdus = &services.db["pduid_pdu"];
	let saved = pdus.get(&pdu_id).await?.to_vec();
	pdus.remove(&pdu_id).await?;
	services.clear_cache().await;

	assert_context_failure(&owner, &room, &event, &topic, "corrupt state").await?;

	pdus.raw_put(&pdu_id, &saved).await?;
	services.clear_cache().await;
	assert_context_state_contains(&owner, &room, &event, &topic, "restored PDU").await?;

	let shorteventid = services.short.get_shorteventid(&event).await?;
	let key = shorteventid.to_be_bytes();
	let event_states = &services.db["shorteventid_shortstatehash"];
	let saved = event_states.get(&key).await?.to_vec();
	event_states.remove(&key).await?;
	services.clear_cache().await;

	assert_context_hidden(&owner, &room, &event, &topic, "missing event state").await?;

	event_states.raw_put(&key, &saved).await?;
	services.clear_cache().await;
	assert_context_state_contains(&owner, &room, &event, &topic, "restored event state").await
}

async fn set_topic(client: &Client<'_>, room: &RoomId) -> Result {
	client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/state/m.room.topic")))
		.bearer_auth(client.token)
		.json(&json!({ "topic": "context completeness anchor" }))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn send_message(client: &Client<'_>, room: &RoomId) -> Result<OwnedEventId> {
	let response: Value = client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/context-state")))
		.bearer_auth(client.token)
		.json(&json!({ "msgtype": "m.text", "body": "context state anchor" }))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;

	response
		.get("event_id")
		.and_then(Value::as_str)
		.expect("send response must contain event_id")
		.try_into()
		.map_err(Into::into)
}

async fn assert_context_state_contains(
	client: &Client<'_>,
	room: &RoomId,
	event: &OwnedEventId,
	state_event: &OwnedEventId,
	phase: &str,
) -> Result {
	let (status, body) = context_response(client, room, event).await?;
	assert_eq!(status, 200, "{phase}: {body}");

	let response: Value = serde_json::from_str(&body)?;
	let state = response
		.get("state")
		.and_then(Value::as_array)
		.expect("context response must contain a state array");
	assert!(
		state.iter().any(|entry| {
			entry.get("event_id").and_then(Value::as_str) == Some(state_event.as_str())
		}),
		"{phase}: context state omitted {state_event}: {body}"
	);

	Ok(())
}

async fn assert_context_failure(
	client: &Client<'_>,
	room: &RoomId,
	event: &OwnedEventId,
	state_event: &OwnedEventId,
	phase: &str,
) -> Result {
	let (status, body) = context_response(client, room, event).await?;
	assert_eq!(status, 500, "{phase}: {body}");

	let error: Value = serde_json::from_str(&body)?;
	assert_eq!(
		error.get("errcode").and_then(Value::as_str),
		Some("M_UNKNOWN"),
		"{phase}: {body}"
	);
	assert!(
		!body.contains(state_event.as_str()),
		"{phase} exposed the corrupt event: {body}"
	);
	assert!(!body.contains("pduid_pdu"), "{phase} exposed a map name: {body}");

	Ok(())
}

async fn assert_context_hidden(
	client: &Client<'_>,
	room: &RoomId,
	event: &OwnedEventId,
	state_event: &OwnedEventId,
	phase: &str,
) -> Result {
	let (status, body) = context_response(client, room, event).await?;
	assert_eq!(status, 404, "{phase}: {body}");

	let error: Value = serde_json::from_str(&body)?;
	assert_eq!(
		error.get("errcode").and_then(Value::as_str),
		Some("M_NOT_FOUND"),
		"{phase}: {body}"
	);
	assert!(
		!body.contains(state_event.as_str()),
		"{phase} exposed the current-state event: {body}"
	);
	assert!(!body.contains("pduid_pdu"), "{phase} exposed a map name: {body}");

	Ok(())
}

async fn context_response(
	client: &Client<'_>,
	room: &RoomId,
	event: &OwnedEventId,
) -> Result<(u16, String)> {
	let response = client
		.services
		.client
		.clients
		.default
		.get(client.url(&format!("rooms/{room}/context/{event}")))
		.bearer_auth(client.token)
		.send()
		.await?;
	let status = response.status().as_u16();
	let body = response.text().await?;

	Ok((status, body))
}
