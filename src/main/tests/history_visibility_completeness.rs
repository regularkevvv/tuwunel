#![cfg(test)]

use std::{env::var, net::TcpListener, path::PathBuf, process::id as process_id};

use futures::future::join;
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result,
	pdu::PduBuilder,
	ruma::{
		CanonicalJsonValue, OwnedEventId, RoomId, ServerName, UserId,
		events::{StateEventType, room::message::RoomMessageEventContent},
	},
};
use tuwunel_service::{Services, rooms::short::ShortStateHash};

use self::client::{Client, register, wait_until_ready};

mod client;

const OWNER_TOKEN: &str = "history-visibility-completeness-owner-token";
const FORMER_MEMBER_TOKEN: &str = "history-visibility-completeness-former-member-token";
const SECRET: &str = "history-visibility-before-join-secret";

#[test]
fn corrupt_history_visibility_never_grants_event_access() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path =
		PathBuf::from(root).join(format!("tuwunel-history-visibility-{}", process_id()));

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

	let owner_id = register(services, "historyvisibilityowner", OWNER_TOKEN).await?;
	let former_member_id =
		register(services, "historyvisibilityformer", FORMER_MEMBER_TOKEN).await?;
	let owner = Client { services, base, token: OWNER_TOKEN };
	let former_member = Client {
		services,
		base,
		token: FORMER_MEMBER_TOKEN,
	};
	let room = owner
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;

	set_history_visibility(&owner, &room).await?;
	let event = send_message(&owner, &room).await?;
	join_room(&former_member, &room).await?;
	leave_room(&former_member, &room).await?;
	assert_event_hidden(&former_member, &room, &event, "healthy joined history").await?;
	verify_mismatched_history_mapping(&owner, &former_member, &room, &event).await?;
	verify_foreign_state_hash(services, &owner, &former_member, &room, &event).await?;
	verify_missing_timeline_snapshot(services, &owner, &former_member, &room, &event).await?;
	verify_outlier_uses_current_state(services, &owner, &former_member, &room, &owner_id).await?;
	let create = services
		.state_accessor
		.room_state_get_id(&room, &StateEventType::RoomCreate, "")
		.await?;
	let unjoined_server = ServerName::parse("unjoined.example")?;
	assert!(
		services
			.state
			.pdu_shortstatehash(&create)
			.await
			.is_err(),
		"the initial create event must have no predecessor state"
	);
	assert!(
		services
			.state_accessor
			.user_can_see_event(&former_member_id, &room, &create)
			.await,
		"the initial create event must retain default visibility"
	);
	assert!(
		services
			.state_accessor
			.server_can_see_event(&unjoined_server, &room, &create)
			.await,
		"the initial create event must retain default federation visibility"
	);

	let history_event = services
		.state_accessor
		.room_state_get_id(&room, &StateEventType::RoomHistoryVisibility, "")
		.await?;
	let pdu_id = services
		.timeline
		.get_pdu_id(&history_event)
		.await?;
	let pdus = &services.db["pduid_pdu"];
	let saved = pdus.get(&pdu_id).await?.to_vec();
	pdus.remove(&pdu_id).await?;
	services.clear_cache().await;

	assert!(
		!services
			.state_accessor
			.server_can_see_event(&unjoined_server, &room, &event)
			.await,
		"corrupt history visibility granted federation access"
	);
	assert_event_hidden(&former_member, &room, &event, "corrupt history visibility").await?;

	pdus.raw_put(&pdu_id, &saved).await?;
	services.clear_cache().await;
	assert_event_hidden(&former_member, &room, &event, "restored history visibility").await
}

async fn verify_mismatched_history_mapping(
	owner: &Client<'_>,
	former_member: &Client<'_>,
	room: &RoomId,
	event: &OwnedEventId,
) -> Result {
	let history_shortstatekey = owner
		.services
		.short
		.get_shortstatekey(&StateEventType::RoomHistoryVisibility, "")
		.await?;
	let create_shortstatekey = owner
		.services
		.short
		.get_shortstatekey(&StateEventType::RoomCreate, "")
		.await?;
	assert_ne!(
		history_shortstatekey, create_shortstatekey,
		"fixture requires distinct history and create short state keys"
	);

	let key = tuwunel_database::serialize_key((&StateEventType::RoomHistoryVisibility, ""))?;
	let forward = &owner.services.db["statekey_shortstatekey"];
	let saved = forward.get(&key).await?.to_vec();
	forward
		.raw_put(&key, create_shortstatekey)
		.await?;
	owner.services.clear_cache().await;

	assert_event_visible(owner, room, event, "mismatched forward history key").await?;
	assert_event_hidden(former_member, room, event, "mismatched forward history key").await?;

	forward.raw_put(&key, &saved).await?;
	owner.services.clear_cache().await;

	Ok(())
}

async fn verify_foreign_state_hash(
	services: &Services,
	owner: &Client<'_>,
	former_member: &Client<'_>,
	room: &RoomId,
	event: &OwnedEventId,
) -> Result {
	let foreign_room = owner
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;
	let foreign_state = services
		.state
		.get_room_shortstatehash(&foreign_room)
		.await?;
	let original_state = services.state.pdu_shortstatehash(event).await?;
	assert_ne!(
		foreign_state, original_state,
		"fixture requires a distinct foreign room snapshot"
	);

	let shorteventid = services.short.get_shorteventid(event).await?;
	let key = shorteventid.to_be_bytes();
	let event_states = &services.db["shorteventid_shortstatehash"];
	let saved = event_states.get(&key).await?.to_vec();
	event_states
		.raw_aput::<{ size_of::<ShortStateHash>() }, _, _>(&key, foreign_state)
		.await?;
	services.clear_cache().await;

	assert_event_hidden(owner, room, event, "foreign predecessor state").await?;
	assert_event_hidden(former_member, room, event, "foreign predecessor state").await?;

	event_states.raw_put(&key, &saved).await?;
	services.clear_cache().await;
	assert_eq!(
		services.state.pdu_shortstatehash(event).await?,
		original_state,
		"restored event state hash did not round-trip"
	);

	Ok(())
}

/// A normal timeline event always has a state snapshot, so losing it must not
/// fall back to any other state.
async fn verify_missing_timeline_snapshot(
	services: &Services,
	owner: &Client<'_>,
	former_member: &Client<'_>,
	room: &RoomId,
	event: &OwnedEventId,
) -> Result {
	let shorteventid = services.short.get_shorteventid(event).await?;
	let key = shorteventid.to_be_bytes();
	let event_states = &services.db["shorteventid_shortstatehash"];
	let saved = event_states.get(&key).await?.to_vec();
	event_states.remove(&key).await?;
	services.clear_cache().await;

	assert_event_hidden(owner, room, event, "missing timeline snapshot").await?;
	assert_event_hidden(former_member, room, event, "missing timeline snapshot").await?;

	event_states.raw_put(&key, &saved).await?;
	services.clear_cache().await;
	assert_event_visible(owner, room, event, "restored timeline snapshot").await
}

/// An outlier never had a snapshot, so it is judged by the room's current
/// state: visible to a current member under `joined` history, hidden from a
/// former one.
async fn verify_outlier_uses_current_state(
	services: &Services,
	owner: &Client<'_>,
	former_member: &Client<'_>,
	room: &RoomId,
	owner_id: &UserId,
) -> Result {
	let state_lock = services.state.mutex.lock(room).await;
	let (pdu, mut json) = services
		.timeline
		.create_hash_and_sign_event(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(SECRET)),
			owner_id,
			room,
			&state_lock,
		)
		.await?;
	drop(state_lock);

	json.insert("event_id".into(), CanonicalJsonValue::String(pdu.event_id.as_str().into()));
	services
		.timeline
		.add_pdu_outlier(&pdu.event_id, &json)
		.await?;
	assert!(
		services
			.state
			.pdu_shortstatehash(&pdu.event_id)
			.await
			.is_err(),
		"an outlier must have no state snapshot"
	);

	assert_event_visible(owner, room, &pdu.event_id, "outlier by current state").await?;
	assert_event_hidden(former_member, room, &pdu.event_id, "outlier by current state").await
}

async fn set_history_visibility(client: &Client<'_>, room: &RoomId) -> Result {
	client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/state/m.room.history_visibility")))
		.bearer_auth(client.token)
		.json(&json!({ "history_visibility": "joined" }))
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
		.put(client.url(&format!("rooms/{room}/send/m.room.message/history-visibility")))
		.bearer_auth(client.token)
		.json(&json!({ "msgtype": "m.text", "body": SECRET }))
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

async fn leave_room(client: &Client<'_>, room: &RoomId) -> Result {
	client
		.services
		.client
		.clients
		.default
		.post(client.url(&format!("rooms/{room}/leave")))
		.bearer_auth(client.token)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn assert_event_hidden(
	client: &Client<'_>,
	room: &RoomId,
	event: &OwnedEventId,
	phase: &str,
) -> Result {
	let response = client
		.services
		.client
		.clients
		.default
		.get(client.url(&format!("rooms/{room}/event/{event}")))
		.bearer_auth(client.token)
		.send()
		.await?;
	let status = response.status().as_u16();
	let body = response.text().await?;

	assert_eq!(status, 404, "{phase}: {body}");
	assert!(!body.contains(SECRET), "{phase} exposed the event body: {body}");

	Ok(())
}

async fn assert_event_visible(
	client: &Client<'_>,
	room: &RoomId,
	event: &OwnedEventId,
	phase: &str,
) -> Result {
	let response = client
		.services
		.client
		.clients
		.default
		.get(client.url(&format!("rooms/{room}/event/{event}")))
		.bearer_auth(client.token)
		.send()
		.await?;
	let status = response.status().as_u16();
	let body = response.text().await?;

	assert_eq!(status, 200, "{phase}: {body}");
	assert!(body.contains(SECRET), "{phase} omitted the event body: {body}");

	Ok(())
}
