#![cfg(test)]

use std::{env::var, net::TcpListener, path::PathBuf, process::id as process_id};

use futures::{StreamExt, future::join};
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	matrix::pdu::PduCount,
	ruma::{EventId, RoomId, UserId, events::StateEventType},
};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

mod client;

const OWNER_TOKEN: &str = "sync-v5-e2ee-owner-access-token-0001";
const PEER_TOKEN: &str = "sync-v5-e2ee-peer-access-token-00001";

#[test]
fn corrupt_e2ee_membership_delta_fails_closed() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path = PathBuf::from(root).join(format!("tuwunel-sync-v5-e2ee-{}", process_id()));

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={db_path:?}"),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"log=\"warn\"".to_owned(),
		"device_key_update_encrypted_rooms_only=true".to_owned(),
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

	let owner_user = register(services, "syncv5e2eeowner", OWNER_TOKEN).await?;
	let peer_user = register(services, "syncv5e2eepeer", PEER_TOKEN).await?;
	let owner = Client { services, base, token: OWNER_TOKEN };
	let peer = Client { services, base, token: PEER_TOKEN };
	let room = owner
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;
	set_encryption(&owner, &room).await?;
	send_message(&owner, &room).await?;

	let healthy_since = initial_position(&owner, "healthy-e2ee").await?;
	let corrupt_since = initial_position(&owner, "corrupt-e2ee").await?;
	let restored_since = initial_position(&owner, "restored-e2ee").await?;
	let delta_since = initial_position(&owner, "delta-e2ee").await?;
	let forward_since = initial_position(&owner, "forward-e2ee").await?;

	join_room(&peer, &room).await?;
	let member = services
		.state_accessor
		.room_state_get_id(&room, &StateEventType::RoomMember, peer_user.as_str())
		.await?;
	assert_delta_preconditions(services, &owner_user, &room, &healthy_since, &peer_user, &member)
		.await?;

	assert_e2ee_changed(&owner, "healthy-e2ee", &healthy_since, &peer_user, "healthy").await?;

	let pdu_id = services.timeline.get_pdu_id(&member).await?;
	let pdus = &services.db["pduid_pdu"];
	let saved = pdus.get(&pdu_id).await?.to_vec();
	pdus.remove(&pdu_id).await?;
	services.clear_cache().await;
	let shorteventid = services.short.get_shorteventid(&member).await?;
	assert!(
		services
			.timeline
			.get_pdu_from_shorteventid(shorteventid)
			.await
			.is_err(),
		"test setup did not make the membership PDU unreadable"
	);

	assert_e2ee_failure(&owner, "corrupt-e2ee", &corrupt_since, &peer_user).await?;

	pdus.raw_put(&pdu_id, &saved).await?;
	services.clear_cache().await;
	assert_e2ee_changed(
		&owner,
		"restored-e2ee",
		&restored_since,
		&peer_user,
		"restored membership PDU",
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

	assert_e2ee_failure(&owner, "delta-e2ee", &delta_since, &peer_user).await?;

	statediffs.raw_put(&key, &saved).await?;
	services.clear_cache().await;

	let key = tuwunel_database::serialize_key((&StateEventType::RoomEncryption, ""))?;
	let forward = &services.db["statekey_shortstatekey"];
	let saved = forward.get(&key).await?.to_vec();
	forward.remove(&key).await?;
	services.clear_cache().await;

	assert_e2ee_changed(
		&owner,
		"forward-e2ee",
		&forward_since,
		&peer_user,
		"missing encryption forward key",
	)
	.await?;

	forward.raw_put(&key, &saved).await?;
	services.clear_cache().await;

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

async fn assert_delta_preconditions(
	services: &Services,
	owner_user: &UserId,
	room: &RoomId,
	since: &str,
	peer_user: &UserId,
	member: &EventId,
) -> Result {
	let since: u64 = since.parse()?;
	let previous = services
		.timeline
		.prev_shortstatehash(room, PduCount::Normal(since).saturating_add(1))
		.await?;
	services
		.state_accessor
		.state_get_shortid(previous, &StateEventType::RoomEncryption, "")
		.await?;

	let room_key_changes = services
		.users
		.room_keys_changed(room, since, None)
		.map(|(user_id, _)| user_id.to_owned())
		.collect::<Vec<_>>()
		.await;
	assert!(
		!room_key_changes
			.iter()
			.any(|user_id| user_id == peer_user),
		"fixture has an independent device-key update for {peer_user}: {room_key_changes:?}"
	);

	let current = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let member_shorteventid = services.short.get_shorteventid(member).await?;
	let state_delta = services
		.state_accessor
		.state_added((previous, current))
		.collect::<Vec<_>>()
		.await;
	assert!(
		state_delta
			.iter()
			.any(|(_, shorteventid)| *shorteventid == member_shorteventid),
		"fixture delta does not contain the peer membership event"
	);
	assert!(
		services
			.state_cache
			.get_joined_count(room, owner_user)
			.await? <= since,
		"fixture would trigger the joined-since-last-sync member burst"
	);

	Ok(())
}

async fn set_encryption(client: &Client<'_>, room: &RoomId) -> Result {
	client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/state/m.room.encryption")))
		.bearer_auth(client.token)
		.json(&json!({ "algorithm": "m.megolm.v1.aes-sha2" }))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn send_message(client: &Client<'_>, room: &RoomId) -> Result {
	client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/e2ee-baseline")))
		.bearer_auth(client.token)
		.json(&json!({ "msgtype": "m.text", "body": "E2EE delta baseline" }))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn initial_position(client: &Client<'_>, connection: &str) -> Result<String> {
	let response = e2ee_response(client, connection, None).await?;

	response
		.get("pos")
		.and_then(Value::as_str)
		.map(ToOwned::to_owned)
		.ok_or_else(|| err!("initial E2EE sync omitted a position: {response}"))
}

async fn assert_e2ee_changed(
	client: &Client<'_>,
	connection: &str,
	since: &str,
	peer_user: &UserId,
	phase: &str,
) -> Result {
	let response = e2ee_response(client, connection, Some(since)).await?;
	let changed = response
		.pointer("/extensions/e2ee/device_lists/changed")
		.and_then(Value::as_array)
		.ok_or_else(|| err!("{phase}: E2EE sync omitted device-list changes: {response}"))?;

	assert!(
		changed
			.iter()
			.any(|entry| entry.as_str() == Some(peer_user.as_str())),
		"{phase}: E2EE sync omitted changed peer {peer_user}: {response}"
	);

	Ok(())
}

async fn assert_e2ee_failure(
	client: &Client<'_>,
	connection: &str,
	since: &str,
	peer_user: &UserId,
) -> Result {
	let response = e2ee_request(client, connection, Some(since)).await?;
	let status = response.status();
	let body = response.text().await?;

	assert!(
		!status.is_success(),
		"corrupt E2EE membership delta returned a successful partial response: {body}"
	);
	assert!(
		!body.contains(peer_user.as_str()),
		"corrupt E2EE membership delta exposed the affected user in its error: {body}"
	);

	Ok(())
}

async fn e2ee_response(
	client: &Client<'_>,
	connection: &str,
	since: Option<&str>,
) -> Result<Value> {
	e2ee_request(client, connection, since)
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}

async fn e2ee_request(
	client: &Client<'_>,
	connection: &str,
	since: Option<&str>,
) -> Result<reqwest::Response> {
	let query = since.map_or_else(String::new, |since| format!("?pos={since}"));
	let url = format!(
		"{}/_matrix/client/unstable/org.matrix.simplified_msc3575/sync{query}",
		client.base
	);

	client
		.services
		.client
		.clients
		.default
		.post(url)
		.bearer_auth(client.token)
		.json(&json!({
			"conn_id": connection,
			"extensions": { "e2ee": { "enabled": true } },
		}))
		.send()
		.await
		.map_err(Into::into)
}
