#![cfg(test)]

use std::{env::var, net::TcpListener, path::PathBuf, process::id as process_id, time::Duration};

use futures::{StreamExt, future::join};
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, PduEvent, Result, err,
	matrix::{RoomVersionRules, room_version},
	ruma::{
		OwnedEventId, OwnedRoomId, RoomId, UserId,
		events::{StateEventType, TimelineEventType},
	},
};
use tuwunel_service::{Services, rooms::state_res::StateMap, users::Register};

#[test]
fn corrupt_power_levels_cannot_relax_event_authorization() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();

	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path =
		PathBuf::from(root).join(format!("tuwunel-current-state-auth-{}", process_id()));

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

	let owner = UserId::parse_with_server_name("auth-owner", services.globals.server_name())?;
	let member = UserId::parse_with_server_name("auth-member", services.globals.server_name())?;
	let owner_token = "current-state-auth-owner-token-00000000";
	let member_token = "current-state-auth-member-token-00000000";

	register(services, &owner, owner_token).await?;
	register(services, &member, member_token).await?;

	let room = create_public_room(services, base, owner_token).await?;
	join_room(services, base, member_token, &room).await?;
	require_power_for_messages(services, base, owner_token, &room).await?;
	// Keep the frontier readable so a missing prev event cannot mask the auth bug.
	services
		.client
		.clients
		.default
		.put(format!("{base}/_matrix/client/v3/rooms/{room}/state/m.room.topic"))
		.bearer_auth(owner_token)
		.json(&json!({ "topic": "readable frontier" }))
		.send()
		.await?
		.error_for_status()?;

	let healthy = send_message(services, base, member_token, &room, "healthy-denial").await?;
	assert_eq!(
		healthy.0, 403,
		"configured power levels must deny the low-power member: {}",
		healthy.1
	);
	assert_all_state_routes_succeed(services, base, member_token, &room, "healthy").await?;

	let version = services.state.get_room_version(&room).await?;
	let rules = room_version::rules(&version)?;
	verify_corruptions(services, base, member_token, &room, &member, &rules).await?;

	let unknown = RoomId::parse("!new-auth-room:localhost")?;
	if read_auth(services, &unknown, &owner, &rules)
		.await
		.is_ok()
	{
		return Err!("auth loading unexpectedly accepted a room without state");
	}
	let content = serde_json::value::to_raw_value(&json!({}))?;
	assert!(
		services
			.state
			.get_auth_events(
				&unknown,
				&TimelineEventType::RoomCreate,
				&owner,
				Some(""),
				&content,
				&rules.authorization,
				true
			)
			.await?
			.is_empty()
	);
	// A not-yet-joined sender is legitimately absent from this room's snapshot.
	let outsider =
		UserId::parse_with_server_name("not-a-member", services.globals.server_name())?;
	let auth = read_auth(services, &room, &outsider, &rules).await?;
	assert!(!auth.contains_key(&(StateEventType::RoomMember, outsider.as_str().into())));

	Ok(())
}

struct CorruptionContext<'a> {
	services: &'a Services,
	base: &'a str,
	member_token: &'a str,
	room: &'a RoomId,
	member: &'a UserId,
	rules: &'a RoomVersionRules,
}

async fn verify_corruptions(
	services: &Services,
	base: &str,
	member_token: &str,
	room: &RoomId,
	member: &UserId,
	rules: &RoomVersionRules,
) -> Result {
	let context = CorruptionContext {
		services,
		base,
		member_token,
		room,
		member,
		rules,
	};
	let power_levels = context
		.services
		.state_accessor
		.room_state_get_id(context.room, &StateEventType::RoomPowerLevels, "")
		.await?;

	verify_missing_or_malformed_storage(&context, &power_levels).await?;
	let restored = send_message(
		context.services,
		context.base,
		context.member_token,
		context.room,
		"restored-denial",
	)
	.await?;
	assert_eq!(restored.0, 403, "restoring storage must restore the original denial");
	verify_mismatched_state_events(&context, &power_levels).await?;
	verify_forward_mapping_recovery(&context, &power_levels).await
}

async fn verify_missing_or_malformed_storage(
	context: &CorruptionContext<'_>,
	power_levels: &OwnedEventId,
) -> Result {
	let before = forward_extremities(context.services, context.room).await;
	let pdu_id = context
		.services
		.timeline
		.get_pdu_id(power_levels)
		.await?;
	let shorteventid = context
		.services
		.short
		.get_shorteventid(power_levels)
		.await?;
	let shortstatekey = context
		.services
		.short
		.get_shortstatekey(&StateEventType::RoomPowerLevels, "")
		.await?;
	let state = context
		.services
		.state
		.get_room_shortstatehash(context.room)
		.await?;
	let targets = [
		("pduid_pdu", pdu_id.as_ref().to_vec(), true),
		("shorteventid_eventid", shorteventid.to_be_bytes().to_vec(), true),
		("shortstatekey_statekey", shortstatekey.to_be_bytes().to_vec(), true),
		("shortstatehash_statediff", state.to_be_bytes().to_vec(), false),
		("roomid_shortstatehash", context.room.as_bytes().to_vec(), false),
	];
	for (name, key, check_http) in targets {
		let map = &context.services.db[name];
		let saved = map.get(&key).await?.to_vec();
		for malformed in [false, true] {
			assert!(
				!read_auth(context.services, context.room, context.member, context.rules)
					.await?
					.is_empty()
			);
			if malformed {
				map.raw_put(&key, b"{").await?;
			} else {
				map.remove(&key).await?;
			}
			context.services.clear_cache().await;
			if read_auth(context.services, context.room, context.member, context.rules)
				.await
				.is_ok()
			{
				return Err!("{name}/{malformed}: auth loading succeeded with incomplete state");
			}
			if check_http {
				let transaction = format!("corrupt-{name}-{malformed}");
				let response = send_message(
					context.services,
					context.base,
					context.member_token,
					context.room,
					&transaction,
				)
				.await?;
				assert_eq!(response.0, 500, "{name}/{malformed}: {}", response.1);
				let body: Value = serde_json::from_str(&response.1)?;
				assert_eq!(body.get("errcode").and_then(Value::as_str), Some("M_UNKNOWN"));
				assert!(!response.1.contains(power_levels.as_str()));
				assert!(!response.1.contains(name));
			}
			let label = format!("{name}/{malformed}");
			assert_all_state_routes_fail(
				context.services,
				context.base,
				context.member_token,
				context.room,
				power_levels,
				&label,
				name,
			)
			.await?;
			assert_eq!(
				forward_extremities(context.services, context.room).await,
				before,
				"{name}/{malformed}: failed auth loading appended an event"
			);
			map.raw_put(&key, &saved).await?;
			context.services.clear_cache().await;
			assert!(
				!read_auth(context.services, context.room, context.member, context.rules)
					.await?
					.is_empty()
			);
		}
	}
	let map = &context.services.db["shortstatehash_statediff"];
	let key = state.to_be_bytes();
	let saved = map.get(&key).await?.to_vec();
	for (name, malformed) in
		[("truncated-event", vec![0_u8; 9]), ("empty-removed-run", vec![0_u8; 16])]
	{
		map.raw_put(&key, &malformed).await?;
		context.services.clear_cache().await;
		if read_auth(context.services, context.room, context.member, context.rules)
			.await
			.is_ok()
		{
			return Err!("{name}: auth loading accepted malformed StateDiff framing");
		}
		assert_eq!(
			forward_extremities(context.services, context.room).await,
			before,
			"{name}: failed auth loading appended an event"
		);
		map.raw_put(&key, &saved).await?;
		context.services.clear_cache().await;
	}

	Ok(())
}

async fn verify_mismatched_state_events(
	context: &CorruptionContext<'_>,
	power_levels: &OwnedEventId,
) -> Result {
	let pdu_id = context
		.services
		.timeline
		.get_pdu_id(power_levels)
		.await?;
	let pdus = &context.services.db["pduid_pdu"];
	let saved = pdus.get(&pdu_id).await?.to_vec();
	for (field, value) in [
		("room_id", "!foreign:localhost"),
		("event_id", "$different:localhost"),
		("type", "m.room.topic"),
		("state_key", "different"),
	] {
		let mut mismatched: Value = serde_json::from_slice(&saved)?;
		mismatched[field] = json!(value);
		pdus.raw_put(&pdu_id, serde_json::to_vec(&mismatched)?)
			.await?;
		context.services.clear_cache().await;
		if read_auth(context.services, context.room, context.member, context.rules)
			.await
			.is_ok()
		{
			return Err!("a mismatched {field} entered the auth map");
		}
		let label = format!("mismatched {field}");
		assert_all_state_routes_fail(
			context.services,
			context.base,
			context.member_token,
			context.room,
			power_levels,
			&label,
			field,
		)
		.await?;
		pdus.raw_put(&pdu_id, &saved).await?;
		context.services.clear_cache().await;
	}

	Ok(())
}

async fn verify_forward_mapping_recovery(
	context: &CorruptionContext<'_>,
	power_levels: &OwnedEventId,
) -> Result {
	// A missing forward dictionary row is not proof that the state cell is absent.
	let key = tuwunel_database::serialize_key((&StateEventType::RoomPowerLevels, ""))?;
	let map = &context.services.db["statekey_shortstatekey"];
	let saved = map.get(&key).await?.to_vec();
	map.remove(&key).await?;
	context.services.clear_cache().await;
	let auth = read_auth(context.services, context.room, context.member, context.rules).await?;
	assert_eq!(
		auth.get(&(StateEventType::RoomPowerLevels, "".into()))
			.map(|pdu| &pdu.event_id),
		Some(power_levels)
	);
	assert_all_state_routes_succeed(
		context.services,
		context.base,
		context.member_token,
		context.room,
		"a missing forward dictionary row",
	)
	.await?;
	map.raw_put(&key, &saved).await?;
	context.services.clear_cache().await;

	Ok(())
}

async fn read_auth(
	services: &Services,
	room: &RoomId,
	sender: &UserId,
	rules: &RoomVersionRules,
) -> Result<StateMap<PduEvent>> {
	let content = serde_json::value::to_raw_value(&json!({"msgtype":"m.text", "body":"probe"}))?;
	services
		.state
		.get_auth_events(
			room,
			&TimelineEventType::RoomMessage,
			sender,
			None,
			&content,
			&rules.authorization,
			true,
		)
		.await
}

async fn register(services: &Services, user_id: &UserId, token: &str) -> Result {
	services
		.users
		.full_register(Register {
			user_id: Some(user_id),
			password: Some("current-state-auth-password"),
			..Default::default()
		})
		.await?;

	services
		.users
		.create_device(user_id, None, (Some(token), None), None, None, None)
		.await?;

	Ok(())
}

async fn wait_until_ready(services: &Services, base: &str) -> Result {
	let url = format!("{base}/_matrix/client/versions");

	timeout(Duration::from_secs(10), async {
		loop {
			if services
				.client
				.clients
				.default
				.get(&url)
				.send()
				.await
				.is_ok()
			{
				break;
			}

			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| err!("server listener did not become ready"))?;

	Ok(())
}

async fn create_public_room(services: &Services, base: &str, token: &str) -> Result<OwnedRoomId> {
	let response = services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/createRoom"))
		.bearer_auth(token)
		.json(&json!({ "preset": "public_chat" }))
		.send()
		.await?
		.error_for_status()?
		.json::<Value>()
		.await?;

	response
		.get("room_id")
		.and_then(Value::as_str)
		.ok_or_else(|| err!("createRoom response omitted room_id"))?
		.try_into()
		.map_err(Into::into)
}

async fn join_room(services: &Services, base: &str, token: &str, room: &OwnedRoomId) -> Result {
	services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/rooms/{room}/join"))
		.bearer_auth(token)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn require_power_for_messages(
	services: &Services,
	base: &str,
	token: &str,
	room: &OwnedRoomId,
) -> Result {
	let url = format!("{base}/_matrix/client/v3/rooms/{room}/state/m.room.power_levels");
	let mut content = services
		.client
		.clients
		.default
		.get(&url)
		.bearer_auth(token)
		.send()
		.await?
		.error_for_status()?
		.json::<Value>()
		.await?;
	content["events_default"] = json!(50);

	services
		.client
		.clients
		.default
		.put(url)
		.bearer_auth(token)
		.json(&content)
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn send_message(
	services: &Services,
	base: &str,
	token: &str,
	room: &RoomId,
	transaction_id: &str,
) -> Result<(u16, String)> {
	let response = services
		.client
		.clients
		.default
		.put(format!(
			"{base}/_matrix/client/v3/rooms/{room}/send/m.room.message/{transaction_id}"
		))
		.bearer_auth(token)
		.json(&json!({ "msgtype": "m.text", "body": "authorization probe" }))
		.send()
		.await?;

	Ok((response.status().as_u16(), response.text().await?))
}

async fn get_room_endpoint(
	services: &Services,
	base: &str,
	token: &str,
	room: &RoomId,
	endpoint: &str,
) -> Result<(u16, String)> {
	let response = services
		.client
		.clients
		.default
		.get(format!("{base}/_matrix/client/v3/rooms/{room}/{endpoint}"))
		.bearer_auth(token)
		.send()
		.await?;

	Ok((response.status().as_u16(), response.text().await?))
}

async fn assert_all_state_routes_succeed(
	services: &Services,
	base: &str,
	token: &str,
	room: &RoomId,
	label: &str,
) -> Result {
	for endpoint in ["state", "members", "joined_members"] {
		let response = get_room_endpoint(services, base, token, room, endpoint).await?;
		assert_eq!(response.0, 200, "{label} {endpoint}: {}", response.1);
	}

	Ok(())
}

async fn assert_all_state_routes_fail(
	services: &Services,
	base: &str,
	token: &str,
	room: &RoomId,
	power_levels: &OwnedEventId,
	label: &str,
	forbidden: &str,
) -> Result {
	for endpoint in ["state", "members", "joined_members"] {
		let response = get_room_endpoint(services, base, token, room, endpoint).await?;
		assert_eq!(response.0, 500, "{endpoint}/{label}: {}", response.1);
		let body: Value = serde_json::from_str(&response.1)?;
		assert_eq!(body.get("errcode").and_then(Value::as_str), Some("M_UNKNOWN"));
		assert!(!response.1.contains(power_levels.as_str()));
		assert!(!response.1.contains(forbidden));
	}

	Ok(())
}

async fn forward_extremities(services: &Services, room: &RoomId) -> Vec<OwnedEventId> {
	let mut event_ids = services
		.state
		.get_forward_extremities(room)
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>()
		.await;
	event_ids.sort_unstable();
	event_ids
}
