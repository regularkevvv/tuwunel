#![cfg(test)]

use std::{
	env::var, fs::remove_dir_all, net::TcpListener, path::PathBuf, process::id as process_id,
	time::Duration,
};

use futures::{TryStreamExt, future::join};
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	pdu::PduBuilder,
	ruma::{
		EventId, OwnedEventId, OwnedRoomId, RoomId, UserId,
		api::{
			OutgoingRequest, OutgoingRequestExt,
			federation::{
				authentication::{ServerSignatures, ServerSignaturesInput},
				authorization::get_event_authorization::v1::Request as EventAuthRequest,
				event::{
					get_room_state::v1::Request as StateRequest,
					get_room_state_ids::v1::Request as StateIdsRequest,
				},
				membership::create_join_event::v2::Request as SendJoinRequest,
			},
			path_builder::SinglePath,
		},
		events::{
			StateEventType,
			room::member::{MembershipState, RoomMemberEventContent},
		},
	},
};
use tuwunel_database::keyval::serialize_key;
use tuwunel_service::{Services, users::Register};

const ROUTES: [Route; 3] = [Route::State, Route::StateIds, Route::EventAuth];

#[derive(Clone, Copy, Debug)]
enum Route {
	State,
	StateIds,
	EventAuth,
}

#[derive(Clone, Copy, Debug)]
enum PduFailure {
	Missing,
	Malformed,
}

#[derive(Clone, Copy, Debug)]
enum ServerIndexFailure {
	MissingLocal,
	Malformed,
}

#[test]
fn federation_routes_bind_events_to_rooms() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();

	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path =
		PathBuf::from(root).join(format!("tuwunel-room-event-binding-{}", process_id()));
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
	remove_dir_all(&db_path).ok();

	result
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let user_id = UserId::parse_with_server_name("binding", services.globals.server_name())?;
	let token = "room-event-binding-regression-token";

	services
		.users
		.full_register(Register {
			user_id: Some(&user_id),
			password: Some("room-event-binding-password"),
			..Default::default()
		})
		.await?;

	services
		.users
		.create_device(&user_id, None, (Some(token), None), None, None, None)
		.await?;

	let room = create_room(services, base, token, None).await?;
	let event = state_event_id(services, &room).await?;
	let room_v12 = create_room(services, base, token, Some("12")).await?;
	let event_v12 = state_event_id(services, &room_v12).await?;
	let create_v12 = create_event_id(services, &room_v12).await?;
	let unknown = EventId::parse("$room-event-binding-unknown")?;

	for route in ROUTES {
		let response = route.get(services, base, &room, &event).await?;

		assert_eq!(response.0, 200, "{route:?}: {}", response.1);

		let foreign = route
			.get(services, base, &room, &event_v12)
			.await?;

		let unknown = route.get(services, base, &room, &unknown).await?;

		assert_eq!(foreign.0, 404);
		assert_eq!(unknown, foreign);

		let response_v12 = route
			.get(services, base, &room_v12, &event_v12)
			.await?;

		assert_eq!(response_v12.0, 200, "{route:?}: {}", response_v12.1);
	}

	let create_response = Route::EventAuth
		.get(services, base, &room_v12, &create_v12)
		.await?;

	assert_eq!(create_response.0, 200, "v12 create event: {}", create_response.1);

	for omit_members in [false, true] {
		send_join_requires_complete_prior_state(services, base, token, &user_id, omit_members)
			.await?;
		for failure in [PduFailure::Missing, PduFailure::Malformed] {
			send_join_rejects_incomplete_response_state(
				services,
				base,
				token,
				&user_id,
				omit_members,
				failure,
			)
			.await?;
		}
	}
	send_join_rejects_missing_statekey_mapping(services, base, token, &user_id).await?;
	for failure in [ServerIndexFailure::MissingLocal, ServerIndexFailure::Malformed] {
		send_join_rejects_incomplete_server_index(services, base, token, &user_id, failure)
			.await?;
	}
	missing_pdu_refuses_partial_responses(services, base, token).await?;
	missing_reverse_mapping_refuses_partial_responses(services, base, token, &user_id).await?;

	Ok(())
}

async fn missing_pdu_refuses_partial_responses(
	services: &Services,
	base: &str,
	token: &str,
) -> Result {
	for failure in [PduFailure::Missing, PduFailure::Malformed] {
		let room = create_room(services, base, token, Some("11")).await?;
		let event = state_event_id(services, &room).await?;
		let power_levels = services
			.state_accessor
			.room_state_get_id(&room, &StateEventType::RoomPowerLevels, "")
			.await?;

		for route in ROUTES {
			let response = route.get(services, base, &room, &event).await?;
			assert_eq!(response.0, 200, "{route:?}: healthy PDU fixture failed: {}", response.1);
		}

		corrupt_timeline_pdu(services, &power_levels, failure).await?;

		for route in ROUTES {
			let response = route.get(services, base, &room, &event).await?;
			assert_internal_failure(
				&response,
				&format!("{route:?}/{failure:?}"),
				power_levels.as_str(),
			);
		}
	}

	Ok(())
}

async fn missing_reverse_mapping_refuses_partial_responses(
	services: &Services,
	base: &str,
	token: &str,
	user_id: &UserId,
) -> Result {
	let room = create_room(services, base, token, Some("11")).await?;
	let event = state_event_id(services, &room).await?;
	let membership = services
		.state_accessor
		.room_state_get_id(&room, &StateEventType::RoomMember, user_id.as_str())
		.await?;
	let short = services
		.short
		.get_shorteventid(&membership)
		.await?;
	let map = &services.db["shorteventid_eventid"];
	map.exists(&short.to_be_bytes()).await?;
	for route in ROUTES {
		let response = route.get(services, base, &room, &event).await?;
		assert_eq!(response.0, 200, "{route:?}: healthy fixture failed: {}", response.1);
	}
	map.remove(&short.to_be_bytes()).await?;
	services.clear_cache().await;
	assert!(
		map.exists(&short.to_be_bytes())
			.await
			.is_err_and(|error| error.is_not_found()),
		"fixture must remove the exact reverse-mapping row"
	);
	let mut statuses = [0; ROUTES.len()];
	for (index, route) in ROUTES.into_iter().enumerate() {
		statuses[index] = route.get(services, base, &room, &event).await?.0;
	}
	assert_eq!(
		statuses,
		[500; ROUTES.len()],
		"missing reverse mappings must not produce partial successful federation responses"
	);
	Ok(())
}

async fn send_join_requires_complete_prior_state(
	services: &Services,
	base: &str,
	token: &str,
	user_id: &UserId,
	omit_members: bool,
) -> Result {
	let room = create_room(services, base, token, Some("11")).await?;
	let request = rejoin_request(services, &room, user_id, omit_members, "healthy").await?;
	let response = send(services, base, request).await?;
	assert_eq!(response.0, 200, "healthy send_join fixture failed: {}", response.1);
	let response: Value = serde_json::from_str(&response.1)?;
	assert!(
		response
			.get("state")
			.and_then(Value::as_array)
			.is_some_and(|state| !state.is_empty()),
		"healthy send_join must contain prior state"
	);
	let request = rejoin_request(services, &room, user_id, omit_members, "missing-state").await?;
	let guest = state_event_id(services, &room).await?;
	let short = services.short.get_shorteventid(&guest).await?;
	let state_before = services
		.state
		.get_room_shortstatehash(&room)
		.await?;
	let reverse = &services.db["shorteventid_eventid"];
	reverse.exists(&short.to_be_bytes()).await?;
	reverse.remove(&short.to_be_bytes()).await?;
	services.clear_cache().await;
	assert!(
		reverse
			.exists(&short.to_be_bytes())
			.await
			.is_err_and(|error| error.is_not_found()),
		"fixture must remove the prior-state reverse row"
	);
	let response = send(services, base, request).await?;
	assert_eq!(response.0, 500, "incomplete prior state must not produce successful send_join");
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(&room)
			.await?,
		state_before,
		"failed prior-state collection must not first commit the join"
	);
	Ok(())
}

async fn send_join_rejects_incomplete_response_state(
	services: &Services,
	base: &str,
	token: &str,
	user_id: &UserId,
	omit_members: bool,
	failure: PduFailure,
) -> Result {
	let room = create_room(services, base, token, Some("11")).await?;
	send_topic(services, base, token, &room).await?;
	let topic = services
		.state_accessor
		.room_state_get_id(&room, &StateEventType::RoomTopic, "test")
		.await?;
	let request = rejoin_request(services, &room, user_id, omit_members, "corrupt-pdu").await?;
	let state_before = services
		.state
		.get_room_shortstatehash(&room)
		.await?;

	corrupt_timeline_pdu(services, &topic, failure).await?;

	let response = send(services, base, request).await?;
	assert_eq!(response.0, 500, "{failure:?}: incomplete response state must fail send_join");
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(&room)
			.await?,
		state_before,
		"{failure:?}: response-state failure must not first commit the join"
	);

	Ok(())
}

async fn send_join_rejects_missing_statekey_mapping(
	services: &Services,
	base: &str,
	token: &str,
	user_id: &UserId,
) -> Result {
	let room = create_room(services, base, token, Some("11")).await?;
	send_topic(services, base, token, &room).await?;
	let request = rejoin_request(services, &room, user_id, true, "missing-statekey").await?;
	let state_before = services
		.state
		.get_room_shortstatehash(&room)
		.await?;
	let short = services
		.short
		.get_shortstatekey(&StateEventType::RoomTopic, "test")
		.await?;
	let reverse = &services.db["shortstatekey_statekey"];
	reverse.exists(&short.to_be_bytes()).await?;
	reverse.remove(&short.to_be_bytes()).await?;
	services.clear_cache().await;
	assert!(
		services
			.short
			.get_statekey_from_short(short)
			.await
			.is_err(),
		"fixture must remove the exact state-key reverse row"
	);

	let response = send(services, base, request).await?;
	assert_eq!(response.0, 500, "incomplete state-key mapping must fail send_join");
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(&room)
			.await?,
		state_before,
		"state-key mapping failure must not first commit the join"
	);

	Ok(())
}

async fn send_join_rejects_incomplete_server_index(
	services: &Services,
	base: &str,
	token: &str,
	user_id: &UserId,
	failure: ServerIndexFailure,
) -> Result {
	let room = create_room(services, base, token, Some("11")).await?;
	let request = rejoin_request(services, &room, user_id, true, "bad-server-index").await?;
	let state_before = services
		.state
		.get_room_shortstatehash(&room)
		.await?;
	let map = &services.db["roomserverids"];
	let room_id: &RoomId = &room;

	match failure {
		| ServerIndexFailure::MissingLocal => {
			let key = serialize_key((room_id, services.globals.server_name()))?;
			map.exists(&key).await?;
			map.remove(&key).await?;
			assert!(
				services
					.state_cache
					.room_servers_fallible(&room)
					.try_collect::<Vec<_>>()
					.await?
					.into_iter()
					.all(|server| server != services.globals.server_name()),
				"fixture must remove the local server membership row"
			);
		},
		| ServerIndexFailure::Malformed => {
			let key = serialize_key((room_id, "not a valid server name"))?;
			map.raw_put(&key, b"").await?;
			assert!(
				services
					.state_cache
					.room_servers_fallible(&room)
					.try_collect::<Vec<_>>()
					.await
					.is_err(),
				"fixture must make the typed server-membership scan fail"
			);
		},
	}

	let response = send(services, base, request).await?;
	assert_eq!(response.0, 500, "{failure:?}: incomplete server index must fail send_join");
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(&room)
			.await?,
		state_before,
		"{failure:?}: server-index failure must not first commit the join"
	);

	Ok(())
}

async fn rejoin_request(
	services: &Services,
	room: &RoomId,
	user: &UserId,
	omit_members: bool,
	label: &str,
) -> Result<SendJoinRequest> {
	let mut content = RoomMemberEventContent::new(MembershipState::Join);
	content.displayname = Some(label.to_owned());
	let builder = PduBuilder::state(user.to_string(), &content);
	let (event, json) = {
		let guard = services.state.mutex.lock(room).await;
		services
			.timeline
			.create_hash_and_sign_event(builder, user, room, &guard)
			.await?
	};
	let version = services.state.get_room_version(room).await?;
	let json = services
		.federation
		.format_pdu_into(json, Some(&version))
		.await;
	let mut request = SendJoinRequest::new(room.to_owned(), event.event_id.clone(), json);
	request.omit_members = omit_members;
	Ok(request)
}

async fn corrupt_timeline_pdu(
	services: &Services,
	event_id: &EventId,
	failure: PduFailure,
) -> Result {
	let pdu_id = services.timeline.get_pdu_id(event_id).await?;
	let pdus = &services.db["pduid_pdu"];
	pdus.exists(&pdu_id).await?;

	match failure {
		| PduFailure::Missing => pdus.remove(&pdu_id).await?,
		| PduFailure::Malformed => pdus.raw_put(&pdu_id, b"{").await?,
	}

	services.clear_cache().await;
	assert!(
		services
			.timeline
			.get_pdu_json(event_id)
			.await
			.is_err(),
		"{failure:?}: corrupt response-state PDU remained readable"
	);

	Ok(())
}

impl Route {
	async fn get(
		self,
		services: &Services,
		base: &str,
		room_id: &RoomId,
		event_id: &EventId,
	) -> Result<(u16, String)> {
		match self {
			| Self::State => {
				let request = StateRequest::new(event_id.to_owned(), room_id.to_owned());

				send(services, base, request).await
			},
			| Self::StateIds => {
				let request = StateIdsRequest::new(event_id.to_owned(), room_id.to_owned());

				send(services, base, request).await
			},
			| Self::EventAuth => {
				let request = EventAuthRequest::new(room_id.to_owned(), event_id.to_owned());

				send(services, base, request).await
			},
		}
	}
}

async fn send<T>(services: &Services, base: &str, request: T) -> Result<(u16, String)>
where
	T: OutgoingRequest<Authentication = ServerSignatures, PathBuilder = SinglePath>,
{
	let server_name = services.globals.server_name().to_owned();
	let auth = ServerSignaturesInput::new(
		server_name.clone(),
		server_name,
		services.server_keys.keypair(),
	);
	let request = request.try_into_http_request::<Vec<u8>>(base, auth, ())?;
	let response = services
		.client
		.clients
		.default
		.execute(request.try_into()?)
		.await?;

	let status = response.status().as_u16();
	let body = response.text().await?;

	Ok((status, body))
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

fn assert_internal_failure(response: &(u16, String), context: &str, hidden: &str) {
	assert_eq!(response.0, 500, "{context}: {}", response.1);
	let body: Value = serde_json::from_str(&response.1)
		.unwrap_or_else(|error| panic!("{context}: invalid Matrix error JSON: {error}"));
	assert_eq!(body.get("errcode").and_then(Value::as_str), Some("M_UNKNOWN"), "{context}");
	assert!(!response.1.contains(hidden), "{context}: response exposed the corrupt event id");
	assert!(!response.1.contains("pduid_pdu"), "{context}: response exposed a map name");
}

async fn create_room(
	services: &Services,
	base: &str,
	token: &str,
	room_version: Option<&str>,
) -> Result<OwnedRoomId> {
	let response = services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/createRoom"))
		.bearer_auth(token)
		.json(&json!({ "room_version": room_version }))
		.send()
		.await?
		.error_for_status()?
		.json::<Value>()
		.await?;

	let room_id = response
		.get("room_id")
		.and_then(Value::as_str)
		.ok_or_else(|| err!("createRoom response omitted room_id"))?;

	Ok(room_id.try_into()?)
}

async fn send_topic(services: &Services, base: &str, token: &str, room_id: &RoomId) -> Result {
	services
		.client
		.clients
		.default
		.put(format!("{base}/_matrix/client/v3/rooms/{room_id}/state/m.room.topic/test"))
		.bearer_auth(token)
		.json(&json!({ "topic": "response completeness" }))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn state_event_id(services: &Services, room_id: &RoomId) -> Result<OwnedEventId> {
	services
		.state_accessor
		.room_state_get_id(room_id, &StateEventType::RoomGuestAccess, "")
		.await
}

async fn create_event_id(services: &Services, room_id: &RoomId) -> Result<OwnedEventId> {
	services
		.state_accessor
		.room_state_get_id(room_id, &StateEventType::RoomCreate, "")
		.await
}
