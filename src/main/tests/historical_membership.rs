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
	matrix::Event,
	ruma::{EventId, OwnedEventId, OwnedRoomId, UserId, events::StateEventType},
};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

const OWNER: &str = "historical-membership-owner-token";
const PEER: &str = "historical-membership-peer-token";
const LATE: &str = "historical-membership-late-token";

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

struct Fixture<'a> {
	client: Client<'a>,
	room: OwnedRoomId,
	peer: &'a UserId,
}
#[test]
fn historical_membership_stays_at_visible_pagination_and_departure_boundaries() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = DatabasePath(
		temp_dir().join(format!("tuwunel-historical-membership-{}-{port}", process_id())),
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
				.map_err(|_| err!("historical membership fixture exceeded its deadline"))
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

impl Fixture<'_> {
	async fn post(&self, path: &str, token: &str, body: &Value) -> Result<Value> {
		Ok(self
			.client
			.services
			.client
			.clients
			.default
			.post(self.client.url(path))
			.bearer_auth(token)
			.json(body)
			.send()
			.await?
			.error_for_status()?
			.json()
			.await?)
	}

	async fn profile(&self, name: &str) -> Result {
		self.client
			.services
			.client
			.clients
			.default
			.put(
				self.client
					.url(&format!("rooms/{}/state/m.room.member/{}", self.room, self.peer)),
			)
			.bearer_auth(PEER)
			.json(&json!({"membership":"join", "displayname":name}))
			.send()
			.await?
			.error_for_status()?;
		Ok(())
	}

	async fn marker(&self, transaction: &str) -> Result<OwnedEventId> {
		let value: Value = self
			.client
			.services
			.client
			.clients
			.default
			.put(
				self.client
					.url(&format!("rooms/{}/send/m.room.message/{transaction}", self.room)),
			)
			.bearer_auth(OWNER)
			.json(&json!({"msgtype":"m.text", "body":transaction}))
			.send()
			.await?
			.error_for_status()?
			.json()
			.await?;
		Ok(value["event_id"]
			.as_str()
			.expect("marker event ID")
			.try_into()?)
	}

	async fn members(
		&self,
		token: &str,
		at: Option<&str>,
		expected: http::StatusCode,
	) -> Result<Value> {
		let request = self
			.client
			.services
			.client
			.clients
			.default
			.get(
				self.client
					.url(&format!("rooms/{}/members", self.room)),
			)
			.bearer_auth(token);
		let request = if let Some(at) = at {
			request.query(&[("at", at)])
		} else {
			request
		};
		let response = request.send().await?;
		let status = response.status();
		let body: Value = response.json().await?;
		assert_eq!(status, expected, "member snapshot: {body}");
		if !expected.is_success() {
			assert!(body.get("chunk").is_none(), "refusal cannot contain a partial snapshot");
		}
		Ok(body)
	}

	async fn historical(&self, at: &str) -> Result {
		for token in [OWNER, PEER] {
			let body = self
				.members(token, Some(at), http::StatusCode::OK)
				.await?;
			assert_eq!(
				body["chunk"]
					.as_array()
					.expect("historical chunk")
					.len(),
				2
			);
			assert_eq!(member(&body, self.peer)["content"]["displayname"], "before");
			assert_eq!(member(&body, self.peer)["content"]["membership"], "join");
		}
		Ok(())
	}

	async fn departed(&self) -> Result {
		let body = self
			.members(PEER, None, http::StatusCode::OK)
			.await?;
		assert_eq!(
			body["chunk"]
				.as_array()
				.expect("departure chunk")
				.len(),
			2
		);
		assert_eq!(member(&body, self.peer)["content"]["membership"], "leave");
		assert_eq!(member(&body, self.peer)["content"]["displayname"], "after");
		Ok(())
	}
}

fn member<'a>(body: &'a Value, user: &UserId) -> &'a Value {
	body["chunk"]
		.as_array()
		.expect("complete member chunk")
		.iter()
		.find(|event| event["state_key"] == user.as_str())
		.expect("expected member")
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	let _owner = register(services, "historical-owner", OWNER).await?;
	let peer = register(services, "historical-peer", PEER).await?;
	let late = register(services, "historical-late", LATE).await?;
	let client = Client { services, base, token: OWNER };
	let room = client
		.create_room(&json!({"preset":"public_chat", "visibility":"private"}))
		.await?;
	let fixture = Fixture { client, room, peer: &peer };
	fixture
		.post(&format!("join/{}", fixture.room), PEER, &json!({}))
		.await?;
	fixture.profile("before").await?;
	let profile_event = services
		.state_accessor
		.room_state_get_id(&fixture.room, &StateEventType::RoomMember, peer.as_str())
		.await?;
	let profile_at = services
		.timeline
		.get_pdu_count(&profile_event)
		.await?
		.to_string();
	fixture.historical(&profile_at).await?;
	let marker = fixture.marker("before-leave").await?;
	let at = services
		.timeline
		.get_pdu_count(&marker)
		.await?
		.to_string();
	fixture.historical(&at).await?;

	sync_boundary(&fixture).await?;

	fixture.profile("after").await?;
	fixture
		.post(&format!("rooms/{}/leave", fixture.room), PEER, &json!({}))
		.await?;
	fixture.departed().await?;
	let departure = services
		.state_cache
		.get_left_count(&fixture.room, &peer)
		.await?
		.to_string();
	let departed_at = fixture
		.members(PEER, Some(&departure), http::StatusCode::OK)
		.await?;
	assert_eq!(
		departed_at["chunk"]
			.as_array()
			.expect("departure-boundary chunk")
			.len(),
		2
	);
	assert_eq!(member(&departed_at, &peer)["content"]["membership"], "leave");
	assert_eq!(member(&departed_at, &peer)["content"]["displayname"], "after");
	fixture
		.post(&format!("join/{}", fixture.room), LATE, &json!({}))
		.await?;
	let later = fixture.marker("after-departure").await?;
	let later_at = services
		.timeline
		.get_pdu_count(&later)
		.await?
		.to_string();
	fixture.departed().await?;
	fixture.historical(&at).await?;
	fixture
		.members(PEER, Some(&later_at), http::StatusCode::FORBIDDEN)
		.await?;
	let current = fixture
		.members(OWNER, None, http::StatusCode::OK)
		.await?;
	assert_eq!(
		current["chunk"]
			.as_array()
			.expect("current chunk")
			.len(),
		3
	);
	assert_eq!(member(&current, &late)["content"]["membership"], "join");

	for token in ["not-a-token", "9223372036854775808", "-9223372036854775809"] {
		fixture
			.members(OWNER, Some(token), http::StatusCode::BAD_REQUEST)
			.await?;
	}
	fixture
		.members(OWNER, Some("0"), http::StatusCode::NOT_FOUND)
		.await?;

	record_failures(&fixture, &marker, &at).await?;

	// A later history-policy change does not move the former user's default
	// snapshot into the present.
	services
		.client
		.clients
		.default
		.put(
			fixture
				.client
				.url(&format!("rooms/{}/state/m.room.history_visibility", fixture.room)),
		)
		.bearer_auth(OWNER)
		.json(&json!({"history_visibility":"joined"}))
		.send()
		.await?
		.error_for_status()?;
	fixture.departed().await?;
	fixture.historical(&at).await?;
	let latest = fixture
		.marker("joined-history-after-departure")
		.await?;
	let latest_at = services
		.timeline
		.get_pdu_count(&latest)
		.await?
		.to_string();
	fixture
		.members(PEER, Some(&latest_at), http::StatusCode::FORBIDDEN)
		.await?;
	// The last source event really is in the target room, not a foreign fixture.
	assert_eq!(
		services
			.timeline
			.get_pdu(&latest)
			.await?
			.room_id(),
		fixture.room
	);
	assert_eq!(
		services
			.state_accessor
			.room_state_get(&fixture.room, &StateEventType::RoomHistoryVisibility, "")
			.await?
			.get_content::<Value>()?["history_visibility"],
		"joined"
	);
	invitation_history(&fixture).await?;
	Ok(())
}

async fn sync_boundary(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	// Use a real prev_batch token produced by sync, rather than only a count
	// obtained directly from storage. With limit one, the boundary is our
	// final message and does not alter membership state.
	let filter = json!({"room":{"rooms":[fixture.room],"timeline":{"limit":1}}});
	let sync: Value = services
		.client
		.clients
		.default
		.get(fixture.client.url("sync"))
		.bearer_auth(OWNER)
		.query(&[("timeout", "0"), ("filter", &filter.to_string())])
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	let prev = sync["rooms"]["join"][fixture.room.as_str()]["timeline"]["prev_batch"]
		.as_str()
		.expect("actual sync prev_batch")
		.to_owned();
	fixture.historical(&prev).await?;

	Ok(())
}

async fn record_failures(fixture: &Fixture<'_>, marker: &EventId, at: &str) -> Result {
	let services = fixture.client.services;
	let peer = fixture.peer;
	// Damaged per-event snapshots must never select current state instead.
	let short_event = services.short.get_shorteventid(marker).await?;
	let key = short_event.to_be_bytes();
	let map = &services.db["shorteventid_shortstatehash"];
	let saved = map.get(&key).await?.to_vec();
	let mut trailing = saved.clone();
	trailing.push(0);
	let mut separator = saved.clone();
	separator.push(0xFF);
	for invalid in [None, Some(vec![b'{']), Some(trailing), Some(separator)] {
		if let Some(invalid) = invalid {
			map.raw_put(&key, &invalid).await?;
		} else {
			map.remove(&key).await?;
		}
		services.clear_cache().await;
		fixture
			.members(OWNER, Some(at), http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		map.raw_put(&key, &saved).await?;
		services.clear_cache().await;
		fixture.historical(at).await?;
	}

	let count_map = &services.db["roomuserid_leftcount"];
	let count_key = (&fixture.room, peer);
	let saved = count_map.qry(&count_key).await?.to_vec();
	let mut trailing = saved.clone();
	trailing.push(0);
	let mut separator = saved.clone();
	separator.push(0xFF);
	for invalid in [None, Some(vec![b'{']), Some(trailing), Some(separator)] {
		if let Some(invalid) = invalid {
			count_map.put_raw(&count_key, &invalid).await?;
		} else {
			count_map.del(&count_key).await?;
		}
		services.clear_cache().await;
		fixture
			.members(PEER, None, http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		count_map.put_raw(&count_key, &saved).await?;
		services.clear_cache().await;
		fixture.departed().await?;
	}

	// The room/event compact IDs also require an exact numeric value. A valid
	// prefix followed by extra bytes or a separator cannot panic or select data.
	for (map_name, key) in [
		("roomid_shortroomid", fixture.room.as_bytes()),
		("eventid_shorteventid", marker.as_bytes()),
	] {
		let map = &services.db[map_name];
		let saved = map.get(key).await?.to_vec();
		for suffix in [0_u8, 0xFF] {
			let mut invalid = saved.clone();
			invalid.push(suffix);
			map.raw_put(key, &invalid).await?;
			services.clear_cache().await;
			fixture
				.members(OWNER, Some(at), http::StatusCode::INTERNAL_SERVER_ERROR)
				.await?;
			map.raw_put(key, &saved).await?;
			services.clear_cache().await;
			fixture.historical(at).await?;
		}
	}

	// A valid state value stored under a foreign room cannot be served as a
	// historical boundary merely because its count fits the request.
	let marker_id = services.timeline.get_pdu_id(marker).await?;
	let event_map = &services.db["pduid_pdu"];
	let saved = event_map.get(&marker_id).await?.to_vec();
	let mut corrupted: Value = serde_json::from_slice(&saved)?;
	corrupted["room_id"] = json!("!foreign-historical-boundary:localhost");
	event_map
		.raw_put(&marker_id, serde_json::to_vec(&corrupted)?)
		.await?;
	services.clear_cache().await;
	fixture
		.members(OWNER, Some(at), http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	event_map.raw_put(&marker_id, &saved).await?;
	services.clear_cache().await;
	fixture.historical(at).await?;
	fixture.departed().await?;

	Ok(())
}

async fn invitation_history(context: &Fixture<'_>) -> Result {
	let services = context.client.services;
	let client = Client {
		services,
		base: context.client.base,
		token: OWNER,
	};
	let room = client
		.create_room(&json!({"preset":"private_chat", "initial_state":[{
			"type":"m.room.history_visibility", "state_key":"", "content":{"history_visibility":"invited"}
		}]}))
		.await?;
	let fixture = Fixture { client, room, peer: context.peer };
	let before = fixture.marker("before-invitation").await?;
	let before_at = services
		.timeline
		.get_pdu_count(&before)
		.await?
		.to_string();
	fixture
		.members(PEER, Some(&before_at), http::StatusCode::FORBIDDEN)
		.await?;
	fixture
		.post(
			&format!("rooms/{}/invite", fixture.room),
			OWNER,
			&json!({"user_id":fixture.peer}),
		)
		.await?;
	let invite = services
		.state_accessor
		.room_state_get_id(&fixture.room, &StateEventType::RoomMember, fixture.peer.as_str())
		.await?;
	let invite_at = services
		.timeline
		.get_pdu_count(&invite)
		.await?
		.to_string();
	let members = fixture
		.members(PEER, Some(&invite_at), http::StatusCode::OK)
		.await?;
	assert_eq!(
		members["chunk"]
			.as_array()
			.expect("invited snapshot")
			.len(),
		2
	);
	assert_eq!(member(&members, fixture.peer)["content"]["membership"], "invite");
	fixture
		.post(&format!("join/{}", fixture.room), PEER, &json!({}))
		.await?;
	// Joining now does not grant state from before an invited-history cutoff.
	fixture
		.members(PEER, Some(&before_at), http::StatusCode::FORBIDDEN)
		.await?;
	services
		.client
		.clients
		.default
		.put(
			fixture
				.client
				.url(&format!("rooms/{}/state/m.room.history_visibility", fixture.room)),
		)
		.bearer_auth(OWNER)
		.json(&json!({"history_visibility":"joined"}))
		.send()
		.await?
		.error_for_status()?;
	let private = fixture.marker("joined-private").await?;
	let private_at = services
		.timeline
		.get_pdu_count(&private)
		.await?
		.to_string();
	fixture
		.members(LATE, Some(&private_at), http::StatusCode::FORBIDDEN)
		.await?;
	services
		.client
		.clients
		.default
		.put(
			fixture
				.client
				.url(&format!("rooms/{}/state/m.room.history_visibility", fixture.room)),
		)
		.bearer_auth(OWNER)
		.json(&json!({"history_visibility":"world_readable"}))
		.send()
		.await?
		.error_for_status()?;
	let public = fixture.marker("world-readable").await?;
	let public_at = services
		.timeline
		.get_pdu_count(&public)
		.await?
		.to_string();
	let members = fixture
		.members(LATE, Some(&public_at), http::StatusCode::OK)
		.await?;
	assert_eq!(
		members["chunk"]
			.as_array()
			.expect("public snapshot")
			.len(),
		2
	);
	// The current public policy does not overwrite a private historical policy.
	fixture
		.members(LATE, Some(&private_at), http::StatusCode::FORBIDDEN)
		.await?;
	Ok(())
}
