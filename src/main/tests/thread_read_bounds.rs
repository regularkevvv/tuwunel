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
	matrix::{PduCount, PduId, RawPduId},
	ruma::{EventId, OwnedEventId, RoomId, UserId, api::error::ErrorKind},
};
use tuwunel_service::Services;

use self::client::{Client, poll_until, register, wait_until_ready};

const TOKEN: &str = "disposable-thread-owner-access-token";
const OTHER: &str = "disposable-thread-observer-access-token";

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

struct Fixture<'a> {
	client: Client<'a>,
	room: &'a RoomId,
	owner: &'a UserId,
	root: &'a EventId,
	root_id: RawPduId,
	activity: RawPduId,
}

#[test]
fn thread_reads_refuse_corruption_and_bound_filtered_scans() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path =
		DatabasePath(temp_dir().join(format!("tuwunel-thread-read-{}-{port}", process_id())));
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
				.map_err(|_| err!("thread-read fixture exceeded its deadline"))
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

async fn read(services: &Services, map: &'static str, key: &[u8]) -> Result<Option<Vec<u8>>> {
	match services.db[map].get(key).await {
		| Ok(value) => Ok(Some(value.to_vec())),
		| Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
		| Err(error) => Err(error),
	}
}

impl Fixture<'_> {
	async fn request(
		&self,
		token: &str,
		include: &str,
		expected: http::StatusCode,
	) -> Result<Value> {
		let response = self
			.client
			.services
			.client
			.clients
			.default
			.get(format!("{}/_matrix/client/v1/rooms/{}/threads", self.client.base, self.room))
			.query(&[("include", include), ("limit", "10")])
			.bearer_auth(token)
			.send()
			.await?;
		let status = response.status();
		let body: Value = response.json().await?;
		assert_eq!(status, expected, "thread response: {body}");
		if !expected.is_success() {
			assert!(body.get("chunk").is_none(), "errors cannot contain a partial thread page");
		}
		Ok(body)
	}

	async fn healthy(&self) -> Result {
		for (token, include, expected_roots) in [
			(TOKEN, "all", 1),
			(TOKEN, "participated", 1),
			(OTHER, "all", 1),
			(OTHER, "participated", 0),
		] {
			let body = self
				.request(token, include, http::StatusCode::OK)
				.await?;
			let roots = body["chunk"]
				.as_array()
				.expect("complete thread page");
			assert_eq!(roots.len(), expected_roots);
			if expected_roots == 1 {
				assert_eq!(roots[0]["event_id"].as_str(), Some(self.root.as_str()));
			}
		}
		Ok(())
	}

	async fn refused(&self, include: &str, expected: http::StatusCode) -> Result {
		let records = [
			("threadactivityid_rootid", self.activity.as_ref()),
			("threadrootid_latestcount", self.root_id.as_ref()),
			("threadid_userids", self.root_id.as_ref()),
			("pduid_pdu", self.root_id.as_ref()),
		];
		let mut before = Vec::new();
		for (map, key) in records {
			before.push(read(self.client.services, map, key).await?);
		}
		self.request(TOKEN, include, expected).await?;
		for ((map, key), expected) in records.into_iter().zip(before) {
			assert_eq!(
				read(self.client.services, map, key).await?,
				expected,
				"thread refusal cannot mutate the stored root or its indexes"
			);
		}
		Ok(())
	}
}

async fn send(
	client: &Client<'_>,
	room: &RoomId,
	transaction: &str,
	content: Value,
) -> Result<OwnedEventId> {
	let body: Value = client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/{transaction}")))
		.bearer_auth(client.token)
		.json(&content)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	Ok(body["event_id"]
		.as_str()
		.expect("committed message ID")
		.try_into()?)
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	let owner = register(services, "thread-owner", TOKEN).await?;
	register(services, "thread-observer", OTHER).await?;
	let client = Client { services, base, token: TOKEN };
	let room = client
		.create_room(&json!({"preset": "public_chat", "visibility": "private"}))
		.await?;
	services
		.client
		.clients
		.default
		.post(client.url(&format!("join/{room}")))
		.bearer_auth(OTHER)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;
	let root = send(&client, &room, "root", json!({"msgtype": "m.text", "body": "root"})).await?;
	let reply = send(
		&client,
		&room,
		"reply",
		json!({
			"msgtype": "m.text", "body": "reply",
			"m.relates_to": {"rel_type": "m.thread", "event_id": root,
				"is_falling_back": true, "m.in_reply_to": {"event_id": root}},
		}),
	)
	.await?;
	let root_id = services.timeline.get_pdu_id(&root).await?;
	let activity = services.timeline.get_pdu_id(&reply).await?;
	assert!(
		poll_until(Duration::from_secs(10), async || {
			services.db["threadrootid_latestcount"]
				.get(&root_id)
				.await
				.is_ok() && services.db["pduid_pdu"]
				.get(&root_id)
				.await
				.is_ok_and(|bytes| {
					serde_json::from_slice::<Value>(&bytes).is_ok_and(|body| {
						body["unsigned"]["m.relations"]["m.thread"]["latest_event"]["event_id"]
							== reply.as_str()
					})
				})
		})
		.await,
		"thread index and bundled root must settle before fault injection"
	);
	let fixture = Fixture {
		client,
		room: &room,
		owner: &owner,
		root: &root,
		root_id,
		activity,
	};
	fixture.healthy().await?;
	assert_eq!(
		services.db["threadid_userids"]
			.get(&root_id)
			.await?
			.as_ref(),
		owner.as_bytes(),
		"repeat participation cannot append duplicate user IDs"
	);
	index_failures(&fixture).await?;
	pointer_failures(&fixture).await?;
	participant_failures(&fixture).await?;
	root_failures(&fixture).await?;
	filtered_scan_limits(&fixture).await?;
	response_limit(&fixture).await
}

async fn index_failures(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let map = &services.db["threadactivityid_rootid"];
	let saved = map.get(&fixture.activity).await?.to_vec();
	let mut invalid_marker = [0_u8; 24];
	invalid_marker[..8].copy_from_slice(&fixture.root_id.shortroomid());
	invalid_marker[8] = 1;
	let foreign: RawPduId = PduId {
		shortroomid: u64::MAX,
		count: PduCount::Normal(1),
	}
	.into();
	for invalid in
		[vec![0_u8], vec![0_u8; 17], invalid_marker.to_vec(), foreign.as_ref().to_vec()]
	{
		map.raw_put(&fixture.activity, &invalid).await?;
		fixture
			.refused("all", http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		map.raw_put(&fixture.activity, &saved).await?;
		fixture.healthy().await?;
	}
	let mut key = fixture.activity.as_ref().to_vec();
	key.push(0);
	map.raw_put(&key, &saved).await?;
	fixture
		.refused("all", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	map.remove(&key).await?;
	fixture.healthy().await?;
	for (size, expected) in [
		(512 * 1024 - 16, http::StatusCode::INTERNAL_SERVER_ERROR),
		(512 * 1024 - 15, http::StatusCode::TOO_MANY_REQUESTS),
	] {
		map.raw_put(&fixture.activity, &vec![0_u8; size])
			.await?;
		fixture.refused("all", expected).await?;
		map.raw_put(&fixture.activity, &saved).await?;
		fixture.healthy().await?;
	}
	Ok(())
}

async fn pointer_failures(fixture: &Fixture<'_>) -> Result {
	let map = &fixture.client.services.db["threadrootid_latestcount"];
	let saved = map.get(&fixture.root_id).await?.to_vec();
	let mut trailing = saved.clone();
	trailing.push(0);
	let mut separator = saved.clone();
	separator.push(0xFF);
	for invalid in [None, Some(vec![0_u8]), Some(trailing), Some(separator)] {
		if let Some(invalid) = invalid {
			map.raw_put(&fixture.root_id, &invalid).await?;
		} else {
			map.remove(&fixture.root_id).await?;
		}
		fixture
			.refused("all", http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		map.raw_put(&fixture.root_id, &saved).await?;
		fixture.healthy().await?;
	}
	Ok(())
}

async fn participant_failures(fixture: &Fixture<'_>) -> Result {
	let map = &fixture.client.services.db["threadid_userids"];
	let saved = map.get(&fixture.root_id).await?.to_vec();
	let mut invalid_tail = fixture.owner.as_bytes().to_vec();
	invalid_tail.extend_from_slice(b"\xFFnot-a-user-id");
	for invalid in [None, Some(vec![0xFE_u8]), Some(invalid_tail)] {
		if let Some(invalid) = invalid {
			map.raw_put(&fixture.root_id, &invalid).await?;
		} else {
			map.remove(&fixture.root_id).await?;
		}
		fixture
			.refused("participated", http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		map.raw_put(&fixture.root_id, &saved).await?;
		fixture.healthy().await?;
	}
	let mut participants = fixture.owner.as_bytes().to_vec();
	for _ in 0..4095 {
		participants.extend_from_slice(b"\xFF@x:l");
	}
	map.raw_put(&fixture.root_id, &participants)
		.await?;
	fixture
		.request(TOKEN, "participated", http::StatusCode::OK)
		.await?;
	participants.extend_from_slice(b"\xFF@x:l");
	map.raw_put(&fixture.root_id, &participants)
		.await?;
	fixture
		.refused("participated", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	let long_user = format!("@{}:localhost", "x".repeat(180));
	UserId::parse(&long_user)?;
	participants = fixture.owner.as_bytes().to_vec();
	for _ in 0..700 {
		participants.push(0xFF);
		participants.extend_from_slice(long_user.as_bytes());
	}
	map.raw_put(&fixture.root_id, &participants)
		.await?;
	fixture
		.refused("participated", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	map.raw_put(&fixture.root_id, &saved).await?;
	fixture.healthy().await
}

async fn root_failures(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let map = &services.db["pduid_pdu"];
	let saved = map.get(&fixture.root_id).await?.to_vec();
	for invalid in [None, Some(vec![b'{'])] {
		if let Some(invalid) = invalid {
			map.raw_put(&fixture.root_id, &invalid).await?;
		} else {
			map.remove(&fixture.root_id).await?;
		}
		services.clear_cache().await;
		fixture
			.refused("all", http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		map.raw_put(&fixture.root_id, &saved).await?;
		services.clear_cache().await;
		fixture.healthy().await?;
	}
	for (field, value) in [
		("room_id", "!foreign-thread:localhost"),
		("event_id", "$foreign-thread:localhost"),
	] {
		let mut invalid: Value = serde_json::from_slice(&saved)?;
		invalid[field] = json!(value);
		map.raw_put(&fixture.root_id, &serde_json::to_vec(&invalid)?)
			.await?;
		services.clear_cache().await;
		fixture
			.refused("all", http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		map.raw_put(&fixture.root_id, &saved).await?;
		services.clear_cache().await;
		fixture.healthy().await?;
	}
	let reverse = &services.db["eventid_pduid"];
	let saved = reverse.get(fixture.root).await?.to_vec();
	reverse.remove(fixture.root).await?;
	services.clear_cache().await;
	fixture
		.refused("all", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	reverse.raw_put(fixture.root, &saved).await?;
	services.clear_cache().await;
	fixture.healthy().await
}

async fn filtered_scan_limits(fixture: &Fixture<'_>) -> Result {
	let map = &fixture.client.services.db["threadactivityid_rootid"];
	let base = fixture
		.activity
		.pdu_count()
		.into_unsigned()
		.saturating_add(100_000);
	let shortroomid = u64::from_be_bytes(fixture.root_id.shortroomid());
	let mut keys = Vec::new();
	for offset in 0..4096 {
		let key: RawPduId = PduId {
			shortroomid,
			count: PduCount::Normal(base.saturating_add(offset)),
		}
		.into();
		map.raw_put(&key, fixture.root_id.as_ref())
			.await?;
		keys.push(key);
		if offset == 4094 {
			fixture
				.request(TOKEN, "all", http::StatusCode::OK)
				.await?;
		}
	}
	fixture
		.refused("all", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	for key in keys {
		map.remove(&key).await?;
	}
	fixture.healthy().await
}

async fn response_limit(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let map = &services.db["pduid_pdu"];
	let saved = map.get(&fixture.root_id).await?.to_vec();
	for (size, expected) in
		[(10_000, http::StatusCode::OK), (300_000, http::StatusCode::TOO_MANY_REQUESTS)]
	{
		let mut value: Value = serde_json::from_slice(&saved)?;
		value["content"]["body"] = json!("x".repeat(size));
		map.raw_put(&fixture.root_id, &serde_json::to_vec(&value)?)
			.await?;
		services.clear_cache().await;
		if expected.is_success() {
			fixture.healthy().await?;
		} else {
			fixture.refused("all", expected).await?;
		}
	}
	map.raw_put(&fixture.root_id, &saved).await?;
	services.clear_cache().await;
	fixture.healthy().await
}
