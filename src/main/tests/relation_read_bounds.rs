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
	ruma::{EventId, OwnedEventId, RoomId, api::Direction},
};
use tuwunel_service::Services;

use self::client::{Client, poll_until, register, wait_until_ready};

const TOKEN: &str = "disposable-relation-owner-access-token";

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

struct Fixture<'a> {
	client: Client<'a>,
	room: &'a RoomId,
	root: &'a EventId,
	root_id: RawPduId,
	child: &'a EventId,
	child_id: RawPduId,
}

#[test]
fn relation_reads_refuse_corruption_and_bound_complete_scans() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path =
		DatabasePath(temp_dir().join(format!("tuwunel-relation-read-{}-{port}", process_id())));
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
				.map_err(|_| err!("relation-read fixture exceeded its deadline"))
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

fn key(parent: PduCount, child: PduCount) -> Vec<u8> {
	[parent.to_be_bytes(), child.to_be_bytes()].concat()
}

async fn send(
	client: &Client<'_>,
	room: &RoomId,
	txn: &str,
	content: Value,
) -> Result<OwnedEventId> {
	let body: Value = client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/{txn}")))
		.bearer_auth(client.token)
		.json(&content)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	Ok(body["event_id"]
		.as_str()
		.expect("send must return an event ID")
		.try_into()?)
}

impl Fixture<'_> {
	async fn request(&self, query: &[(&str, &str)], expected: http::StatusCode) -> Result<Value> {
		let response = self
			.client
			.services
			.client
			.clients
			.default
			.get(format!(
				"{}/_matrix/client/v1/rooms/{}/relations/{}",
				self.client.base, self.room, self.root
			))
			.bearer_auth(self.client.token)
			.query(query)
			.send()
			.await?;
		let status = response.status();
		let body: Value = response.json().await?;
		assert_eq!(status, expected, "relation response: {body}");
		if !expected.is_success() {
			assert!(body.get("chunk").is_none(), "errors cannot contain a partial relation page");
		}
		Ok(body)
	}

	async fn healthy(&self) -> Result {
		let body = self
			.request(&[("dir", "f")], http::StatusCode::OK)
			.await?;
		assert_eq!(
			body["chunk"]
				.as_array()
				.expect("relation chunk")
				.len(),
			2,
			"both direct children must survive"
		);
		let body = self
			.request(&[("dir", "f"), ("recurse", "true")], http::StatusCode::OK)
			.await?;
		assert_eq!(
			body["chunk"]
				.as_array()
				.expect("recursive chunk")
				.len(),
			3,
			"recursive child must survive"
		);
		Ok(())
	}

	async fn refused(&self, expected: http::StatusCode) -> Result {
		let services = self.client.services;
		let root = services.db["pduid_pdu"]
			.get(&self.root_id)
			.await?
			.to_vec();
		let child = services.db["pduid_pdu"]
			.get(&self.child_id)
			.await?
			.to_vec();
		self.request(&[("dir", "f"), ("limit", "1")], expected)
			.await?;
		let error = services
			.pdu_metadata
			.event_has_relation(self.root, None, None, None)
			.await
			.expect_err("complete relation predicate must refuse an invalid tail");
		assert_eq!(error.status_code(), expected, "predicate preserves the error classification");
		assert_eq!(
			services.db["pduid_pdu"]
				.get(&self.root_id)
				.await?
				.as_ref(),
			root.as_slice(),
			"refusal must preserve the root"
		);
		assert_eq!(
			services.db["pduid_pdu"]
				.get(&self.child_id)
				.await?
				.as_ref(),
			child.as_slice(),
			"refusal must preserve the child"
		);
		Ok(())
	}

	async fn direct(&self) -> Result<usize> {
		let root: PduId = self.root_id.into();
		Ok(self
			.client
			.services
			.pdu_metadata
			.get_relations(root.shortroomid, root.count, None, Direction::Forward, None)
			.await?
			.len())
	}
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	register(services, "relation-owner", TOKEN).await?;
	let client = Client { services, base, token: TOKEN };
	let room = client
		.create_room(&json!({"preset": "public_chat", "visibility": "private"}))
		.await?;
	let root = send(&client, &room, "root", json!({"msgtype": "m.text", "body": "root"})).await?;
	let first = send(&client, &room, "first", json!({"msgtype": "m.text", "body": "first", "m.relates_to": {"rel_type": "m.reference", "event_id": root}})).await?;
	let child = send(&client, &room, "second", json!({"msgtype": "m.text", "body": "second", "m.relates_to": {"rel_type": "m.reference", "event_id": root}})).await?;
	let nested = send(&client, &room, "nested", json!({"msgtype": "m.text", "body": "nested", "m.relates_to": {"rel_type": "m.reference", "event_id": first}})).await?;
	let root_id = services.timeline.get_pdu_id(&root).await?;
	let first_id = services.timeline.get_pdu_id(&first).await?;
	let child_id = services.timeline.get_pdu_id(&child).await?;
	let nested_id = services.timeline.get_pdu_id(&nested).await?;
	let root_count = root_id.pdu_count();
	assert!(
		poll_until(Duration::from_secs(10), async || {
			services.db["tofrom_relation"]
				.get(key(root_count, child_id.pdu_count()).as_slice())
				.await
				.is_ok() && services.db["tofrom_relation"]
				.get(key(first_id.pdu_count(), nested_id.pdu_count()).as_slice())
				.await
				.is_ok()
		})
		.await,
		"all relation writes must settle before fault injection"
	);
	let fixture = Fixture {
		client,
		room: &room,
		root: &root,
		root_id,
		child: &child,
		child_id,
	};
	fixture.healthy().await?;
	let forward = fixture
		.request(&[("dir", "f"), ("limit", "1")], http::StatusCode::OK)
		.await?;
	assert_eq!(
		forward["chunk"][0]["event_id"],
		first.as_str(),
		"forward page starts with oldest child"
	);
	let backward = fixture
		.request(&[("dir", "b"), ("limit", "1")], http::StatusCode::OK)
		.await?;
	assert_eq!(
		backward["chunk"][0]["event_id"],
		child.as_str(),
		"backward page starts with newest child"
	);
	corrupt_records(&fixture).await?;
	corrupt_children(&fixture).await?;
	row_limits(&fixture).await?;
	byte_limits(&fixture).await?;
	recursive_limits(&fixture, first_id).await?;
	response_limit(&fixture).await?;
	fixture.healthy().await?;
	Ok(())
}

async fn recursive_limits(fixture: &Fixture<'_>, first_id: RawPduId) -> Result {
	let services = fixture.client.services;
	let map = &services.db["tofrom_relation"];
	let parent = first_id.pdu_count();
	let extras: Vec<_> = (0_u64..4094)
		.map(|offset| key(parent, PduCount::Normal(3_000_000_u64.saturating_add(offset))))
		.collect();
	for batch in extras.chunks(64) {
		let mut txn = services.db.txn();
		for key in batch {
			txn.insert_raw(map, key.as_slice(), []);
		}
		txn.execute().await?;
	}
	assert_eq!(fixture.direct().await?, 2, "the root's own inventory remains within budget");
	fixture
		.request(
			&[("dir", "f"), ("recurse", "true"), ("limit", "1")],
			http::StatusCode::TOO_MANY_REQUESTS,
		)
		.await?;
	assert!(
		map.get(extras[0].as_slice()).await?.is_empty(),
		"aggregate recursion refusal must preserve child indexes"
	);
	for batch in extras.chunks(64) {
		let mut txn = services.db.txn();
		for key in batch {
			txn.del_raw(map, key.as_slice());
		}
		txn.execute().await?;
	}
	fixture.healthy().await?;
	Ok(())
}

async fn response_limit(fixture: &Fixture<'_>) -> Result {
	let pdus = &fixture.client.services.db["pduid_pdu"];
	let saved = pdus.get(&fixture.child_id).await?.to_vec();
	let mut body: Value = serde_json::from_slice(&saved)?;
	body["content"]["body"] = json!("x".repeat(256 * 1024));
	let large = serde_json::to_vec(&body)?;
	pdus.raw_put(&fixture.child_id, large.as_slice())
		.await?;
	assert_eq!(
		fixture.direct().await?,
		2,
		"source inventory fits before applying the response bound"
	);
	fixture
		.request(&[("dir", "b"), ("limit", "1")], http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	assert_eq!(
		pdus.get(&fixture.child_id).await?.as_ref(),
		large.as_slice(),
		"response refusal must preserve the event"
	);
	pdus.raw_put(&fixture.child_id, saved.as_slice())
		.await?;
	fixture.healthy().await?;
	Ok(())
}

async fn corrupt_records(fixture: &Fixture<'_>) -> Result {
	let map = &fixture.client.services.db["tofrom_relation"];
	let valid = key(fixture.root_id.pdu_count(), fixture.child_id.pdu_count());
	let mut long = valid.clone();
	long.push(0);
	for invalid in [valid[..8].to_vec(), valid[..15].to_vec(), long] {
		map.raw_put(invalid.as_slice(), []).await?;
		fixture
			.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		assert!(
			map.get(invalid.as_slice()).await?.is_empty(),
			"corrupt index key must remain stored"
		);
		map.remove(invalid.as_slice()).await?;
		fixture.healthy().await?;
	}
	map.raw_put(valid.as_slice(), &[1_u8][..]).await?;
	fixture
		.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	filter_and_write_refusal(fixture).await?;
	assert_eq!(
		map.get(valid.as_slice()).await?.as_ref(),
		&[1_u8],
		"nonempty index value must remain stored"
	);
	map.raw_put(valid.as_slice(), []).await?;
	fixture.healthy().await?;
	Ok(())
}

async fn filter_and_write_refusal(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let filter =
		json!({"types": ["m.room.message"], "related_by_rel_types": ["m.reference"]}).to_string();
	let response = services
		.client
		.clients
		.default
		.get(
			fixture
				.client
				.url(&format!("rooms/{}/messages", fixture.room)),
		)
		.bearer_auth(fixture.client.token)
		.query(&[("dir", "f"), ("from", "0"), ("limit", "1"), ("filter", filter.as_str())])
		.send()
		.await?;
	assert_eq!(
		response.status(),
		http::StatusCode::INTERNAL_SERVER_ERROR,
		"reverse relation filtering must propagate a corrupt inventory"
	);
	let body: Value = response.json().await?;
	assert!(
		body.get("chunk").is_none(),
		"failed filter cannot serve a truncated message page"
	);
	let before = services.globals.current_count();
	let response = services.client.clients.default
		.put(fixture.client.url(&format!("rooms/{}/send/m.reaction/refused-annotation", fixture.room)))
		.bearer_auth(fixture.client.token)
		.json(&json!({"m.relates_to": {"rel_type": "m.annotation", "event_id": fixture.root, "key": "reaction"}}))
		.send().await?;
	assert_eq!(
		response.status(),
		http::StatusCode::INTERNAL_SERVER_ERROR,
		"duplicate-reaction checking must not turn a failed inventory into absence"
	);
	assert_eq!(
		services.globals.current_count(),
		before,
		"refused reaction must not allocate or commit an event"
	);
	Ok(())
}

async fn corrupt_children(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let pdus = &services.db["pduid_pdu"];
	let saved = pdus.get(&fixture.child_id).await?.to_vec();
	let body: Value = serde_json::from_slice(&saved)?;
	let mut wrong_target = body.clone();
	wrong_target["content"]["m.relates_to"]["event_id"] = json!("$unrelated:localhost");
	let mut wrong_room = body.clone();
	wrong_room["room_id"] = json!("!foreign:localhost");
	for invalid in [
		b"{".to_vec(),
		serde_json::to_vec(&wrong_target)?,
		serde_json::to_vec(&wrong_room)?,
	] {
		pdus.raw_put(&fixture.child_id, invalid.as_slice())
			.await?;
		fixture
			.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		pdus.raw_put(&fixture.child_id, saved.as_slice())
			.await?;
		fixture.healthy().await?;
	}
	let reverse = &services.db["eventid_pduid"];
	let saved_reverse = reverse.get(fixture.child).await?.to_vec();
	for invalid in [Vec::new(), fixture.root_id.as_ref().to_vec()] {
		reverse
			.raw_put(fixture.child, invalid.as_slice())
			.await?;
		fixture
			.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		assert_eq!(
			reverse.get(fixture.child).await?.as_ref(),
			invalid.as_slice(),
			"refusal cannot repair a reverse mapping"
		);
		reverse
			.raw_put(fixture.child, saved_reverse.as_slice())
			.await?;
	}
	pdus.remove(&fixture.child_id).await?;
	assert_eq!(fixture.direct().await?, 1, "history purge permits a genuinely missing child");
	pdus.raw_put(&fixture.child_id, saved.as_slice())
		.await?;
	fixture.healthy().await?;
	Ok(())
}

async fn row_limits(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let map = &services.db["tofrom_relation"];
	let parent = fixture.root_id.pdu_count();
	let extras: Vec<_> = (0_u64..4094)
		.map(|offset| key(parent, PduCount::Normal(1_000_000_u64.saturating_add(offset))))
		.collect();
	for batch in extras.chunks(64) {
		let mut txn = services.db.txn();
		for key in batch {
			txn.insert_raw(map, key.as_slice(), []);
		}
		txn.execute().await?;
	}
	assert_eq!(fixture.direct().await?, 2, "exactly 4096 examined rows include purged children");
	let overflow = key(parent, PduCount::Normal(2_000_000));
	map.raw_put(overflow.as_slice(), []).await?;
	fixture
		.refused(http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	assert!(
		map.get(overflow.as_slice()).await?.is_empty(),
		"overflow cannot delete the examined row"
	);
	map.remove(overflow.as_slice()).await?;
	for batch in extras.chunks(64) {
		let mut txn = services.db.txn();
		for key in batch {
			txn.del_raw(map, key.as_slice());
		}
		txn.execute().await?;
	}
	fixture.healthy().await?;
	Ok(())
}

async fn byte_limits(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let pdus = &services.db["pduid_pdu"];
	let saved = pdus.get(&fixture.child_id).await?.to_vec();
	let root_bytes = pdus.get(&fixture.root_id).await?.len();
	let mut body: Value = serde_json::from_slice(&saved)?;
	body["padding"] = json!("");
	let empty_bytes = serde_json::to_vec(&body)?.len();
	// Two index rows, the parent, and the other child are charged first.
	let other = fixture
		.request(&[("dir", "f"), ("limit", "1")], http::StatusCode::OK)
		.await?;
	let first = EventId::parse(
		other["chunk"][0]["event_id"]
			.as_str()
			.expect("first relation ID"),
	)?;
	let first_id = services.timeline.get_pdu_id(&first).await?;
	let first_bytes = pdus.get(&first_id).await?.len();
	let used = 32_usize
		.saturating_add(root_bytes)
		.saturating_add(first_bytes)
		.saturating_add(empty_bytes);
	let padding = (512_usize * 1024)
		.checked_sub(used)
		.expect("valid baseline records must leave byte-budget padding");
	body["padding"] = json!("x".repeat(padding));
	let exact = serde_json::to_vec(&body)?;
	pdus.raw_put(&fixture.child_id, exact.as_slice())
		.await?;
	assert_eq!(fixture.direct().await?, 2, "exact encoded-byte boundary must succeed");
	body["padding"] = json!(
		"x".repeat(
			padding
				.checked_add(1)
				.expect("padding must permit the one-byte overflow control")
		)
	);
	let overflow = serde_json::to_vec(&body)?;
	pdus.raw_put(&fixture.child_id, overflow.as_slice())
		.await?;
	fixture
		.refused(http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	pdus.raw_put(&fixture.child_id, saved.as_slice())
		.await?;
	fixture.healthy().await?;
	Ok(())
}
