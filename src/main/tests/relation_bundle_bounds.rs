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
	matrix::{Event, PduId, RawPduId},
	ruma::{EventId, OwnedEventId, RoomId},
	utils::json::serialized_len,
};
use tuwunel_service::Services;

use self::client::{Client, poll_until, register, wait_until_ready};

const TOKEN: &str = "disposable-relation-bundle-owner-access-token";
const PEER_TOKEN: &str = "disposable-relation-bundle-peer-access-token";

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

struct Child {
	event: OwnedEventId,
	id: RawPduId,
	key: Vec<u8>,
	short: u64,
}

struct Fixture<'a> {
	client: Client<'a>,
	room: &'a RoomId,
	root: &'a EventId,
	root_id: RawPduId,
	children: Vec<Child>,
}

#[test]
fn typed_bundles_refuse_invalid_tails_and_shared_budget_overflow() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path =
		DatabasePath(temp_dir().join(format!("tuwunel-bundle-{}-{port}", process_id())));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={:?}", db_path.0),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		"listening=true".into(),
		"log=\"warn\"".into(),
		"bundle_edit_relations=true".into(),
		"bundle_reference_relations=true".into(),
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
			let outcome = timeout(Duration::from_mins(5), exercise(&services, &base))
				.await
				.map_err(|_| err!("typed bundle fixture exceeded its deadline"))
				.and_then(|result| result);
			outcome.and(server.server.shutdown())
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

async fn send(
	client: &Client<'_>,
	room: &RoomId,
	txn: &str,
	kind: &str,
	content: Value,
) -> Result<OwnedEventId> {
	let response: Value = client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/{kind}/{txn}")))
		.bearer_auth(client.token)
		.json(&content)
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	Ok(response["event_id"]
		.as_str()
		.expect("send event ID")
		.try_into()?)
}

fn key(parent: RawPduId, tag: u8, timestamp: u64, count: u64) -> Vec<u8> {
	let mut key = parent.as_ref().to_vec();
	key.push(tag);
	key.extend(timestamp.to_be_bytes());
	key.extend(count.to_be_bytes());
	key
}

async fn child(
	services: &Services,
	root: RawPduId,
	event: OwnedEventId,
	tag: u8,
) -> Result<Child> {
	let id = services.timeline.get_pdu_id(&event).await?;
	let pdu = services.timeline.get_pdu(&event).await?;
	let short = services.short.get_shorteventid(&event).await?;
	let count: PduId = id.into();
	let key =
		key(root, tag, u64::from(pdu.origin_server_ts().get()), count.count.into_unsigned());
	Ok(Child { event, id, key, short })
}

impl Fixture<'_> {
	async fn get(&self, path: &str, expected: http::StatusCode) -> Result<Value> {
		let response = self
			.client
			.services
			.client
			.clients
			.default
			.get(self.client.url(path))
			.bearer_auth(self.client.token)
			.send()
			.await?;
		let status = response.status();
		let body: Value = response.json().await?;
		assert_eq!(status, expected, "bundle endpoint {path}: {body}");
		if !expected.is_success() {
			assert!(
				body.get("event_id").is_none() && body.get("event").is_none(),
				"refusal cannot serve a partial event"
			);
		}
		Ok(body)
	}

	async fn search(&self, expected: http::StatusCode) -> Result<Value> {
		let response = self.client.services.client.clients.default
			.post(self.client.url("search")).bearer_auth(self.client.token)
			.json(&json!({"search_categories": {"room_events": {"search_term": "bundleanchor", "filter": {"rooms": [self.room]}}}}))
			.send().await?;
		let status = response.status();
		let body: Value = response.json().await?;
		assert_eq!(status, expected, "search bundle response: {body}");
		if expected.is_success() {
			assert!(
				body["search_categories"]["room_events"]["results"]
					.as_array()
					.is_some_and(|results| results.iter().any(|result| result["result"]
						["event_id"]
						.as_str() == Some(
						self.root.as_str()
					))),
				"healthy search returns the bundled parent"
			);
		} else {
			assert!(
				body.get("search_categories").is_none(),
				"search cannot return partial categories"
			);
		}
		Ok(body)
	}

	async fn healthy(&self) -> Result {
		let body = self
			.get(&format!("rooms/{}/event/{}", self.room, self.root), http::StatusCode::OK)
			.await?;
		let relations = &body["unsigned"]["m.relations"];
		assert_eq!(
			relations["m.replace"]["event_id"].as_str(),
			Some(self.children[0].event.as_str()),
			"latest eligible edit survives"
		);
		let references = relations["m.reference"]["chunk"]
			.as_array()
			.expect("reference bundle");
		assert_eq!(references.len(), 2, "both checked references survive");
		assert_eq!(
			references[0]["event_id"].as_str(),
			Some(self.children[1].event.as_str()),
			"reference ordering retains the first child"
		);
		assert_eq!(
			references[1]["event_id"].as_str(),
			Some(self.children[2].event.as_str()),
			"reference ordering retains the second child"
		);
		Ok(())
	}

	async fn refused(&self, expected: http::StatusCode) -> Result {
		let map = &self.client.services.db["pduid_pdu"];
		let root = map.get(&self.root_id).await?.to_vec();
		self.get(&format!("rooms/{}/event/{}", self.room, self.root), expected)
			.await?;
		self.get(&format!("rooms/{}/context/{}?limit=1", self.room, self.root), expected)
			.await?;
		assert_eq!(
			map.get(&self.root_id).await?.as_ref(),
			root.as_slice(),
			"bundle refusal preserves stored parent"
		);
		Ok(())
	}
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	register(services, "bundle-owner", TOKEN).await?;
	let client = Client { services, base, token: TOKEN };
	let room = client
		.create_room(&json!({"preset": "public_chat"}))
		.await?;
	let root = send(
		&client,
		&room,
		"root",
		"m.room.message",
		json!({"msgtype": "m.text", "body": "bundleanchor"}),
	)
	.await?;
	let edit = send(&client, &room, "edit", "m.room.message", json!({"msgtype": "m.text", "body": "edited", "m.new_content": {"msgtype": "m.text", "body": "edited"}, "m.relates_to": {"rel_type": "m.replace", "event_id": root}})).await?;
	let first = send(&client, &room, "first", "m.room.message", json!({"msgtype": "m.text", "body": "first", "m.relates_to": {"rel_type": "m.reference", "event_id": root}})).await?;
	let second = send(&client, &room, "second", "m.room.message", json!({"msgtype": "m.text", "body": "second", "m.relates_to": {"rel_type": "m.reference", "event_id": root}})).await?;
	let root_id = services.timeline.get_pdu_id(&root).await?;
	let children = vec![
		child(services, root_id, edit, 1).await?,
		child(services, root_id, first, 2).await?,
		child(services, root_id, second, 2).await?,
	];
	let fixture = Fixture {
		client,
		room: &room,
		root: &root,
		root_id,
		children,
	};
	assert!(
		poll_until(Duration::from_secs(10), async || {
			for child in &fixture.children {
				if services.db["relatesto_typed"]
					.get(child.key.as_slice())
					.await
					.is_err()
				{
					return false;
				}
			}
			true
		})
		.await,
		"all typed indexes settle before mutation"
	);
	fixture.healthy().await?;
	corrupt_index(&fixture).await?;
	corrupt_child(&fixture).await?;
	purged_child(&fixture).await?;
	shared_rows(&fixture).await?;
	encoded_bytes(&fixture).await?;
	request_error_controls(&fixture).await?;
	thread_identity(&fixture).await?;
	eligibility(&fixture).await?;
	reference_cap(&fixture).await?;
	Ok(())
}

async fn corrupt_index(fixture: &Fixture<'_>) -> Result {
	let map = &fixture.client.services.db["relatesto_typed"];
	let child = &fixture.children[2];
	let mut long = child.key.clone();
	long.push(0);
	for invalid in [child.key[..17].to_vec(), child.key[..32].to_vec(), long] {
		map.raw_put(invalid.as_slice(), child.short.to_be_bytes().as_slice())
			.await?;
		fixture
			.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		assert_eq!(
			map.get(invalid.as_slice()).await?.as_ref(),
			child.short.to_be_bytes().as_slice(),
			"invalid key preserved"
		);
		map.remove(invalid.as_slice()).await?;
		fixture.healthy().await?;
	}
	for invalid in [&[0_u8; 0][..], &[0_u8; 7][..], &[0_u8; 9][..], &[0_u8; 8][..]] {
		map.raw_put(child.key.as_slice(), invalid).await?;
		fixture
			.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		assert_eq!(
			map.get(child.key.as_slice()).await?.as_ref(),
			invalid,
			"invalid value preserved"
		);
		map.raw_put(child.key.as_slice(), child.short.to_be_bytes().as_slice())
			.await?;
		fixture.healthy().await?;
	}
	let invalid = key(fixture.root_id, 2, u64::MAX, 0);
	map.raw_put(invalid.as_slice(), child.short.to_be_bytes().as_slice())
		.await?;
	fixture
		.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	map.remove(invalid.as_slice()).await?;
	fixture.healthy().await?;
	Ok(())
}

async fn corrupt_child(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let child = &fixture.children[2];
	let pdus = &services.db["pduid_pdu"];
	let saved = pdus.get(&child.id).await?.to_vec();
	let stored: Value = serde_json::from_slice(&saved)?;
	let mut target = stored.clone();
	target["content"]["m.relates_to"]["event_id"] = json!(fixture.children[0].event);
	let mut timestamp = stored.clone();
	timestamp["origin_server_ts"] = json!(0);
	let mut room = stored.clone();
	room["room_id"] = json!("!wrong:localhost");
	let mut kind = stored.clone();
	kind["content"]["m.relates_to"]["rel_type"] = json!("m.replace");
	for invalid in [
		b"{".to_vec(),
		serde_json::to_vec(&target)?,
		serde_json::to_vec(&timestamp)?,
		serde_json::to_vec(&room)?,
		serde_json::to_vec(&kind)?,
	] {
		pdus.raw_put(&child.id, invalid.as_slice())
			.await?;
		fixture
			.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		assert_eq!(
			pdus.get(&child.id).await?.as_ref(),
			invalid.as_slice(),
			"invalid child preserved"
		);
		pdus.raw_put(&child.id, saved.as_slice()).await?;
		fixture.healthy().await?;
	}
	let forward = &services.db["eventid_shorteventid"];
	for invalid in [&[0_u8; 7][..], &[0_u8; 8][..]] {
		forward.raw_put(&child.event, invalid).await?;
		fixture
			.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		assert_eq!(
			forward.get(&child.event).await?.as_ref(),
			invalid,
			"invalid compact binding preserved"
		);
		forward
			.raw_put(&child.event, child.short.to_be_bytes().as_slice())
			.await?;
		fixture.healthy().await?;
	}
	let reverse = &services.db["shorteventid_eventid"];
	let saved_reverse = reverse
		.get(&child.short.to_be_bytes())
		.await?
		.to_vec();
	reverse
		.raw_put(child.short.to_be_bytes().as_slice(), b"invalid".as_slice())
		.await?;
	fixture
		.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	reverse
		.raw_put(child.short.to_be_bytes().as_slice(), saved_reverse.as_slice())
		.await?;
	fixture.healthy().await?;
	Ok(())
}

async fn purged_child(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let child = &fixture.children[2];
	let pdus = &services.db["pduid_pdu"];
	let accepted = &services.db["eventid_pduid"];
	let saved = pdus.get(&child.id).await?.to_vec();
	let saved_accepted = accepted.get(&child.event).await?.to_vec();
	pdus.remove(&child.id).await?;
	fixture
		.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	accepted.remove(&child.event).await?;
	let body = fixture
		.get(&format!("rooms/{}/event/{}", fixture.room, fixture.root), http::StatusCode::OK)
		.await?;
	assert_eq!(
		body["unsigned"]["m.relations"]["m.reference"]["chunk"]
			.as_array()
			.expect("purged reference bundle")
			.len(),
		1,
		"genuinely purged child is omitted"
	);
	assert!(
		services.db["relatesto_typed"]
			.get(child.key.as_slice())
			.await
			.is_ok(),
		"purged child index is preserved"
	);
	pdus.raw_put(&child.id, saved.as_slice()).await?;
	accepted
		.raw_put(&child.event, saved_accepted.as_slice())
		.await?;
	fixture.healthy().await?;
	Ok(())
}

async fn shared_rows(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let map = &services.db["relatesto_typed"];
	let purged: OwnedEventId = "$purgedbundle:localhost".try_into()?;
	let short = services
		.short
		.get_or_create_shorteventid(&purged)
		.await?;
	let keys: Vec<_> = (1_000_000_u64..1_004_093)
		.map(|count| key(fixture.root_id, 2, 0, count))
		.collect();
	for batch in keys.chunks(64) {
		let mut txn = services.db.txn();
		for key in batch {
			txn.insert_raw(map, key.as_slice(), short.to_be_bytes());
		}
		txn.execute().await?;
	}
	fixture.healthy().await?;
	let overflow = key(fixture.root_id, 2, 0, 1_004_093);
	map.raw_put(overflow.as_slice(), short.to_be_bytes().as_slice())
		.await?;
	fixture
		.refused(http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	assert_eq!(
		map.get(overflow.as_slice()).await?.as_ref(),
		short.to_be_bytes().as_slice(),
		"overflow index preserved"
	);
	map.remove(overflow.as_slice()).await?;
	for batch in keys.chunks(64) {
		let mut txn = services.db.txn();
		for key in batch {
			txn.del_raw(map, key.as_slice());
		}
		txn.execute().await?;
	}
	fixture.healthy().await?;
	Ok(())
}

async fn encoded_bytes(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let root = services.timeline.get_pdu(fixture.root).await?;
	let user = root.sender().to_owned();
	let pdus = &services.db["pduid_pdu"];
	let mut charged = serialized_len(root.as_pdu())?.saturating_add(
		pdus.get(&fixture.root_id)
			.await?
			.len()
			.saturating_mul(2),
	);
	for child in &fixture.children {
		charged = charged
			.saturating_add(41)
			.saturating_add(8)
			.saturating_add(
				services.db["shorteventid_eventid"]
					.get(&child.short.to_be_bytes())
					.await?
					.len(),
			)
			.saturating_add(pdus.get(&child.id).await?.len());
	}
	let child = &fixture.children[2];
	let saved = pdus.get(&child.id).await?.to_vec();
	let padding = (512_usize * 1024)
		.checked_sub(charged)
		.expect("fixture baseline below byte limit");
	let mut exact = saved.clone();
	exact.resize(saved.len().saturating_add(padding), b' ');
	pdus.raw_put(&child.id, exact.as_slice()).await?;
	services
		.pdu_metadata
		.bundle_aggregations(&user, root.clone())
		.await?;
	exact.push(b' ');
	pdus.raw_put(&child.id, exact.as_slice()).await?;
	let error = services
		.pdu_metadata
		.bundle_aggregations(&user, root)
		.await
		.expect_err("one encoded byte over shared limit refuses");
	assert_eq!(
		error.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS,
		"encoded byte overflow classification"
	);
	assert_eq!(
		pdus.get(&child.id).await?.as_ref(),
		exact.as_slice(),
		"oversized child preserved"
	);
	pdus.raw_put(&child.id, saved.as_slice()).await?;
	fixture.healthy().await?;
	Ok(())
}

async fn request_error_controls(fixture: &Fixture<'_>) -> Result {
	fixture.search(http::StatusCode::OK).await?;
	let child = &fixture.children[2];
	let map = &fixture.client.services.db["relatesto_typed"];
	map.raw_put(child.key.as_slice(), &[0_u8; 7][..])
		.await?;
	fixture
		.search(http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	let body = fixture
		.get("sync?timeout=0&full_state=true", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	assert!(body.get("next_batch").is_none(), "bundle failure cannot advance sync cursor");
	assert!(body.get("rooms").is_none(), "bundle failure cannot return partial rooms");
	map.raw_put(child.key.as_slice(), child.short.to_be_bytes().as_slice())
		.await?;
	fixture.healthy().await?;
	fixture.search(http::StatusCode::OK).await?;
	Ok(())
}

async fn eligibility(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let peer = register(services, "bundle-peer", PEER_TOKEN).await?;
	let client = Client {
		services,
		base: fixture.client.base,
		token: PEER_TOKEN,
	};
	client
		.services
		.client
		.clients
		.default
		.post(client.url(&format!("join/{}", fixture.room)))
		.bearer_auth(client.token)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;
	let wrong_sender = send(&client, fixture.room, "peer-edit", "m.room.message", json!({"msgtype": "m.text", "body": "peer", "m.new_content": {"msgtype": "m.text", "body": "peer"}, "m.relates_to": {"rel_type": "m.replace", "event_id": fixture.root}})).await?;
	let wrong_kind = send(
		&fixture.client,
		fixture.room,
		"wrong-kind",
		"com.example.other",
		json!({"body": "other", "m.relates_to": {"rel_type": "m.replace", "event_id": fixture.root}}),
	)
	.await?;
	for event in [wrong_sender, wrong_kind] {
		let child = child(services, fixture.root_id, event, 1).await?;
		assert!(
			poll_until(Duration::from_secs(10), async || services.db["relatesto_typed"]
				.get(child.key.as_slice())
				.await
				.is_ok())
			.await,
			"non-applicable typed edit settles"
		);
		fixture.healthy().await?;
	}
	let newer = send(&fixture.client, fixture.room, "newer-eligible", "m.room.message",
		json!({"msgtype": "m.text", "body": "newest", "m.new_content": {"msgtype": "m.text", "body": "newest"}, "m.relates_to": {"rel_type": "m.replace", "event_id": fixture.root}})).await?;
	let newest = child(services, fixture.root_id, newer, 1).await?;
	assert!(
		poll_until(Duration::from_secs(10), async || services.db["relatesto_typed"]
			.get(newest.key.as_slice())
			.await
			.is_ok())
		.await,
		"newest eligible edit settles"
	);
	let body = fixture
		.get(&format!("rooms/{}/event/{}", fixture.room, fixture.root), http::StatusCode::OK)
		.await?;
	assert_eq!(
		body["unsigned"]["m.relations"]["m.replace"]["event_id"].as_str(),
		Some(newest.event.as_str()),
		"newest eligible edit wins the timestamp/count ordering"
	);
	assert_ne!(
		peer,
		services
			.timeline
			.get_pdu(fixture.root)
			.await?
			.sender()
			.to_owned(),
		"peer eligibility control uses a different sender"
	);
	Ok(())
}

async fn reference_cap(fixture: &Fixture<'_>) -> Result {
	for number in 0..99 {
		send(&fixture.client, fixture.room, &format!("capped-{number}"), "m.room.message", json!({"msgtype": "m.text", "body": "capped", "m.relates_to": {"rel_type": "m.reference", "event_id": fixture.root}})).await?;
	}
	let body = fixture
		.get(&format!("rooms/{}/event/{}", fixture.room, fixture.root), http::StatusCode::OK)
		.await?;
	assert_eq!(
		body["unsigned"]["m.relations"]["m.reference"]["chunk"]
			.as_array()
			.expect("capped references")
			.len(),
		100,
		"valid complete inventory retains served reference cap"
	);
	let invalid = key(fixture.root_id, 2, u64::MAX, 0);
	let map = &fixture.client.services.db["relatesto_typed"];
	map.raw_put(invalid.as_slice(), fixture.children[2].short.to_be_bytes().as_slice())
		.await?;
	fixture
		.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	map.remove(invalid.as_slice()).await?;
	fixture
		.get(&format!("rooms/{}/event/{}", fixture.room, fixture.root), http::StatusCode::OK)
		.await?;
	Ok(())
}

async fn thread_identity(fixture: &Fixture<'_>) -> Result {
	let services = fixture.client.services;
	let reply = send(&fixture.client, fixture.room, "thread-reply", "m.room.message",
		json!({"msgtype": "m.text", "body": "thread", "m.relates_to": {"rel_type": "m.thread", "event_id": fixture.root, "is_falling_back": true, "m.in_reply_to": {"event_id": fixture.root}}})).await?;
	let map = &services.db["pduid_pdu"];
	assert!(
		poll_until(Duration::from_secs(10), async || {
			let Ok(value) = map.get(&fixture.root_id).await else {
				return false;
			};
			let Ok(value) = serde_json::from_slice::<Value>(&value) else {
				return false;
			};
			value["unsigned"]["m.relations"]["m.thread"]["latest_event"]["event_id"].as_str()
				== Some(reply.as_str())
		})
		.await,
		"thread bundle settles before identity checks"
	);
	fixture.healthy().await?;
	let saved = map.get(&fixture.root_id).await?.to_vec();
	let stored: Value = serde_json::from_slice(&saved)?;
	let mut malformed = stored.clone();
	malformed["unsigned"]["m.relations"]["m.thread"]["latest_event"] = json!({});
	let mut missing = stored.clone();
	missing["unsigned"]["m.relations"]["m.thread"]["latest_event"]["event_id"] =
		json!("$missingbundle:localhost");
	let mut sender = stored;
	sender["unsigned"]["m.relations"]["m.thread"]["latest_event"]["sender"] =
		json!("@wrong:localhost");
	for invalid in [malformed, missing, sender] {
		let invalid = serde_json::to_vec(&invalid)?;
		map.raw_put(&fixture.root_id, invalid.as_slice())
			.await?;
		fixture
			.refused(http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		assert_eq!(
			map.get(&fixture.root_id).await?.as_ref(),
			invalid.as_slice(),
			"invalid latest identity preserved"
		);
		map.raw_put(&fixture.root_id, saved.as_slice())
			.await?;
		fixture.healthy().await?;
	}
	Ok(())
}
