#![cfg(test)]

//! Actual room deletion protects operator access and erases storage atomically.
//! Every injected commit refusal preserves all inspected table bytes; corrupt
//! or oversized source refuses before user eviction. All databases are owned,
//! disposable fixtures, with real HTTP and no provider credentials.

mod client;

use std::{
	collections::BTreeMap, env::temp_dir, fs::remove_dir_all, net::TcpListener, path::PathBuf,
	process::id, time::Duration,
};

use futures::{TryStreamExt, pin_mut};
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Result, err, ruma::RoomId, utils::hash::sha256};
use tuwunel_database::{Interfix, refusal, serialize_key};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

const TOKEN: &str = "disposable-room-storage-erasure-admin-token";
const MAPS: &[&str] = &[
	"alias_roomid",
	"alias_userid",
	"aliasid_alias",
	"publicroomids",
	"threadid_userids",
	"threadactivityid_rootid",
	"threadrootid_latestcount",
	"tokenids",
	"relatesto_typed",
	"roomid_pduleaves",
	"referencedevents",
	"roomuserid_privateread",
	"roomuserid_lastprivatereadupdate",
	"roomuserid_privatereadsync",
	"readreceiptid_readreceipt",
	"roomuserid_lastnotificationread",
	"roomid_tscount_pducount",
	"pduid_pdu",
	"eventid_pduid",
	"eventid_outlierpdu",
	"roomid_shortroomid",
	"roomid_shortstatehash",
	"roomserverids",
	"serverroomids",
	"roomuserid_joined",
	"userroomid_joined",
	"roomuserid_invitecount",
	"userroomid_invitestate",
	"roomuserid_knockedcount",
	"userroomid_knockedstate",
	"roomuserid_leftcount",
	"userroomid_leftstate",
	"roomid_joinedcount",
	"roomid_invitedcount",
	"roomid_knockedcount",
	"roomid_inviteviaservers",
	"tofrom_relation",
	"softfailedeventids",
	"eventid_policysigstate",
	"eventid_originalpdu",
	"timeredacted_eventid",
	"notificationid_index",
	"useridcount_notification",
];

type Snapshot = BTreeMap<(&'static str, Vec<u8>), sha256::Digest>;

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn room_storage_erasure_is_atomic_and_protects_operator_access() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let directory =
		DatabasePath(temp_dir().join(format!("room-storage-erasure-{}-{port}", id())));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={:?}", directory.0),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		"listening=true".into(),
		"save_unredacted_events=true".into(),
		"log=\"error\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	drop(listener);
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");
		let exercise = async {
			let outcome = exercise(&services, &base).await;
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

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	let user = register(services, "storage-erasure-admin", TOKEN).await?;
	services.admin.make_user_admin(&user).await?;
	let client = Client { services, base, token: TOKEN };
	admin_room_protection(services, &client).await?;
	let foreign = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	unknown_admin_protection(services, &foreign).await?;
	preflight_preservation(services, &client, &foreign).await?;
	atomic_refusals(services, &client, &foreign).await?;
	history_event_atomicity(services, &client).await?;
	retention_expiry_atomicity(services, &foreign).await
}

async fn snapshot(services: &Services, room: &RoomId) -> Result<Snapshot> {
	let mut out = BTreeMap::new();
	for &name in MAPS {
		let rows = services.db[name].raw_stream();
		pin_mut!(rows);
		while let Some((key, value)) = rows.try_next().await? {
			out.insert((name, key.to_vec()), sha256::hash(value));
		}
	}
	for marker in ["membership_recount_pending", "membership_recount_generation_v1"] {
		let key = serialize_key((marker, room))?;
		match services.db["global"].get(&key).await {
			| Ok(value) => {
				out.insert(("global", key.to_vec()), sha256::hash(&value));
			},
			| Err(error) if error.is_not_found() => {},
			| Err(error) => return Err(error),
		}
	}
	Ok(out)
}

async fn admin_room_protection(services: &Services, client: &Client<'_>) -> Result {
	let room = services.admin.get_admin_room().await?;
	let baseline = snapshot(services, &room).await?;
	for purge in [false, true] {
		let response = services
			.client
			.clients
			.default
			.delete(format!("{}/_synapse/admin/v1/rooms/{room}", client.base))
			.bearer_auth(TOKEN)
			.json(&json!({"purge":purge}))
			.send()
			.await?;
		assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
		let body: Value = response.json().await?;
		assert_eq!(body["errcode"], "M_FORBIDDEN");
		assert_eq!(snapshot(services, &room).await?, baseline);
	}
	for force in [false, true] {
		let guard = services.state.mutex.lock(&room).await;
		let error = services
			.delete
			.delete_room(&room, force, guard)
			.await
			.expect_err("force cannot erase the protected admin room");
		assert_eq!(error.status_code(), tuwunel_core::http::StatusCode::FORBIDDEN);
		assert_eq!(snapshot(services, &room).await?, baseline);
	}
	let scheduled: Value = services
		.client
		.clients
		.default
		.delete(format!("{}/_synapse/admin/v2/rooms/{room}", client.base))
		.bearer_auth(TOKEN)
		.json(&json!({"purge":true}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	let delete_id = scheduled["delete_id"]
		.as_str()
		.expect("scheduled task");
	let status = failed_task_status(services, client, delete_id).await?;
	assert_eq!(status["room_id"], room.as_str());
	assert_eq!(snapshot(services, &room).await?, baseline);
	assert_eq!(services.admin.get_admin_room().await?, room);
	assert!(
		services
			.state_cache
			.is_joined_checked(&services.globals.server_user, &room)
			.await?
	);
	Ok(())
}

async fn unknown_admin_protection(services: &Services, room: &RoomId) -> Result {
	let alias = services.admin.admin_alias.alias();
	let original = services.db["alias_roomid"]
		.get(alias)
		.await?
		.to_vec();
	for corrupt in [None, Some(b"invalid-room-id".as_slice())] {
		match corrupt {
			| None => services.db["alias_roomid"].remove(alias).await?,
			| Some(value) =>
				services.db["alias_roomid"]
					.insert(alias, value)
					.await?,
		}
		let baseline = snapshot(services, room).await?;
		let guard = services.state.mutex.lock(room).await;
		services
			.delete
			.shutdown_room(room, &guard)
			.await
			.expect_err("unknown admin protection cannot permit shutdown");
		drop(guard);
		for force in [false, true] {
			let guard = services.state.mutex.lock(room).await;
			services
				.delete
				.delete_room(room, force, guard)
				.await
				.expect_err("unknown admin protection cannot permit deletion");
		}
		assert_eq!(snapshot(services, room).await?, baseline);
		services.db["alias_roomid"]
			.insert(alias, &original)
			.await?;
	}
	services.admin.get_admin_room().await?;
	Ok(())
}

async fn failed_task_status(services: &Services, client: &Client<'_>, id: &str) -> Result<Value> {
	let url = format!("{}/_synapse/admin/v2/rooms/delete_status/{id}", client.base);
	let status = tokio::time::timeout(Duration::from_secs(10), async {
		loop {
			let status: Value = services
				.client
				.clients
				.default
				.get(&url)
				.bearer_auth(TOKEN)
				.send()
				.await?
				.error_for_status()?
				.json()
				.await?;
			if matches!(status["status"].as_str(), Some("failed" | "complete")) {
				return Ok::<Value, tuwunel_core::Error>(status);
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| err!("erasure task exceeded its deadline"))??;
	assert_eq!(status["status"], "failed");
	assert!(status["shutdown_room"].is_null());
	let task = services
		.tasks
		.get(id)
		.await?
		.expect("owned failed task");
	assert_eq!(task.status, tuwunel_service::tasks::Status::Failed);
	assert!(task.result.is_none());
	Ok(status)
}

async fn first_pdu(services: &Services, room: &RoomId) -> Result<(Vec<u8>, Vec<u8>, Value)> {
	let short = services.short.get_shortroomid(room).await?;
	let rows = services.db["pduid_pdu"].stream_prefix_capped::<&[u8], &[u8], _>(&short, 1);
	pin_mut!(rows);
	let (key, value) = rows
		.try_next()
		.await?
		.expect("actual room has PDUs");
	Ok((key.to_vec(), value.to_vec(), serde_json::from_slice(value)?))
}

async fn assert_preflight_refusal(
	services: &Services,
	client: &Client<'_>,
	room: &RoomId,
	expected: reqwest::StatusCode,
) -> Result {
	let before = snapshot(services, room).await?;
	let response = services
		.client
		.clients
		.default
		.delete(format!("{}/_synapse/admin/v1/rooms/{room}", client.base))
		.bearer_auth(TOKEN)
		.json(&json!({"purge":true}))
		.send()
		.await?;
	assert_eq!(response.status(), expected);
	let body: Value = response.json().await?;
	if expected == reqwest::StatusCode::TOO_MANY_REQUESTS {
		assert_eq!(body["errcode"], "M_LIMIT_EXCEEDED");
	}
	assert_eq!(
		snapshot(services, room).await?,
		before,
		"preflight refusal cannot evict users or erase any table bytes"
	);
	services.db["roomid_shortroomid"]
		.get(room)
		.await?;
	Ok(())
}

async fn preflight_preservation(
	services: &Services,
	client: &Client<'_>,
	foreign: &RoomId,
) -> Result {
	let room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	let (key, original, pdu) = first_pdu(services, &room).await?;
	let event = pdu["event_id"].as_str().expect("stored event ID");
	let (foreign_key, ..) = first_pdu(services, foreign).await?;
	let mut foreign_pdu = pdu.clone();
	foreign_pdu["room_id"] = json!(foreign);
	for corrupt in [b"{".to_vec(), serde_json::to_vec(&foreign_pdu)?] {
		services.db["pduid_pdu"]
			.insert(&key, &corrupt)
			.await?;
		assert_preflight_refusal(
			services,
			client,
			&room,
			reqwest::StatusCode::INTERNAL_SERVER_ERROR,
		)
		.await?;
		services.db["pduid_pdu"]
			.insert(&key, &original)
			.await?;
	}
	services.db["eventid_pduid"]
		.insert(event, &foreign_key)
		.await?;
	assert_preflight_refusal(services, client, &room, reqwest::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	services.db["eventid_pduid"].remove(event).await?;
	assert_preflight_refusal(services, client, &room, reqwest::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	services.db["eventid_pduid"]
		.insert(event, &key)
		.await?;
	let short = services.short.get_shortroomid(&room).await?;
	let mut invalid_key = short.to_be_bytes().to_vec();
	invalid_key.extend_from_slice(b"invalid");
	services.db["pduid_pdu"]
		.insert(&invalid_key, &original)
		.await?;
	assert_preflight_refusal(services, client, &room, reqwest::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	services.db["pduid_pdu"]
		.remove(&invalid_key)
		.await?;
	key_budget_preservation(services, client, &room).await?;
	value_budget_preservation(services, client, &room).await?;
	let raw = services.db["roomid_shortroomid"]
		.get(&room)
		.await?
		.to_vec();
	services.db["roomid_shortroomid"]
		.insert(&room, b"invalid")
		.await?;
	assert_preflight_refusal(services, client, &room, reqwest::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	services.db["roomid_shortroomid"]
		.insert(&room, raw)
		.await?;
	let response = services
		.client
		.clients
		.default
		.delete(format!("{}/_synapse/admin/v1/rooms/{room}", client.base))
		.bearer_auth(TOKEN)
		.json(&json!({"purge":true}))
		.send()
		.await?;
	assert!(response.status().is_success(), "restored storage can actually be erased");
	assert!(!services.metadata.exists_checked(&room).await?);
	Ok(())
}

async fn key_budget_preservation(
	services: &Services,
	client: &Client<'_>,
	room: &RoomId,
) -> Result {
	let short = services.short.get_shortroomid(room).await?;
	for (count, padding) in [(4097_u64, 0_usize), (100, 6000), (1000, 0)] {
		let mut txn = services.db.txn();
		let mut keys = Vec::new();
		for n in 0..count {
			let mut token = short.to_be_bytes().to_vec();
			token.extend_from_slice(&n.to_be_bytes());
			token.extend(std::iter::repeat_n(b'x', padding));
			txn.insert_raw(&services.db["tokenids"], &token, b"");
			keys.push(token);
		}
		txn.execute().await?;
		assert_preflight_refusal(services, client, room, reqwest::StatusCode::TOO_MANY_REQUESTS)
			.await?;
		let mut txn = services.db.txn();
		for key in keys {
			txn.del_raw(&services.db["tokenids"], key);
		}
		txn.execute().await?;
	}
	Ok(())
}

async fn value_budget_preservation(
	services: &Services,
	client: &Client<'_>,
	room: &RoomId,
) -> Result {
	let (key, original, _) = first_pdu(services, room).await?;
	services.db["pduid_pdu"]
		.insert(&key, vec![b'x'; tuwunel_bridge::MAX_VALUE_BYTES + 1])
		.await?;
	assert_preflight_refusal(services, client, room, reqwest::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	services.db["pduid_pdu"]
		.insert(&key, original)
		.await?;

	let short = services.short.get_shortroomid(room).await?;
	let rows = services.db["pduid_pdu"].stream_prefix_capped::<&[u8], &[u8], _>(&short, 4);
	pin_mut!(rows);
	let mut originals = Vec::new();
	while let Some((key, value)) = rows.try_next().await? {
		originals.push((key.to_vec(), value.to_vec()));
	}
	assert_eq!(originals.len(), 4, "aggregate limit must examine four real PDUs");
	for (key, value) in &originals {
		let mut pdu: Value = serde_json::from_slice(value)?;
		pdu["unsigned"] = json!({"erasure_fixture_padding":"x".repeat(1_100_000)});
		let padded = serde_json::to_vec(&pdu)?;
		assert!(padded.len() < tuwunel_bridge::MAX_VALUE_BYTES);
		let _: tuwunel_core::matrix::pdu::PduEvent = serde_json::from_slice(&padded)?;
		services.db["pduid_pdu"]
			.insert(key, padded)
			.await?;
	}
	assert_preflight_refusal(services, client, room, reqwest::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	for (key, value) in originals {
		services.db["pduid_pdu"]
			.insert(&key, value)
			.await?;
	}
	Ok(())
}

async fn atomic_refusals(services: &Services, client: &Client<'_>, foreign: &RoomId) -> Result {
	let room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	let response = services
		.client
		.clients
		.default
		.delete(format!("{}/_synapse/admin/v1/rooms/{room}", client.base))
		.bearer_auth(TOKEN)
		.json(&json!({"purge":false}))
		.send()
		.await?;
	assert!(response.status().is_success());
	let short = services.short.get_shortroomid(&room).await?;
	let foreign_short = services.short.get_shortroomid(foreign).await?;
	let mut owned = Vec::new();
	let mut txn = services.db.txn();
	for &name in &MAPS[4..17] {
		let short_key = matches!(
			name,
			"threadid_userids"
				| "threadactivityid_rootid"
				| "threadrootid_latestcount"
				| "tokenids" | "relatesto_typed"
		);
		let mut key = if short_key {
			short.to_be_bytes().to_vec()
		} else {
			serialize_key((&room, Interfix))?.to_vec()
		};
		key.extend_from_slice(b"atomic-fixture");
		txn.insert_raw(&services.db[name], &key, b"owned");
		owned.push((name, key));
		let mut foreign_key = if short_key {
			foreign_short.to_be_bytes().to_vec()
		} else {
			serialize_key((foreign, Interfix))?.to_vec()
		};
		foreign_key.extend_from_slice(b"foreign-fixture");
		txn.insert_raw(&services.db[name], &foreign_key, b"foreign");
	}
	txn.execute().await?;
	let baseline = snapshot(services, &room).await?;
	for map in [
		"threadid_userids",
		"tokenids",
		"relatesto_typed",
		"roomid_pduleaves",
		"referencedevents",
		"roomuserid_privateread",
		"roomuserid_lastnotificationread",
		"roomid_joinedcount",
		"roomid_shortstatehash",
		"pduid_pdu",
		"eventid_pduid",
		"eventid_outlierpdu",
		"roomid_shortroomid",
		"global",
	] {
		refusal::refuse_next(map);
		let guard = services.state.mutex.lock(&room).await;
		services
			.delete
			.delete_room(&room, false, guard)
			.await
			.expect_err("any refused map preserves the entire room storage batch");
		assert_eq!(refusal::pending(), 0, "the intended batch must consume the refusal");
		assert_eq!(
			snapshot(services, &room).await?,
			baseline,
			"all inspected bytes survive a refused erasure"
		);
	}
	let response = services
		.client
		.clients
		.default
		.delete(format!("{}/_synapse/admin/v1/rooms/{room}", client.base))
		.bearer_auth(TOKEN)
		.json(&json!({"purge":true}))
		.send()
		.await?;
	assert!(response.status().is_success());
	for (name, key) in owned {
		assert!(
			services.db[name]
				.get(&key)
				.await
				.expect_err("owned room record erased")
				.is_not_found()
		);
	}
	assert!(!services.metadata.exists_checked(&room).await?);
	assert!(services.metadata.exists_checked(foreign).await?);
	// The full-table snapshot compared each foreign sentinel after every refusal;
	// the positive purge must preserve them too.
	let after = snapshot(services, &room).await?;
	for (at, hash) in baseline
		.iter()
		.filter(|((_, key), _)| key.ends_with(b"foreign-fixture"))
	{
		assert_eq!(after.get(at), Some(hash));
	}
	Ok(())
}

/// A real client event plus owned physical relation rows spanning scan pages.
/// Every refused map preserves the PDU, reverse/timestamp bindings, search,
/// metadata and retained original, including unrelated events/rooms.
async fn history_event_atomicity(services: &Services, client: &Client<'_>) -> Result {
	use tuwunel_core::ruma::OwnedEventId;
	let room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	let mut events = Vec::new();
	for (tx, body) in [
		("purged", "visible search words İİİİİİİİİİİİİİİİİİİİİİİİİ"),
		("kept", "boundary retained words"),
	] {
		let response: Value = services
			.client
			.clients
			.default
			.put(client.url(&format!("rooms/{room}/send/m.room.message/{tx}")))
			.bearer_auth(TOKEN)
			.json(&json!({"msgtype":"m.text", "body":body}))
			.send()
			.await?
			.error_for_status()?
			.json()
			.await?;
		events.push(OwnedEventId::try_from(
			response["event_id"]
				.as_str()
				.expect("client event"),
		)?);
	}
	let event = &events[0];
	let raw = services.timeline.get_pdu_id(event).await?;
	let until = services
		.timeline
		.get_pdu_id(&events[1])
		.await?
		.pdu_count();
	let short = services.short.get_shortroomid(&room).await?;
	history_original_atomicity(services, &room, event, &raw, short).await?;
	let relation_keys = history_metadata(services, &room, event, &raw, short).await?;
	let baseline = snapshot(services, &room).await?;
	assert_eq!(
		services
			.timeline
			.purge_history(&room, until, false)
			.await?,
		0
	);
	assert_eq!(snapshot(services, &room).await?, baseline, "local retention unchanged");
	history_refusal_atomicity(services, &room, until, event, &events[1], &relation_keys).await?;
	history_corruption_and_limits(services, &room, until, event, &raw, short).await?;
	assert_eq!(
		services
			.timeline
			.purge_history(&room, until, true)
			.await?,
		1
	);
	assert_eq!(
		services
			.timeline
			.purge_history(&room, until, true)
			.await?,
		0,
		"repeat cannot count deleted event twice"
	);
	services.timeline.get_pdu(&events[1]).await?;
	assert!(
		services
			.timeline
			.get_pdu(event)
			.await
			.expect_err("target PDU erased")
			.is_not_found()
	);
	for (map, key) in relation_keys {
		assert!(
			services.db[map]
				.get(&key)
				.await
				.expect_err("target relations erased")
				.is_not_found()
		);
	}
	for map in [
		"eventid_pduid",
		"eventid_outlierpdu",
		"eventid_originalpdu",
		"softfailedeventids",
		"eventid_policysigstate",
	] {
		assert!(
			services.db[map]
				.get(event)
				.await
				.expect_err("target metadata erased")
				.is_not_found()
		);
	}
	let tokens = services.db["tokenids"]
		.raw_rows_prefix_after(&short.to_be_bytes(), None, 100)
		.await?;
	assert!(
		tokens
			.iter()
			.all(|(key, _)| !key.ends_with(raw.as_ref())),
		"both current and retained-original tokens erased"
	);
	// Expiry retains ownership of the timestamp housekeeping row; purge
	// removes its payload without inventing a retention timestamp.
	let after = snapshot(services, &room).await?;
	for (key, hash) in baseline
		.iter()
		.filter(|((name, _), _)| *name == "timeredacted_eventid")
	{
		assert_eq!(after.get(key), Some(hash));
	}
	Ok(())
}

async fn retention_expiry_atomicity(services: &Services, room: &RoomId) -> Result {
	let mut txn = services.db.txn();
	let mut expired = Vec::new();
	for n in 0..130_u64 {
		let event =
			tuwunel_core::ruma::OwnedEventId::try_from(format!("$retention{n}:example.org"))?;
		let key = serialize_key((1_600_000_000_u64, &event))?;
		txn.insert_raw(
			&services.db["eventid_originalpdu"],
			event.as_bytes(),
			b"owned expired payload",
		);
		txn.insert_raw(&services.db["timeredacted_eventid"], &key, b"");
		expired.push((event, key.to_vec()));
	}
	let future = tuwunel_core::ruma::OwnedEventId::try_from("$retentionFuture:example.org")?;
	txn.insert_raw(
		&services.db["eventid_originalpdu"],
		future.as_bytes(),
		b"foreign future payload",
	);
	txn.put_raw(&services.db["timeredacted_eventid"], (u64::MAX - 1, &future), []);
	txn.execute().await?;
	let baseline = snapshot(services, room).await?;
	for map in ["eventid_originalpdu", "timeredacted_eventid"] {
		refusal::refuse_next(map);
		let error =
			tokio::time::timeout(Duration::from_secs(10), services.retention.expire_originals())
				.await
				.map_err(|_| err!("retention refusal exceeded its deadline"))?
				.expect_err("expiry refuses both rows atomically");
		assert!(error.to_string().contains("refus"));
		assert_eq!(refusal::pending(), 0);
		assert_eq!(snapshot(services, room).await?, baseline);
	}
	assert_eq!(services.retention.expire_originals().await?, 130);

	let outcome = tokio::time::timeout(Duration::from_secs(10), async {
		loop {
			if services.db["timeredacted_eventid"]
				.get(&expired.last().expect("expiry rows").1)
				.await
				.is_err_and(|error| error.is_not_found())
			{
				break;
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		for (event, key) in &expired {
			assert!(
				services.db["eventid_originalpdu"]
					.get(event)
					.await
					.expect_err("expired payload")
					.is_not_found()
			);
			assert!(
				services.db["timeredacted_eventid"]
					.get(key)
					.await
					.expect_err("expired index")
					.is_not_found()
			);
		}
		assert_eq!(
			services.db["eventid_originalpdu"]
				.get(&future)
				.await?
				.as_ref(),
			b"foreign future payload"
		);
		Ok::<(), tuwunel_core::Error>(())
	})
	.await;
	outcome.map_err(|_| err!("retention page cleanup exceeded its deadline"))?
}

async fn history_original_atomicity(
	services: &Services,
	room: &RoomId,
	event: &tuwunel_core::ruma::EventId,
	raw: &tuwunel_core::matrix::pdu::RawPduId,
	short: u64,
) -> Result {
	use tuwunel_core::ruma::CanonicalJsonValue;
	let mut original = services.timeline.get_pdu_json(event).await?;
	original
		.get_mut("content")
		.expect("event content")
		.as_object_mut()
		.expect("content object")
		.insert("body".into(), CanonicalJsonValue::String("original stale words".into()));
	let before_save = snapshot(services, room).await?;
	for map in ["eventid_originalpdu", "timeredacted_eventid"] {
		refusal::refuse_next(map);
		let lock = services.state.mutex.lock(room).await;
		services
			.retention
			.save_original_pdu(event, &original, &lock)
			.await
			.expect_err("retention pair is atomic and propagates refusal");
		drop(lock);
		assert_eq!(refusal::pending(), 0);
		assert_eq!(snapshot(services, room).await?, before_save);
	}
	let lock = services.state.mutex.lock(room).await;
	services
		.retention
		.save_original_pdu(event, &original, &lock)
		.await?;
	drop(lock);
	let saved = snapshot(services, room).await?;
	let lock = services.state.mutex.lock(room).await;
	services
		.retention
		.save_original_pdu(event, &original, &lock)
		.await?;
	drop(lock);
	assert_eq!(snapshot(services, room).await?, saved, "repeat does not extend retention");
	services
		.search
		.index_pdu(short, raw, "original stale words")
		.await?;
	Ok(())
}

async fn history_corruption_and_limits(
	services: &Services,
	room: &RoomId,
	until: tuwunel_core::matrix::pdu::PduCount,
	event: &tuwunel_core::ruma::EventId,
	raw: &tuwunel_core::matrix::pdu::RawPduId,
	short: u64,
) -> Result {
	let stored = services.db["pduid_pdu"].get(raw).await?.to_vec();
	let mut foreign_pdu: Value = serde_json::from_slice(&stored)?;
	foreign_pdu["room_id"] = json!("!foreign:example.org");
	for map in ["pduid_pdu", "eventid_originalpdu"] {
		let key = if map == "pduid_pdu" {
			raw.as_ref()
		} else {
			event.as_bytes()
		};
		let original = services.db[map].get(key).await?.to_vec();
		for value in [b"{".to_vec(), serde_json::to_vec(&foreign_pdu)?] {
			services.db[map].insert(key, value).await?;
			let corrupt = snapshot(services, room).await?;
			services
				.timeline
				.purge_history(room, until, true)
				.await
				.expect_err("corrupt event or original refuses cleanup");
			assert_eq!(snapshot(services, room).await?, corrupt);
		}
		services.db[map].insert(key, original).await?;
	}
	let mut bad_key = short.to_be_bytes().to_vec();
	bad_key.push(0);
	services.db["pduid_pdu"]
		.insert(&bad_key, &stored)
		.await?;
	let corrupt = snapshot(services, room).await?;
	services
		.timeline
		.purge_history(room, until, true)
		.await
		.expect_err("invalid packed key is fallible");
	assert_eq!(snapshot(services, room).await?, corrupt);
	services.db["pduid_pdu"].remove(&bad_key).await?;
	// Large fan-out requires a future journaled multi-batch executor. Until
	// then, refuse atomically rather than losing the event before cleanup.
	let mut txn = services.db.txn();
	let mut large_keys = Vec::new();
	for n in 1000..1650_u64 {
		let mut key = raw.count().to_vec();
		key.extend_from_slice(&n.to_be_bytes());
		txn.insert_raw(&services.db["tofrom_relation"], &key, b"");
		large_keys.push(key);
	}
	txn.execute().await?;
	let large = snapshot(services, room).await?;
	let error = services
		.timeline
		.purge_history(room, until, true)
		.await
		.expect_err("oversized event batch refuses cleanup");
	assert_eq!(error.status_code(), tuwunel_core::http::StatusCode::TOO_MANY_REQUESTS);
	assert_eq!(snapshot(services, room).await?, large);
	let mut txn = services.db.txn();
	for key in large_keys {
		txn.del_raw(&services.db["tofrom_relation"], key);
	}
	txn.execute().await?;
	Ok(())
}

async fn history_metadata(
	services: &Services,
	room: &RoomId,
	event: &tuwunel_core::ruma::EventId,
	raw: &tuwunel_core::matrix::pdu::RawPduId,
	short: u64,
) -> Result<Vec<(&'static str, Vec<u8>)>> {
	let mut txn = services.db.txn();
	let mut relation_keys = Vec::new();
	for n in 1..=130_u64 {
		let mut legacy = raw.count().to_vec();
		legacy.extend_from_slice(&n.to_be_bytes());
		txn.insert_raw(&services.db["tofrom_relation"], &legacy, b"");
		let mut typed = short.to_be_bytes().to_vec();
		typed.extend_from_slice(&raw.count());
		typed.push(1);
		typed.extend_from_slice(&n.to_be_bytes());
		typed.extend_from_slice(&n.to_be_bytes());
		txn.insert_raw(&services.db["relatesto_typed"], &typed, n.to_be_bytes());
		relation_keys.push(("tofrom_relation", legacy));
		relation_keys.push(("relatesto_typed", typed));
	}
	txn.put_raw(&services.db["referencedevents"], (room, event), []);
	for map in ["softfailedeventids", "eventid_policysigstate", "eventid_outlierpdu"] {
		txn.insert_raw(&services.db[map], event.as_bytes(), b"owned event metadata");
	}
	txn.execute().await?;
	Ok(relation_keys)
}

async fn history_refusal_atomicity(
	services: &Services,
	room: &RoomId,
	until: tuwunel_core::matrix::pdu::PduCount,
	event: &tuwunel_core::ruma::EventId,
	boundary: &tuwunel_core::ruma::EventId,
	relation_keys: &[(&'static str, Vec<u8>)],
) -> Result {
	let baseline = snapshot(services, room).await?;
	for map in [
		"pduid_pdu",
		"eventid_pduid",
		"eventid_outlierpdu",
		"roomid_tscount_pducount",
		"tokenids",
		"tofrom_relation",
		"relatesto_typed",
		"referencedevents",
		"softfailedeventids",
		"eventid_policysigstate",
		"eventid_originalpdu",
	] {
		refusal::refuse_next(map);
		services
			.timeline
			.purge_history(room, until, true)
			.await
			.expect_err("refused event cleanup is atomic");
		assert_eq!(refusal::pending(), 0);
		assert_eq!(
			snapshot(services, room).await?,
			baseline,
			"{map} refusal preserves all inspected bytes"
		);
	}
	// Corruption on the last relation scan page cannot erase earlier pages.
	let (map, key) = relation_keys.last().expect("typed row");
	let value = services.db[map].get(key).await?.to_vec();
	services.db[map].insert(key, b"invalid").await?;
	let corrupt = snapshot(services, room).await?;
	services
		.timeline
		.purge_history(room, until, true)
		.await
		.expect_err("bad typed relation refuses entire event");
	assert_eq!(snapshot(services, room).await?, corrupt);
	services.db[map].insert(key, value).await?;
	let binding = services.db["eventid_pduid"]
		.get(event)
		.await?
		.to_vec();
	services.db["eventid_pduid"]
		.insert(event, services.timeline.get_pdu_id(boundary).await?)
		.await?;
	let corrupt = snapshot(services, room).await?;
	services
		.timeline
		.purge_history(room, until, true)
		.await
		.expect_err("foreign reverse binding refuses purge");
	assert_eq!(snapshot(services, room).await?, corrupt);
	services.db["eventid_pduid"]
		.insert(event, binding)
		.await?;
	Ok(())
}
