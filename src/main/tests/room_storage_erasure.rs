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
	atomic_refusals(services, &client, &foreign).await
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
