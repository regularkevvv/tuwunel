#![cfg(test)]

//! Complete member erasure inventory, preservation before refusal, and honest
//! propagation of a refused purge step through the actual admin HTTP route.

mod client;

use std::{
	env::temp_dir, fs::remove_dir_all, net::TcpListener, path::PathBuf, process::id,
	time::Duration,
};

use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{RoomAliasId, RoomId, UserId},
};
use tuwunel_database::{Interfix, refusal, serialize_key};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

const TOKEN: &str = "disposable-member-erasure-admin-token";
const PENDING: &str = "membership_recount_pending";
const GENERATION: &str = "membership_recount_generation_v1";

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn corrupt_or_oversized_erasure_preserves_rooms_and_failed_purge_refuses_success() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let directory = DatabasePath(temp_dir().join(format!("membership-erasure-{}-{port}", id())));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={:?}", directory.0),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		"listening=true".into(),
		"delete_rooms_after_leave=true".into(),
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
	let user = register(services, "erasure-admin", TOKEN).await?;
	services.admin.make_user_admin(&user).await?;
	assert!(
		services
			.admin
			.user_is_admin_checked(&user)
			.await?
	);
	let client = Client { services, base, token: TOKEN };
	let room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	services.db["global"]
		.put((PENDING, &room), b"".as_slice())
		.await?;
	let baseline = snapshot(services, &room).await?;
	for map in [
		"roomserverids",
		"roomuserid_invitecount",
		"roomuserid_joined",
		"roomuserid_knockedcount",
		"roomuserid_leftcount",
	] {
		let mut key = serialize_key((&room, Interfix))?.to_vec();
		key.extend_from_slice(b"\xffinvalid");
		services.db[map].insert(&key, b"retained").await?;
		services
			.state_cache
			.delete_room_join_counts(&room, false)
			.await
			.expect_err("invalid member erasure must refuse atomically");
		assert_eq!(snapshot(services, &room).await?, baseline);
		let guard = services.state.mutex.lock(&room).await;
		services
			.delete
			.delete_room(&room, false, guard)
			.await
			.expect_err("preflight must precede user eviction and all purge steps");
		assert_eq!(snapshot(services, &room).await?, baseline);
		assert_eq!(services.db[map].get(&key).await?.as_ref(), b"retained");
		services.db[map].remove(&key).await?;
	}
	for bytes in [false, true] {
		let count = if bytes { 2300 } else { 4097 };
		let mut txn = services.db.txn();
		let mut keys = Vec::new();
		for n in 0..count {
			let suffix = if bytes { "x".repeat(220) } else { String::new() };
			let departed = UserId::parse(format!("@erasure-{n:04}{suffix}:localhost"))?;
			let key = serialize_key((&room, &departed))?.to_vec();
			txn.insert_raw(&services.db["roomuserid_leftcount"], &key, 1_u64.to_be_bytes());
			keys.push(key);
		}
		txn.execute().await?;
		let error = services
			.state_cache
			.delete_room_join_counts(&room, false)
			.await
			.expect_err("retained local departures still charge the complete source budget");
		assert_eq!(error.status_code(), tuwunel_core::http::StatusCode::TOO_MANY_REQUESTS);
		assert!(
			error
				.to_string()
				.contains("Membership erasure inventory limit reached")
		);
		assert_eq!(snapshot(services, &room).await?, baseline);
		assert_eq!(
			services.db["roomuserid_leftcount"]
				.get(&keys[0])
				.await?
				.as_ref(),
			1_u64.to_be_bytes()
		);
		assert_eq!(
			services.db["roomuserid_leftcount"]
				.get(keys.last().expect("owned nonempty inventory"))
				.await?
				.as_ref(),
			1_u64.to_be_bytes()
		);
		let mut txn = services.db.txn();
		for key in keys {
			txn.del_raw(&services.db["roomuserid_leftcount"], key);
		}
		txn.execute().await?;
	}
	refusal::refuse_next("roomid_joinedcount");
	services
		.state_cache
		.delete_room_join_counts(&room, true)
		.await
		.expect_err("armed erasure refuses");
	assert_eq!(refusal::pending(), 0);
	assert_eq!(snapshot(services, &room).await?, baseline);
	exact_row_boundary(services, &room).await?;
	retained_departures(services, &room, &user).await?;
	alias_shutdown_refusal(services, &client, &user).await?;
	purge_http_refusal(services, &client).await?;
	auto_cleanup_refusal(services, &client).await
}

async fn exact_row_boundary(services: &Services, room: &RoomId) -> Result {
	// The live room contributes one joined member and one server key.
	// Another 4,094 retained departure keys complete the exact 4,096-row cap.
	let mut txn = services.db.txn();
	let mut keys = Vec::new();
	for n in 0..4094 {
		let departed = UserId::parse(format!("@erasure-boundary-{n:04}:localhost"))?;
		let key = serialize_key((room, &departed))?.to_vec();
		txn.insert_raw(&services.db["roomuserid_leftcount"], &key, 1_u64.to_be_bytes());
		keys.push(key);
	}
	txn.execute().await?;
	services
		.state_cache
		.delete_room_join_counts(room, false)
		.await?;
	assert!(
		services.db["roomid_joinedcount"]
			.get(room)
			.await
			.expect_err("exact cap permits erasure")
			.is_not_found()
	);
	let mut txn = services.db.txn();
	for key in keys {
		assert_eq!(
			services.db["roomuserid_leftcount"]
				.get(&key)
				.await?
				.as_ref(),
			1_u64.to_be_bytes()
		);
		txn.del_raw(&services.db["roomuserid_leftcount"], key);
	}
	txn.execute().await
}

async fn snapshot(services: &Services, room: &RoomId) -> Result<Vec<Vec<u8>>> {
	let mut snapshot = Vec::new();
	for map in [
		"roomid_joinedcount",
		"roomid_invitedcount",
		"roomid_knockedcount",
		"roomid_shortstatehash",
		"roomid_shortroomid",
	] {
		snapshot.push(services.db[map].get(room).await?.to_vec());
	}
	for namespace in [PENDING, GENERATION] {
		snapshot.push(
			services.db["global"]
				.qry(&(namespace, room))
				.await?
				.to_vec(),
		);
	}
	Ok(snapshot)
}

async fn retained_departures(services: &Services, room: &RoomId, user: &UserId) -> Result {
	let remote = UserId::parse("@departed:remote.example")?;
	for departed in [user, &remote] {
		services.db["roomuserid_leftcount"]
			.put((room, departed), 1_u64)
			.await?;
		services.db["userroomid_leftstate"]
			.put((departed, room), b"preserved".as_slice())
			.await?;
	}
	services
		.state_cache
		.delete_room_join_counts(room, false)
		.await?;
	assert_eq!(
		services.db["userroomid_leftstate"]
			.qry(&(user, room))
			.await?
			.as_ref(),
		b"preserved"
	);
	assert!(
		services.db["userroomid_leftstate"]
			.qry(&(&remote, room))
			.await
			.expect_err("remote departure erased")
			.is_not_found()
	);
	for namespace in [PENDING, GENERATION] {
		assert!(
			services.db["global"]
				.qry(&(namespace, room))
				.await
				.expect_err("erased metadata absent")
				.is_not_found()
		);
	}
	services
		.state_cache
		.delete_room_join_counts(room, true)
		.await?;
	assert!(
		services.db["userroomid_leftstate"]
			.qry(&(user, room))
			.await
			.expect_err("force erases local departure")
			.is_not_found()
	);
	Ok(())
}

async fn alias_shutdown_refusal(
	services: &Services,
	client: &Client<'_>,
	user: &UserId,
) -> Result {
	let room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	let other = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	let first = RoomAliasId::parse("#erasure-first:localhost")?;
	let second = RoomAliasId::parse(format!("#{}:localhost", "b".repeat(210)))?;
	services
		.alias
		.set_alias_by(&first, &room, user)
		.await?;
	services
		.alias
		.set_alias_by(&second, &room, user)
		.await?;
	let aliases = services
		.alias
		.bounded_local_aliases_for_room(&room)
		.await?;
	assert_eq!(aliases.len(), 2);
	refusal::refuse_next("aliasid_alias");
	services
		.alias
		.remove_alias(&first)
		.await
		.expect_err("alias removal is one atomic batch");
	assert_eq!(refusal::pending(), 0);
	assert_eq!(
		services
			.alias
			.bounded_local_aliases_for_room(&room)
			.await?,
		aliases
	);
	assert_eq!(services.alias.resolve_local_alias(&first).await?, room);
	services.db["alias_userid"]
		.get(first.alias())
		.await?;
	services.alias.remove_alias(&first).await?;
	assert_eq!(
		services
			.alias
			.bounded_local_aliases_for_room(&room)
			.await?,
		vec![second.clone()]
	);
	assert_eq!(
		services
			.alias
			.resolve_local_alias(&second)
			.await?,
		room
	);
	services
		.alias
		.set_alias_by(&first, &room, user)
		.await?;
	refusal::refuse_next("alias_roomid");
	services
		.alias
		.set_alias_by(&first, &other, user)
		.await
		.expect_err("replacement cannot split alias indexes");
	assert_eq!(refusal::pending(), 0);
	assert_eq!(services.alias.resolve_local_alias(&first).await?, room);
	assert_eq!(
		services
			.alias
			.bounded_local_aliases_for_room(&room)
			.await?
			.len(),
		2
	);
	assert!(
		services
			.alias
			.bounded_local_aliases_for_room(&other)
			.await?
			.is_empty()
	);
	services
		.alias
		.set_alias_by(&first, &other, user)
		.await?;
	assert_eq!(
		services
			.alias
			.bounded_local_aliases_for_room(&room)
			.await?,
		vec![second.clone()]
	);
	assert_eq!(
		services
			.alias
			.bounded_local_aliases_for_room(&other)
			.await?,
		vec![first.clone()]
	);
	alias_shutdown_inventory(services, client, &room, &first, &second, user).await?;
	alias_publication_refusal(services, client, &room, &second).await?;
	alias_shutdown_boundary(services, client, user).await
}

async fn alias_shutdown_inventory(
	services: &Services,
	client: &Client<'_>,
	room: &RoomId,
	first: &RoomAliasId,
	second: &RoomAliasId,
	user: &UserId,
) -> Result {
	services.db["global"]
		.put((PENDING, room), b"".as_slice())
		.await?;
	let baseline = snapshot(services, room).await?;
	for (suffix, value) in [
		(vec![0xFF], second.as_bytes().to_vec()),
		(8_000_001_u64.to_be_bytes().to_vec(), vec![0xFF]),
		(8_000_002_u64.to_be_bytes().to_vec(), first.as_bytes().to_vec()),
	] {
		let mut key = serialize_key((room, Interfix))?.to_vec();
		key.extend_from_slice(&suffix);
		services.db["aliasid_alias"]
			.insert(&key, &value)
			.await?;
		services
			.alias
			.bounded_local_aliases_for_room(room)
			.await
			.expect_err("malformed or foreign alias refuses complete inventory");
		assert_shutdown_refused(services, client, room, &baseline).await?;
		assert_eq!(
			services.db["aliasid_alias"]
				.get(&key)
				.await?
				.as_ref(),
			value
		);
		services.db["aliasid_alias"].remove(&key).await?;
	}
	// Joined records must be validated even when the request does not purge.
	let mut key = serialize_key((room, Interfix))?.to_vec();
	key.extend_from_slice(b"\xffbroken-member");
	services.db["roomuserid_joined"]
		.insert(&key, b"retained")
		.await?;
	assert_shutdown_refused(services, client, room, &baseline).await?;
	services.db["roomuserid_joined"]
		.remove(&key)
		.await?;
	let budget_alias = RoomAliasId::parse("#erasure-budget:localhost")?;
	services
		.alias
		.set_alias_by(&budget_alias, room, user)
		.await?;
	for count in [1025, 700, 900] {
		let mut txn = services.db.txn();
		let mut keys = Vec::new();
		for n in 0..count {
			let key = serialize_key((room, 9_000_000_u64.saturating_add(n)))?.to_vec();
			// The long alias charges its bytes again for each physical row.
			let alias = if count == 700 { second } else { &budget_alias };
			txn.insert_raw(&services.db["aliasid_alias"], &key, alias);
			keys.push(key);
		}
		txn.execute().await?;
		assert_shutdown_refused(services, client, room, &baseline).await?;
		services.db["aliasid_alias"]
			.get(&keys[0])
			.await
			.expect("overflow record is preserved");
		services.db["aliasid_alias"]
			.get(keys.last().expect("seeded rows"))
			.await
			.expect("overflow record is preserved");
		let mut txn = services.db.txn();
		for key in keys {
			txn.del_raw(&services.db["aliasid_alias"], key);
		}
		txn.execute().await?;
	}
	services.alias.remove_alias(&budget_alias).await?;
	Ok(())
}

async fn alias_publication_refusal(
	services: &Services,
	client: &Client<'_>,
	room: &RoomId,
	second: &RoomAliasId,
) -> Result {
	// Closing aliases and directory publication shares a commit; a late refusal
	// cannot delete the aliases while leaving the room published.
	services.directory.set_public(room, None).await?;
	refusal::refuse_next("publicroomids");
	let response = services
		.client
		.clients
		.default
		.delete(format!("{}/_synapse/admin/v1/rooms/{room}", client.base))
		.bearer_auth(TOKEN)
		.json(&json!({"purge":false}))
		.send()
		.await?;
	assert_eq!(response.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
	assert_eq!(refusal::pending(), 0);
	assert_eq!(services.alias.resolve_local_alias(second).await?, room);
	assert_eq!(
		services
			.alias
			.bounded_local_aliases_for_room(room)
			.await?,
		vec![second.to_owned()]
	);
	assert!(
		services
			.directory
			.is_public_room_checked(room)
			.await?
	);
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
	assert!(
		services
			.alias
			.bounded_local_aliases_for_room(room)
			.await?
			.is_empty()
	);
	assert!(
		!services
			.directory
			.is_public_room_checked(room)
			.await?
	);
	assert!(services.metadata.exists_checked(room).await?);
	Ok(())
}

async fn alias_shutdown_boundary(
	services: &Services,
	client: &Client<'_>,
	user: &UserId,
) -> Result {
	let room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	let alias = RoomAliasId::parse("#erasure-boundary:localhost")?;
	services
		.alias
		.set_alias_by(&alias, &room, user)
		.await?;
	services.directory.set_public(&room, None).await?;
	services.db["global"]
		.put((PENDING, &room), b"".as_slice())
		.await?;
	let baseline = snapshot(services, &room).await?;
	let mut txn = services.db.txn();
	let mut keys = Vec::new();
	for n in 0..897_u64 {
		let key = serialize_key((&room, 10_000_000_u64.saturating_add(n)))?.to_vec();
		txn.insert_raw(&services.db["aliasid_alias"], &key, &alias);
		keys.push(key);
	}
	txn.execute().await?;
	assert_shutdown_refused(services, client, &room, &baseline).await?;
	assert_eq!(
		services
			.alias
			.bounded_local_aliases_for_room(&room)
			.await?,
		vec![alias.clone()],
		"duplicates are examined before deduplication"
	);
	// Original inverse + 896 duplicates + forward + owner + publication = 900.
	services.db["aliasid_alias"]
		.remove(keys.last().expect("boundary rows"))
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
	assert!(
		response.status().is_success(),
		"the exact bridge mutation boundary must succeed"
	);
	let body: Value = response.json().await?;
	assert_eq!(
		body["local_aliases"],
		json!([alias]),
		"one alias is reported despite duplicate rows"
	);
	assert!(
		services
			.alias
			.bounded_local_aliases_for_room(&room)
			.await?
			.is_empty()
	);
	for key in keys {
		assert!(
			services.db["aliasid_alias"]
				.get(&key)
				.await
				.expect_err("all duplicate inverse rows removed")
				.is_not_found()
		);
	}
	assert!(
		!services
			.directory
			.is_public_room_checked(&room)
			.await?
	);
	assert!(services.metadata.exists_checked(&room).await?);
	Ok(())
}

async fn assert_shutdown_refused(
	services: &Services,
	client: &Client<'_>,
	room: &RoomId,
	baseline: &[Vec<u8>],
) -> Result {
	let response = services
		.client
		.clients
		.default
		.delete(format!("{}/_synapse/admin/v1/rooms/{room}", client.base))
		.bearer_auth(TOKEN)
		.json(&json!({"purge":false}))
		.send()
		.await?;
	assert!(
		!response.status().is_success(),
		"shutdown cannot succeed with an incomplete inventory"
	);
	assert_eq!(&snapshot(services, room).await?, baseline, "refusal precedes user eviction");
	Ok(())
}

async fn purge_http_refusal(services: &Services, client: &Client<'_>) -> Result {
	let room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	let short = services.short.get_shortroomid(&room).await?;
	let mut token = short.to_be_bytes().to_vec();
	token.extend_from_slice(b"fixture\xff");
	token.extend_from_slice(&short.to_be_bytes());
	token.extend_from_slice(&1_u64.to_be_bytes());
	services.db["tokenids"]
		.insert(&token, b"")
		.await?;
	let url = format!("{}/_synapse/admin/v1/rooms/{room}", client.base);
	refusal::refuse_next("tokenids");
	let response = services
		.client
		.clients
		.default
		.delete(&url)
		.bearer_auth(TOKEN)
		.json(&json!({"purge":true}))
		.send()
		.await?;
	assert_eq!(response.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
	assert_eq!(refusal::pending(), 0);
	assert!(services.metadata.exists_checked(&room).await?);
	assert_eq!(
		services.db["tokenids"]
			.get(&token)
			.await?
			.as_ref(),
		b""
	);
	services.db["roomid_shortstatehash"]
		.get(&room)
		.await?;
	async_purge_refusal(services, client, &room).await?;
	let response = services
		.client
		.clients
		.default
		.delete(&url)
		.bearer_auth(TOKEN)
		.json(&json!({"purge":true}))
		.send()
		.await?;
	assert!(response.status().is_success());
	assert!(!services.metadata.exists_checked(&room).await?);
	Ok(())
}

async fn async_purge_refusal(services: &Services, client: &Client<'_>, room: &RoomId) -> Result {
	refusal::refuse_next("tokenids");
	let response: Value = services
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
	let delete_id = response["delete_id"]
		.as_str()
		.expect("scheduled deletion ID");
	let url = format!("{}/_synapse/admin/v2/rooms/delete_status/{delete_id}", client.base);
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
	.map_err(|_| err!("asynchronous purge status exceeded its deadline"))??;
	assert_eq!(status["status"], "failed");
	assert_eq!(status["room_id"], room.as_str());
	assert!(status["shutdown_room"].is_null());
	let task = services
		.tasks
		.get(delete_id)
		.await?
		.expect("owned failed task");
	assert_eq!(task.status, tuwunel_service::tasks::Status::Failed);
	assert!(task.result.is_none(), "failed task has no fabricated success summary");
	assert!(
		task.error
			.as_deref()
			.expect("recorded task failure")
			.contains("armed to refuse")
	);
	assert_eq!(refusal::pending(), 0);
	assert!(services.metadata.exists_checked(room).await?);
	Ok(())
}

async fn auto_cleanup_refusal(services: &Services, client: &Client<'_>) -> Result {
	let room = client
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	let mut bad_key = serialize_key((&room, Interfix))?.to_vec();
	bad_key.extend_from_slice(b"invalid-local-departure");
	services.db["roomuserid_leftcount"]
		.insert(&bad_key, b"preserved")
		.await?;
	let response = services
		.client
		.clients
		.default
		.post(client.url(&format!("rooms/{room}/leave")))
		.bearer_auth(TOKEN)
		.json(&json!({}))
		.send()
		.await?;
	assert!(
		response.status().is_success(),
		"a committed leave survives refused optional cleanup"
	);
	assert!(services.metadata.exists_checked(&room).await?);
	assert_eq!(
		services.db["roomuserid_leftcount"]
			.get(&bad_key)
			.await?
			.as_ref(),
		b"preserved"
	);
	services.db["roomuserid_leftcount"]
		.remove(&bad_key)
		.await?;
	let guard = services.state.mutex.lock(&room).await;
	services
		.delete
		.delete_if_empty_local(&room, guard)
		.await;
	assert!(!services.metadata.exists_checked(&room).await?, "restored cleanup succeeds");
	Ok(())
}
