#![cfg(test)]
//! Notification API and sync consumers must preserve unread/read semantics
//! and refuse corrupt storage without publishing partial counts or cursors.
mod client;

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, temp_dir, var},
	fs::{DirBuilder, remove_dir_all},
	net::TcpListener,
	path::{Path, PathBuf},
	process::{Child, Command, id},
	thread::sleep,
	time::{Duration, Instant},
};

use serde_json::{Value, json};
use tokio::time::timeout;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err, http,
	ruma::{EventId, OwnedEventId, RoomId, UInt, UserId},
};
use tuwunel_database::serialize_key;
use tuwunel_service::Services;

use self::client::{Client, field, poll_until, register, wait_until_ready};

const OWNER: &str = "disposable-notification-reader-owner-token";
const WRITER: &str = "disposable-notification-reader-writer-token";
struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

const PHASE: &str = "TUWUNEL_NOTIFICATION_READER_PHASE";
const DIRECTORY: &str = "TUWUNEL_NOTIFICATION_READER_DIRECTORY";
struct OwnedChild(Child);
impl Drop for OwnedChild {
	fn drop(&mut self) {
		self.0.kill().ok();
		self.0.wait().ok();
	}
}

#[test]
fn notification_reads_refuse_corruption_and_use_actual_read_cutoffs() -> Result {
	if let Ok(phase) = var(PHASE) {
		return child(
			&PathBuf::from(var(DIRECTORY).expect("owned database path")),
			phase.parse()?,
		);
	}
	let db = DatabasePath(temp_dir().join(format!("tuwunel-notification-readers-{}", id())));
	let mut builder = DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&db.0)?;
	for phase in 0..3 {
		let mut child = OwnedChild(
			Command::new(current_exe()?)
				.env(PHASE, phase.to_string())
				.env(DIRECTORY, &db.0)
				.spawn()?,
		);
		let started = Instant::now();
		loop {
			if let Some(status) = child.0.try_wait()? {
				assert!(status.success(), "notification reader phase {phase} failed: {status}");
				break;
			}
			if started.elapsed() > Duration::from_mins(3) {
				return Err(err!("notification reader child exceeded deadline"));
			}
			sleep(Duration::from_millis(20));
		}
	}
	Ok(())
}

fn child(path: &Path, phase: u8) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let modes: &[&str] = if phase == 0 { &["fresh"] } else { &[] };
	let mut args = Args::default_test(modes);
	args.option.extend([
		format!("database_path={path:?}"),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		"listening=true".into(),
		"log=\"warn\"".into(),
		"client_sync_timeout_min=0".into(),
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
			let outcome = timeout(Duration::from_mins(3), exercise(&services, &base, phase))
				.await
				.map_err(|_| err!("notification reader fixture exceeded deadline"))
				.and_then(|result| result);
			let shutdown = server.server.shutdown();
			outcome.and(shutdown)
		};
		let (run, outcome) = tokio::join!(async_run(&server), exercise);
		drop(services);
		outcome.and(run).and(async_stop(&server).await)
	});
	drop(server);
	drop(runtime);
	result
}

async fn exercise(services: &Services, base: &str, phase: u8) -> Result {
	wait_until_ready(services, base).await?;
	if phase != 0 {
		return upgraded_read_controls(services, base).await;
	}
	let user = register(services, "notification-reader-owner", OWNER).await?;
	register(services, "notification-reader-writer", WRITER).await?;
	let owner = Client { services, base, token: OWNER };
	let writer = Client { services, base, token: WRITER };
	let room = owner
		.create_room(&json!({"preset":"public_chat"}))
		.await?;
	services
		.client
		.clients
		.default
		.post(writer.url(&format!("join/{room}")))
		.bearer_auth(WRITER)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;
	services.client.clients.default.put(owner.url("pushrules/global/override/notification-reader-control"))
        .bearer_auth(OWNER).json(&json!({"conditions":[{"kind":"event_match","key":"type","pattern":"m.room.message"}], "actions":["notify",{"set_tweak":"highlight","value":true}]}))
        .send().await?.error_for_status()?;
	let root = send(&writer, &room, "root", None).await?;
	let reply = send(&writer, &room, "reply", Some(&root)).await?;
	assert!(
		poll_until(Duration::from_secs(10), async || {
			services
				.pusher
				.notification_state(&user, &room)
				.await
				.is_ok_and(|state| {
					state.notifications >= 1
						&& state
							.threads
							.get(&root)
							.is_some_and(|pair| pair.0 == 1)
				})
		})
		.await,
		"real main/thread notification completion must run"
	);
	let page = notifications(&owner, None, http::StatusCode::OK).await?;
	assert_read(&page, &root, false);
	assert_read(&page, &reply, false);
	count_corruption(&owner, &user, &room, &root).await?;
	pagination_controls(&owner, &user, &root).await?;
	// A main read must not label the independent thread notification as read.
	receipt(&owner, &room, &root, "main").await?;
	let page = notifications(&owner, None, http::StatusCode::OK).await?;
	assert_read(&page, &root, true);
	assert_read(&page, &reply, false);
	let main_sync = sync(&owner, None, http::StatusCode::OK).await?;
	let cursor = field(&main_sync, "next_batch")?.to_owned();
	receipt(&owner, &room, &reply, root.as_str()).await?;
	let page = notifications(&owner, None, http::StatusCode::OK).await?;
	assert_read(&page, &reply, true);
	let reset = sync(&owner, Some(&cursor), http::StatusCode::OK).await?;
	assert_eq!(
		reset["rooms"]["join"][room.as_str()]["unread_notifications"]["notification_count"],
		0,
		"thread-only read must publish the folded zero reset"
	);
	// A storage failure while reading the actual cutoff refuses the whole page.
	let map = &services.db["roomuserid_notificationcutoff"];
	let key = serialize_key((&room, &user, root.as_str()))?;
	let saved = map.get(&key).await?.to_vec();
	map.insert(&key, b"broken-cutoff").await?;
	notifications(&owner, None, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	assert_eq!(map.get(&key).await?.as_ref(), b"broken-cutoff");
	map.insert(&key, &saved).await?;
	assert_read(&notifications(&owner, None, http::StatusCode::OK).await?, &reply, true);
	prepare_legacy(&owner, &writer, &user, &room, &root, &reply).await
}

async fn send(
	client: &Client<'_>,
	room: &RoomId,
	txn: &str,
	root: Option<&EventId>,
) -> Result<OwnedEventId> {
	let mut content = json!({"msgtype":"m.text", "body":txn});
	if let Some(root) = root {
		content["m.relates_to"] = json!({"rel_type":"m.thread","event_id":root});
	}
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
	Ok(field(&body, "event_id")?.try_into()?)
}

async fn receipt(client: &Client<'_>, room: &RoomId, event: &EventId, thread: &str) -> Result {
	client
		.services
		.client
		.clients
		.default
		.post(client.url(&format!("rooms/{room}/receipt/m.read.private/{event}")))
		.bearer_auth(client.token)
		.json(&json!({"thread_id":thread}))
		.send()
		.await?
		.error_for_status()?;
	Ok(())
}

async fn notifications(
	client: &Client<'_>,
	from: Option<&str>,
	expected: http::StatusCode,
) -> Result<Value> {
	let mut request = client
		.services
		.client
		.clients
		.default
		.get(client.url("notifications"))
		.bearer_auth(client.token);
	if let Some(from) = from {
		request = request.query(&[("from", from)]);
	}
	let response = request.send().await?;
	let status = response.status();
	let body: Value = response.json().await?;
	assert_eq!(status, expected, "notification response: {body}");
	if !expected.is_success() {
		assert!(
			body.get("notifications").is_none() && body.get("next_token").is_none(),
			"refused page cannot advance or publish partial rows"
		);
	}
	Ok(body)
}

fn assert_read(body: &Value, event: &EventId, read: bool) {
	let entry = body["notifications"]
		.as_array()
		.expect("notification list")
		.iter()
		.find(|entry| entry["event"]["event_id"].as_str() == Some(event.as_str()))
		.unwrap_or_else(|| panic!("real notification {event} missing: {body}"));
	assert_eq!(entry["read"], read, "actual read scope: {entry}");
}

async fn sync(
	client: &Client<'_>,
	since: Option<&str>,
	expected: http::StatusCode,
) -> Result<Value> {
	let mut request = client
		.services
		.client
		.clients
		.default
		.get(client.url("sync"))
		.bearer_auth(client.token)
		.query(&[("timeout", "0"), ("full_state", "true")]);
	if let Some(since) = since {
		request = request.query(&[("since", since)]);
	}
	let response = request.send().await?;
	let status = response.status();
	let body: Value = response.json().await?;
	assert_eq!(status, expected, "sync response: {body}");
	if !expected.is_success() {
		for field in ["next_batch", "rooms", "to_device"] {
			assert!(body.get(field).is_none(), "failed sync cannot publish {field}");
		}
	}
	Ok(body)
}

async fn sliding(client: &Client<'_>) -> Result<Value> {
	Ok(client.services.client.clients.default.post(format!("{}/_matrix/client/unstable/org.matrix.simplified_msc3575/sync", client.base))
        .bearer_auth(client.token).json(&json!({"lists":{"all":{"ranges":[[0,99]],"required_state":[["m.room.name","*"]],"timeline_limit":0}}}))
        .send().await?.error_for_status()?.json().await?)
}

async fn count_corruption(
	client: &Client<'_>,
	user: &UserId,
	room: &RoomId,
	root: &EventId,
) -> Result {
	let services = client.services;
	let before = sync(client, None, http::StatusCode::OK).await?;
	let cursor = field(&before, "next_batch")?;
	for (name, key) in [
		("userroomid_notificationcount", serialize_key((user, room))?),
		("userroomid_highlightcount", serialize_key((user, room))?),
		("userroomid_notificationcount", serialize_key((user, room, root))?),
		("userroomid_highlightcount", serialize_key((user, room, root))?),
		("roomuserid_lastnotificationread", serialize_key((room, user))?),
		("roomuserid_lastnotificationread", serialize_key((room, user, root))?),
	] {
		let map = &services.db[name];
		let saved = map.get(&key).await?.to_vec();
		for corrupt in [vec![], vec![0; 7], vec![0; 9], u64::MAX.to_be_bytes().to_vec()] {
			map.insert(&key, &corrupt).await?;
			services
				.pusher
				.notification_state(user, room)
				.await
				.expect_err("invalid field cannot become zero/none");
			sync(client, Some(cursor), http::StatusCode::INTERNAL_SERVER_ERROR).await?;
			assert!(
				sliding(client).await?["rooms"]
					.get(room.as_str())
					.is_none(),
				"sliding sync withholds complete room"
			);
			assert_eq!(
				map.get(&key).await?.as_ref(),
				corrupt,
				"consumer never repairs raw corruption"
			);
		}
		map.insert(&key, &saved).await?;
		sync(client, Some(cursor), http::StatusCode::OK).await?;
		assert!(
			sliding(client).await?["rooms"]
				.get(room.as_str())
				.is_some(),
			"healthy retry serves complete room"
		);
	}
	// Genuine absence is distinct from corrupt bytes. Missing main rows do
	// not suppress the independent, complete thread counts.
	for name in ["userroomid_notificationcount", "userroomid_highlightcount"] {
		let map = &services.db[name];
		let key = serialize_key((user, room))?;
		let saved = map.get(&key).await?.to_vec();
		map.remove(&key).await?;
		let state = services
			.pusher
			.notification_state(user, room)
			.await?;
		if name == "userroomid_notificationcount" {
			assert_eq!(state.notifications, 0);
		} else {
			assert_eq!(state.highlights, 0);
		}
		let body = sync(client, None, http::StatusCode::OK).await?;
		let field = if name == "userroomid_notificationcount" {
			"notification_count"
		} else {
			"highlight_count"
		};
		assert_eq!(body["rooms"]["join"][room.as_str()]["unread_notifications"][field], 1);
		map.insert(&key, &saved).await?;
	}

	let map = &services.db["userroomid_notificationcount"];
	let main = serialize_key((user, room))?;
	let saved = map.get(&main).await?.to_vec();
	map.insert(&main, &u64::from(UInt::MAX).to_be_bytes())
		.await?;
	services
		.pusher
		.global_notification_count(user)
		.await
		.expect_err("main plus thread overflow cannot saturate a badge");
	sync(client, Some(cursor), http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	assert!(
		sliding(client).await?["rooms"]
			.get(room.as_str())
			.is_none()
	);
	map.insert(&main, &saved).await?;
	// Invalid trailing fields cannot disappear from a thread or account scan.
	let bad = serialize_key((user, room, "not-an-event"))?;
	map.insert(&bad, &1_u64.to_be_bytes()).await?;
	services
		.pusher
		.notification_state(user, room)
		.await
		.expect_err("invalid thread key refuses");
	services
		.pusher
		.global_notification_count(user)
		.await
		.expect_err("invalid thread key refuses badge");
	map.remove(&bad).await?;
	// An unrelated user's corrupt records cannot contaminate this account.
	let other = UserId::parse("@notification-reader-unrelated:localhost")?;
	let foreign = serialize_key((&other, room, root))?;
	map.insert(&foreign, b"broken").await?;
	services
		.pusher
		.notification_state(user, room)
		.await?;
	services
		.pusher
		.global_notification_count(user)
		.await?;
	map.remove(&foreign).await?;
	// An exact complete inventory succeeds; one extra row refuses the same request.
	let mut keys = Vec::new();
	for n in 0..4093 {
		let root = format!("$notification-budget-{n:04}:localhost");
		let key = serialize_key((user, room, root.as_str()))?;
		keys.push(key);
	}
	let mut txn = services.db.txn();
	for chunk in keys.chunks(800) {
		for key in chunk {
			txn.insert_raw(map, key, 0_u64.to_be_bytes());
		}
		txn.execute().await?;
		txn = services.db.txn();
	}
	services
		.pusher
		.notification_state(user, room)
		.await?; // 4093 + 3 real thread rows
	let overflow = serialize_key((user, room, "$notification-budget-overflow:localhost"))?;
	map.insert(&overflow, &0_u64.to_be_bytes())
		.await?;
	services
		.pusher
		.notification_state(user, room)
		.await
		.expect_err("complete snapshot exceeds shared row bound");
	map.remove(&overflow).await?;
	for chunk in keys.chunks(800) {
		let mut txn = services.db.txn();
		for key in chunk {
			txn.del_raw(map, key);
		}
		txn.execute().await?;
	}
	services
		.pusher
		.notification_state(user, room)
		.await?;
	Ok(())
}

async fn pagination_controls(client: &Client<'_>, user: &UserId, root: &EventId) -> Result {
	let services = client.services;
	let raw = services.timeline.get_pdu_id(root).await?;
	let count = tuwunel_core::matrix::PduId::from(raw)
		.count
		.into_unsigned();
	let map = &services.db["useridcount_notification"];
	let key = serialize_key((user, count))?;
	let saved = map.get(&key).await?.to_vec();
	for corrupt in [b"{".to_vec(), vec![b' '; 65537]] {
		map.insert(&key, &corrupt).await?;
		notifications(client, None, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
		assert_eq!(map.get(&key).await?.as_ref(), corrupt);
	}
	map.insert(&key, &saved).await?;
	let zero = notifications(client, Some("0"), http::StatusCode::OK).await?;
	assert!(
		zero["notifications"]
			.as_array()
			.expect("zero page")
			.is_empty()
	);
	let exclusive = notifications(client, Some(&count.to_string()), http::StatusCode::OK).await?;
	assert!(
		!exclusive["notifications"]
			.as_array()
			.expect("exclusive page")
			.iter()
			.any(|entry| entry["event"]["event_id"] == root.as_str())
	);
	notifications(client, Some(&u64::MAX.to_string()), http::StatusCode::BAD_REQUEST).await?;
	// Synthetic filtered rows exercise request work bounds and cursor progress,
	// without claiming hundreds of real client events.
	let mut filtered: Value = serde_json::from_slice(&saved)?;
	filtered["actions"] = json!(["notify"]);
	let filtered = serde_json::to_vec(&filtered)?;
	let mut keys = Vec::new();
	let mut txn = services.db.txn();
	for _ in 0..513 {
		let count = services.globals.next_count().await?;
		let key = serialize_key((user, *count))?.to_vec();
		txn.insert_raw(map, &key, &filtered);
		keys.push(key);
	}
	txn.execute().await?;
	let first: Value = services
		.client
		.clients
		.default
		.get(client.url("notifications"))
		.bearer_auth(client.token)
		.query(&[("only", "highlight")])
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	assert!(
		first["notifications"]
			.as_array()
			.expect("bounded filtered page")
			.is_empty()
	);
	let from = field(&first, "next_token")?;
	let second: Value = services
		.client
		.clients
		.default
		.get(client.url("notifications"))
		.bearer_auth(client.token)
		.query(&[("only", "highlight"), ("from", from)])
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	assert_read(&second, root, false);
	let mut txn = services.db.txn();
	for key in keys {
		txn.del_raw(map, &key);
	}
	txn.execute().await?;

	Ok(())
}

async fn prepare_legacy(
	owner: &Client<'_>,
	writer: &Client<'_>,
	user: &UserId,
	room: &RoomId,
	root: &EventId,
	reply: &EventId,
) -> Result {
	let public_root = send(writer, room, "public-upgrade-root", None).await?;
	let public_reply = send(writer, room, "public-upgrade-reply", Some(&public_root)).await?;
	owner
		.services
		.client
		.clients
		.default
		.post(owner.url(&format!("rooms/{room}/receipt/m.read/{public_reply}")))
		.bearer_auth(OWNER)
		.json(&json!({"thread_id":public_root}))
		.send()
		.await?
		.error_for_status()?;
	let unread = send(writer, room, "upgrade-still-unread", None).await?;
	assert!(
		poll_until(Duration::from_secs(10), async || {
			owner
				.services
				.pusher
				.notification_count(user, room)
				.await
				.is_ok_and(|count| count >= 2)
		})
		.await
	);
	let metadata = json!({"room":room,"user":user,"root":root,"reply":reply,"public_reply":public_reply,"unread":unread});
	let db = &owner.services.db;
	let global = &db["global"];
	global
		.insert(b"notification_reader_fixture_v1", &serde_json::to_vec(&metadata)?)
		.await?;
	// Reproduce a version-18 database: actual receipt sources exist, but the
	// new cutoff family and migration markers do not. Change stamps include
	// the newer unread event, so copying them would fail the restart control.
	let keys = db["roomuserid_notificationcutoff"]
		.raw_keys_after(None, 64)
		.await?;
	let mut txn = db.txn();
	for key in keys {
		txn.del_raw(&db["roomuserid_notificationcutoff"], &key);
	}
	txn.del_raw(global, b"notification_read_cutoffs_v1");
	txn.raw_put(global, b"version", 18_u64);
	txn.execute().await
}

async fn upgraded_read_controls(services: &Services, base: &str) -> Result {
	let owner = Client { services, base, token: OWNER };
	let metadata: Value = serde_json::from_slice(
		&services.db["global"]
			.get(b"notification_reader_fixture_v1")
			.await?,
	)?;
	let page = notifications(&owner, None, http::StatusCode::OK).await?;
	for (name, read) in
		[("root", true), ("reply", true), ("public_reply", true), ("unread", false)]
	{
		let event = EventId::parse(field(&metadata, name)?)?;
		assert_read(&page, &event, read);
	}
	assert!(
		services.db["global"]
			.get(b"notification_read_cutoffs_v1")
			.await?
			.is_empty()
	);
	assert!(
		services.db["global"]
			.get(b"notification_read_cutoffs_cursor_v1")
			.await
			.expect_err("completed migration removes cursor")
			.is_not_found()
	);
	Ok(())
}
