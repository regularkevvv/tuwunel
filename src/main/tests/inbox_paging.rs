#![cfg(test)]
//! Real HTTP cursors drain bounded inbox pages without losing E2EE events.
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
	ruma::{DeviceId, OwnedDeviceId, RoomId, UserId},
};
use tuwunel_database::{Json, Row, serialize_key};
use tuwunel_service::Services;

use self::client::{Client, field, register, wait_until_ready};

const V3_TOKEN: &str = "inbox-paging-v3-owner-access-token-0001";
const V5_TOKEN: &str = "inbox-paging-v5-owner-access-token-0001";
const DEHYDRATED_TOKEN: &str = "inbox-paging-dehydrated-access-token-0001";

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn http_inbox_pages_preserve_backlog_retries_and_failed_read_claims() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path =
		DatabasePath(temp_dir().join(format!("tuwunel-inbox-paging-{}-{port}", process_id())));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={:?}", db_path.0),
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
			let outcome = timeout(Duration::from_mins(3), exercise(&services, &base))
				.await
				.map_err(|_| err!("inbox paging HTTP fixture exceeded deadline"))
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

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	let user = register(services, "inboxpagingv3", V3_TOKEN).await?;
	let v3 = Client { services, base, token: V3_TOKEN };
	let device = only_device(services, &user).await?;
	v3_backlog(&v3, &user, &device).await?;
	v3_refusals(&v3, &user, &device).await?;

	let user = register(services, "inboxpagingv5", V5_TOKEN).await?;
	let v5 = Client { services, base, token: V5_TOKEN };
	let device = only_device(services, &user).await?;
	v5_backlog(&v5, &user, &device).await?;
	v5_refusal(&v5, &user, &device).await?;

	let user = register(services, "inboxpagingdehydrated", DEHYDRATED_TOKEN).await?;
	let dehydrated = Client { services, base, token: DEHYDRATED_TOKEN };
	let device = only_device(services, &user).await?;
	services.db["userid_dehydrateddevice"]
		.raw_put(
			&user,
			Json(json!({
				"device_id":device, "device_data":{"algorithm":"test","account":"encrypted"}
			})),
		)
		.await?;
	dehydrated_backlog(&dehydrated, &user, &device).await?;
	dehydrated_refusal(&dehydrated, &user, &device).await?;
	Ok(())
}

async fn only_device(services: &Services, user: &UserId) -> Result<OwnedDeviceId> {
	let ids = services.users.bounded_device_ids(user).await?;
	match ids.as_slice() {
		| [id] => Ok(id.clone()),
		| _ => Err(err!("fixture requires exactly one device")),
	}
}

async fn append(client: &Client<'_>, user: &UserId, device: &DeviceId, n: u64) -> u64 {
	client
		.services
		.users
		.add_to_device_event(user, user, device, "m.test.ping", &json!({"n":n}))
		.await
}

async fn snapshot(client: &Client<'_>, user: &UserId, device: &DeviceId) -> Result<Vec<Row>> {
	let prefix = serialize_key((user, device, tuwunel_database::Interfix))?;
	client.services.db["todeviceid_events"]
		.raw_rows_prefix_after(&prefix, None, 1_000)
		.await
}

fn numbers(body: &Value, pointer: &str) -> Result<Vec<u64>> {
	body.pointer(pointer)
		.and_then(Value::as_array)
		.ok_or_else(|| err!("response omitted inbox events {pointer}"))?
		.iter()
		.map(|event| {
			event["content"]["n"]
				.as_u64()
				.ok_or_else(|| err!("inbox event omitted sequence marker"))
		})
		.collect()
}

async fn v3_request(
	client: &Client<'_>,
	since: Option<&str>,
	status: http::StatusCode,
) -> Result<Value> {
	let mut query = vec![("timeout", "0")];
	if let Some(since) = since {
		query.push(("since", since));
	}
	let response = client
		.services
		.client
		.clients
		.default
		.get(client.url("sync"))
		.bearer_auth(client.token)
		.query(&query)
		.send()
		.await?;
	let actual = response.status();
	let body: Value = response.json().await?;
	assert_eq!(actual, status, "v3 HTTP outcome: {body}");
	if !status.is_success() {
		assert!(body.get("next_batch").is_none(), "refused v3 response cannot advance cursor");
		assert!(body.get("to_device").is_none(), "refused v3 response has no partial inbox");
	}
	Ok(body)
}

async fn room_message(client: &Client<'_>, room: &RoomId) -> Result<String> {
	let body: Value = client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/between-pages")))
		.bearer_auth(client.token)
		.json(&json!({"msgtype":"m.text","body":"room event between inbox pages"}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	Ok(field(&body, "event_id")?.into())
}

fn room_contains(body: &Value, room: &RoomId, event_id: &str) -> bool {
	body["rooms"]["join"][room.as_str()]["timeline"]["events"]
		.as_array()
		.is_some_and(|events| {
			events
				.iter()
				.any(|event| event["event_id"].as_str() == Some(event_id))
		})
}

async fn v3_backlog(client: &Client<'_>, user: &UserId, device: &DeviceId) -> Result {
	let room = client
		.create_room(&json!({"preset":"private_chat"}))
		.await?;
	let baseline = v3_request(client, None, http::StatusCode::OK).await?;
	let since = field(&baseline, "next_batch")?.to_owned();
	let mut room_event = String::new();
	for index in 0..300 {
		append(client, user, device, index).await;
		if index == 150 {
			room_event = room_message(client, &room).await?;
		}
	}
	let first = v3_request(client, Some(&since), http::StatusCode::OK).await?;
	let retry = v3_request(client, Some(&since), http::StatusCode::OK).await?;
	assert_eq!(
		numbers(&first, "/to_device/events")?,
		numbers(&retry, "/to_device/events")?,
		"v3 unacknowledged page replays exactly"
	);
	assert_eq!(
		field(&first, "next_batch")?,
		field(&retry, "next_batch")?,
		"v3 retry preserves page cursor"
	);
	assert!(
		!room_contains(&first, &room, &room_event),
		"early shared watermark excludes later room event"
	);
	let mut received = numbers(&first, "/to_device/events")?;
	assert_eq!(received.len(), 128, "v3 page is row bounded");
	let mut cursor = field(&first, "next_batch")?.to_owned();
	let mut room_deliveries = 0;
	for _ in 0..3 {
		if received.len() == 300 {
			break;
		}
		let body = v3_request(client, Some(&cursor), http::StatusCode::OK).await?;
		room_deliveries += usize::from(room_contains(&body, &room, &room_event));
		received.extend(numbers(&body, "/to_device/events")?);
		cursor = field(&body, "next_batch")?.to_owned();
	}
	assert_eq!(
		received,
		(0..300).collect::<Vec<u64>>(),
		"v3 backlog drains in order without loss or duplicates"
	);
	assert_eq!(
		room_deliveries, 1,
		"lowering the global watermark preserves the later room event"
	);
	Ok(())
}

async fn damage_after_claim(
	client: &Client<'_>,
	user: &UserId,
	device: &DeviceId,
) -> Result<(String, u64)> {
	let claim = append(client, user, device, 900).await;
	let damaged = append(client, user, device, 901).await;
	client.services.db["todeviceid_events"]
		.put_raw((user, device, damaged), b"{invalid")
		.await?;
	Ok((claim.to_string(), damaged))
}

async fn repair(client: &Client<'_>, user: &UserId, device: &DeviceId, count: u64) -> Result {
	client.services.db["todeviceid_events"]
		.put_raw(
			(user, device, count),
			serde_json::to_vec(&json!({"type":"m.test.ping","sender":user,"content":{"n":901}}))?,
		)
		.await
}

async fn v3_refusals(client: &Client<'_>, user: &UserId, device: &DeviceId) -> Result {
	let (claim, damaged) = damage_after_claim(client, user, device).await?;
	let before = snapshot(client, user, device).await?;
	v3_request(client, Some(&claim), http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	assert_eq!(
		snapshot(client, user, device).await?,
		before,
		"corrupt v3 page performs no ACK deletion"
	);
	repair(client, user, device, damaged).await?;
	let before = snapshot(client, user, device).await?;
	tuwunel_database::refusal::refuse_next("todeviceid_events");
	v3_request(client, Some(&claim), http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	assert_eq!(snapshot(client, user, device).await?, before, "refused ACK commit is atomic");
	assert_eq!(tuwunel_database::refusal::pending(), 0, "ACK refusal actually fired");
	let retried = v3_request(client, Some(&claim), http::StatusCode::OK).await?;
	assert_eq!(
		numbers(&retried, "/to_device/events")?,
		[901],
		"same v3 cursor delivers after repair and ACK retry"
	);
	Ok(())
}

async fn v5_request(
	client: &Client<'_>,
	connection: &str,
	pos: Option<&str>,
	since: &str,
	status: http::StatusCode,
) -> Result<Value> {
	let url =
		format!("{}/_matrix/client/unstable/org.matrix.simplified_msc3575/sync", client.base);
	let mut request = client
		.services
		.client
		.clients
		.default
		.post(url)
		.bearer_auth(client.token)
		.json(
			&json!({"conn_id":connection,"extensions":{"to_device":{"enabled":true,"since":since}}}),
		);
	if let Some(pos) = pos {
		request = request.query(&[("pos", pos)]);
	}
	let response = request.send().await?;
	let actual = response.status();
	let body: Value = response.json().await?;
	assert_eq!(actual, status, "v5 HTTP outcome: {body}");
	if !status.is_success() {
		assert!(body.get("pos").is_none(), "refused v5 response cannot advance global position");
		assert!(
			body.get("extensions").is_none(),
			"refused v5 response cannot advance inbox cursor"
		);
	}
	Ok(body)
}

fn inbox_cursor(body: &Value) -> Result<&str> {
	body.pointer("/extensions/to_device/next_batch")
		.and_then(Value::as_str)
		.ok_or_else(|| err!("v5 response omitted independent inbox cursor"))
}

async fn v5_backlog(client: &Client<'_>, user: &UserId, device: &DeviceId) -> Result {
	for index in 0..300 {
		append(client, user, device, index).await;
	}
	let first = v5_request(client, "backlog", None, "0", http::StatusCode::OK).await?;
	let retry = v5_request(client, "backlog", None, "0", http::StatusCode::OK).await?;
	assert_eq!(
		numbers(&first, "/extensions/to_device/events")?,
		numbers(&retry, "/extensions/to_device/events")?,
		"v5 unacknowledged page replays exactly"
	);
	assert_eq!(inbox_cursor(&first)?, inbox_cursor(&retry)?, "v5 retry preserves inbox cursor");
	let mut received = numbers(&first, "/extensions/to_device/events")?;
	assert_eq!(received.len(), 128, "v5 page is row bounded");
	let mut pos = field(&retry, "pos")?.to_owned();
	let mut since = inbox_cursor(&retry)?.to_owned();
	assert!(
		since.parse::<u64>()? < pos.parse::<u64>()?,
		"independent inbox cursor trails global position"
	);
	for _ in 0..3 {
		if received.len() == 300 {
			break;
		}
		let body =
			v5_request(client, "backlog", Some(&pos), &since, http::StatusCode::OK).await?;
		received.extend(numbers(&body, "/extensions/to_device/events")?);
		pos = field(&body, "pos")?.to_owned();
		since = inbox_cursor(&body)?.to_owned();
	}
	assert_eq!(
		received,
		(0..300).collect::<Vec<u64>>(),
		"v5 drains independently with no unrelated global activity"
	);
	Ok(())
}

async fn v5_refusal(client: &Client<'_>, user: &UserId, device: &DeviceId) -> Result {
	let (claim, damaged) = damage_after_claim(client, user, device).await?;
	let before = snapshot(client, user, device).await?;
	v5_request(client, "corrupt", None, &claim, http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	assert_eq!(
		snapshot(client, user, device).await?,
		before,
		"corrupt v5 page performs no ACK deletion"
	);
	repair(client, user, device, damaged).await?;
	let retried = v5_request(client, "corrupt", None, &claim, http::StatusCode::OK).await?;
	assert_eq!(
		numbers(&retried, "/extensions/to_device/events")?,
		[901],
		"same v5 inbox cursor retries repaired row"
	);
	Ok(())
}

async fn dehydrated_request(
	client: &Client<'_>,
	device: &DeviceId,
	since: Option<&str>,
	status: http::StatusCode,
) -> Result<Value> {
	let url = format!(
		"{}/_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device/{device}/events",
		client.base
	);
	let response = client
		.services
		.client
		.clients
		.default
		.post(url)
		.bearer_auth(client.token)
		.json(&json!({"next_batch":since}))
		.send()
		.await?;
	let actual = response.status();
	let body: Value = response.json().await?;
	assert_eq!(actual, status, "dehydrated HTTP outcome: {body}");
	if !status.is_success() {
		assert!(
			body.get("next_batch").is_none(),
			"refused dehydrated response cannot advance cursor"
		);
		assert!(body.get("events").is_none(), "refused dehydrated response has no partial inbox");
	}
	Ok(body)
}

async fn dehydrated_backlog(client: &Client<'_>, user: &UserId, device: &DeviceId) -> Result {
	for index in 0..125 {
		append(client, user, device, index).await;
	}
	let before = snapshot(client, user, device).await?;
	let first = dehydrated_request(client, device, None, http::StatusCode::OK).await?;
	let retry = dehydrated_request(client, device, None, http::StatusCode::OK).await?;
	assert_eq!(first, retry, "dehydrated unacknowledged page replays exactly");
	let mut received = numbers(&first, "/events")?;
	assert_eq!(received.len(), 50, "dehydrated preserves its existing row cap");
	let mut cursor = field(&first, "next_batch")?.to_owned();
	for _ in 0..3 {
		if received.len() == 125 {
			break;
		}
		let body =
			dehydrated_request(client, device, Some(&cursor), http::StatusCode::OK).await?;
		received.extend(numbers(&body, "/events")?);
		cursor = field(&body, "next_batch")?.to_owned();
	}
	assert_eq!(
		received,
		(0..125).collect::<Vec<u64>>(),
		"dehydrated backlog drains without loss or duplicates"
	);
	assert_eq!(
		snapshot(client, user, device).await?,
		before,
		"dehydrated pagination does not ACK or delete inbox rows"
	);
	Ok(())
}

async fn dehydrated_refusal(client: &Client<'_>, user: &UserId, device: &DeviceId) -> Result {
	let (claim, damaged) = damage_after_claim(client, user, device).await?;
	let before = snapshot(client, user, device).await?;
	dehydrated_request(client, device, Some(&claim), http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	assert_eq!(
		snapshot(client, user, device).await?,
		before,
		"corrupt dehydrated page changes no inbox rows"
	);
	repair(client, user, device, damaged).await?;
	let retried = dehydrated_request(client, device, Some(&claim), http::StatusCode::OK).await?;
	assert_eq!(
		numbers(&retried, "/events")?,
		[901],
		"same dehydrated cursor retries repaired row"
	);
	Ok(())
}
