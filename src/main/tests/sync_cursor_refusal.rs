#![cfg(test)]

mod client;

use std::{
	env::temp_dir, fs::remove_dir_all, net::TcpListener, path::PathBuf,
	process::id as process_id, time::Duration,
};

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::time::timeout;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err, http,
	ruma::{OwnedEventId, RoomId, events::StateEventType},
};
use tuwunel_service::Services;

use self::client::{Client, poll_until, register, wait_until_ready};

const TOKEN: &str = "disposable-sync-cursor-owner-access-token";
const MARKER: &str = "pending-inbox-survives-room-refusal";

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn failed_room_sync_preserves_the_cursor_and_retry_deliveries() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path =
		DatabasePath(temp_dir().join(format!("tuwunel-sync-cursor-{}-{port}", process_id())));
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
			let outcome = timeout(Duration::from_mins(2), exercise(&services, &base))
				.await
				.map_err(|_| err!("sync cursor fixture exceeded its deadline"))
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

async fn sync(
	client: &Client<'_>,
	since: Option<&str>,
	expected: http::StatusCode,
) -> Result<Value> {
	let mut query = vec![
		("timeout", "0"),
		("full_state", "true"),
		("filter", r#"{"room":{"timeline":{"limit":100}}}"#),
	];
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
	let status = response.status();
	let body: Value = response.json().await?;
	assert_eq!(status, expected, "sync cursor response: {body}");
	if !expected.is_success() {
		assert!(body.get("next_batch").is_none(), "failed sync cannot advance the cursor");
		assert!(body.get("rooms").is_none(), "failed sync cannot serve a partial room set");
		assert!(
			body.get("to_device").is_none(),
			"failed sync cannot acknowledge an unsent inbox"
		);
	}
	Ok(body)
}

fn token(body: &Value) -> Result<&str> {
	body["next_batch"]
		.as_str()
		.ok_or_else(|| err!("successful sync omitted next_batch"))
}

async fn send(client: &Client<'_>, room: &RoomId, txn: &str) -> Result<OwnedEventId> {
	let body: Value = client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/{txn}")))
		.bearer_auth(client.token)
		.json(&json!({"msgtype": "m.text", "body": txn}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	Ok(body["event_id"]
		.as_str()
		.expect("sent event ID")
		.try_into()?)
}

async fn topic(client: &Client<'_>, room: &RoomId) -> Result<OwnedEventId> {
	client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/state/m.room.topic")))
		.bearer_auth(client.token)
		.json(&json!({"topic": "pending delta"}))
		.send()
		.await?
		.error_for_status()?;
	client
		.services
		.state_accessor
		.room_state_get_id(room, &StateEventType::RoomTopic, "")
		.await
}

fn contains(body: &Value, section: &str, room: &RoomId, event: &OwnedEventId) -> bool {
	body["rooms"][section][room.as_str()]["timeline"]["events"]
		.as_array()
		.is_some_and(|events| {
			events
				.iter()
				.any(|value| value["event_id"].as_str() == Some(event.as_str()))
		})
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	let user = register(services, "cursor-owner", TOKEN).await?;
	let client = Client { services, base, token: TOKEN };
	let healthy = client
		.create_room(&json!({"preset": "public_chat"}))
		.await?;
	let damaged = client
		.create_room(&json!({"preset": "public_chat"}))
		.await?;
	let baseline = sync(&client, None, http::StatusCode::OK).await?;
	let since = token(&baseline)?.to_owned();
	let old = send(&client, &healthy, "healthy-older").await?;
	let newest = send(&client, &healthy, "healthy-newer").await?;
	let pending = send(&client, &damaged, "damaged-pending").await?;
	let state = topic(&client, &damaged).await?;
	let (_, device, _) = services.users.find_from_token(TOKEN).await?;
	let delivery = services
		.users
		.add_to_device_event(
			&user,
			&user,
			&device,
			"com.example.cursor",
			&json!({"marker": MARKER}),
		)
		.await;
	assert!(
		delivery > since.parse::<u64>()?,
		"inbox control is newer than the last acknowledged token"
	);
	let pdus = &services.db["pduid_pdu"];
	let state_id = services.timeline.get_pdu_id(&state).await?;
	let saved = pdus.get(&state_id).await?.to_vec();
	pdus.remove(&state_id).await?;
	services.clear_cache().await;
	for _ in 0..2 {
		sync(&client, Some(&since), http::StatusCode::INTERNAL_SERVER_ERROR).await?;
		let error = pdus
			.get(&state_id)
			.await
			.expect_err("refusal cannot recreate missing state");
		assert!(error.is_not_found(), "refusal preserves genuine missing state");
		let inbox: Vec<_> = services
			.users
			.get_to_device_events(&user, &device, Some(since.parse()?), None)
			.collect()
			.await;
		assert!(
			inbox.iter().any(|(count, _)| *count == delivery),
			"unacknowledged inbox delivery survives refusal and retry"
		);
	}
	pdus.raw_put(&state_id, saved.as_slice()).await?;
	services.clear_cache().await;
	let repaired = sync(&client, Some(&since), http::StatusCode::OK).await?;
	assert!(
		contains(&repaired, "join", &healthy, &old),
		"old cursor retries the healthy older message"
	);
	assert!(
		contains(&repaired, "join", &healthy, &newest),
		"old cursor retries the healthy newest message"
	);
	assert!(
		contains(&repaired, "join", &damaged, &pending),
		"repair retries the previously withheld room message"
	);
	assert!(
		repaired["to_device"]["events"]
			.as_array()
			.is_some_and(|events| events
				.iter()
				.any(|value| value["content"]["marker"].as_str() == Some(MARKER))),
		"repair delivers the unacknowledged inbox event"
	);
	let after = token(&repaired)?.to_owned();
	let acknowledged = sync(&client, Some(&after), http::StatusCode::OK).await?;
	assert!(
		!acknowledged.to_string().contains(MARKER),
		"only the successful response token acknowledges the inbox"
	);
	let inbox: Vec<_> = services
		.users
		.get_to_device_events(&user, &device, None, None)
		.collect()
		.await;
	assert!(
		inbox.iter().all(|(count, _)| *count != delivery),
		"successful acknowledgement removes the durable inbox event"
	);
	malformed_timeline(&client, &healthy, &since, &old, &newest).await?;
	left_timeline(&client, &after).await?;
	Ok(())
}

async fn malformed_timeline(
	client: &Client<'_>,
	room: &RoomId,
	since: &str,
	older: &OwnedEventId,
	newest: &OwnedEventId,
) -> Result {
	let services = client.services;
	let id = services.timeline.get_pdu_id(older).await?;
	let map = &services.db["pduid_pdu"];
	let saved = map.get(&id).await?.to_vec();
	map.raw_put(&id, b"{".as_slice()).await?;
	services.clear_cache().await;
	sync(client, Some(since), http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	assert_eq!(
		map.get(&id).await?.as_ref(),
		b"{",
		"malformed joined timeline record is preserved"
	);
	map.raw_put(&id, saved.as_slice()).await?;
	services.clear_cache().await;
	let body = sync(client, Some(since), http::StatusCode::OK).await?;
	assert!(
		contains(&body, "join", room, older) && contains(&body, "join", room, newest),
		"restoration retries both joined timeline positions"
	);
	Ok(())
}

async fn left_timeline(client: &Client<'_>, since: &str) -> Result {
	let room = client
		.create_room(&json!({"preset": "public_chat"}))
		.await?;
	let older = send(client, &room, "left-older").await?;
	let newest = send(client, &room, "left-newer").await?;
	client
		.services
		.client
		.clients
		.default
		.post(client.url(&format!("rooms/{room}/leave")))
		.bearer_auth(client.token)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;
	let (user, ..) = client
		.services
		.users
		.find_from_token(client.token)
		.await?;
	assert!(
		poll_until(Duration::from_secs(10), async || client
			.services
			.state_cache
			.rooms_left_state(&user)
			.any(async |(id, _)| id == room)
			.await)
		.await,
		"leave metadata settles before corruption"
	);
	let id = client
		.services
		.timeline
		.get_pdu_id(&older)
		.await?;
	let map = &client.services.db["pduid_pdu"];
	let saved = map.get(&id).await?.to_vec();
	map.raw_put(&id, b"{".as_slice()).await?;
	client.services.clear_cache().await;
	sync(client, Some(since), http::StatusCode::INTERNAL_SERVER_ERROR).await?;
	assert_eq!(
		map.get(&id).await?.as_ref(),
		b"{",
		"malformed left timeline record is preserved"
	);
	map.raw_put(&id, saved.as_slice()).await?;
	client.services.clear_cache().await;
	let body = sync(client, Some(since), http::StatusCode::OK).await?;
	assert!(
		contains(&body, "leave", &room, &older) && contains(&body, "leave", &room, &newest),
		"old cursor retries restored left-room timeline positions"
	);
	Ok(())
}
