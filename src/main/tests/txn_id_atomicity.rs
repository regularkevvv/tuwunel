#![cfg(test)]

//! Transaction ids commit with the effect they deduplicate (phase 2 gate F2).
//! A room send records its transaction in the event's own commit, and a
//! to-device send records it in the one commit that fills every local inbox,
//! so no interruption can leave the effect durable without the record and let
//! a retry apply the request twice. Each case retries over HTTP, then stages
//! an interruption by making the commit directly, as the route would, and
//! retries that too.

use std::{env::var, net::TcpListener, path::PathBuf, process::id as process_id, time::Duration};

use futures::{StreamExt, future::join};
use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	sync::mpsc::unbounded_channel,
	time::timeout,
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result,
	matrix::pdu::PduBuilder,
	ruma::{
		DeviceId, OwnedDeviceId, RoomId, TransactionId, UserId,
		api::appservice::{Namespace, Namespaces, Registration, RegistrationInit},
		events::room::message::RoomMessageEventContent,
		to_device::DeviceIdOrAllDevices,
	},
};
use tuwunel_database::refusal;
use tuwunel_service::{Services, transaction_ids, users::ToDeviceTarget};

use self::client::{Client, field, register, wait_until_ready};

mod client;

const ALICE_TOKEN: &str = "txn-id-atomicity-alice-access-token-01";
const BOB_TOKEN: &str = "txn-id-atomicity-bob-access-token-00001";
const BOB_SECOND_TOKEN: &str = "txn-id-atomicity-bob-second-token-0001";

#[test]
fn transaction_ids_commit_with_their_effects() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/tmp".into());
	let db_path = PathBuf::from(root).join(format!("tuwunel-txn-id-atomicity-{}", process_id()));

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={db_path:?}"),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"log=\"warn\"".to_owned(),
	]);

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);

		let exercise = async {
			let outcome = exercise(&services, &base).await;
			let shutdown = server.server.shutdown();

			outcome.and(shutdown)
		};
		let (run_result, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run_result?;

		outcome
	});

	drop(runtime);

	result
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let alice = register(services, "txnidalice", ALICE_TOKEN).await?;
	let bob = register(services, "txnidbob", BOB_TOKEN).await?;
	let client = Client { services, base, token: ALICE_TOKEN };
	let room = client
		.create_room(&json!({ "preset": "private_chat" }))
		.await?;

	room_send(services, &client, &alice, &room).await?;
	to_device(services, &client, &alice, &bob).await?;
	atomic_appservice_forwarding(services, &client, &alice, &bob).await?;

	Ok(())
}

/// A retried room send answers with the event the first attempt appended,
/// and that event's commit carries the transaction record.
async fn room_send(
	services: &Services,
	client: &Client<'_>,
	alice: &UserId,
	room: &RoomId,
) -> Result {
	let device = only_device(services, alice).await?;

	let first = send_message(client, room, "send-retry").await?;
	let second = send_message(client, room, "send-retry").await?;
	if first != second {
		return Err!("a retried send appended {second} after {first}");
	}

	let latest = services.timeline.latest_pdu_in_room(room).await?;
	if latest.event_id.as_str() != first {
		return Err!("a retried send appended {} after {first}", latest.event_id);
	}

	// The event and its record commit, and the response never leaves.
	let txn_id: &TransactionId = "send-interrupted".into();
	let txnid = transaction_ids::key(alice, Some(&device), txn_id);
	let state_lock = services.state.mutex.lock(room).await;
	let committed = services
		.timeline
		.build_and_append_pdu_with_txnid(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain("interrupted")),
			alice,
			room,
			Some(&txnid),
			&state_lock,
		)
		.await?;
	drop(state_lock);

	let retried = send_message(client, room, txn_id.as_str()).await?;
	if retried != committed.as_str() {
		return Err!("the retry of an interrupted send answered {retried}, not {committed}");
	}

	let latest = services.timeline.latest_pdu_in_room(room).await?;
	if latest.event_id != committed {
		return Err!("the retry of an interrupted send appended {}", latest.event_id);
	}

	// The commit that made the event durable is the one holding the record.
	let pdu_id = services.timeline.get_pdu_id(&committed).await?;
	let pdu = services.timeline.get_pdu(&committed).await?;
	let json = services.timeline.get_pdu_json(&committed).await?;
	let txn = services
		.timeline
		.append_pdu_txn(&pdu_id, &pdu, &json, Some(&txnid), None);

	let keys: Vec<(String, Vec<u8>)> = txn
		.keys()
		.map(|(map, key)| (map.name().to_owned(), key.to_vec()))
		.collect();

	let pdu_key: &[u8] = pdu_id.as_ref();
	for (map, key) in [
		("pduid_pdu", pdu_key),
		("eventid_pduid", committed.as_bytes()),
		("userdevicetxnid_response", txnid.as_slice()),
	] {
		if !keys
			.iter()
			.any(|(name, queued)| name == map && queued == key)
		{
			return Err!("the event's commit lacks its {map} key");
		}
	}

	Ok(())
}

/// A retried to-device send delivers once, to one device or to all of them;
/// the retry of an interrupted one delivers nothing; and the commit holds
/// every delivery with the transaction record.
async fn to_device(
	services: &Services,
	client: &Client<'_>,
	alice: &UserId,
	bob: &UserId,
) -> Result {
	let alice_device = only_device(services, alice).await?;
	let bob_first = only_device(services, bob).await?;
	let bob_second = services
		.users
		.create_device(bob, None, (Some(BOB_SECOND_TOKEN), None), None, None, None)
		.await?;

	let one = json!({ "messages": { (bob.as_str()): { (bob_first.as_str()): { "n": 1 } } } });
	send_to_device(client, "to-device-one", &one).await?;
	send_to_device(client, "to-device-one", &one).await?;
	expect_inbox(services, bob, &bob_first, 1).await?;
	expect_inbox(services, bob, &bob_second, 0).await?;

	let all = json!({ "messages": { (bob.as_str()): { "*": { "n": 2 } } } });
	send_to_device(client, "to-device-all", &all).await?;
	send_to_device(client, "to-device-all", &all).await?;
	expect_inbox(services, bob, &bob_first, 2).await?;
	expect_inbox(services, bob, &bob_second, 1).await?;

	// Every delivery and the record commit, and the response never leaves.
	let txn_id: &TransactionId = "to-device-interrupted".into();
	let txnid = transaction_ids::key(alice, Some(&alice_device), txn_id);
	let content = json!({ "n": 3 });
	let targets = [ToDeviceTarget {
		user_id: bob.to_owned(),
		device: DeviceIdOrAllDevices::AllDevices,
		content: content.clone(),
	}];

	services
		.users
		.deliver_to_device(alice, "m.test.ping", &targets, Some(&txnid))
		.await?;
	expect_inbox(services, bob, &bob_first, 3).await?;
	expect_inbox(services, bob, &bob_second, 2).await?;

	let retry = json!({ "messages": { (bob.as_str()): { "*": content.clone() } } });
	send_to_device(client, txn_id.as_str(), &retry).await?;
	expect_inbox(services, bob, &bob_first, 3).await?;
	expect_inbox(services, bob, &bob_second, 2).await?;

	let record = services
		.transaction_ids
		.existing_txnid(alice, Some(&alice_device), txn_id)
		.await?;
	if !record.is_empty() {
		return Err!("a to-device transaction record carried a response");
	}

	// Both inbox rows and the record are one commit.
	let txn = services.users.to_device_txn(
		alice,
		"m.test.ping",
		[(bob, &*bob_first, 1_u64, &content), (bob, &*bob_second, 2, &content)],
		Some(&txnid),
	);

	let inbox_rows = txn
		.keys()
		.filter(|(map, _)| map.name() == "todeviceid_events")
		.count();
	let records: Vec<Vec<u8>> = txn
		.keys()
		.filter(|(map, _)| map.name() == "userdevicetxnid_response")
		.map(|(_, key)| key.to_vec())
		.collect();

	if txn.len() != 3 || inbox_rows != 2 || records != [txnid] {
		return Err!("the to-device commit held {inbox_rows} inbox rows and {records:?} records");
	}

	Ok(())
}

async fn send_message(client: &Client<'_>, room: &RoomId, txn_id: &str) -> Result<String> {
	let response: Value = client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/{txn_id}")))
		.bearer_auth(client.token)
		.json(&json!({ "msgtype": "m.text", "body": "once" }))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;

	Ok(field(&response, "event_id")?.to_owned())
}

async fn send_to_device(client: &Client<'_>, txn_id: &str, body: &Value) -> Result {
	client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("sendToDevice/m.test.ping/{txn_id}")))
		.bearer_auth(client.token)
		.json(body)
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn expect_inbox(
	services: &Services,
	user: &UserId,
	device: &DeviceId,
	expected: usize,
) -> Result {
	let held = services
		.users
		.get_to_device_events(user, device, None, None)
		.count()
		.await;

	if held != expected {
		return Err!("{user} {device} holds {held} to-device events, expected {expected}");
	}

	Ok(())
}

async fn only_device(services: &Services, user: &UserId) -> Result<OwnedDeviceId> {
	let devices: Vec<OwnedDeviceId> = services
		.users
		.all_device_ids(user)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	match devices.as_slice() {
		| [device] => Ok(device.clone()),
		| _ => Err!("{user} has {} devices, expected one", devices.len()),
	}
}

/// Refusing the coupled AS row refuses inbox and dedup; a real HTTP retry
/// delivers one copy and a repeated request remains a no-op.
async fn atomic_appservice_forwarding(
	services: &Services,
	client: &Client<'_>,
	alice: &UserId,
	bob: &UserId,
) -> Result {
	let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
	let url = format!("http://{}", listener.local_addr()?);
	let (tx, mut rx) = unbounded_channel();
	let gateway = tokio::spawn(async move {
		while let Ok((mut socket, _)) = listener.accept().await {
			let mut bytes = Vec::new();
			let mut block = [0_u8; 4096];
			loop {
				let count = socket
					.read(&mut block)
					.await
					.expect("owned gateway read");
				if count == 0 {
					break;
				}
				bytes.extend_from_slice(&block[..count]);
				if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
					let head = std::str::from_utf8(&bytes[..end]).expect("HTTP headers");
					let len = head
						.lines()
						.find_map(|line| {
							line.split_once(':')
								.filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
								.and_then(|(_, v)| v.trim().parse::<usize>().ok())
						})
						.expect("HTTP body length");
					let body_start = end.saturating_add(4);
					let body_end = body_start.saturating_add(len);
					if bytes.len() >= body_end {
						tx.send(bytes[body_start..body_end].to_vec())
							.expect("owned receiver");
						break;
					}
				}
			}
			socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.expect("owned gateway ACK");
		}
	});
	let mut namespaces = Namespaces::new();
	namespaces.users = vec![Namespace::new(true, "^@txnidbob:.*$".into())];
	let registration: Registration = RegistrationInit {
		id: "atomic-forwarding".into(),
		url: Some(url),
		as_token: "disposable-atomic-AS-token".into(),
		hs_token: "disposable-atomic-HS-token".into(),
		sender_localpart: "atomic_forwarding_bot".into(),
		namespaces,
		rate_limited: None,
		protocols: None,
	}
	.into();
	services
		.appservice
		.register_appservice(registration)
		.await?;
	let alice_device = only_device(services, alice).await?;
	let device = services
		.users
		.bounded_device_ids(bob)
		.await?
		.into_iter()
		.next()
		.expect("Bob has existing inbox devices");
	let before = services
		.users
		.get_to_device_events(bob, &device, None, None)
		.count()
		.await;
	let txn_id: &TransactionId = "coupled-forwarding-refusal".into();
	let body = json!({"messages":{(bob.as_str()):{(device.as_str()):{"atomic":true}}}});
	refusal::refuse_next("servernameevent_data");
	send_to_device(client, txn_id.as_str(), &body)
		.await
		.expect_err("queue refusal must refuse entire inbox admission");
	assert_eq!(refusal::pending(), 0, "actual coupled transaction reached queue refusal");
	assert_eq!(
		services
			.users
			.get_to_device_events(bob, &device, None, None)
			.count()
			.await,
		before
	);
	let absent = services
		.transaction_ids
		.existing_txnid(alice, Some(&alice_device), txn_id)
		.await
		.expect_err("refused inbox has no dedup marker");
	assert!(absent.is_not_found());
	timeout(Duration::from_millis(200), rx.recv())
		.await
		.expect_err("refusal cannot dispatch AS copy");
	send_to_device(client, txn_id.as_str(), &body).await?;
	send_to_device(client, txn_id.as_str(), &body).await?;
	assert_eq!(
		services
			.users
			.get_to_device_events(bob, &device, None, None)
			.count()
			.await,
		before.saturating_add(1)
	);
	let payload = timeout(Duration::from_secs(10), rx.recv())
		.await
		.expect("forwarding deadline")
		.expect("forwarding body");
	let payload: Value = serde_json::from_slice(&payload)?;
	let events = payload["de.sorunome.msc2409.to_device"]
		.as_array()
		.expect("actual to-device extension");
	assert_eq!(events.len(), 1);
	assert_eq!(events[0]["content"]["atomic"], true);
	timeout(Duration::from_millis(200), rx.recv())
		.await
		.expect_err("dedup retry cannot forward twice");
	gateway.abort();
	Ok(())
}
