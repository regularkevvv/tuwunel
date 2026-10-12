#![cfg(test)]

use std::{
	fs::remove_dir_all, process::id as process_id, str::from_utf8, sync::Arc, time::Duration,
};

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	spawn,
	sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
	time::timeout,
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	ruma::{
		DeviceId, UserId,
		api::appservice::{Namespace, Namespaces, Registration, RegistrationInit},
		device_id,
		to_device::DeviceIdOrAllDevices,
	},
};
use tuwunel_service::{
	Services,
	users::{Register, ToDeviceTarget},
};

const TO_DEVICE: &str = "de.sorunome.msc2409.to_device";
const DEVICE_LISTS: &str = "org.matrix.msc3202.device_lists";
const OTK_COUNT: &str = "org.matrix.msc3202.device_one_time_keys_count";
const FALLBACK: &str = "org.matrix.msc3202.device_unused_fallback_key_types";

/// Boots a server with a stub appservice listener and asserts the transactions
/// it receives: to-device delivery (MSC4203), the MSC3202 transaction
/// extensions gated on the registration opt-in, and stable-yet-distinct
/// transaction IDs.
#[test]
fn appservice_e2ee_transactions() -> Result {
	let db_path = format!("/tmp/tuwunel-test-appservice-txn-{}", process_id());

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.push(format!("database_path=\"{db_path}\""));

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;

	let result: Result = runtime.block_on(async {
		let services = async_start(&server).await?;

		let outcome = run_cases(&services).await;

		server.server.shutdown()?;
		drop(services);

		async_run(&server).await?;
		async_stop(&server).await?;

		outcome
	});

	drop(runtime);
	remove_dir_all(&db_path).ok();

	result
}

async fn run_cases(services: &Arc<Services>) -> Result {
	let listener = TcpListener::bind("127.0.0.1:0").await?;
	let url = format!("http://{}", listener.local_addr()?);

	let (tx, mut rx) = unbounded_channel();
	let stub = spawn(stub_appservice(listener, tx));

	let server_name = services.globals.server_name();
	let on_ghost = UserId::parse_with_server_name("_von_ghost", server_name)?;
	let off_ghost = UserId::parse_with_server_name("_voff_ghost", server_name)?;
	let sender = UserId::parse("@alice:remote.example")?;
	let device = device_id!("GHOSTDEV");
	let content = json!({ "algorithm": "m.olm.v1.curve25519-aes-sha2", "ciphertext": {} });
	for ghost in [&on_ghost, &off_ghost] {
		services
			.users
			.full_register(Register {
				user_id: Some(ghost),
				is_appservice: true,
				..Default::default()
			})
			.await?;
		services
			.users
			.create_device(ghost, Some(device), (None, None), None, None, None)
			.await?;
	}
	register(services, "voicebridge_on", &url, "_von", "@_von_.*", true).await?;
	register(services, "voicebridge_off", &url, "_voff", "@_voff_.*", false).await?;

	// The opted-in bridge gets the to-device event plus an MSC3202 one-time-key
	// count for the device (empty here: the drained-pool signal).
	send_to_device(services, &sender, &on_ghost, device, &content).await?;
	let (first_txn_id, body) = recv_txn(&mut rx).await?;
	let txn = parse(&body)?;
	assert_to_device(&txn, &on_ghost, device, &sender)?;
	assert_otk_entry(&txn, &on_ghost, device)?;

	// Gate isolation: an opted-out bridge receives the to-device event but none
	// of the three MSC3202 fields.
	send_to_device(services, &sender, &off_ghost, device, &content).await?;
	let (_, body) = recv_txn(&mut rx).await?;
	let txn = parse(&body)?;
	assert_to_device(&txn, &off_ghost, device, &sender)?;
	assert_no_msc3202(&txn)?;

	// A device-key change fans out a `device_lists.changed` marker.
	services
		.users
		.mark_device_key_update(&on_ghost)
		.await?;
	let (_, body) = recv_txn(&mut rx).await?;
	let txn = parse(&body)?;
	assert_device_lists_changed(&txn, &on_ghost)?;

	// A byte-identical repeat still ships under a distinct transaction ID so the
	// bridge's txn-id dedup does not drop it.
	send_to_device(services, &sender, &on_ghost, device, &content).await?;
	let (second_txn_id, _) = recv_txn(&mut rx).await?;
	if first_txn_id == second_txn_id {
		return Err!("identical consecutive transactions shared txn id {first_txn_id}");
	}

	stub.abort();

	Ok(())
}

async fn send_to_device(
	services: &Services,
	sender: &UserId,
	target: &UserId,
	device: &DeviceId,
	content: &Value,
) -> Result {
	let before = services
		.users
		.get_to_device_events(target, device, None, None)
		.count()
		.await;
	services
		.users
		.deliver_to_device(
			sender,
			"m.room.encrypted",
			&[ToDeviceTarget {
				user_id: target.to_owned(),
				device: DeviceIdOrAllDevices::DeviceId(device.to_owned()),
				content: content.clone(),
			}],
			None,
		)
		.await?;
	let after = services
		.users
		.get_to_device_events(target, device, None, None)
		.count()
		.await;
	assert_eq!(
		after,
		before.saturating_add(1),
		"appservice copy accompanies admitted inbox event"
	);
	Ok(())
}

async fn register(
	services: &Services,
	id: &str,
	url: &str,
	sender_localpart: &str,
	user_regex: &str,
	msc3202: bool,
) -> Result {
	let mut namespaces = Namespaces::new();
	namespaces.users = vec![Namespace::new(true, user_regex.to_owned())];

	let mut registration: Registration = RegistrationInit {
		id: id.to_owned(),
		url: Some(url.to_owned()),
		as_token: format!("{id}_as_token"),
		hs_token: format!("{id}_hs_token"),
		sender_localpart: sender_localpart.to_owned(),
		namespaces,
		rate_limited: None,
		protocols: None,
	}
	.into();

	registration.msc3202_transaction_extensions = msc3202;

	services
		.appservice
		.register_appservice(registration)
		.await
}

async fn recv_txn(rx: &mut UnboundedReceiver<(String, Vec<u8>)>) -> Result<(String, Vec<u8>)> {
	timeout(Duration::from_secs(10), rx.recv())
		.await
		.map_err(|_| err!("timed out waiting for an appservice transaction"))?
		.ok_or_else(|| err!("stub appservice channel closed"))
}

fn parse(body: &[u8]) -> Result<Value> {
	serde_json::from_slice(body)
		.map_err(|e| err!("appservice transaction body was not json: {e}"))
}

fn assert_to_device(txn: &Value, target: &UserId, device: &DeviceId, sender: &UserId) -> Result {
	let event = txn
		.get(TO_DEVICE)
		.and_then(Value::as_array)
		.and_then(|events| events.first())
		.ok_or_else(|| err!("transaction had no {TO_DEVICE} entry"))?;

	let field = |name: &str| {
		event
			.get(name)
			.and_then(Value::as_str)
			.map(ToOwned::to_owned)
			.unwrap_or_default()
	};

	if field("type") != "m.room.encrypted"
		|| field("sender") != sender.as_str()
		|| field("to_user_id") != target.as_str()
		|| field("to_device_id") != device.as_str()
		|| event.get("content").is_none()
	{
		return Err!("unexpected to-device entry: {event}");
	}

	Ok(())
}

fn assert_otk_entry(txn: &Value, target: &UserId, device: &DeviceId) -> Result {
	txn.get(OTK_COUNT)
		.and_then(|counts| counts.get(target.as_str()))
		.and_then(|devices| devices.get(device.as_str()))
		.ok_or_else(|| {
			err!("transaction lacked an {OTK_COUNT} entry for the addressed device")
		})?;

	Ok(())
}

fn assert_no_msc3202(txn: &Value) -> Result {
	for field in [DEVICE_LISTS, OTK_COUNT, FALLBACK] {
		if txn.get(field).is_some() {
			return Err!("opted-out bridge received the gated field {field}");
		}
	}

	Ok(())
}

fn assert_device_lists_changed(txn: &Value, target: &UserId) -> Result {
	let changed = txn
		.get(DEVICE_LISTS)
		.and_then(|lists| lists.get("changed"))
		.and_then(Value::as_array)
		.ok_or_else(|| err!("transaction had no {DEVICE_LISTS}.changed array"))?;

	changed
		.iter()
		.any(|user| user.as_str() == Some(target.as_str()))
		.then_some(())
		.ok_or_else(|| err!("{DEVICE_LISTS}.changed did not contain the re-keyed user"))
}

/// Minimal appservice transaction endpoint: captures the `{txnId}` path segment
/// and the request body from each `PUT /transactions/{txnId}`, then answers an
/// empty `200`.
async fn stub_appservice(listener: TcpListener, tx: UnboundedSender<(String, Vec<u8>)>) {
	while let Ok((mut socket, _)) = listener.accept().await {
		if let Some((txn_id, body)) = read_request(&mut socket).await {
			tx.send((txn_id, body)).ok();
		}

		socket
			.write_all(
				b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
			)
			.await
			.ok();
		socket.flush().await.ok();
	}
}

async fn read_request(socket: &mut TcpStream) -> Option<(String, Vec<u8>)> {
	let mut buf = Vec::new();
	let mut chunk = [0_u8; 4096];
	loop {
		if let Some(head_end) = find(&buf, b"\r\n\r\n") {
			let content_length = content_length(&buf[..head_end])?;
			let body_start = head_end.checked_add(4)?;
			let body_end = body_start.checked_add(content_length)?;
			if buf.len() >= body_end {
				let txn_id = txn_id(&buf[..head_end])?;
				let body = buf[body_start..body_end].to_vec();

				return Some((txn_id, body));
			}
		}

		let read = socket.read(&mut chunk).await.ok()?;
		if read == 0 {
			return None;
		}

		buf.extend_from_slice(&chunk[..read]);
	}
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
	haystack
		.windows(needle.len())
		.position(|window| window == needle)
}

fn content_length(head: &[u8]) -> Option<usize> {
	from_utf8(head)
		.ok()?
		.lines()
		.find_map(|line| {
			line.split_once(':')
				.filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
		})
		.and_then(|(_, value)| value.trim().parse().ok())
}

fn txn_id(head: &[u8]) -> Option<String> {
	from_utf8(head)
		.ok()?
		.lines()
		.next()?
		.split(' ')
		.nth(1)?
		.rsplit('/')
		.next()?
		.split('?')
		.next()
		.map(ToOwned::to_owned)
}

const COLD_PHASE: &str = "TUWUNEL_ATOMIC_AS_PHASE";
const COLD_DIRECTORY: &str = "TUWUNEL_ATOMIC_AS_DIRECTORY";
const COLD_GATEWAY: &str = "TUWUNEL_ATOMIC_AS_GATEWAY";
const COLD_TOKEN: &str = "disposable-canonical-AS-client-token";
const COLD_TEST: &str = "canonical_to_device_appservice_copy_survives_sigkill";

struct ColdChild(std::process::Child);
impl Drop for ColdChild {
	fn drop(&mut self) {
		self.0.kill().ok();
		self.0.wait().ok();
	}
}
struct ColdDirectory(std::path::PathBuf);
impl Drop for ColdDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

/// An acknowledged inbox+dedup+AS admission survives actual process SIGKILL.
/// The unacknowledged HTTP attempt replays unchanged; the API retry adds none.
#[test]
fn canonical_to_device_appservice_copy_survives_sigkill() -> Result {
	if let Ok(phase) = std::env::var(COLD_PHASE) {
		let directory = std::env::var(COLD_DIRECTORY)
			.map_err(|_| err!("child database directory is missing or invalid"))?;
		return cold_child(&std::path::PathBuf::from(directory), &phase);
	}
	let directory = ColdDirectory(std::env::temp_dir().join(format!(
		"atomic-AS-cold-{}-{}",
		process_id(),
		tuwunel_core::utils::rand::string(12)
	)));
	std::fs::DirBuilder::new().create(&directory.0)?;
	tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?.block_on(async {
		let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
		let url = format!("http://{}", listener.local_addr()?);
		let mut accepted = cold_spawn(&directory.0, &url, "accept")?;
		let first = timeout(Duration::from_secs(30), listener.accept()).await
			.expect("accepted appservice HTTP deadline")?;
		let mut socket = first.0;
		let (first_id, first_body) = timeout(Duration::from_secs(10), read_request(&mut socket)).await
			.expect("first HTTP body deadline").expect("complete first HTTP attempt");
		let deadline = cold_deadline(10);
		while !directory.0.join("accepted").exists() {
			assert!(accepted.0.try_wait()?.is_none(), "accepting child exited before API acknowledgement");
			assert!(tokio::time::Instant::now() < deadline, "API acknowledgement deadline");
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		accepted.0.kill()?;
		let status = accepted.0.wait()?;
		assert!(!status.success());
		#[cfg(unix)]
		assert_eq!(std::os::unix::process::ExitStatusExt::signal(&status), Some(9));
		drop(socket);
		let mut reopened = cold_spawn(&directory.0, &url, "reopen")?;
		let (mut socket, _) = timeout(Duration::from_secs(30), listener.accept()).await
			.expect("cold replay HTTP deadline")?;
		let (next_id, next_body) = timeout(Duration::from_secs(10), read_request(&mut socket)).await
			.expect("replay HTTP body deadline").expect("complete replay attempt");
		assert_eq!(next_id, first_id, "immutable transaction identity survives SIGKILL");
		assert_eq!(next_body, first_body, "frozen to-device body survives SIGKILL");
		socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await?;
		socket.flush().await?;
		drop(socket);
		let deadline = cold_deadline(30);
		loop {
			if let Some(status) = reopened.0.try_wait()? {
				assert!(status.success(), "cold recovery and API dedup child must pass");
				break;
			}
			assert!(tokio::time::Instant::now() < deadline, "recovery child deadline");
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		timeout(Duration::from_millis(250), listener.accept()).await
			.expect_err("API retry must not create another AS transaction");
		Ok(())
	})
}

fn cold_spawn(directory: &std::path::Path, gateway: &str, phase: &str) -> Result<ColdChild> {
	Ok(ColdChild(
		std::process::Command::new(std::env::current_exe()?)
			.args(["--exact", COLD_TEST, "--nocapture"])
			.env(COLD_PHASE, phase)
			.env(COLD_DIRECTORY, directory)
			.env(COLD_GATEWAY, gateway)
			.spawn()?,
	))
}

fn cold_deadline(seconds: u64) -> tokio::time::Instant {
	tokio::time::Instant::now()
		.checked_add(Duration::from_secs(seconds))
		.expect("bounded cold-test deadline")
}

fn cold_child(directory: &std::path::Path, phase: &str) -> Result {
	let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let mut args = Args::default_test(if phase == "accept" { &["fresh"] } else { &[] });
	args.option.extend([
		format!("database_path={:?}", directory.join("database")),
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
			let deadline = cold_deadline(10);
			loop {
				if services
					.client
					.clients
					.default
					.get(format!("{base}/_matrix/client/versions"))
					.send()
					.await
					.is_ok()
				{
					break;
				}
				assert!(tokio::time::Instant::now() < deadline, "client listener ready deadline");
				tokio::time::sleep(Duration::from_millis(20)).await;
			}
			let sender =
				UserId::parse_with_server_name("atomic_sender", services.globals.server_name())?;
			let target =
				UserId::parse_with_server_name("_kill_ghost", services.globals.server_name())?;
			let device = device_id!("KILLDEV");
			if phase == "accept" {
				services
					.users
					.full_register(Register {
						user_id: Some(&sender),
						password: Some("disposable-atomic-password"),
						..Default::default()
					})
					.await?;
				services
					.users
					.create_device(&sender, None, (Some(COLD_TOKEN), None), None, None, None)
					.await?;
				services
					.users
					.full_register(Register {
						user_id: Some(&target),
						is_appservice: true,
						..Default::default()
					})
					.await?;
				services
					.users
					.create_device(&target, Some(device), (None, None), None, None, None)
					.await?;
				register(
					&services,
					"atomic_cold",
					&std::env::var(COLD_GATEWAY)
						.map_err(|_| err!("child gateway address is missing or invalid"))?,
					"_kill_bot",
					"^@_kill_ghost:.*$",
					false,
				)
				.await?;
			}
			let content = json!({"algorithm":"m.olm.v1.curve25519-aes-sha2","ciphertext":{}});
			let body = json!({"messages":{(target.as_str()):{(device.as_str()):content}}});
			let response = services
				.client
				.clients
				.default
				.put(format!(
					"{base}/_matrix/client/v3/sendToDevice/m.room.encrypted/atomic-cold-txn"
				))
				.bearer_auth(COLD_TOKEN)
				.json(&body)
				.send()
				.await?
				.error_for_status()?;
			assert_eq!(response.status(), reqwest::StatusCode::OK);
			assert_eq!(
				services
					.users
					.get_to_device_events(&target, device, None, None)
					.count()
					.await,
				1
			);
			let devices = services.users.bounded_device_ids(&sender).await?;
			assert_eq!(devices.len(), 1);
			let marker = services
				.transaction_ids
				.existing_txnid(&sender, Some(&devices[0]), "atomic-cold-txn".into())
				.await?;
			assert!(marker.is_empty());
			if phase == "accept" {
				std::fs::write(directory.join("accepted"), b"API inbox/AS/dedup acknowledged")?;
				std::future::pending::<()>().await;
			}
			let deadline = cold_deadline(20);
			loop {
				let pending = services.db["servernameevent_data"]
					.raw_keys_prefix(b"+atomic_cold\xff")
					.count()
					.await;
				let active = services.db["servercurrentevent_data"]
					.raw_keys_prefix(b"+atomic_cold\xff")
					.count()
					.await;
				let attempts = services.db["sendingtransaction_record"]
					.raw_keys_prefix(b"+atomic_cold\xff")
					.count()
					.await;
				let witness = services.db["global"]
					.get(b"\x06+atomic_cold\xff")
					.await;
				if pending == 0 && active == 0 && attempts == 0 {
					let absent =
						witness.expect_err("ACK removes the destination ownership witness");
					assert!(absent.is_not_found());
					break;
				}
				assert!(tokio::time::Instant::now() < deadline, "owned AS rows drain after ACK");
				tokio::time::sleep(Duration::from_millis(20)).await;
			}
			Ok(())
		};
		let exercise = async {
			let result = exercise.await;
			let shutdown = server.server.shutdown();
			result.and(shutdown)
		};
		let (run, result) = tokio::join!(async_run(&server), exercise);
		drop(services);
		let stop = async_stop(&server).await;
		result.and(run).and(stop)
	});
	drop(runtime);
	result
}
