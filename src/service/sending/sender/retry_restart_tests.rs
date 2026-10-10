//! Actual HTTP response loss, SIGKILL and immutable replay after cold restart.
//! This fixture also runs against the predecessor without journal APIs.

#[cfg(unix)]
use std::os::unix::{fs::DirBuilderExt, process::ExitStatusExt};
use std::{
	collections::BTreeSet,
	fs,
	io::{Read, Write},
	net::TcpListener,
	path::Path,
	process::{Child, Command},
	time::{Duration, Instant},
};

use futures::TryStreamExt;
use ruma::{
	api::appservice::{Namespaces, Registration, RegistrationInit},
	device_id,
};
use tuwunel_core::{Result, utils::rand};

use super::{
	Delivery, Destination,
	ack_tests::{enqueue, rows},
	edu_tests::Fixture,
};

const TEST: &str = "sending::sender::retry_restart_tests::response_loss_and_sigkill_replay_identical_http_transaction";
const PHASE: &str = "TUWUNEL_TRANSACTION_RESTART_PHASE";
const DIRECTORY: &str = "TUWUNEL_TRANSACTION_RESTART_DIRECTORY";
const ADDRESS: &str = "TUWUNEL_TRANSACTION_RESTART_ADDRESS";

struct OwnedChild(Child);

impl std::ops::Deref for OwnedChild {
	type Target = Child;

	fn deref(&self) -> &Child { &self.0 }
}

impl std::ops::DerefMut for OwnedChild {
	fn deref_mut(&mut self) -> &mut Child { &mut self.0 }
}

impl Drop for OwnedChild {
	fn drop(&mut self) {
		if self.0.try_wait().ok().flatten().is_none() {
			self.0.kill().ok();
			self.0.wait().ok();
		}
	}
}

#[test]
fn response_loss_and_sigkill_replay_identical_http_transaction() -> Result {
	if let Ok(phase) = std::env::var(PHASE) {
		let directory = std::env::var(DIRECTORY).expect("owned fixture root");
		return tokio::runtime::Builder::new_multi_thread()
			.worker_threads(2)
			.enable_all()
			.build()?
			.block_on(child(Path::new(&directory), &phase));
	}
	let root =
		std::env::temp_dir().join(format!("matrix-transaction-restart-{}", rand::string(20)));
	let mut builder = fs::DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&root)?;
	let outcome = parent(&root);
	fs::remove_dir_all(&root)?;
	outcome
}

fn spawn(root: &Path, address: &str, phase: &str) -> Result<OwnedChild> {
	Ok(OwnedChild(
		Command::new(std::env::current_exe()?)
			.args(["--exact", TEST, "--nocapture", "--test-threads=1"])
			.env(PHASE, phase)
			.env(DIRECTORY, root)
			.env(ADDRESS, address)
			.spawn()?,
	))
}

fn wait(child: &mut Child, ready: Option<&Path>) -> Result {
	let deadline = Instant::now()
		.checked_add(Duration::from_secs(45))
		.expect("bounded test deadline");
	loop {
		if let Some(status) = child.try_wait()? {
			assert!(
				ready.is_none() && status.success(),
				"cold phase terminated unexpectedly: {status}"
			);
			return Ok(());
		}
		if ready.is_some_and(Path::exists) {
			return Ok(());
		}
		if Instant::now() >= deadline {
			child.kill()?;
			child.wait()?;
			panic!("cold transaction phase exceeded deadline");
		}
		std::thread::sleep(Duration::from_millis(20));
	}
}

fn capture(
	listener: &TcpListener,
	child: &mut Child,
	acknowledge: bool,
) -> Result<(String, Vec<u8>)> {
	let deadline = Instant::now()
		.checked_add(Duration::from_secs(45))
		.expect("bounded test deadline");
	let mut socket = loop {
		match listener.accept() {
			| Ok((socket, _)) => break socket,
			| Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {},
			| Err(error) => return Err(error.into()),
		}
		if child.try_wait()?.is_some() || Instant::now() >= deadline {
			child.kill().ok();
			child.wait().ok();
			panic!("cold sender did not reach actual HTTP");
		}
		std::thread::sleep(Duration::from_millis(20));
	};
	socket.set_read_timeout(Some(Duration::from_secs(10)))?;
	socket.set_write_timeout(Some(Duration::from_secs(10)))?;
	let mut bytes = Vec::new();
	let (path, body) = loop {
		let mut buffer = [0_u8; 1024];
		let size = socket.read(&mut buffer)?;
		assert_ne!(size, 0, "complete owned HTTP request");
		bytes.extend_from_slice(&buffer[..size]);
		assert!(bytes.len() < 16 * 1024, "owned HTTP fixture request bound");
		let Some(start) = bytes
			.windows(4)
			.position(|part| part == b"\r\n\r\n")
		else {
			continue;
		};
		let headers = std::str::from_utf8(&bytes[..start]).expect("HTTP headers");
		let length = headers
			.lines()
			.find_map(|line| {
				let (key, value) = line.split_once(':')?;
				key.eq_ignore_ascii_case("content-length")
					.then(|| {
						value
							.trim()
							.parse::<usize>()
							.expect("content length")
					})
			})
			.expect("request body length");
		let begin = start
			.checked_add(4)
			.expect("bounded header length");
		let end = begin
			.checked_add(length)
			.expect("bounded content length");
		if bytes.len() >= end {
			let path = headers
				.lines()
				.next()
				.expect("request line")
				.split_whitespace()
				.nth(1)
				.expect("request path")
				.to_owned();
			break (path, bytes[begin..end].to_vec());
		}
	};
	if acknowledge {
		socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")?;
	}
	// The first connection closes after accepting the complete body, without
	// any response. The peer may have committed it; the sender cannot know.
	Ok((path, body))
}

fn parent(root: &Path) -> Result {
	let listener = TcpListener::bind("127.0.0.1:0")?;
	listener.set_nonblocking(true)?;
	let address = format!("http://{}", listener.local_addr()?);
	let mut first = spawn(root, &address, "prepare")?;
	let original = capture(&listener, &mut first, false)?;
	let ready = root.join("ready");
	wait(&mut first, Some(&ready))?;
	first.kill()?;
	let status = first.wait()?;
	#[cfg(unix)]
	assert_eq!(status.signal(), Some(9));
	let mut changed = spawn(root, &address, "changed-owner")?;
	wait(&mut changed, None)?;
	let mut second = spawn(root, &address, "recover")?;
	let replay = capture(&listener, &mut second, true)?;
	wait(&mut second, None)?;
	assert_eq!(replay.0, original.0, "retry must retain transaction ID/path");
	assert_eq!(
		replay.1, original.1,
		"retry body must survive response loss, kill, changed counts and new active tail \
		 byte-for-byte"
	);
	let body: serde_json::Value = serde_json::from_slice(&replay.1)?;
	let devices =
		&body["org.matrix.msc3202.device_one_time_keys_count"]["@restart-bot:localhost"];
	assert!(devices.get("FIRST").is_some());
	assert!(
		devices.get("SECOND").is_none(),
		"post-restart device must not alter the frozen body"
	);
	let mut final_child = spawn(root, &address, "verify")?;
	wait(&mut final_child, None)?;
	Ok(())
}

async fn child(root: &Path, phase: &str) -> Result {
	let fixture = Fixture::open(root).await?;
	let services = &fixture.services;
	let destination = Destination::Appservice("restart-transaction".into());
	if phase == "verify" {
		assert_eq!(
			rows(services, "servercurrentevent_data")
				.await?
				.len(),
			1,
			"unsent successor survives another cold restart"
		);
		fixture.finish().await;
		return Ok(());
	}
	let mut registration: Registration = RegistrationInit {
		id: "restart-transaction".into(),
		url: Some(std::env::var(ADDRESS).expect("owned endpoint")),
		as_token: "disposable-restart-as-token".into(),
		hs_token: "disposable-restart-hs-token".into(),
		sender_localpart: "restart-bot".into(),
		namespaces: Namespaces::new(),
		rate_limited: None,
		protocols: None,
	}
	.into();
	if phase == "changed-owner" {
		registration.hs_token = "disposable-replacement-hs-token".into();
	}
	registration.receive_ephemeral = true;
	registration.msc3202_transaction_extensions = true;
	services
		.appservice
		.load_appservice(registration)
		.await?;
	if phase == "changed-owner" {
		let active = rows(services, "servercurrentevent_data").await?;
		let journal = rows(services, "sendingtransaction_record").await?;
		let global = rows(services, "global").await?;
		let outcome = services
			.sending
			.send_events(destination, vec![super::SendingEvent::Flush])
			.await;
		assert!(
			matches!(outcome, Ok(Delivery::Unprepared(..))),
			"replacement credentials must refuse frozen replay before HTTP"
		);
		assert_eq!(rows(services, "servercurrentevent_data").await?, active);
		assert_eq!(rows(services, "sendingtransaction_record").await?, journal);
		assert_eq!(rows(services, "global").await?, global);
		fixture.finish().await;
		return Ok(());
	}
	let sender = ruma::user_id!("@restart-bot:localhost");
	if phase == "prepare" {
		services
			.users
			.create_device(sender, Some(device_id!("FIRST")), (None, None), None, None, None)
			.await?;
		let item = enqueue(services, &destination, 0).await?;
		services
			.sending
			.db
			.mark_as_active(std::iter::once(&item))
			.await?;
		services
			.sending
			.send_events(destination, vec![item.1])
			.await
			.expect_err("response was lost after peer accepted request");
		fs::write(root.join("ready"), b"ambiguous-http")?;
		// No fixture flush: production persistence owns the durable boundary.
		return std::future::pending().await;
	}
	assert_eq!(phase, "recover");
	services
		.users
		.create_device(sender, Some(device_id!("SECOND")), (None, None), None, None, None)
		.await?;
	let (counts, _) = services
		.sending
		.msc3202_key_counts(BTreeSet::from([sender.to_owned()]), BTreeSet::new())
		.await?;
	assert_eq!(counts[sender].len(), 2, "fresh composition would change MSC3202 counts");
	let tail = enqueue(services, &destination, 1).await?;
	services
		.sending
		.db
		.mark_as_active(std::iter::once(&tail))
		.await?;
	let active = rows(services, "servercurrentevent_data").await?;
	assert_eq!(active.len(), 2);
	let events = services
		.sending
		.db
		.active_requests_for(&destination)
		.map_ok(|(_, event)| event)
		.try_collect::<Vec<_>>()
		.await?;
	let response = services
		.sending
		.send_events(destination.clone(), events)
		.await
		.expect("owned peer acknowledges retry");
	let Delivery::Acknowledged(owner, selected) = response else {
		panic!("actual HTTP acknowledgement required")
	};
	assert_eq!(owner, destination);
	services
		.sending
		.db
		.acknowledge_active(&destination, &selected)
		.await?;
	assert_eq!(
		rows(services, "servercurrentevent_data").await?,
		std::collections::BTreeMap::from([(tail.0.clone(), active[&tail.0].clone())])
	);
	fixture.finish().await;
	Ok(())
}
