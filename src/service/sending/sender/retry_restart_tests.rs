//! Actual HTTP response loss, SIGKILL and immutable replay after cold restart.
//! This fixture also runs against the predecessor without journal APIs.

#[cfg(unix)]
use std::os::unix::{fs::DirBuilderExt, process::ExitStatusExt};
use std::{
	collections::{BTreeMap, BTreeSet},
	fs,
	io::{Read, Write},
	net::TcpListener,
	path::Path,
	process::{Child, Command},
	time::{Duration, Instant},
};

use futures::TryStreamExt;
use ruma::{
	OneTimeKeyAlgorithm, OwnedDeviceId, OwnedOneTimeKeyId, UserId,
	api::appservice::{Namespaces, Registration, RegistrationInit},
	device_id,
	encryption::{OneTimeKey, SignedKey},
	serde::{Base64, Raw},
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
const BOUNDARY_TEST: &str = "sending::sender::retry_restart_tests::maximum_producer_singleton_with_full_key_rows_replays_and_drains";
const BOUNDARY_PHASE: &str = "TUWUNEL_SINGLETON_BOUNDARY_PHASE";
const BOUNDARY_DEVICE_COUNT: usize = 128;
const BOUNDARY_ALGORITHM_COUNT: usize = 16;
const BOUNDARY_ESCAPES: &str = "quote\" slash\\ newline\n snowman☃ ";

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
	spawn_test(root, address, phase, TEST, PHASE)
}

fn spawn_test(
	root: &Path,
	address: &str,
	phase: &str,
	test: &str,
	phase_variable: &str,
) -> Result<OwnedChild> {
	Ok(OwnedChild(
		Command::new(std::env::current_exe()?)
			.args(["--exact", test, "--nocapture", "--test-threads=1"])
			.env(phase_variable, phase)
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
	capture_with_limit(listener, child, acknowledge, 16 * 1024)
}

fn capture_with_limit(
	listener: &TcpListener,
	child: &mut Child,
	acknowledge: bool,
	request_limit: usize,
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
		assert!(bytes.len() < request_limit, "owned HTTP fixture request bound");
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
		BTreeMap::from([(tail.0.clone(), active[&tail.0].clone())])
	);
	fixture.finish().await;
	Ok(())
}

#[test]
fn maximum_producer_singleton_with_full_key_rows_replays_and_drains() -> Result {
	if let Ok(phase) = std::env::var(BOUNDARY_PHASE) {
		let directory = std::env::var(DIRECTORY).expect("owned boundary fixture root");
		return tokio::runtime::Builder::new_multi_thread()
			.worker_threads(2)
			.enable_all()
			.build()?
			.block_on(boundary_child(Path::new(&directory), &phase));
	}
	let root =
		std::env::temp_dir().join(format!("matrix-singleton-boundary-{}", rand::string(20)));
	let mut builder = fs::DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&root)?;
	let outcome = boundary_parent(&root);
	fs::remove_dir_all(&root)?;
	outcome
}

fn boundary_device(index: usize) -> OwnedDeviceId {
	format!("D{index:03}{}", "x".repeat(92)).into()
}

fn boundary_parent(root: &Path) -> Result {
	let listener = TcpListener::bind("127.0.0.1:0")?;
	listener.set_nonblocking(true)?;
	let address = format!("http://{}", listener.local_addr()?);
	let request_limit = super::super::data::BODY_LIMIT + 16 * 1024;
	let mut first = spawn_test(root, &address, "prepare", BOUNDARY_TEST, BOUNDARY_PHASE)?;
	let original = capture_with_limit(&listener, &mut first, false, request_limit)?;
	wait(&mut first, Some(&root.join("boundary-ready")))?;
	first.kill()?;
	let status = first.wait()?;
	#[cfg(unix)]
	assert_eq!(status.signal(), Some(9));
	let mut second = spawn_test(root, &address, "recover", BOUNDARY_TEST, BOUNDARY_PHASE)?;
	let replay = capture_with_limit(&listener, &mut second, true, request_limit)?;
	let successor = capture_with_limit(&listener, &mut second, true, request_limit)?;
	wait(&mut second, None)?;
	assert_eq!(original, replay, "maximum accepted bytes and transaction ID survive SIGKILL");
	assert_ne!(successor.0, replay.0, "successor owns a fresh transaction identity");
	assert!(replay.1.len() > tuwunel_bridge::MAX_VALUE_BYTES);
	assert!(replay.1.len() <= super::super::data::BODY_LIMIT);
	let body: serde_json::Value = serde_json::from_slice(&replay.1)?;
	let sender = "@singleton-bot:localhost";
	let counts = body["org.matrix.msc3202.device_one_time_keys_count"][sender]
		.as_object()
		.expect("full sender device metadata");
	let fallbacks = body["org.matrix.msc3202.device_unused_fallback_key_types"][sender]
		.as_object()
		.expect("full unused fallback metadata");
	assert_eq!(counts.len(), BOUNDARY_DEVICE_COUNT);
	assert_eq!(fallbacks.len(), BOUNDARY_DEVICE_COUNT);
	for device in counts.values() {
		assert_eq!(
			device
				.as_object()
				.expect("algorithm counts")
				.len(),
			BOUNDARY_ALGORITHM_COUNT
		);
		assert_eq!(device["signed_curve25519"], 1);
	}
	for device in fallbacks.values() {
		assert_eq!(
			device
				.as_array()
				.expect("unused algorithms")
				.len(),
			BOUNDARY_ALGORITHM_COUNT
		);
	}
	assert_eq!(
		body["org.matrix.msc3202.device_one_time_keys_count"]["@singleton-target:localhost"]
			["TARGET"]["signed_curve25519"],
		0
	);
	assert_eq!(
		body["de.sorunome.msc2409.to_device"]
			.as_array()
			.expect("one admitted delivery")
			.len(),
		1
	);
	let content = body["de.sorunome.msc2409.to_device"][0]["content"]["value"]
		.as_str()
		.expect("accepted escaped content");
	assert!(content.starts_with(BOUNDARY_ESCAPES));
	let next: serde_json::Value = serde_json::from_slice(&successor.1)?;
	assert_eq!(next["ephemeral"][0]["type"], "m.typing");
	assert_eq!(
		next["org.matrix.msc3202.device_one_time_keys_count"][sender]
			[boundary_device(0).as_str()]["signed_curve25519"],
		0,
		"successor composes fresh counts after retry ACK"
	);
	let mut final_child = spawn_test(root, &address, "verify", BOUNDARY_TEST, BOUNDARY_PHASE)?;
	wait(&mut final_child, None)?;
	Ok(())
}

async fn boundary_seed(services: &crate::Services, sender: &UserId) -> Result {
	let target = ruma::user_id!("@singleton-target:localhost");
	services.users.create(target, None, None).await?;
	services
		.users
		.create_device(target, Some(device_id!("TARGET")), (None, None), None, None, None)
		.await?;
	services.users.create(sender, None, None).await?;
	let key = Raw::new(&OneTimeKey::SignedKey(SignedKey::new(
		Base64::new(vec![42_u8; 32]),
		Default::default(),
	)))?;
	let fallback = Raw::new(&OneTimeKey::SignedKey(SignedKey::new_fallback(
		Base64::new(vec![43_u8; 32]),
		Default::default(),
	)))?;
	let algorithms = std::iter::once("signed_curve25519".to_owned())
		.chain(
			(1..BOUNDARY_ALGORITHM_COUNT).map(|index| format!("org.example.boundary{index:02}")),
		)
		.collect::<Vec<_>>();
	let keys = algorithms
		.iter()
		.map(|algorithm| {
			Ok((OwnedOneTimeKeyId::try_from(format!("{algorithm}:otk"))?, key.clone()))
		})
		.collect::<Result<BTreeMap<_, _>>>()?;
	let fallbacks = algorithms
		.iter()
		.map(|algorithm| {
			Ok((OwnedOneTimeKeyId::try_from(format!("{algorithm}:fallback"))?, fallback.clone()))
		})
		.collect::<Result<BTreeMap<_, _>>>()?;
	for index in 0..BOUNDARY_DEVICE_COUNT {
		let device = boundary_device(index);
		services
			.users
			.create_device(sender, Some(&device), (None, None), None, None, None)
			.await?;
		services
			.users
			.add_one_time_keys(sender, &device, &keys, keys.len())
			.await?;
		services
			.users
			.add_fallback_keys(
				sender,
				&device,
				fallbacks
					.iter()
					.map(|(id, key)| (id.as_ref(), key)),
			)
			.await?;
	}
	assert_eq!(
		rows(services, "onetimekeyid4225_otk")
			.await?
			.len(),
		2048
	);
	assert_eq!(
		rows(services, "userdeviceidalgorithm_fallback")
			.await?
			.len(),
		2048
	);
	let (counts, fallbacks) = services
		.sending
		.msc3202_key_counts(
			BTreeSet::from([sender.to_owned()]),
			BTreeSet::from([(target.to_owned(), device_id!("TARGET").to_owned())]),
		)
		.await?;
	assert_eq!(counts[sender].len(), BOUNDARY_DEVICE_COUNT);
	assert_eq!(counts[target].len(), 1);
	assert!(
		counts[sender]
			.values()
			.all(|algorithms| algorithms.len() == BOUNDARY_ALGORITHM_COUNT)
	);
	assert!(
		fallbacks[sender]
			.values()
			.all(|algorithms| algorithms.len() == BOUNDARY_ALGORITHM_COUNT)
	);
	Ok(())
}

async fn boundary_register(services: &crate::Services) -> Result {
	let mut registration: Registration = RegistrationInit {
		id: "singleton-boundary".into(),
		url: Some(std::env::var(ADDRESS).expect("owned boundary endpoint")),
		as_token: "disposable-boundary-as-token".into(),
		hs_token: "disposable-boundary-hs-token".into(),
		sender_localpart: "singleton-bot".into(),
		namespaces: serde_json::from_value::<Namespaces>(serde_json::json!({
			"users":[{"exclusive":false,"regex":"^@singleton-target:localhost$"}]
		}))?,
		rate_limited: None,
		protocols: None,
	}
	.into();
	registration.receive_ephemeral = true;
	registration.msc3202_transaction_extensions = true;
	services
		.appservice
		.load_appservice(registration)
		.await
}

async fn boundary_admit(
	services: &crate::Services,
	destination: &Destination,
	sender: &UserId,
) -> Result {
	let device = device_id!("TARGET");
	let target = ruma::user_id!("@singleton-target:localhost");
	let baseline = super::super::to_device_payloads(
		sender,
		target,
		std::iter::once((device, 1)),
		"org.example.boundary",
		&serde_json::json!({"value": BOUNDARY_ESCAPES}),
	)?;
	let maximum = tuwunel_bridge::MAX_VALUE_BYTES - 10;
	let padding = maximum
		.checked_sub(baseline[0].len())
		.expect("maximum content padding");
	let content =
		serde_json::json!({"value": format!("{BOUNDARY_ESCAPES}{}", "x".repeat(padding))});
	services
		.sending
		.send_to_device_appservices(
			sender,
			target,
			std::iter::once((device, 1)),
			"org.example.boundary",
			&content,
		)
		.await?;
	let pending = rows(services, "servernameevent_data").await?;
	assert_eq!(pending.len(), 1, "one actual producer admission");
	assert_eq!(
		pending
			.values()
			.next()
			.expect("admitted row")
			.len(),
		maximum
	);
	let global = rows(services, "global").await?;
	let oversized = serde_json::json!({"value": format!("{BOUNDARY_ESCAPES}{}", "x".repeat(padding.checked_add(1).expect("one-byte overflow control")))});
	services
		.sending
		.send_to_device_appservices(
			sender,
			target,
			std::iter::once((device, 1)),
			"org.example.boundary",
			&oversized,
		)
		.await
		.expect_err("one byte beyond the accepted maximum refuses before admission");
	assert_eq!(rows(services, "servernameevent_data").await?, pending);
	assert_eq!(rows(services, "global").await?, global);
	boundary_freeze(services, destination).await
}

async fn boundary_freeze(services: &crate::Services, destination: &Destination) -> Result {
	let queued = services
		.sending
		.db
		.queued_batch(destination)
		.await?;
	assert_eq!(queued.len(), 1);
	services
		.sending
		.db
		.mark_as_active(queued.iter())
		.await?;
	let active = rows(services, "servercurrentevent_data").await?;
	assert_eq!(active.len(), 1);
	assert_eq!(
		active.values().next().expect("active row").len(),
		tuwunel_bridge::MAX_VALUE_BYTES,
		"active identity uses the exact remaining ten bytes"
	);
	services
		.sending
		.send_events(
			destination.clone(),
			queued
				.into_iter()
				.map(|(_, event)| event)
				.collect(),
		)
		.await
		.expect_err("complete maximum HTTP request loses its response");
	assert!(
		rows(services, "sendingtransaction_record")
			.await?
			.len() > 2,
		"maximum transaction persists multiple immutable body chunks"
	);
	Ok(())
}

async fn boundary_ack(services: &crate::Services, destination: &Destination) -> Result {
	let events = services
		.sending
		.db
		.active_requests_for(destination)
		.map_ok(|(_, event)| event)
		.try_collect::<Vec<_>>()
		.await?;
	let response = services
		.sending
		.send_events(destination.clone(), events)
		.await
		.expect("actual HTTP peer acknowledges admitted delivery");
	let Delivery::Acknowledged(owner, selected) = response else {
		panic!("owned HTTP acknowledgement required")
	};
	assert_eq!(&owner, destination);
	assert_eq!(selected.selected_rows().len(), 1);
	services
		.sending
		.db
		.acknowledge_active(destination, &selected)
		.await?;
	Ok(())
}

async fn boundary_empty(services: &crate::Services, destination: &Destination) -> Result {
	for map in ["servernameevent_data", "servercurrentevent_data", "sendingtransaction_record"] {
		assert!(rows(services, map).await?.is_empty(), "ACK retires every owned {map} row");
	}
	assert!(
		services
			.sending
			.db
			.load_attempt(destination)
			.await?
			.is_none(),
		"cold ACK cleanup leaves no orphan ownership witness or body"
	);
	Ok(())
}

async fn boundary_child(root: &Path, phase: &str) -> Result {
	let fixture = Fixture::open(root).await?;
	let services = &fixture.services;
	let sender = ruma::user_id!("@singleton-bot:localhost");
	let destination = Destination::Appservice("singleton-boundary".into());
	if phase == "verify" {
		boundary_empty(services, &destination).await?;
		fixture.finish().await;
		return Ok(());
	}
	if phase == "prepare" {
		boundary_seed(services, sender).await?;
	}
	boundary_register(services).await?;
	if phase == "prepare" {
		boundary_admit(services, &destination, sender).await?;
		fs::write(root.join("boundary-ready"), b"maximum-ambiguous-http")?;
		return std::future::pending().await;
	}
	assert_eq!(phase, "recover");
	let _claimed = services
		.users
		.take_one_time_key(sender, &boundary_device(0), &OneTimeKeyAlgorithm::SignedCurve25519)
		.await?;
	let tail = enqueue(services, &destination, 1).await?;
	services
		.sending
		.db
		.mark_as_active(std::iter::once(&tail))
		.await?;
	assert_eq!(
		rows(services, "servercurrentevent_data")
			.await?
			.len(),
		2
	);
	boundary_ack(services, &destination).await?;
	let remaining = rows(services, "servercurrentevent_data").await?;
	assert_eq!(remaining.len(), 1, "maximum retry ACK retains the unsent successor");
	assert!(remaining.contains_key(&tail.0));
	assert!(
		rows(services, "sendingtransaction_record")
			.await?
			.is_empty()
	);
	boundary_ack(services, &destination).await?;
	boundary_empty(services, &destination).await?;
	fixture.finish().await;
	Ok(())
}
