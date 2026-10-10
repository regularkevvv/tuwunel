//! Actual queue promotion, schema refusal and process-kill identity controls.
//! Owned RocksDB fixtures qualify local rows, not frozen wire transactions/D1.

#[cfg(unix)]
use std::os::unix::{fs::DirBuilderExt, process::ExitStatusExt};
use std::{
	collections::BTreeMap,
	fs,
	process::Command,
	time::{Duration, Instant},
};

use tuwunel_core::Result;

use super::{
	Destination, SendingEvent,
	ack_tests::{enqueue, rows},
	edu_tests::Fixture,
};
use crate::rooms::timeline::RawPduId;

fn pdu() -> Result<SendingEvent> {
	let mut bytes = [0_u8; 16];
	bytes[7] = 1;
	bytes[15] = 1;
	Ok(SendingEvent::Pdu(RawPduId::from_bytes(&bytes)?))
}

async fn promote(fixture: &Fixture, destination: &Destination, event: &SendingEvent) -> Result {
	fixture.retain_pdu(event).await?;
	let data = &fixture.services.sending.db;
	let keys = data
		.queue_requests(std::iter::once((event, destination)))
		.await?;
	data.mark_as_active(std::iter::once(&(keys[0].clone(), event.clone())))
		.await
}

async fn verify_repeated_promotion_keeps_identity_and_independent_pending_admission() -> Result {
	let fixture = Fixture::new().await?;
	let data = &fixture.services.sending.db;
	let destination = Destination::Appservice("repeated-promotion".into());
	let event = pdu()?;
	promote(&fixture, &destination, &event).await?;
	let active = rows(&fixture.services, "servercurrentevent_data").await?;
	let selected = data
		.selected_acknowledgement(&destination, std::slice::from_ref(&event))
		.await?;
	promote(&fixture, &destination, &event).await?;
	let pending = rows(&fixture.services, "servernameevent_data").await?;
	assert_eq!(pending.len(), 1, "independent pending admission retained");
	assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, active);
	data.acknowledge_active(&destination, &selected)
		.await?;
	assert_eq!(rows(&fixture.services, "servernameevent_data").await?, pending);
	let item = (
		pending
			.first_key_value()
			.expect("pending admission")
			.0
			.clone(),
		event,
	);
	data.mark_as_active(std::iter::once(&item))
		.await?;
	let next = rows(&fixture.services, "servercurrentevent_data").await?;
	assert_ne!(next, active, "new active admission owns new persisted identity");
	data.acknowledge_active(&destination, &selected)
		.await?;
	assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, next);
	fixture.finish().await;
	Ok(())
}

async fn verify_unsupported_schema_refuses_identity_writes_and_cursor_consumption() -> Result {
	let fixture = Fixture::new().await?;
	let destination = Destination::Federation("remote.example".try_into()?);
	let event = pdu()?;
	fixture.retain_pdu(&event).await?;
	let data = &fixture.services.sending.db;
	let keys = data
		.queue_requests(std::iter::once((&event, &destination)))
		.await?;
	fixture
		.services
		.globals
		.db
		.bump_database_version(20)
		.await?;
	let active = rows(&fixture.services, "servercurrentevent_data").await?;
	let pending = rows(&fixture.services, "servernameevent_data").await?;
	let cursors = rows(&fixture.services, "servername_educount").await?;
	data.mark_as_active(std::iter::once(&(keys[0].clone(), event)))
		.await
		.expect_err("incompatible schema must refuse active promotion");
	let Destination::Federation(server) = destination else { unreachable!() };
	data.persist_edus(&server, &[], &[], 37)
		.await
		.expect_err("incompatible schema cannot consume an EDU watermark");
	assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, active);
	assert_eq!(rows(&fixture.services, "servernameevent_data").await?, pending);
	assert_eq!(rows(&fixture.services, "servername_educount").await?, cursors);
	fixture.finish().await;
	Ok(())
}

async fn verify_an_identity_beyond_the_durable_counter_refuses_the_whole_ack() -> Result {
	let fixture = Fixture::new().await?;
	let data = &fixture.services.sending.db;
	let destination = Destination::Appservice("corrupt-identity".into());
	let items = [
		enqueue(&fixture.services, &destination, 0).await?,
		enqueue(&fixture.services, &destination, 1).await?,
	];
	data.mark_as_active(items.iter()).await?;
	let selected = data
		.selected_acknowledgement(&destination, &[items[0].1.clone(), items[1].1.clone()])
		.await?;
	let original = rows(&fixture.services, "servercurrentevent_data").await?;
	let mut corrupt = original[&items[1].0].clone();
	corrupt[2..10].copy_from_slice(&u64::MAX.to_be_bytes());
	fixture.services.db["servercurrentevent_data"]
		.insert(&items[1].0, corrupt)
		.await?;
	let before = rows(&fixture.services, "servercurrentevent_data").await?;
	data.acknowledge_active(&destination, &selected)
		.await
		.expect_err("unallocated identity is corruption, not re-admission");
	assert_eq!(
		rows(&fixture.services, "servercurrentevent_data").await?,
		before,
		"all earlier members remain"
	);
	fixture.services.db["servercurrentevent_data"]
		.insert(&items[1].0, original[&items[1].0].as_slice())
		.await?;
	data.acknowledge_active(&destination, &selected)
		.await?;
	assert!(
		rows(&fixture.services, "servercurrentevent_data")
			.await?
			.is_empty()
	);
	fixture.finish().await;
	Ok(())
}

async fn verify_promotion_bound_preserves_pending_and_reserves_one_identity() -> Result {
	let fixture = Fixture::new().await?;
	let data = &fixture.services.sending.db;
	let destination = Destination::Appservice("promotion-bound".into());
	let mut items = Vec::new();
	for number in 0..=super::super::data::ACTIVE_PROMOTION_LIMIT {
		items.push(enqueue(&fixture.services, &destination, number).await?);
	}
	let pending = rows(&fixture.services, "servernameevent_data").await?;
	let count = fixture.services.globals.current_count();
	data.mark_as_active(items.iter())
		.await
		.expect_err("oversized promotion must refuse before consuming state");
	assert_eq!(rows(&fixture.services, "servernameevent_data").await?, pending);
	assert!(
		rows(&fixture.services, "servercurrentevent_data")
			.await?
			.is_empty()
	);
	assert_eq!(fixture.services.globals.current_count(), count);
	let admitted = &items[..super::super::data::ACTIVE_PROMOTION_LIMIT];
	data.mark_as_active(admitted.iter()).await?;
	assert_eq!(
		fixture.services.globals.current_count(),
		count
			.checked_add(1)
			.expect("fixture counter headroom")
	);
	assert_eq!(
		rows(&fixture.services, "servercurrentevent_data")
			.await?
			.len(),
		admitted.len()
	);
	let tail = rows(&fixture.services, "servernameevent_data").await?;
	assert_eq!(tail.len(), 1);
	assert_eq!(tail.first_key_value(), pending.last_key_value());
	let events = admitted
		.iter()
		.map(|(_, event)| event.clone())
		.collect::<Vec<_>>();
	let selected = data
		.selected_acknowledgement(&destination, &events)
		.await?;
	data.acknowledge_active(&destination, &selected)
		.await?;
	assert!(
		rows(&fixture.services, "servercurrentevent_data")
			.await?
			.is_empty()
	);
	assert_eq!(rows(&fixture.services, "servernameevent_data").await?, tail);
	fixture.finish().await;
	Ok(())
}

const PHASE: &str = "TUWUNEL_ACTIVE_IDENTITY_TEST_PHASE";
const DIRECTORY: &str = "TUWUNEL_ACTIVE_IDENTITY_TEST_DIRECTORY";
const TEST: &str = "sending::sender::incarnation_tests::active_identity_survives_sigkill_and_is_not_reused_after_restart";

#[test]
fn active_identity_survives_sigkill_and_is_not_reused_after_restart() -> Result {
	if let Ok(phase) = std::env::var(PHASE) {
		let root = std::path::PathBuf::from(std::env::var(DIRECTORY).expect("owned root"));
		return tokio::runtime::Builder::new_multi_thread()
			.worker_threads(2)
			.enable_all()
			.build()?
			.block_on(cold_child(&root, &phase));
	}
	let root = std::env::temp_dir()
		.join(format!("matrix-active-identity-{}", tuwunel_core::utils::rand::string(20)));
	let mut builder = fs::DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&root)?;
	let result = cold_parent(&root);
	fs::remove_dir_all(&root)?;
	result
}

fn child_command(root: &std::path::Path, phase: &str) -> Result<Command> {
	let mut command = Command::new(std::env::current_exe()?);
	command
		.args(["--exact", TEST, "--nocapture", "--test-threads=1"])
		.env(PHASE, phase)
		.env(DIRECTORY, root);
	Ok(command)
}

fn cold_parent(root: &std::path::Path) -> Result {
	let mut child = child_command(root, "prepare")?.spawn()?;
	let deadline = Instant::now()
		.checked_add(Duration::from_secs(30))
		.expect("valid deadline");
	while !root.join("ready").exists() {
		if child.try_wait()?.is_some() || Instant::now() >= deadline {
			child.kill().ok();
			child.wait().ok();
			panic!("active identity must reach its acknowledged kill boundary");
		}
		std::thread::sleep(Duration::from_millis(20));
	}
	child.kill()?;
	let status = child.wait()?;
	#[cfg(unix)]
	assert_eq!(status.signal(), Some(9));
	for phase in ["recover", "again"] {
		assert!(child_command(root, phase)?.status()?.success(), "cold phase {phase}");
	}
	Ok(())
}

async fn cold_child(root: &std::path::Path, phase: &str) -> Result {
	let fixture = Fixture::open(root).await?;
	let data = &fixture.services.sending.db;
	let destination = Destination::Appservice("cold-identity".into());
	let event = pdu()?;
	if phase == "prepare" {
		promote(&fixture, &destination, &event).await?;
		let rows = rows(&fixture.services, "servercurrentevent_data").await?;
		fs::write(
			root.join("before.json"),
			serde_json::to_vec(&rows.into_iter().collect::<Vec<_>>())?,
		)?;
		// No fixture flush: the production promotion acknowledgement owns WAL
		// durability.
		fs::write(root.join("ready"), b"acknowledged")?;
		return std::future::pending().await;
	}
	if phase == "recover" {
		let expected: Vec<(Vec<u8>, Vec<u8>)> =
			serde_json::from_slice(&fs::read(root.join("before.json"))?)?;
		let expected: BTreeMap<_, _> = expected.into_iter().collect();
		assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, expected);
		let old_ack = data
			.selected_acknowledgement(&destination, std::slice::from_ref(&event))
			.await?;
		data.delete_all_requests_for(&destination).await?;
		promote(&fixture, &destination, &event).await?;
		let readmitted = rows(&fixture.services, "servercurrentevent_data").await?;
		assert_ne!(readmitted, expected, "reopened durable counter must issue a fresh identity");
		data.acknowledge_active(&destination, &old_ack)
			.await?;
		assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, readmitted);
		let own_ack = data
			.selected_acknowledgement(&destination, std::slice::from_ref(&event))
			.await?;
		data.acknowledge_active(&destination, &own_ack)
			.await?;
	} else {
		assert_eq!(phase, "again");
	}
	assert!(
		rows(&fixture.services, "servercurrentevent_data")
			.await?
			.is_empty()
	);
	assert!(
		rows(&fixture.services, "servernameevent_data")
			.await?
			.is_empty()
	);
	fixture.finish().await;
	Ok(())
}

async fn verify_an_empty_account_inventory_cannot_overwrite_a_newer_native_schema() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	assert_eq!(services.users.count().await, 0);
	services.db["global"]
		.insert(b"server_name", services.server.name.as_str())
		.await?;
	services
		.globals
		.db
		.bump_database_version(crate::migrations::DATABASE_VERSION + 1)
		.await?;
	let before = rows(services, "global").await?;
	crate::migrations::migrations(services)
		.await
		.expect_err("empty accounts do not make incompatible state fresh");
	assert_eq!(
		rows(services, "global").await?,
		before,
		"schema refusal precedes all fresh-database writes"
	);
	fixture.finish().await;
	Ok(())
}

#[test]
fn repeated_promotion_keeps_identity_and_independent_pending_admission() -> Result {
	isolated("sending::sender::incarnation_tests::repeated_promotion_keeps_identity_and_independent_pending_admission", verify_repeated_promotion_keeps_identity_and_independent_pending_admission())
}

#[test]
fn unsupported_schema_refuses_identity_writes_and_cursor_consumption() -> Result {
	isolated("sending::sender::incarnation_tests::unsupported_schema_refuses_identity_writes_and_cursor_consumption", verify_unsupported_schema_refuses_identity_writes_and_cursor_consumption())
}

#[test]
fn an_identity_beyond_the_durable_counter_refuses_the_whole_ack() -> Result {
	isolated("sending::sender::incarnation_tests::an_identity_beyond_the_durable_counter_refuses_the_whole_ack", verify_an_identity_beyond_the_durable_counter_refuses_the_whole_ack())
}

#[test]
fn an_empty_account_inventory_cannot_overwrite_a_newer_native_schema() -> Result {
	isolated("sending::sender::incarnation_tests::an_empty_account_inventory_cannot_overwrite_a_newer_native_schema", verify_an_empty_account_inventory_cannot_overwrite_a_newer_native_schema())
}

#[test]
fn promotion_bound_preserves_pending_and_reserves_one_identity() -> Result {
	isolated("sending::sender::incarnation_tests::promotion_bound_preserves_pending_and_reserves_one_identity", verify_promotion_bound_preserves_pending_and_reserves_one_identity())
}

pub(super) fn isolated(test: &str, exercise: impl Future<Output = Result>) -> Result {
	const CHILD: &str = "TUWUNEL_INCARNATION_TEST_CHILD";
	if std::env::var(CHILD).as_deref() != Ok(test) {
		// Isolate native fixtures so failed cases and global backend settings
		// cannot contaminate another case. Lifecycle tests also verify release
		// and database reopening inside their child process.
		let mut child = Command::new(std::env::current_exe()?)
			.args(["--exact", test, "--nocapture", "--test-threads=1"])
			.env(CHILD, test)
			.spawn()?;
		let deadline = Instant::now()
			.checked_add(Duration::from_secs(90))
			.expect("valid deadline");
		loop {
			if let Some(status) = child.try_wait()? {
				assert!(status.success(), "isolated native case {test}");
				return Ok(());
			}
			if Instant::now() >= deadline {
				child.kill().ok();
				child.wait().ok();
				panic!("isolated native case {test} exceeded deadline");
			}
			std::thread::sleep(Duration::from_millis(20));
		}
	}
	tokio::runtime::Builder::new_multi_thread()
		.worker_threads(2)
		.enable_all()
		.build()?
		.block_on(exercise)
}

async fn verify_legacy_active_migration_refuses_without_mutating_retained_rows() -> Result {
	for (version, foreign) in [(13, false), (17, false), (21, false), (99, true)] {
		let fixture = Fixture::new().await?;
		let services = &fixture.services;
		// Empty accounts must not let fresh-database initialization bypass the
		// gate. No worker runs; these are the old sender's actual row shapes.
		assert_eq!(services.users.count().await, 0);
		services.db["global"]
			.insert(b"server_name", services.server.name.as_str())
			.await?;
		if foreign {
			services.db["global"]
				.insert(b"populate_userroomid_leftstate_table", b"".as_slice())
				.await?;
		}
		services
			.globals
			.db
			.bump_database_version(version)
			.await?;
		let destination = Destination::Appservice("legacy-active-migration".into());
		let mut active = destination.get_prefix();
		active.extend_from_slice(&[1_u8; 16]);
		let mut pending = destination.get_prefix();
		pending.extend_from_slice(&[2_u8; 16]);
		let mut txn = services.db.txn();
		txn.insert_raw(&services.db["servercurrentevent_data"], &active, b"");
		txn.insert_raw(&services.db["servernameevent_data"], &pending, b"");
		txn.insert_raw(&services.db["pduid_pdu"], [1_u8; 16], b"retained active source");
		txn.insert_raw(&services.db["pduid_pdu"], [2_u8; 16], b"retained pending source");
		txn.execute_flushed().await?;
		let maps = ["global", "servercurrentevent_data", "servernameevent_data", "pduid_pdu"];
		let mut before = Vec::new();
		for map in maps {
			before.push(rows(services, map).await?);
		}
		let error = crate::migrations::migrations(services)
			.await
			.expect_err("old active sends cannot acquire a replacement transaction identity");
		assert!(
			error
				.to_string()
				.contains("Drain active deliveries with the previous writer")
		);
		for (map, expected) in maps.into_iter().zip(before) {
			assert_eq!(rows(services, map).await?, expected, "refusal changed {map}");
		}
		fixture.finish().await;
	}
	Ok(())
}

#[test]
fn legacy_active_migration_refuses_without_mutating_retained_rows() -> Result {
	isolated("sending::sender::incarnation_tests::legacy_active_migration_refuses_without_mutating_retained_rows", verify_legacy_active_migration_refuses_without_mutating_retained_rows())
}
