//! Canonical local events must retain their owed federation delivery.
//! Every phase owns a native scratch database, starts no service workers, and
//! exits before a cold restart reopens the database. No transport is used.

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{fs, path::Path, process::Command};

use futures::TryStreamExt;
use ruma::{
	EventId, OwnedEventId, RoomVersionId, TransactionId, device_id,
	events::room::{
		create::RoomCreateEventContent,
		join_rules::{JoinRule, RoomJoinRulesEventContent},
		member::{MembershipState, RoomMemberEventContent},
		message::RoomMessageEventContent,
	},
	room_id, server_name, user_id,
};
use tuwunel_core::{Result, matrix::pdu::PduBuilder, utils::rand};

use super::{Destination, SendingEvent, edu_tests::Fixture};
use crate::{Services, transaction_ids, users::Register};

const PHASE: &str = "TUWUNEL_SOURCE_HANDOFF_PHASE";
const DIRECTORY: &str = "TUWUNEL_SOURCE_HANDOFF_DIRECTORY";
const PREFIX: &str = "sending::sender::source_handoff_tests::";

#[test]
fn a_stored_local_event_keeps_its_delivery_and_retry_record_after_cold_restart() -> Result {
	run(
		"a_stored_local_event_keeps_its_delivery_and_retry_record_after_cold_restart",
		&["store", "restart"],
	)
}

#[test]
fn refused_federation_work_cannot_acknowledge_an_unqueued_local_event() -> Result {
	run("refused_federation_work_cannot_acknowledge_an_unqueued_local_event", &[
		"refuse", "restart",
	])
}

#[test]
fn a_leaving_members_server_still_owns_the_membership_delivery() -> Result {
	run("a_leaving_members_server_still_owns_the_membership_delivery", &["leave"])
}

#[test]
fn refused_source_plan_does_not_accept_event_or_publish_state() -> Result {
	run("refused_source_plan_does_not_accept_event_or_publish_state", &["source-refuse"])
}

#[test]
fn refused_incoming_commit_preserves_current_state_and_membership() -> Result {
	run("refused_incoming_commit_preserves_current_state_and_membership", &[
		"incoming-refuse",
	])
}

#[test]
fn membership_handshake_keeps_delivery_after_queue_refusal() -> Result {
	run("membership_handshake_keeps_delivery_after_queue_refusal", &["handshake"])
}

#[test]
fn cancelled_delivery_is_not_recreated_by_its_pending_source() -> Result {
	run("cancelled_delivery_is_not_recreated_by_its_pending_source", &["cancel"])
}

#[test]
fn refused_source_cancellation_resumes_before_queue_recreation() -> Result {
	run("refused_source_cancellation_resumes_before_queue_recreation", &[
		"cancel-refuse",
	])
}

#[test]
fn large_fanout_resumes_after_ack_and_membership_change() -> Result {
	run("large_fanout_resumes_after_ack_and_membership_change", &[
		"large",
		"large-restart",
	])
}

#[test]
fn source_worker_stops_on_service_interrupt_without_server_shutdown() -> Result {
	run("source_worker_stops_on_service_interrupt_without_server_shutdown", &[
		"interrupt",
	])
}

#[test]
fn interrupted_canonical_handoff_recovers_after_sigkill() -> Result {
	run("interrupted_canonical_handoff_recovers_after_sigkill", &["crash", "restart"])
}

#[test]
fn missing_source_header_refuses_queue_recreation() -> Result {
	run("missing_source_header_refuses_queue_recreation", &["corrupt-header"])
}

#[test]
fn missing_source_witness_refuses_queue_recreation() -> Result {
	run("missing_source_witness_refuses_queue_recreation", &["corrupt-witness"])
}

#[test]
fn history_erasure_retires_the_pending_federation_source() -> Result {
	run("history_erasure_retires_the_pending_federation_source", &["purge"])
}

fn run(test: &str, phases: &[&str]) -> Result {
	if let Ok(phase) = std::env::var(PHASE) {
		assert!(phases.contains(&phase.as_str()), "owned child phase");
		let directory = std::env::var(DIRECTORY).expect("owned fixture directory");
		return tokio::runtime::Builder::new_multi_thread()
			.worker_threads(2)
			.enable_all()
			.build()?
			.block_on(child(Path::new(&directory), &phase));
	}
	let root = std::env::temp_dir().join(format!("matrix-source-handoff-{}", rand::string(20)));
	let mut builder = fs::DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&root)?;
	let outcome = (|| {
		for phase in phases {
			let mut command = Command::new(std::env::current_exe()?);
			command
				.args(["--exact", &format!("{PREFIX}{test}"), "--nocapture", "--test-threads=1"])
				.env(PHASE, phase)
				.env(DIRECTORY, &root);
			let status = if *phase == "crash" {
				kill_paused_child(&mut command, &root)?
			} else {
				command.status()?
			};
			if *phase != "crash" {
				assert!(status.success(), "source handoff phase {phase} failed: {status}");
			}
		}
		Ok(())
	})();
	fs::remove_dir_all(&root)?;
	outcome
}

async fn setup(services: &Services) -> Result {
	let alice = user_id!("@source:localhost");
	let room = room_id!("!source-handoff:localhost");
	services
		.users
		.full_register(Register {
			user_id: Some(alice),
			..Default::default()
		})
		.await?;
	services
		.short
		.get_or_create_shortroomid(room)
		.await?;
	let lock = services.state.mutex.lock(room).await;
	let mut create = RoomCreateEventContent::new_v1(alice.to_owned());
	create.room_version = RoomVersionId::V10;
	for (sender, builder) in [
		(alice, PduBuilder::state(String::new(), &create)),
		(
			alice,
			PduBuilder::state(
				alice.to_string(),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
		),
		(
			alice,
			PduBuilder::state(String::new(), &RoomJoinRulesEventContent::new(JoinRule::Public)),
		),
		(
			user_id!("@remote:handoff.invalid"),
			PduBuilder::state(
				"@remote:handoff.invalid",
				&RoomMemberEventContent::new(MembershipState::Join),
			),
		),
	] {
		services
			.timeline
			.build_and_append_pdu(builder, sender, room, &lock)
			.await?;
	}
	drop(lock);
	let remote = Destination::Federation(server_name!("handoff.invalid").to_owned());
	services
		.sending
		.db
		.delete_all_requests_for(&remote)
		.await?;
	assert!(
		services
			.sending
			.db
			.queued_requests(&remote)
			.try_collect::<Vec<_>>()
			.await?
			.is_empty()
	);
	Ok(())
}

async fn owns(services: &Services, event: &EventId) -> Result<bool> {
	let raw = services.timeline.get_pdu_id(event).await?;
	let destination = Destination::Federation(server_name!("handoff.invalid").to_owned());
	let queued = services
		.sending
		.db
		.queued_requests(&destination)
		.try_collect::<Vec<_>>()
		.await?;
	Ok(services
		.sending
		.db
		.has_federation_plan(&raw)
		.await?
		|| queued
			.iter()
			.any(|(_, event)| *event == SendingEvent::Pdu(raw)))
}

async fn retry_record(services: &Services) -> Result<Option<OwnedEventId>> {
	let transaction: &TransactionId = "source-message".into();
	match services
		.transaction_ids
		.existing_txnid(
			user_id!("@source:localhost"),
			Some(device_id!("source-device")),
			transaction,
		)
		.await
	{
		| Ok(value) => Ok(Some(EventId::parse(std::str::from_utf8(value.as_ref())?)?)),
		| Err(error) if error.is_not_found() => Ok(None),
		| Err(error) => Err(error),
	}
}

async fn child(root: &Path, phase: &str) -> Result {
	let fixture = Fixture::open(root).await?;
	let services = &fixture.services;
	if phase == "interrupt" {
		let sending = services.sending.clone();
		let worker = tokio::spawn(async move { sending.federation_source_worker().await });
		tokio::task::yield_now().await;
		crate::Service::interrupt(services.sending.as_ref()).await;
		tokio::time::timeout(std::time::Duration::from_secs(2), worker)
			.await
			.expect("owned source worker exits")??;
		fixture.finish().await;
		return Ok(());
	}
	let alice = user_id!("@source:localhost");
	let room = room_id!("!source-handoff:localhost");
	if phase == "large-restart" {
		large_restart(services, root).await?;
		fixture.finish().await;
		return Ok(());
	}
	if phase == "restart" {
		let event = EventId::parse(fs::read_to_string(root.join("stored-event"))?)?;
		assert_eq!(retry_record(services).await?, Some(event.clone()));
		assert_eq!(services.timeline.get_pdu(&event).await?.event_id, event);
		assert!(owns(services, &event).await?, "cold restart lost owed federation delivery");
		let raw = services.timeline.get_pdu_id(&event).await?;
		services
			.sending
			.resume_federation_source(raw)
			.await?;
		assert!(
			!services
				.sending
				.db
				.has_federation_plan(&raw)
				.await?,
			"recovery transferred the source obligation"
		);
		assert!(owns(services, &event).await?, "recovery retains durable queue ownership");
		fixture.finish().await;
		return Ok(());
	}
	setup(services).await?;
	if phase == "crash" {
		crash_store(fixture.services.clone(), root).await?;
		unreachable!("parent kills the paused child");
	}
	if phase == "incoming-refuse" || phase == "handshake" {
		incoming(services, phase).await?;
		fixture.finish().await;
		return Ok(());
	}
	if phase == "large" {
		fs::write(
			root.join("original-state"),
			services
				.state
				.get_room_shortstatehash(room)
				.await?
				.to_string(),
		)?;
		large_state(services).await?;
	}
	let state_before = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let before = services
		.timeline
		.latest_pdu_in_room(room)
		.await?
		.event_id
		.clone();
	let transaction: &TransactionId = "source-message".into();
	let txnid = transaction_ids::key(alice, Some(device_id!("source-device")), transaction);
	let lock = services.state.mutex.lock(room).await;
	if phase == "source-refuse" {
		tuwunel_database::refusal::refuse_next("pduid_federationplan");
	}
	if phase == "refuse"
		|| phase.starts_with("cancel")
		|| phase.starts_with("corrupt")
		|| phase == "purge"
	{
		tuwunel_database::refusal::refuse_next("servernameevent_data");
	}
	let (sender, builder, txnid) = if phase == "leave" || phase == "source-refuse" {
		let remote = user_id!("@remote:handoff.invalid");
		(
			remote,
			PduBuilder::state(
				remote.to_string(),
				&RoomMemberEventContent::new(MembershipState::Leave),
			),
			None,
		)
	} else {
		(
			alice,
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(
				"durable federation handoff",
			)),
			Some(txnid.as_slice()),
		)
	};
	let sent = services
		.timeline
		.build_and_append_pdu_with_txnid(builder, sender, room, txnid, &lock)
		.await;
	drop(lock);
	match sent {
		| Ok(event) => accepted(services, root, phase, event).await?,

		| Err(error) => {
			assert_eq!(phase, "source-refuse", "healthy canonical append failed: {error}");
			assert_eq!(tuwunel_database::refusal::pending(), 0);
			assert_eq!(
				services
					.state
					.get_room_shortstatehash(room)
					.await?,
				state_before
			);
			assert_eq!(retry_record(services).await?, None);
			assert_eq!(
				services
					.timeline
					.latest_pdu_in_room(room)
					.await?
					.event_id,
				before
			);
		},
	}
	if phase == "refuse" {
		assert_eq!(tuwunel_database::refusal::pending(), 0, "injected queue failure must fire");
	}
	fixture.finish().await;
	Ok(())
}

async fn incoming(services: &Services, phase: &str) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let alice = user_id!("@source:localhost");
	let state = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let before = services
		.timeline
		.latest_pdu_in_room(room)
		.await?
		.event_id
		.clone();
	let lock = services.state.mutex.lock(room).await;
	let (pdu, json) = services
		.timeline
		.create_hash_and_sign_event(
			PduBuilder::state(
				alice.to_string(),
				&RoomMemberEventContent::new(MembershipState::Leave),
			),
			alice,
			room,
			&lock,
		)
		.await?;
	drop(lock);
	let json = tuwunel_core::matrix::pdu::into_outgoing_federation(json, &RoomVersionId::V10);
	if phase == "incoming-refuse" {
		tuwunel_database::refusal::refuse_next("pduid_pdu");
	} else {
		tuwunel_database::refusal::refuse_next("servernameevent_data");
	}
	let result = services
		.event_handler
		.handle_incoming_pdu_and_federate(
			services.globals.server_name(),
			room,
			&pdu.event_id,
			json,
		)
		.await;
	assert_eq!(tuwunel_database::refusal::pending(), 0, "canonical/page fault actually fired");
	if phase == "incoming-refuse" {
		result.expect_err("canonical write refuses");
		assert_eq!(
			services
				.state
				.get_room_shortstatehash(room)
				.await?,
			state
		);
		assert_eq!(
			services
				.timeline
				.latest_pdu_in_room(room)
				.await?
				.event_id,
			before
		);
		assert!(
			services
				.timeline
				.get_pdu_id(&pdu.event_id)
				.await
				.is_err_and(|error| error.is_not_found())
		);
		let member = services
			.state_accessor
			.room_state_get_content::<RoomMemberEventContent>(
				room,
				&ruma::events::StateEventType::RoomMember,
				alice.as_str(),
			)
			.await?;
		assert_eq!(member.membership, MembershipState::Join);
	} else {
		let (raw, _) = result?.expect("accepted membership handshake");
		assert!(
			services
				.sending
				.db
				.has_federation_plan(&raw)
				.await?
		);
		services
			.sending
			.resume_federation_source(raw)
			.await?;
		assert!(
			!services
				.sending
				.db
				.has_federation_plan(&raw)
				.await?
		);
		assert!(owns(services, &pdu.event_id).await?);
	}
	Ok(())
}

// Synthetic complete state stresses source fanout, not peer signature/auth.
async fn large_state(services: &Services) -> Result {
	use std::sync::Arc;

	use crate::rooms::state_compressor::{CompressedState, compress_state_event};
	let room = room_id!("!source-handoff:localhost");
	let hash = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let mut cells = services
		.state_accessor
		.state_full_shortids(hash)
		.map_ok(|(key, event)| compress_state_event(key, event))
		.try_collect::<CompressedState>()
		.await?;
	let prototype = services
		.state_accessor
		.room_state_get(
			room,
			&ruma::events::StateEventType::RoomMember,
			"@remote:handoff.invalid",
		)
		.await?;
	for index in 0..1000 {
		let mut pdu = prototype.clone();
		pdu.prev_events.clear();
		pdu.auth_events.clear();
		pdu.origin = None;
		pdu.unsigned = None;
		pdu.event_id = EventId::parse(format!("$fan-{index}"))?;
		pdu.state_key = Some(format!("@member:server{index:04}.invalid").into());
		pdu.sender = ruma::UserId::parse(pdu.state_key.as_ref().unwrap().as_str())?;
		let json = tuwunel_core::utils::to_canonical_object(&pdu)?;
		services
			.timeline
			.add_pdu_outlier(&pdu.event_id, &json)
			.await?;
		let key = services
			.short
			.get_or_create_shortstatekey(
				&ruma::events::StateEventType::RoomMember,
				pdu.state_key.as_ref().unwrap().as_str(),
			)
			.await?;
		cells.insert(
			services
				.state_compressor
				.compress_state_event(key, &pdu.event_id)
				.await?,
		);
	}
	let lock = services.state.mutex.lock(room).await;
	let saved = services
		.state_compressor
		.save_state(room, Arc::new(cells))
		.await?;
	services
		.state
		.set_room_state(room, saved.shortstatehash, &lock)
		.await?;
	Ok(())
}

async fn large_restart(services: &Services, root: &Path) -> Result {
	let event = EventId::parse(fs::read_to_string(root.join("stored-event"))?)?;
	let raw = services.timeline.get_pdu_id(&event).await?;
	assert_eq!(retry_record(services).await?, Some(event));
	assert!(
		services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	for _ in 0..20 {
		services
			.sending
			.resume_federation_source(raw)
			.await?;
	}
	assert!(
		!services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	let acked = Destination::Federation(server_name!("handoff.invalid").to_owned());
	assert!(
		services
			.sending
			.db
			.queued_requests(&acked)
			.try_collect::<Vec<_>>()
			.await?
			.is_empty(),
		"earlier ACK must not be replayed"
	);
	for index in 0..1000 {
		let dest = Destination::Federation(
			format!("server{index:04}.invalid")
				.try_into()
				.unwrap(),
		);
		let queued = services
			.sending
			.db
			.queued_requests(&dest)
			.try_collect::<Vec<_>>()
			.await?;
		assert_eq!(
			queued
				.iter()
				.filter(|(_, event)| *event == SendingEvent::Pdu(raw))
				.count(),
			1
		);
	}
	Ok(())
}

async fn accepted(services: &Services, root: &Path, phase: &str, event: OwnedEventId) -> Result {
	assert_ne!(phase, "source-refuse", "source refusal must refuse canonical acceptance");
	if phase == "refuse" {
		assert_eq!(tuwunel_database::refusal::pending(), 0, "injected page refusal fired");
		assert_eq!(retry_record(services).await?, Some(event.clone()));
		let raw = services.timeline.get_pdu_id(&event).await?;
		assert!(
			services
				.sending
				.db
				.has_federation_plan(&raw)
				.await?,
			"accepted event retains its source plan"
		);
	}
	assert!(
		owns(services, &event).await?,
		"acknowledged canonical event {event} has no owed federation delivery after {phase}"
	);
	if phase.starts_with("corrupt") {
		corrupt_source(services, phase, &event).await?;
	}
	if phase == "purge" {
		purge_source(services, &event).await?;
	}
	if phase.starts_with("cancel") {
		cancel_source(services, phase, &event).await?;
	}
	if phase == "large" {
		ack_large_source(services, root, &event).await?;
	}

	if phase == "store" || phase == "refuse" {
		assert_eq!(retry_record(services).await?, Some(event.clone()));
		fs::write(root.join("stored-event"), event.as_bytes())?;
	}
	Ok(())
}

async fn cancel_source(services: &Services, phase: &str, event: &EventId) -> Result {
	let raw = services.timeline.get_pdu_id(event).await?;
	let remote = Destination::Federation(server_name!("handoff.invalid").to_owned());
	assert_eq!(tuwunel_database::refusal::pending(), 0);
	assert!(
		services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	if phase == "cancel-refuse" {
		tuwunel_database::refusal::refuse_next("pduid_federationplan");
		services
			.sending
			.db
			.delete_all_requests_for(&remote)
			.await
			.expect_err("bitmap rewrite refused");
		assert_eq!(tuwunel_database::refusal::pending(), 0);
	} else {
		services
			.sending
			.db
			.delete_all_requests_for(&remote)
			.await?;
	}
	services
		.sending
		.resume_federation_source(raw)
		.await?;
	assert!(
		!services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	assert!(!owns(services, event).await?, "cancelled work must stay cancelled");
	assert_eq!(retry_record(services).await?, Some(event.to_owned()));

	Ok(())
}

async fn ack_large_source(services: &Services, root: &Path, event: &EventId) -> Result {
	let raw = services.timeline.get_pdu_id(event).await?;
	assert!(
		services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	let dest = Destination::Federation(server_name!("handoff.invalid").to_owned());
	let queue = services
		.sending
		.db
		.queued_requests(&dest)
		.try_collect::<Vec<_>>()
		.await?;
	services
		.sending
		.db
		.mark_as_active(queue.iter())
		.await?;
	let (_, ack) = services.sending.db.active_batch(&dest).await?;
	services
		.sending
		.db
		.acknowledge_active(&dest, &ack)
		.await?;
	assert!(
		!services
			.sending
			.db
			.queued_requests(&dest)
			.try_collect::<Vec<_>>()
			.await?
			.iter()
			.any(|(_, e)| *e == SendingEvent::Pdu(raw))
	);
	// Remove every synthetic member from current state before restart.
	let original: u64 = fs::read_to_string(root.join("original-state"))?
		.parse()
		.unwrap();
	let room = room_id!("!source-handoff:localhost");
	let lock = services.state.mutex.lock(room).await;
	services
		.state
		.set_room_state(room, original, &lock)
		.await?;
	drop(lock);
	fs::write(root.join("stored-event"), event.as_bytes())?;

	Ok(())
}

fn kill_paused_child(command: &mut Command, root: &Path) -> Result<std::process::ExitStatus> {
	use std::{
		os::unix::process::ExitStatusExt,
		thread,
		time::{Duration, Instant},
	};
	struct OwnedChild(std::process::Child);
	impl Drop for OwnedChild {
		fn drop(&mut self) {
			self.0.kill().ok();
			self.0.wait().ok();
		}
	}
	let mut child = OwnedChild(command.spawn()?);
	let start = Instant::now();
	while !root.join("source.ready").exists() {
		assert!(child.0.try_wait()?.is_none(), "child exited before its committed source pause");
		assert!(start.elapsed() < Duration::from_secs(45), "owned process pause timed out");
		thread::sleep(Duration::from_millis(20));
	}
	child.0.kill()?;
	let status = child.0.wait()?;
	assert_eq!(status.signal(), Some(9), "actual source handoff SIGKILL");
	Ok(status)
}

async fn crash_store(services: std::sync::Arc<Services>, root: &Path) -> Result {
	let mut pause = tuwunel_database::refusal::pause_next("servernameevent_data");
	let sending = services.clone();
	let _writer = tokio::spawn(async move {
		let alice = user_id!("@source:localhost");
		let room = room_id!("!source-handoff:localhost");
		let transaction: &TransactionId = "source-message".into();
		let txnid = transaction_ids::key(alice, Some(device_id!("source-device")), transaction);
		let lock = sending.state.mutex.lock(room).await;
		sending
			.timeline
			.build_and_append_pdu_with_txnid(
				PduBuilder::timeline(&RoomMessageEventContent::text_plain(
					"interrupted canonical handoff",
				)),
				alice,
				room,
				Some(&txnid),
				&lock,
			)
			.await
	});
	pause.entered().await?;
	let event = retry_record(&services)
		.await?
		.expect("canonical client retry record committed before queue pause");
	let raw = services.timeline.get_pdu_id(&event).await?;
	assert!(
		services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	assert!(services.globals.current_count() >= u64::from_be_bytes(raw.count()));
	fs::write(root.join("stored-event"), event.as_bytes())?;
	fs::write(root.join("source.ready"), b"canonical source committed, queue not dispatched")?;
	std::future::pending::<()>().await;
	Ok(())
}

async fn corrupt_source(services: &Services, phase: &str, event: &EventId) -> Result {
	let raw = services.timeline.get_pdu_id(event).await?;
	assert_eq!(tuwunel_database::refusal::pending(), 0);
	let count = services.globals.current_count();
	if phase == "corrupt-header" {
		services.db["pduid_federationplan"]
			.remove(raw.as_ref())
			.await?;
	} else {
		let mut witness = vec![0x08];
		witness.extend_from_slice(raw.as_ref());
		services.db["global"].remove(&witness).await?;
	}
	services
		.sending
		.resume_federation_source(raw)
		.await
		.expect_err("missing source ownership refuses queue reconstruction");
	assert_eq!(services.globals.current_count(), count);
	let remote = Destination::Federation(server_name!("handoff.invalid").to_owned());
	assert!(
		services
			.sending
			.db
			.queued_requests(&remote)
			.try_collect::<Vec<_>>()
			.await?
			.is_empty()
	);
	assert_eq!(retry_record(services).await?, Some(event.to_owned()));
	Ok(())
}

async fn purge_source(services: &Services, event: &EventId) -> Result {
	let raw = services.timeline.get_pdu_id(event).await?;
	assert_eq!(tuwunel_database::refusal::pending(), 0);
	assert!(
		services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	let until = *services.globals.next_count().await?;
	assert_eq!(
		services
			.timeline
			.purge_history(
				room_id!("!source-handoff:localhost"),
				tuwunel_core::PduCount::Normal(until),
				true
			)
			.await?,
		1
	);
	assert!(
		!services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	services
		.sending
		.resume_federation_source(raw)
		.await?;
	assert!(
		services
			.timeline
			.get_pdu_id(event)
			.await
			.is_err_and(|error| error.is_not_found())
	);
	Ok(())
}
