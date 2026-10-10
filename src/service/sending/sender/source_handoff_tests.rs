//! Canonical local events must retain their owed federation delivery.
//! Every phase owns a native scratch database, starts no service workers, and
//! exits before a cold restart reopens the database. No transport is used.

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{fs, path::Path, process::Command};

use futures::TryStreamExt;
use ruma::{
	EventId, OwnedEventId, RoomId, RoomVersionId, TransactionId, device_id,
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

mod erasure_controls;
mod history_controls;
mod inventory_controls;
mod membership_controls;
mod notification_controls;
mod presence_controls;
mod quota_controls;
mod receipt_inventory_controls;
mod recount_controls;

const PHASE: &str = "TUWUNEL_SOURCE_HANDOFF_PHASE";
const DIRECTORY: &str = "TUWUNEL_SOURCE_HANDOFF_DIRECTORY";
const PREFIX: &str = "sending::sender::source_handoff_tests::";

#[test]
fn committed_presence_survives_unavailable_timer_hint() -> Result {
	run("committed_presence_survives_unavailable_timer_hint", &[
		"presence-timer",
		"presence-restart",
		"presence-again",
	])
}

#[cfg(debug_assertions)]
#[test]
fn cancelled_presence_update_restores_device_cache_before_dispatch() -> Result {
	run("cancelled_presence_update_restores_device_cache_before_dispatch", &[
		"presence-cancel",
		"presence-restart",
		"presence-again",
	])
}

#[test]
fn malformed_saved_presence_refuses_new_updates_without_mutation() -> Result {
	run("malformed_saved_presence_refuses_new_updates_without_mutation", &[
		"presence-corrupt",
		"presence-restart",
		"presence-again",
	])
}

#[test]
fn refused_presence_body_preserves_saved_pointer_and_cold_status() -> Result {
	run("refused_presence_body_preserves_saved_pointer_and_cold_status", &[
		"presence-body",
		"presence-restart",
		"presence-again",
	])
}

#[test]
fn refused_presence_status_cannot_leak_into_a_later_device_ping() -> Result {
	run("refused_presence_status_cannot_leak_into_a_later_device_ping", &[
		"presence-cache",
		"presence-restart",
		"presence-again",
	])
}

#[test]
fn refused_presence_transition_cannot_schedule_push_before_commit() -> Result {
	run("refused_presence_transition_cannot_schedule_push_before_commit", &[
		"presence-hint",
		"presence-restart",
		"presence-again",
	])
}

#[test]
fn concurrent_presence_updates_keep_one_body_and_survive_cold_restart() -> Result {
	run("concurrent_presence_updates_keep_one_body_and_survive_cold_restart", &[
		"presence-concurrent",
		"presence-restart",
		"presence-again",
	])
}

#[test]
fn receipt_selection_retains_only_nonempty_rooms_and_preserves_late_threads() -> Result {
	run("receipt_selection_retains_only_nonempty_rooms_and_preserves_late_threads", &[
		"receipt-prepare",
		"receipt-restart",
		"receipt-again",
	])
}

#[test]
fn receipt_selection_preserves_cursor_and_late_receipts_on_corruption() -> Result {
	run("receipt_selection_preserves_cursor_and_late_receipts_on_corruption", &[
		"receipt-corrupt",
		"receipt-repair",
		"receipt-again",
	])
}

#[test]
fn receipt_selection_preserves_cursor_on_refused_persistence() -> Result {
	run("receipt_selection_preserves_cursor_on_refused_persistence", &[
		"receipt-refuse",
		"receipt-repair",
		"receipt-again",
	])
}

#[test]
fn source_count_quota_serializes_cross_room_admission_and_recovers_capacity() -> Result {
	run("source_count_quota_serializes_cross_room_admission_and_recovers_capacity", &[
		"quota-count",
		"quota-restart",
		"quota-again",
	])
}

#[test]
fn source_byte_quota_preserves_accepted_deliveries_and_recovers_capacity() -> Result {
	run("source_byte_quota_preserves_accepted_deliveries_and_recovers_capacity", &[
		"quota-bytes",
		"quota-restart",
		"quota-again",
	])
}

#[test]
fn canonical_admission_refuses_understated_source_size() -> Result {
	run("canonical_admission_refuses_understated_source_size", &[
		"inventory-size",
		"inventory-recover",
		"inventory-again",
	])
}

#[test]
fn canonical_admission_refuses_corrupt_source_hash() -> Result {
	run("canonical_admission_refuses_corrupt_source_hash", &[
		"inventory-hash",
		"inventory-recover",
		"inventory-again",
	])
}

#[test]
fn canonical_admission_refuses_invalid_source_codec_with_valid_witness() -> Result {
	run("canonical_admission_refuses_invalid_source_codec_with_valid_witness", &[
		"inventory-codec",
		"inventory-recover",
		"inventory-again",
	])
}

#[test]
fn canonical_admission_refuses_changed_source_body() -> Result {
	run("canonical_admission_refuses_changed_source_body", &[
		"inventory-body",
		"inventory-recover",
		"inventory-again",
	])
}

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

#[test]
fn first_handshake_for_a_known_event_survives_refusal_and_restart() -> Result {
	run("first_handshake_for_a_known_event_survives_refusal_and_restart", &[
		"known-first",
		"known-restart",
		"known-acked",
	])
}

#[test]
fn refused_known_event_role_is_retryable_without_reaccepting_the_event() -> Result {
	run("refused_known_event_role_is_retryable_without_reaccepting_the_event", &[
		"known-refuse",
	])
}

#[test]
fn completed_local_role_is_not_replayed_by_a_handshake_after_restart() -> Result {
	run("completed_local_role_is_not_replayed_by_a_handshake_after_restart", &[
		"owned-ack",
		"known-acked",
	])
}

#[test]
fn duplicate_handshake_refuses_corrupt_pending_plan_without_acknowledging_it() -> Result {
	run("duplicate_handshake_refuses_corrupt_pending_plan_without_acknowledging_it", &[
		"owned-plan-corrupt",
	])
}

#[test]
fn duplicate_handshake_refuses_a_rebound_plan_with_a_valid_witness() -> Result {
	run("duplicate_handshake_refuses_a_rebound_plan_with_a_valid_witness", &[
		"owned-plan-rebound",
	])
}

#[test]
fn missing_completed_role_refuses_duplicate_delivery() -> Result {
	run("missing_completed_role_refuses_duplicate_delivery", &["owned-corrupt"])
}

#[test]
fn concurrent_known_handshakes_do_not_recreate_acknowledged_delivery() -> Result {
	run("concurrent_known_handshakes_do_not_recreate_acknowledged_delivery", &[
		"known-concurrent",
	])
}

#[test]
fn history_erasure_retires_materialized_federation_delivery() -> Result {
	run("history_erasure_retires_materialized_federation_delivery", &[
		"purge-materialized",
	])
}

#[test]
fn whole_room_erasure_retires_pending_sources_and_role_records() -> Result {
	run("whole_room_erasure_retires_pending_sources_and_role_records", &["room-purge"])
}

#[test]
fn refused_whole_room_erasure_preserves_pending_source_for_retry() -> Result {
	run("refused_whole_room_erasure_preserves_pending_source_for_retry", &[
		"room-purge-refuse",
	])
}

#[test]
fn notifications_freeze_accepted_members_and_count_despite_stale_cache() -> Result {
	run("notifications_freeze_accepted_members_and_count_despite_stale_cache", &[
		"notification-snapshot",
		"notification-restart",
		"notification-again",
	])
}

#[test]
fn pending_join_supplies_notification_member_count_before_canonical_commit() -> Result {
	run("pending_join_supplies_notification_member_count_before_canonical_commit", &[
		"notification-overlay",
	])
}

#[test]
fn notification_membership_binding_refuses_before_event_and_state_commit() -> Result {
	run("notification_membership_binding_refuses_before_event_and_state_commit", &[
		"notification-corrupt",
	])
}

#[test]
fn authoritative_notification_inventory_bounds_remote_members_before_filtering() -> Result {
	run(
		"authoritative_notification_inventory_bounds_remote_members_before_filtering",
		&["notification-budget"],
	)
}

#[test]
fn refused_membership_effect_recovers_accepted_state_after_cold_restart() -> Result {
	run("refused_membership_effect_recovers_accepted_state_after_cold_restart", &[
		"membership-refuse",
		"membership-restart",
		"membership-again",
	])
}

#[test]
fn killed_membership_effect_recovers_accepted_state_after_cold_restart() -> Result {
	run("killed_membership_effect_recovers_accepted_state_after_cold_restart", &[
		"membership-crash",
		"membership-restart",
		"membership-again",
	])
}

#[test]
fn refused_forced_state_recovers_removed_members_and_servers() -> Result {
	run("refused_forced_state_recovers_removed_members_and_servers", &[
		"membership-force-refuse",
		"membership-force-restart",
		"membership-force-again",
	])
}

#[test]
fn corrupt_membership_plan_refuses_without_writes_and_forgetting_survives_restart() -> Result {
	run(
		"corrupt_membership_plan_refuses_without_writes_and_forgetting_survives_restart",
		&["membership-corrupt-forget", "membership-restart", "membership-again"],
	)
}

#[test]
fn initial_remote_state_recovers_before_its_first_local_timeline_event() -> Result {
	run("initial_remote_state_recovers_before_its_first_local_timeline_event", &[
		"membership-initial-prepare",
		"membership-initial-restart",
		"membership-initial-again",
	])
}

#[test]
fn resolved_membership_invite_freezes_selected_state_instead_of_losing_incoming_event() -> Result
{
	run(
		"resolved_membership_invite_freezes_selected_state_instead_of_losing_incoming_event",
		&[
			"membership-stripped-prepare",
			"membership-stripped-restart",
			"membership-stripped-again",
		],
	)
}

#[test]
fn refused_second_server_page_recovers_all_membership_additions() -> Result {
	run("refused_second_server_page_recovers_all_membership_additions", &[
		"membership-wide-add",
		"membership-wide-restart",
		"membership-wide-again",
	])
}

#[test]
fn refused_second_server_page_recovers_all_membership_removals() -> Result {
	run("refused_second_server_page_recovers_all_membership_removals", &[
		"membership-wide-remove",
		"membership-wide-restart",
		"membership-wide-again",
	])
}

#[test]
fn killed_final_recount_recovers_after_intermediate_server_pages_commit() -> Result {
	run("killed_final_recount_recovers_after_intermediate_server_pages_commit", &[
		"membership-wide-crash",
		"membership-wide-restart",
		"membership-wide-again",
	])
}

#[test]
fn replacement_writer_finishes_pending_membership_before_next_publication() -> Result {
	run("replacement_writer_finishes_pending_membership_before_next_publication", &[
		"membership-refuse",
		"membership-next",
		"membership-restart",
		"membership-again",
	])
}

#[test]
fn startup_repairs_membership_before_restoring_interrupted_history_exclusion() -> Result {
	run("startup_repairs_membership_before_restoring_interrupted_history_exclusion", &[
		"membership-history-crash",
		"membership-history-start",
		"membership-history-again",
	])
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
			let killed = matches!(
				*phase,
				"crash"
					| "membership-crash"
					| "membership-wide-crash"
					| "membership-history-crash"
			);
			let status = if killed {
				kill_paused_child(&mut command, &root)?
			} else {
				command.status()?
			};
			if !killed {
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
	setup_room(services, room).await
}

async fn setup_room(services: &Services, room: &RoomId) -> Result {
	let alice = user_id!("@source:localhost");
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

async fn interrupt_source_worker(services: &Services) -> Result {
	let sending = services.sending.clone();
	let worker = tokio::spawn(async move { sending.federation_source_worker().await });
	tokio::task::yield_now().await;
	crate::Service::interrupt(services.sending.as_ref()).await;
	tokio::time::timeout(std::time::Duration::from_secs(2), worker)
		.await
		.expect("owned source worker exits")??;
	Ok(())
}

fn auxiliary_phase(phase: &str) -> bool {
	matches!(
		phase.split('-').next(),
		Some(
			"notification"
				| "erasure" | "membership"
				| "interrupt"
				| "inventory"
				| "quota" | "receipt"
				| "presence"
		)
	)
}

async fn child(root: &Path, phase: &str) -> Result {
	let fixture = owned_fixture(root, phase).await?;
	if auxiliary_phase(phase) {
		return auxiliary_child(fixture, root, phase).await;
	}
	let services = &fixture.services;
	let alice = user_id!("@source:localhost");
	let room = room_id!("!source-handoff:localhost");
	if phase == "known-restart" || phase == "known-acked" {
		known_restart(services, root, phase).await?;
		fixture.finish().await;
		return Ok(());
	}
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
	if phase.starts_with("room-purge") {
		crate::admin::create_admin_room(services).await?;
	}

	if phase.starts_with("known-") || phase.starts_with("owned-") {
		known_first(services, root, phase).await?;
		fixture.finish().await;
		return Ok(());
	}
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
		|| phase.starts_with("room-purge")
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

async fn owned_fixture(root: &Path, phase: &str) -> Result<Fixture> {
	if matches!(phase, "membership-history-start" | "membership-history-again") {
		Fixture::open_startup(root).await
	} else {
		Fixture::open(root).await
	}
}

async fn auxiliary_child(fixture: Fixture, root: &Path, phase: &str) -> Result {
	if phase == "interrupt" {
		interrupt_source_worker(&fixture.services).await?;
	} else if phase.starts_with("notification-") {
		notification_controls::child(&fixture.services, root, phase).await?;
	} else if phase.starts_with("inventory-") {
		inventory_controls::child(&fixture.services, root, phase).await?;
	} else if phase.starts_with("presence-") {
		Box::pin(presence_controls::child(&fixture.services, root, phase)).await?;
	} else if phase.starts_with("receipt-") {
		Box::pin(receipt_inventory_controls::child(&fixture.services, root, phase)).await?;
	} else if phase.starts_with("quota-") {
		quota_controls::child(&fixture.services, root, phase).await?;
	} else if phase.starts_with("membership-history-") {
		history_controls::child(&fixture.services, root, phase).await?;
	} else if phase.starts_with("membership-") {
		membership_controls::child(&fixture.services, root, phase).await?;
	} else {
		erasure_controls::child(&fixture.services, root, phase).await?;
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

async fn known_first(services: &Services, root: &Path, phase: &str) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let alice = user_id!("@source:localhost");
	let lock = services.state.mutex.lock(room).await;
	let builder = PduBuilder::state(
		alice.to_string(),
		&RoomMemberEventContent::new(MembershipState::Leave),
	);
	let event = if phase.starts_with("owned-") {
		if phase.starts_with("owned-plan-") {
			tuwunel_database::refusal::refuse_next("servernameevent_data");
		}
		let event = services
			.timeline
			.build_and_append_pdu(builder, alice, room, &lock)
			.await?;
		drop(lock);
		event
	} else {
		let (pdu, json) = services
			.timeline
			.create_hash_and_sign_event(builder, alice, room, &lock)
			.await?;
		drop(lock);
		let json = tuwunel_core::matrix::pdu::into_outgoing_federation(json, &RoomVersionId::V10);
		services
			.event_handler
			.handle_incoming_pdu(services.globals.server_name(), room, &pdu.event_id, json, true)
			.await?
			.expect("ordinary inbound acceptance");
		assert!(!owns(services, &pdu.event_id).await?, "ordinary inbound has no broadcast role");
		pdu.event_id
	};
	fs::write(root.join("stored-event"), event.as_bytes())?;
	if phase.starts_with("owned-") {
		return finish_owned_role(services, &event, phase).await;
	}
	let raw = services.timeline.get_pdu_id(&event).await?;
	let state = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let json = services.timeline.get_pdu_json(&event).await?;
	let json = tuwunel_core::matrix::pdu::into_outgoing_federation(json, &RoomVersionId::V10);
	if phase == "known-concurrent" {
		return concurrent_known(services, &event, json).await;
	}
	let refusal = if phase == "known-refuse" {
		"pduid_federationplan"
	} else {
		"servernameevent_data"
	};
	tuwunel_database::refusal::refuse_next(refusal);
	let outcome = services
		.event_handler
		.handle_incoming_pdu_and_federate(
			services.globals.server_name(),
			room,
			&event,
			json.clone(),
		)
		.await;
	if phase == "known-refuse" {
		outcome.expect_err("first known-event role must report its refused admission");
		assert_eq!(tuwunel_database::refusal::pending(), 0, "role refusal actually fired");
		assert!(!owns(services, &event).await?);
		services
			.event_handler
			.handle_incoming_pdu_and_federate(services.globals.server_name(), room, &event, json)
			.await?
			.expect("retry admits first delivery role");
	} else {
		outcome?.expect("first known-event role accepted");
		assert!(
			owns(services, &event).await?,
			"first handshake skipped known-event delivery ownership"
		);
		assert_eq!(tuwunel_database::refusal::pending(), 0, "queue refusal actually fired");
		assert!(
			services
				.sending
				.db
				.has_federation_plan(&raw)
				.await?
		);
	}
	assert_eq!(services.timeline.get_pdu_id(&event).await?, raw, "no new canonical event");
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
		state,
		"no current-state rewrite"
	);
	Ok(())
}

async fn concurrent_known(
	services: &Services,
	event: &EventId,
	json: ruma::CanonicalJsonObject,
) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let origin = services.globals.server_name();
	let raw = services.timeline.get_pdu_id(event).await?;
	let state = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let mut pause = tuwunel_database::refusal::pause_next("pduid_federationplan");
	let first = services
		.event_handler
		.handle_incoming_pdu_and_federate(origin, room, event, json.clone());
	tokio::pin!(first);
	tokio::select! {
		result = &mut first => panic!("first role finished before its commit pause: {result:?}"),
		entered = pause.entered() => entered?,
	}
	assert!(!owns(services, event).await?, "uncommitted role cannot publish ownership");
	let second = services
		.event_handler
		.handle_incoming_pdu_and_federate(origin, room, event, json.clone());
	tokio::pin!(second);
	tokio::select! {
		biased;
		result = &mut second => panic!("concurrent role escaped canonical exclusion: {result:?}"),
		() = tokio::task::yield_now() => {},
	}
	drop(pause);
	let (first, second) = tokio::join!(first, second);
	assert_eq!(first?.expect("first role accepted").0, raw);
	assert_eq!(second?.expect("concurrent role accepted").0, raw);
	let destination = Destination::Federation(server_name!("handoff.invalid").to_owned());
	let queued = services
		.sending
		.db
		.queued_requests(&destination)
		.try_collect::<Vec<_>>()
		.await?;
	assert_eq!(
		queued
			.iter()
			.filter(|(_, event)| *event == SendingEvent::Pdu(raw))
			.count(),
		1
	);
	ack_known(services).await?;
	let (first, second) = tokio::join!(
		services
			.event_handler
			.handle_incoming_pdu_and_federate(origin, room, event, json.clone()),
		services
			.event_handler
			.handle_incoming_pdu_and_federate(origin, room, event, json),
	);
	first?.expect("completed role retry accepted");
	second?.expect("concurrent completed role retry accepted");
	assert!(!owns(services, event).await?, "concurrent retry recreated ACKed delivery");
	assert_eq!(services.timeline.get_pdu_id(event).await?, raw);
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
		state
	);
	Ok(())
}

async fn finish_owned_role(services: &Services, event: &EventId, phase: &str) -> Result {
	let room = room_id!("!source-handoff:localhost");
	if phase.starts_with("owned-plan-") {
		return refuse_corrupt_owned_plan(services, event, phase).await;
	}
	ack_known(services).await?;
	if phase == "owned-corrupt" {
		let raw = services.timeline.get_pdu_id(event).await?;
		let mut key = vec![0x09];
		key.extend_from_slice(raw.as_ref());
		services.db["global"].remove(&key).await?;
		let json = services.timeline.get_pdu_json(event).await?;
		services
			.event_handler
			.handle_incoming_pdu_and_federate(services.globals.server_name(), room, event, json)
			.await
			.expect_err("missing role cannot recreate acknowledged delivery");
		assert!(!owns(services, event).await?);
	}
	Ok(())
}

#[expect(
	clippy::arithmetic_side_effects,
	reason = "fixed codec offsets in an owned valid fixture"
)]
async fn refuse_corrupt_owned_plan(services: &Services, event: &EventId, phase: &str) -> Result {
	let raw = services.timeline.get_pdu_id(event).await?;
	assert_eq!(tuwunel_database::refusal::pending(), 0, "queue refusal retained the plan");
	let mut key = vec![0x08];
	key.extend_from_slice(raw.as_ref());
	let original = services.db["pduid_federationplan"]
		.get(raw.as_ref())
		.await?
		.as_ref()
		.to_vec();
	let mut witness = services.db["global"]
		.get(&key)
		.await?
		.as_ref()
		.to_vec();
	let mut plan = original.clone();
	if phase == "owned-plan-rebound" {
		// Keep a valid codec and integrity hash while rebinding the event ID.
		let offset = 5 + 8;
		let room_length = usize::from(u16::from_be_bytes(
			plan[offset..offset + 2]
				.try_into()
				.expect("owned length width"),
		));
		let offset = offset + 2 + room_length;
		let event_length = usize::from(u16::from_be_bytes(
			plan[offset..offset + 2]
				.try_into()
				.expect("owned length width"),
		));
		let last = offset + 2 + event_length - 1;
		plan[last] = if plan[last] == b'a' { b'b' } else { b'a' };
		witness[4..].copy_from_slice(&tuwunel_core::utils::hash::sha256::hash(&plan));
		services.db["pduid_federationplan"]
			.insert(raw.as_ref(), &plan)
			.await?;
	} else {
		witness[4] ^= 1;
	}
	services.db["global"]
		.insert(&key, &witness)
		.await?;
	let count = services.globals.current_count();
	let json = services.timeline.get_pdu_json(event).await?;
	services
		.event_handler
		.handle_incoming_pdu_and_federate(
			services.globals.server_name(),
			room_id!("!source-handoff:localhost"),
			event,
			json,
		)
		.await
		.expect_err("owned role cannot hide a corrupt outstanding obligation");
	assert_eq!(services.globals.current_count(), count);
	assert_eq!(
		services.db["pduid_federationplan"]
			.get(raw.as_ref())
			.await?
			.as_ref(),
		plan
	);
	assert_eq!(services.db["global"].get(&key).await?.as_ref(), witness);
	let destination = Destination::Federation(server_name!("handoff.invalid").to_owned());
	assert!(
		services
			.sending
			.db
			.queued_requests(&destination)
			.try_collect::<Vec<_>>()
			.await?
			.is_empty(),
		"refusal does not materialize queue work"
	);
	// Repair only the exclusively owned corrupt fixture, then prove its saved
	// source still transfers once; no canonical event is reaccepted.
	let mut original_witness = u32::try_from(original.len())
		.expect("owned bounded plan")
		.to_be_bytes()
		.to_vec();
	original_witness.extend_from_slice(&tuwunel_core::utils::hash::sha256::hash(&original));
	services.db["pduid_federationplan"]
		.insert(raw.as_ref(), &original)
		.await?;
	services.db["global"]
		.insert(&key, original_witness)
		.await?;
	services
		.sending
		.resume_federation_source(raw)
		.await?;
	assert!(owns(services, event).await?);
	Ok(())
}

async fn ack_known(services: &Services) -> Result {
	let destination = Destination::Federation(server_name!("handoff.invalid").to_owned());
	let queued = services
		.sending
		.db
		.queued_requests(&destination)
		.try_collect::<Vec<_>>()
		.await?;
	assert!(!queued.is_empty(), "owned delivery exists before ACK");
	services
		.sending
		.db
		.mark_as_active(queued.iter())
		.await?;
	let (_, ack) = services
		.sending
		.db
		.active_batch(&destination)
		.await?;
	services
		.sending
		.db
		.acknowledge_active(&destination, &ack)
		.await?;
	Ok(())
}

async fn known_restart(services: &Services, root: &Path, phase: &str) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let event = EventId::parse(fs::read_to_string(root.join("stored-event"))?)?;
	let raw = services.timeline.get_pdu_id(&event).await?;
	let state = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	if phase == "known-restart" {
		assert!(owns(services, &event).await?, "restart retains first-handshake obligation");
		services
			.sending
			.resume_federation_source(raw)
			.await?;
		ack_known(services).await?;
	}
	assert!(!owns(services, &event).await?, "delivery was already acknowledged");
	let json = services.timeline.get_pdu_json(&event).await?;
	let json = tuwunel_core::matrix::pdu::into_outgoing_federation(json, &RoomVersionId::V10);
	services
		.event_handler
		.handle_incoming_pdu_and_federate(services.globals.server_name(), room, &event, json)
		.await?
		.expect("known-event retry succeeds");
	assert!(!owns(services, &event).await?, "retry recreated acknowledged delivery");
	assert_eq!(services.timeline.get_pdu_id(&event).await?, raw);
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
		state
	);
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
	if phase.starts_with("room-purge") {
		purge_room_source(services, &event, phase == "room-purge-refuse").await?;
	}
	if phase.starts_with("purge") {
		purge_source(services, &event, phase == "purge").await?;
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

async fn purge_source(services: &Services, event: &EventId, pending: bool) -> Result {
	let raw = services.timeline.get_pdu_id(event).await?;
	assert_eq!(tuwunel_database::refusal::pending(), 0);
	assert_eq!(
		services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?,
		pending
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
	let destination = Destination::Federation(server_name!("handoff.invalid").to_owned());
	let queued = services
		.sending
		.db
		.queued_requests(&destination)
		.try_collect::<Vec<_>>()
		.await?;
	assert!(
		!queued
			.iter()
			.any(|(_, event)| *event == SendingEvent::Pdu(raw)),
		"history erasure left a materialized federation delivery referencing the deleted PDU"
	);
	assert!(
		services
			.timeline
			.get_pdu_id(event)
			.await
			.is_err_and(|error| error.is_not_found())
	);
	Ok(())
}

async fn purge_room_source(services: &Services, event: &EventId, refuse: bool) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let raw = services.timeline.get_pdu_id(event).await?;
	assert_eq!(tuwunel_database::refusal::pending(), 0, "initial queue refusal fired");
	assert!(
		services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	let mut role = vec![0x09];
	role.extend_from_slice(raw.as_ref());
	let receipt = services.db["global"]
		.get(&role)
		.await?
		.as_ref()
		.to_vec();
	let mut witness = role.clone();
	witness[0] = 0x08;
	if refuse {
		let canonical = services.timeline.get_pdu_json(event).await?;
		let plan = services.db["pduid_federationplan"]
			.get(raw.as_ref())
			.await?
			.as_ref()
			.to_vec();

		// This map is removed only by the room erase commit. Earlier local
		// departure writes must not consume the refusal.
		tuwunel_database::refusal::refuse_next("roomid_shortroomid");
		let lock = services.state.mutex.lock(room).await;
		services
			.delete
			.delete_room(room, true, lock)
			.await
			.expect_err("room erase commit was refused");
		assert_eq!(tuwunel_database::refusal::pending(), 0, "erase refusal fired");
		assert_eq!(services.timeline.get_pdu_id(event).await?, raw);
		assert_eq!(services.timeline.get_pdu_json(event).await?, canonical);
		assert_eq!(
			services.db["pduid_federationplan"]
				.get(raw.as_ref())
				.await?
				.as_ref(),
			plan
		);
		assert_eq!(services.db["global"].get(&role).await?.as_ref(), receipt);
	}
	let lock = services.state.mutex.lock(room).await;
	services
		.delete
		.delete_room(room, true, lock)
		.await?;
	assert!(
		!services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	assert!(
		services.db["global"]
			.get(&role)
			.await
			.is_err_and(|error| error.is_not_found())
	);
	assert!(
		services.db["global"]
			.get(&witness)
			.await
			.is_err_and(|error| error.is_not_found())
	);
	assert!(
		services
			.timeline
			.get_pdu_id(event)
			.await
			.is_err_and(|error| error.is_not_found())
	);
	services
		.sending
		.resume_federation_source(raw)
		.await?;
	Ok(())
}
