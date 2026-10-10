//! Real canonical history controls with independent destinations and saved
//! transaction bodies. No transport or provider is used.
use std::{fs, path::Path};

use futures::TryStreamExt;
use ruma::{EventId, OwnedEventId, room_id, server_name, user_id};
use serde_json::json;
use tuwunel_core::{
	Result,
	matrix::{PduCount, RawPduId, pdu::PduBuilder},
};

use super::{Destination, SendingEvent, Services, run, setup};
use crate::{
	sending::data::PreparedAttempt,
	tasks::history::{Executor, History, Phase},
};

const SAVED: &[u8] = b"test-event-erasure-history";

#[test]
fn active_erasure_preserves_unrelated_deliveries_and_fences_old_ack() -> Result {
	run(
		"erasure_controls::active_erasure_preserves_unrelated_deliveries_and_fences_old_ack",
		&["erasure-active"],
	)
}

#[test]
fn erasure_retires_only_orphaned_push_backoff_and_fences_late_failures() -> Result {
	run(
		"erasure_controls::erasure_retires_only_orphaned_push_backoff_and_fences_late_failures",
		&["erasure-push-backoff"],
	)
}

#[test]
fn paged_erasure_blocks_late_admissions_and_resumes_after_cold_restart() -> Result {
	run(
		"erasure_controls::paged_erasure_blocks_late_admissions_and_resumes_after_cold_restart",
		&["erasure-page", "erasure-restart"],
	)
}

#[test]
fn missing_erasure_marker_refuses_a_saved_cursor_without_mutation() -> Result {
	run(
		"erasure_controls::missing_erasure_marker_refuses_a_saved_cursor_without_mutation",
		&["erasure-page", "erasure-marker-loss"],
	)
}

#[test]
fn wide_chunked_attempt_erasure_resumes_within_the_history_commit_budget() -> Result {
	run(
		"erasure_controls::wide_chunked_attempt_erasure_resumes_within_the_history_commit_budget",
		&["erasure-wide-page", "erasure-wide-restart"],
	)
}

async fn message(services: &Services) -> Result<(OwnedEventId, RawPduId, RawPduId)> {
	setup(services).await?;
	let room = room_id!("!source-handoff:localhost");
	let keep = services
		.timeline
		.latest_pdu_in_room(room)
		.await?
		.event_id
		.clone();
	let keep = services.timeline.get_pdu_id(&keep).await?;
	let lock = services.state.mutex.lock(room).await;
	let event = services
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(
				&ruma::events::room::message::RoomMessageEventContent::text_plain(
					"erased body sentinel",
				),
			),
			user_id!("@source:localhost"),
			room,
			&lock,
		)
		.await?;
	let raw = services.timeline.get_pdu_id(&event).await?;
	Ok((event, raw, keep))
}

async fn attempted(
	services: &Services,
	destination: &Destination,
	raw: RawPduId,
	keep: RawPduId,
) -> Result<PreparedAttempt> {
	let data = &services.sending.db;
	let events = [SendingEvent::Pdu(raw), SendingEvent::Pdu(keep)];
	data.queue_requests(events.iter().map(|event| (event, destination)))
		.await?;
	let queued = data
		.queued_requests(destination)
		.try_collect::<Vec<_>>()
		.await?;
	data.mark_as_active(queued.iter()).await?;
	let (_, selected) = data.active_batch(destination).await?;
	let body = serde_json::to_vec(&json!({"events":["erased body sentinel", "retained state"]}))?;
	let recipient = matches!(destination, Destination::Appservice(_)).then_some([7; 32]);
	data.persist_attempt(destination, selected, body, recipient)
		.await
}

async fn purge(services: &Services) -> Result<usize> {
	let until = *services.globals.next_count().await?;
	services
		.timeline
		.purge_history(room_id!("!source-handoff:localhost"), PduCount::Normal(until), true)
		.await
}

async fn snapshot(services: &Services) -> Result<Vec<Vec<tuwunel_database::Row>>> {
	let mut maps = Vec::new();
	for name in [
		"pduid_pdu",
		"servernameevent_data",
		"servercurrentevent_data",
		"sendingtransaction_record",
		"pduid_federationplan",
		"global",
	] {
		maps.push(
			services.db[name]
				.raw_rows_after(None, 4096)
				.await?,
		);
	}
	Ok(maps)
}

async fn verify_retired(services: &Services, raw: RawPduId) -> Result {
	let missing = services.db["pduid_pdu"]
		.exists(raw.as_ref())
		.await
		.expect_err("canonical PDU erased");
	assert!(missing.is_not_found());
	assert!(
		!services
			.sending
			.db
			.event_erasure_started(&raw)
			.await?,
		"completed erasure retains no marker"
	);
	let destination = Destination::Federation(server_name!("late.invalid").to_owned());
	let before = snapshot(services).await?;
	services
		.sending
		.db
		.queue_requests(std::iter::once((&SendingEvent::Pdu(raw), &destination)))
		.await
		.expect_err("late independent writer cannot recreate erased delivery");
	assert_eq!(snapshot(services).await?, before, "refused admission is mutation-free");
	Ok(())
}

async fn active(services: &Services) -> Result {
	let (event, raw, keep) = message(services).await?;
	let destinations = [
		Destination::Appservice("independent-erasure".into()),
		Destination::Federation(server_name!("independent.invalid").to_owned()),
	];
	let mut attempts = Vec::new();
	for destination in &destinations {
		attempts.push(attempted(services, destination, raw, keep).await?);
	}
	let push = Destination::Push(user_id!("@source:localhost").to_owned(), "erase-push".into());
	services
		.sending
		.db
		.queue_requests(std::iter::once((&SendingEvent::FrozenPush(raw), &push)))
		.await?;
	let before = snapshot(services).await?;
	tuwunel_database::refusal::refuse_next("pduid_pdu");
	purge(services)
		.await
		.expect_err("canonical erasure was refused");
	assert_eq!(tuwunel_database::refusal::pending(), 0);
	// purge captures a new stream boundary; omit only its legitimate global
	// counter change from the erasure atomicity comparison.
	let after = snapshot(services).await?;
	assert_eq!(&before[..5], &after[..5]);
	let metadata = |rows: &Vec<tuwunel_database::Row>| {
		rows.iter()
			.filter(|(key, _)| key.as_slice() != b"c")
			.cloned()
			.collect::<Vec<_>>()
	};
	assert_eq!(metadata(&before[5]), metadata(&after[5]));
	for (destination, old) in destinations.iter().zip(&attempts) {
		services
			.sending
			.db
			.require_current_attempt(destination, old)
			.await?;
	}
	assert_eq!(purge(services).await?, 1);
	assert!(
		services
			.timeline
			.get_pdu_id(&event)
			.await
			.is_err_and(|error| error.is_not_found())
	);
	assert!(
		services
			.sending
			.db
			.queued_requests(&push)
			.try_collect::<Vec<_>>()
			.await?
			.is_empty()
	);
	assert!(
		services.db["sendingtransaction_record"]
			.raw_keys_after(None, 1)
			.await?
			.is_empty(),
		"old headers and all body chunks retired"
	);
	for (destination, old) in destinations.iter().zip(&attempts) {
		let data = &services.sending.db;
		assert!(data.load_attempt(destination).await?.is_none());
		let (events, selected) = data.active_batch(destination).await?;
		assert_eq!(events, [SendingEvent::Pdu(keep)], "unrelated state delivery remains");
		let next = data
			.persist_attempt(
				destination,
				selected,
				serde_json::to_vec(&json!({"events":["retained state"]}))?,
				old.recipient,
			)
			.await?;
		assert!(next.transaction_id() > old.transaction_id());
		data.acknowledge_active(destination, &old.acknowledgement)
			.await?;
		data.require_current_attempt(destination, &next)
			.await?;
		data.acknowledge_active(destination, &next.acknowledgement)
			.await?;
		assert!(
			data.active_requests_for(destination)
				.try_collect::<Vec<_>>()
				.await?
				.is_empty()
		);
	}
	verify_retired(services, raw).await
}

async fn push_backoff(services: &Services) -> Result {
	let (_, raw, keep) = message(services).await?;
	let data = &services.sending.db;
	let orphan = Destination::Push(user_id!("@source:localhost").to_owned(), "erase-only".into());
	let shared =
		Destination::Push(user_id!("@source:localhost").to_owned(), "erase-shared".into());
	let backoff = crate::sending::data::PushBackoff::failed(3)?;
	let mut old = Vec::new();
	for destination in [&orphan, &shared] {
		data.queue_requests(std::iter::once((&SendingEvent::Pdu(raw), destination)))
			.await?;
		let queued = data
			.queued_requests(destination)
			.try_collect::<Vec<_>>()
			.await?;
		data.mark_as_active(queued.iter()).await?;
		if destination == &shared {
			data.queue_requests(std::iter::once((&SendingEvent::Pdu(keep), destination)))
				.await?;
			let queued = data
				.queued_requests(destination)
				.try_collect::<Vec<_>>()
				.await?;
			data.mark_as_active(queued.iter()).await?;
		}
		let (_, rows) = data.active_batch(destination).await?;
		assert!(
			data.persist_push_backoff(destination, &rows, backoff)
				.await?
		);
		old.push(rows);
	}
	let before = snapshot(services).await?;
	tuwunel_database::refusal::refuse_next("pduid_pdu");
	purge(services)
		.await
		.expect_err("erasure commit refuses atomically");
	assert_eq!(tuwunel_database::refusal::pending(), 0);
	assert_eq!(&snapshot(services).await?[..5], &before[..5]);
	for destination in [&orphan, &shared] {
		assert_eq!(data.push_backoff(destination).await?, Some(backoff));
	}
	assert_eq!(purge(services).await?, 1);
	assert!(data.push_backoff(&orphan).await?.is_none(), "erased last owner retires backoff");
	assert_eq!(
		data.push_backoff(&shared).await?,
		Some(backoff),
		"unrelated active work keeps delay"
	);
	for (destination, rows) in [&orphan, &shared].into_iter().zip(&old) {
		assert!(
			!data
				.persist_push_backoff(destination, rows, backoff)
				.await?,
			"late failure is fenced"
		);
	}
	assert!(data.push_backoff(&orphan).await?.is_none());
	let (events, rows) = data.active_batch(&shared).await?;
	assert_eq!(events, [SendingEvent::Pdu(keep)]);
	data.acknowledge_active(&shared, &rows).await?;
	assert!(data.push_backoff(&shared).await?.is_none());
	Ok(())
}

async fn step(services: &Services, history: History) -> Result<History> {
	let room = room_id!("!source-handoff:localhost");
	let _state = services.state.mutex.lock(room).await;
	let _insert = services.timeline.mutex_insert.lock(room).await;
	let _originals = services.retention.lock_originals().await;
	let _source = services
		.sending
		.db
		.lock_federation_sources()
		.await;
	let (mut txn, next) = services
		.timeline
		.prepare_history_step(room, history)
		.await?;
	// Lower-level control persists the production executor's cursor with the
	// same native commit. The separate admin integration suite owns job startup.
	txn.insert_raw(&services.db["global"], SAVED, serde_json::to_vec(&next)?);
	crate::rooms::timeline::check_purge_batch(&txn)?;
	txn.execute_flushed().await?;
	Ok(next)
}

async fn page(services: &Services, root: &Path) -> Result {
	let (event, raw, _) = message(services).await?;
	fs::write(root.join("erasure-event"), event.as_bytes())?;
	let data = &services.sending.db;
	for index in 0..40 {
		let destination = Destination::Federation(format!("page{index:03}.invalid").try_into()?);
		data.queue_requests(std::iter::once((&SendingEvent::Pdu(raw), &destination)))
			.await?;
	}
	let room = room_id!("!source-handoff:localhost");
	let mut history = History {
		executor: Executor::HistoryV1,
		boundary: i64::try_from(*services.globals.next_count().await?)?,
		delete_local_events: true,
		shortroomid: services.short.get_shortroomid(room).await?,
		after: None,
		purged: 0,
		current: None,
		done: false,
	};
	for _ in 0..100 {
		history = step(services, history).await?;
		if history.current.as_ref().is_some_and(|target| {
			target.phase == Phase::OutgoingPending && target.after.is_some()
		}) {
			break;
		}
	}
	assert!(data.event_erasure_started(&raw).await?);
	services.db["pduid_pdu"]
		.exists(raw.as_ref())
		.await?;
	let destination =
		Destination::Federation(server_name!("aaa-behind-cursor.invalid").to_owned());
	let before = snapshot(services).await?;
	data.queue_requests(std::iter::once((&SendingEvent::Pdu(raw), &destination)))
		.await
		.expect_err("durable marker blocks admission behind cursor");
	assert_eq!(snapshot(services).await?, before);
	Ok(())
}

async fn restart(services: &Services, root: &Path) -> Result {
	let event = EventId::parse(fs::read_to_string(root.join("erasure-event"))?)?;
	let raw = services.timeline.get_pdu_id(&event).await?;
	assert!(
		services
			.sending
			.db
			.event_erasure_started(&raw)
			.await?
	);
	let encoded = services.db["global"].get(SAVED).await?;
	let mut history =
		History::decode(&serde_json::from_slice(&encoded)?)?.expect("durable typed progress");
	assert!(
		history.current.as_ref().is_some_and(
			|target| target.phase == Phase::OutgoingPending && target.after.is_some()
		)
	);
	for _ in 0..200 {
		if history.done {
			break;
		}
		history = step(services, history).await?;
	}
	assert!(history.done);
	assert_eq!(history.purged, 1);
	for index in 0..40 {
		let destination = Destination::Federation(format!("page{index:03}.invalid").try_into()?);
		assert!(
			services
				.sending
				.db
				.queued_requests(&destination)
				.try_collect::<Vec<_>>()
				.await?
				.is_empty()
		);
	}
	verify_retired(services, raw).await
}

pub(super) async fn child(services: &Services, root: &Path, phase: &str) -> Result {
	match phase {
		| "erasure-active" => active(services).await,
		| "erasure-push-backoff" => push_backoff(services).await,
		| "erasure-page" => page(services, root).await,
		| "erasure-restart" => restart(services, root).await,
		| "erasure-marker-loss" => marker_loss(services, root).await,
		| "erasure-wide-page" => wide_page(services, root).await,
		| "erasure-wide-restart" => wide_restart(services, root).await,
		| _ => panic!("unknown owned erasure phase"),
	}
}

async fn marker_loss(services: &Services, root: &Path) -> Result {
	let event = EventId::parse(fs::read_to_string(root.join("erasure-event"))?)?;
	let raw = services.timeline.get_pdu_id(&event).await?;
	let encoded = services.db["global"].get(SAVED).await?;
	let history =
		History::decode(&serde_json::from_slice(&encoded)?)?.expect("durable typed progress");
	let mut marker = vec![0x0A];
	marker.extend_from_slice(raw.as_ref());
	services.db["global"].remove(&marker).await?;
	let destination =
		Destination::Federation(server_name!("aaa-behind-cursor.invalid").to_owned());
	services
		.sending
		.db
		.queue_requests(std::iter::once((&SendingEvent::Pdu(raw), &destination)))
		.await?;
	let before = snapshot(services).await?;
	let error = step(services, history)
		.await
		.expect_err("lost admission fence cannot silently resume after saved cursor");
	assert!(
		error
			.to_string()
			.contains("History erasure lost its admission marker")
	);
	assert_eq!(snapshot(services).await?, before);
	Ok(())
}

fn wide_destination(letter: char) -> Destination {
	Destination::Appservice(format!(
		"{letter}{}",
		"é".repeat(tuwunel_bridge::MAX_KEY_BYTES.saturating_sub(20) / 2)
	))
}

async fn wide_page(services: &Services, root: &Path) -> Result {
	let (event, raw, keep) = message(services).await?;
	fs::write(root.join("erasure-event"), event.as_bytes())?;
	for letter in ['a', 'b'] {
		let destination = wide_destination(letter);
		let data = &services.sending.db;
		let events = [SendingEvent::Pdu(raw), SendingEvent::Pdu(keep)];
		data.queue_requests(events.iter().map(|event| (event, &destination)))
			.await?;
		let queued = data
			.queued_requests(&destination)
			.try_collect::<Vec<_>>()
			.await?;
		data.mark_as_active(queued.iter()).await?;
		let (_, selected) = data.active_batch(&destination).await?;
		let mut body = br#"{"events":[""#.to_vec();
		body.resize(crate::sending::data::BODY_LIMIT.saturating_sub(3), b'x');
		body.extend_from_slice(br#""]}"#);
		assert_eq!(body.len(), crate::sending::data::BODY_LIMIT);
		data.persist_attempt(&destination, selected, body, Some([7; 32]))
			.await?;
	}
	// Both immutable attempts exceed a single erase transaction's key-byte
	// budget together. Synchronous refusal must leave every delivery intact.
	let before = snapshot(services).await?;
	purge(services)
		.await
		.expect_err("wide attempts require paged erasure");
	assert_eq!(&snapshot(services).await?[..5], &before[..5]);
	let room = room_id!("!source-handoff:localhost");
	let mut history = History {
		executor: Executor::HistoryV1,
		boundary: i64::try_from(*services.globals.next_count().await?)?,
		delete_local_events: true,
		shortroomid: services.short.get_shortroomid(room).await?,
		after: None,
		purged: 0,
		current: None,
		done: false,
	};
	for _ in 0..100 {
		history = step(services, history).await?;
		if history.current.as_ref().is_some_and(|target| {
			target.phase == Phase::OutgoingActive
				&& target
					.after
					.as_ref()
					.is_some_and(|key| key.len() > 16_000)
		}) {
			break;
		}
	}
	assert!(history.current.as_ref().is_some_and(|target| {
		target.phase == Phase::OutgoingActive
			&& target
				.after
				.as_ref()
				.is_some_and(|key| key.len() > 16_000)
	}));
	assert!(
		services
			.sending
			.db
			.event_erasure_started(&raw)
			.await?
	);
	Ok(())
}

async fn wide_restart(services: &Services, root: &Path) -> Result {
	let event = EventId::parse(fs::read_to_string(root.join("erasure-event"))?)?;
	let raw = services.timeline.get_pdu_id(&event).await?;
	let encoded = services.db["global"].get(SAVED).await?;
	let mut history =
		History::decode(&serde_json::from_slice(&encoded)?)?.expect("durable wide cursor");
	assert!(
		history
			.current
			.as_ref()
			.is_some_and(|target| target.phase == Phase::OutgoingActive)
	);
	for _ in 0..100 {
		if history.done {
			break;
		}
		history = step(services, history).await?;
	}
	assert!(history.done);
	assert_eq!(history.purged, 1);
	for letter in ['a', 'b'] {
		let destination = wide_destination(letter);
		let data = &services.sending.db;
		assert!(data.load_attempt(&destination).await?.is_none());
		let (events, _) = data.active_batch(&destination).await?;
		assert_eq!(events.len(), 1, "unrelated state remains owed after cold restart");
		assert_ne!(events[0], SendingEvent::Pdu(raw));
	}
	assert!(
		services.db["sendingtransaction_record"]
			.raw_keys_after(None, 1)
			.await?
			.is_empty()
	);
	verify_retired(services, raw).await
}
