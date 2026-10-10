//! Canonical admission at count/byte capacity, then cold recovery and refill.
//! Large authoritative state is synthetic; peer authentication is outside
//! scope.

use std::{fs, path::Path, sync::Arc};

use futures::TryStreamExt;
use ruma::{
	EventId, OwnedEventId, OwnedRoomId, OwnedServerName, RoomId,
	events::{StateEventType, room::message::RoomMessageEventContent},
	room_id, server_name, user_id,
};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Result, matrix::pdu::PduBuilder};
use tuwunel_database::refusal;

use crate::{
	Services,
	rooms::state_compressor::{CompressedState, compress_state_event},
	sending::{Destination, SendingEvent},
	transaction_ids,
};

const PLAN: &str = "pduid_federationplan";
const COUNT_LIMIT: usize = 64;
const BYTE_LIMIT: usize = 8 * 1024 * 1024;
const CANCELLATION_RESERVE: usize = 4097 * 2;

#[derive(Deserialize, Serialize)]
struct Accepted {
	event: OwnedEventId,
	room: OwnedRoomId,
	transaction: usize,
}

#[derive(Deserialize, Serialize)]
struct Expected {
	byte_quota: bool,
	events: Vec<Accepted>,
	servers: Vec<OwnedServerName>,
	states: Vec<(OwnedRoomId, u64)>,
	refused_room: OwnedRoomId,
	refused_transaction: usize,
	refused_frontier: OwnedEventId,
	pending_bytes: usize,
	refilled: bool,
}

pub(super) async fn child(services: &Services, root: &Path, phase: &str) -> Result {
	if phase == "quota-restart" || phase == "quota-again" {
		return Box::pin(restart(services, root, phase)).await;
	}
	super::setup(services).await?;
	let expected = if phase == "quota-count" {
		Box::pin(count_capacity(services)).await?
	} else {
		assert_eq!(phase, "quota-bytes");
		Box::pin(byte_capacity(services)).await?
	};
	assert_saturated(services, &expected).await?;
	fs::write(root.join("quota-expected.json"), serde_json::to_vec(&expected)?)?;
	Ok(())
}

async fn append(services: &Services, room: &RoomId, index: usize) -> Result<Accepted> {
	let alice = user_id!("@source:localhost");
	let transaction = format!("quota-{index}");
	let key = transaction_ids::key(
		alice,
		Some(ruma::device_id!("quota-device")),
		transaction.as_str().into(),
	);
	let lock = services.state.mutex.lock(room).await;
	let event = services
		.timeline
		.build_and_append_pdu_with_txnid(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(format!(
				"quota admission {index}"
			))),
			alice,
			room,
			Some(key.as_slice()),
			&lock,
		)
		.await?;
	Ok(Accepted {
		event,
		room: room.to_owned(),
		transaction: index,
	})
}

async fn retry(services: &Services, index: usize) -> Result<Option<OwnedEventId>> {
	let transaction = format!("quota-{index}");
	match services
		.transaction_ids
		.existing_txnid(
			user_id!("@source:localhost"),
			Some(ruma::device_id!("quota-device")),
			transaction.as_str().into(),
		)
		.await
	{
		| Ok(value) => Ok(Some(EventId::parse(std::str::from_utf8(value.as_ref())?)?)),
		| Err(error) if error.is_not_found() => Ok(None),
		| Err(error) => Err(error),
	}
}

async fn count_capacity(services: &Services) -> Result<Expected> {
	let first = room_id!("!source-handoff:localhost");
	let second = room_id!("!quota-second:localhost");
	super::setup_room(services, second).await?;
	let states = vec![
		(
			first.to_owned(),
			services
				.state
				.get_room_shortstatehash(first)
				.await?,
		),
		(
			second.to_owned(),
			services
				.state
				.get_room_shortstatehash(second)
				.await?,
		),
	];
	let mut events = Vec::new();
	for index in 0..COUNT_LIMIT - 1 {
		refusal::refuse_next("servernameevent_data");
		events.push(append(services, first, index).await?);
		assert_eq!(
			refusal::pending(),
			0,
			"accepted source remains pending after actual queue refusal"
		);
	}
	let first_frontier = services
		.timeline
		.latest_pdu_in_room(first)
		.await?
		.event_id
		.clone();
	let second_frontier = services
		.timeline
		.latest_pdu_in_room(second)
		.await?
		.event_id
		.clone();
	refusal::refuse_next("servernameevent_data");
	refusal::refuse_next("servernameevent_data");
	let (a, b) = tokio::join!(append(services, first, 63), append(services, second, 64));
	let (accepted, error, refused_room, refused_transaction, refused_frontier) = match (a, b) {
		| (Ok(accepted), Err(error)) => (accepted, error, second, 64, second_frontier),
		| (Err(error), Ok(accepted)) => (accepted, error, first, 63, first_frontier),
		| _ =>
			panic!("exactly one of two competing rooms must obtain the final global source slot"),
	};
	assert!(
		error
			.to_string()
			.contains("Canonical federation admission is full"),
		"quota refusal: {error}"
	);
	events.push(accepted);
	assert_eq!(refusal::pending(), 1, "refused admission never attempted its queue page");
	let pending_bytes = source_bytes(services, &events).await?;
	Ok(Expected {
		byte_quota: false,
		events,
		servers: vec![server_name!("handoff.invalid").to_owned()],
		states,
		refused_room: refused_room.to_owned(),
		refused_transaction,
		refused_frontier,
		pending_bytes,
		refilled: false,
	})
}

async fn byte_capacity(services: &Services) -> Result<Expected> {
	let room = room_id!("!source-handoff:localhost");
	let servers = wide_state(services).await?;
	let states = vec![(
		room.to_owned(),
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
	)];
	let mut events = Vec::new();
	for index in 0..COUNT_LIMIT {
		let frontier = services
			.timeline
			.latest_pdu_in_room(room)
			.await?
			.event_id
			.clone();
		refusal::refuse_next("servernameevent_data");
		match append(services, room, index).await {
			| Ok(accepted) => {
				events.push(accepted);
				assert_eq!(refusal::pending(), 0);
			},
			| Err(error) => {
				assert!(
					error
						.to_string()
						.contains("Canonical federation admission is full"),
					"byte quota refusal: {error}"
				);
				assert_eq!(refusal::pending(), 1, "quota refuses before the new queue attempt");
				assert!(!events.is_empty() && events.len() < COUNT_LIMIT);
				let pending_bytes = source_bytes(services, &events).await?;
				let raw = services
					.timeline
					.get_pdu_id(&events.last().unwrap().event)
					.await?;
				let next_size = services.db[PLAN].get(raw.as_ref()).await?.len();
				assert!(reserved_size(pending_bytes, events.len()) <= BYTE_LIMIT);
				assert!(
					reserved_size(
						pending_bytes
							.checked_add(next_size)
							.expect("bounded plan bytes"),
						events
							.len()
							.checked_add(1)
							.expect("bounded plan count"),
					) > BYTE_LIMIT,
					"actual bytes plus reserved cancellation growth exhaust capacity before \
					 count limit"
				);
				return Ok(Expected {
					byte_quota: true,
					events,
					servers,
					states,
					refused_room: room.to_owned(),
					refused_transaction: index,
					refused_frontier: frontier,
					pending_bytes,
					refilled: false,
				});
			},
		}
	}
	panic!("large valid sources must fill the byte budget before the count budget")
}

async fn wide_state(services: &Services) -> Result<Vec<OwnedServerName>> {
	let room = room_id!("!source-handoff:localhost");
	let state = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let mut cells = services
		.state_accessor
		.state_full_shortids(state)
		.map_ok(|(key, event)| compress_state_event(key, event))
		.try_collect::<CompressedState>()
		.await?;
	let prototype = services
		.state_accessor
		.room_state_get(room, &StateEventType::RoomMember, "@remote:handoff.invalid")
		.await?;
	let mut servers = vec![server_name!("handoff.invalid").to_owned()];
	for index in 0..512 {
		let server = format!(
			"b{index:03}{}.{}.{}.{}",
			"a".repeat(59),
			"b".repeat(63),
			"c".repeat(63),
			"d".repeat(55)
		);
		assert_eq!(server.len(), 247);
		let user = ruma::UserId::parse(format!("@budget:{server}"))?;
		assert_eq!(user.as_str().len(), 255);
		let mut pdu = prototype.clone();
		pdu.event_id = EventId::parse(format!("$quota-byte-{index}"))?;
		pdu.sender = user.clone();
		pdu.state_key = Some(user.to_string().into());
		pdu.prev_events.clear();
		pdu.auth_events.clear();
		pdu.origin = None;
		pdu.unsigned = None;
		services
			.timeline
			.add_pdu_outlier(&pdu.event_id, &tuwunel_core::utils::to_canonical_object(&pdu)?)
			.await?;
		let key = services
			.short
			.get_or_create_shortstatekey(&StateEventType::RoomMember, user.as_str())
			.await?;
		cells.insert(
			services
				.state_compressor
				.compress_state_event(key, &pdu.event_id)
				.await?,
		);
		servers.push(server.try_into()?);
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
	servers.sort();
	Ok(servers)
}

async fn source_bytes(services: &Services, events: &[Accepted]) -> Result<usize> {
	let mut bytes = 0_usize;
	for accepted in events {
		let raw = services
			.timeline
			.get_pdu_id(&accepted.event)
			.await?;
		assert!(
			services
				.sending
				.db
				.has_federation_plan(&raw)
				.await?,
			"accepted source has a valid retained witness and codec"
		);
		bytes = bytes
			.checked_add(services.db[PLAN].get(raw.as_ref()).await?.len())
			.expect("bounded source inventory");
	}
	Ok(bytes)
}

fn reserved_size(bytes: usize, count: usize) -> usize {
	bytes
		.checked_add(
			count
				.checked_mul(CANCELLATION_RESERVE)
				.expect("bounded cancellation reserve"),
		)
		.expect("bounded pending plan bytes")
}

async fn assert_records(services: &Services, expected: &Expected) -> Result {
	for accepted in &expected.events {
		assert_eq!(
			services
				.timeline
				.get_pdu(&accepted.event)
				.await?
				.room_id,
			accepted.room
		);
		assert_eq!(retry(services, accepted.transaction).await?, Some(accepted.event.clone()));
	}
	assert_eq!(retry(services, expected.refused_transaction).await?, None);
	for (room, state) in &expected.states {
		assert_eq!(
			services
				.state
				.get_room_shortstatehash(room)
				.await?,
			*state
		);
	}
	Ok(())
}

async fn assert_saturated(services: &Services, expected: &Expected) -> Result {
	assert_records(services, expected).await?;
	assert_eq!(
		services
			.timeline
			.latest_pdu_in_room(&expected.refused_room)
			.await?
			.event_id,
		expected.refused_frontier
	);
	assert_eq!(
		services.db[PLAN]
			.raw_keys_after(None, COUNT_LIMIT + 1)
			.await?
			.len(),
		expected.events.len()
	);
	assert_eq!(source_bytes(services, &expected.events).await?, expected.pending_bytes);
	assert_eq!(
		services.db["global"]
			.raw_keys_prefix_after(&[0x08], None, COUNT_LIMIT + 1)
			.await?
			.len(),
		expected.events.len()
	);
	assert!(if expected.byte_quota {
		expected.events.len() < COUNT_LIMIT
	} else {
		expected.events.len() == COUNT_LIMIT
	});
	Ok(())
}

async fn drain(services: &Services, event: &EventId) -> Result {
	let raw = services.timeline.get_pdu_id(event).await?;
	for _ in 0..10 {
		if !services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
		{
			return Ok(());
		}
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
	Ok(())
}

async fn assert_deliveries(services: &Services, expected: &Expected) -> Result {
	let mut wanted = Vec::new();
	for accepted in &expected.events {
		wanted.push(
			services
				.timeline
				.get_pdu_id(&accepted.event)
				.await?
				.as_ref()
				.to_vec(),
		);
	}
	wanted.sort();
	for server in &expected.servers {
		let destination = Destination::Federation(server.clone());
		let queued = services
			.sending
			.db
			.queued_requests(&destination)
			.try_collect::<Vec<_>>()
			.await?;
		let mut actual = queued
			.into_iter()
			.map(|(_, event)| match event {
				| SendingEvent::Pdu(raw) => raw.as_ref().to_vec(),
				| _ => panic!("owned fixture has no other federation work"),
			})
			.collect::<Vec<_>>();
		actual.sort();
		assert_eq!(
			actual, wanted,
			"every accepted event remains queued exactly once for {server}"
		);
	}
	assert!(
		services.db[PLAN]
			.raw_keys_after(None, 1)
			.await?
			.is_empty()
	);
	assert!(
		services.db["global"]
			.raw_keys_prefix_after(&[0x08], None, 1)
			.await?
			.is_empty()
	);
	Ok(())
}

async fn restart(services: &Services, root: &Path, phase: &str) -> Result {
	let path = root.join("quota-expected.json");
	let mut expected: Expected = serde_json::from_slice(&fs::read(&path)?)?;
	if phase == "quota-restart" {
		assert!(!expected.refilled);
		assert_saturated(services, &expected).await?;
		drain(services, &expected.events[0].event).await?;
		let room = if expected.byte_quota {
			room_id!("!source-handoff:localhost")
		} else {
			room_id!("!quota-second:localhost")
		};
		expected
			.events
			.push(append(services, room, 66).await?);
		expected.refilled = true;
		for accepted in &expected.events {
			drain(services, &accepted.event).await?;
		}
		assert_records(services, &expected).await?;
		assert_deliveries(services, &expected).await?;
		fs::write(path, serde_json::to_vec(&expected)?)?;
	} else {
		assert!(expected.refilled);
		assert_records(services, &expected).await?;
		assert_deliveries(services, &expected).await?;
	}
	Ok(())
}
