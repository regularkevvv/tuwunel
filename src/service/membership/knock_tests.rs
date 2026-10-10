//! A real federation wire PDU must survive strict forced-state projection.
//! No workers, listeners or providers are started by this scratch fixture.

use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, RoomVersionId,
	api::federation::membership::{RawStrippedState, create_knock_event},
	room_id,
	serde::Raw,
	user_id,
};
use serde_json::json;
use tuwunel_core::{Event, Result, matrix::event::gen_event_id};

use crate::{Services, services::startup_tests};

#[test]
fn full_wire_knock_state_normalizes_before_membership_projection_and_refuses_incomplete_state()
-> Result {
	startup_tests::isolated(
		"membership::knock::tests::full_wire_knock_state_normalizes_before_membership_projection_and_refuses_incomplete_state",
		async |root| {
			let services = startup_tests::services(root).await?;
			let outcome = exercise(&services).await;
			services.stop().await;
			outcome
		},
	)
}

async fn signed_wire(
	services: &Services,
	event_type: &str,
	state_key: &str,
	content: serde_json::Value,
) -> Result<CanonicalJsonObject> {
	let version = RoomVersionId::V9;
	let mut event: CanonicalJsonObject = serde_json::from_value(json!({
		"type": event_type,
		"state_key": state_key,
		"room_id": "!knock-wire:localhost",
		"sender": "@creator:localhost",
		"origin": "localhost",
		"origin_server_ts": 1_838_188_000,
		"content": content,
		"depth": 1,
		"prev_events": [],
		"auth_events": [],
	}))?;
	services
		.server_keys
		.hash_and_sign_event(&mut event, &version)?;
	let id = gen_event_id(&event, &version)?;
	event.insert("event_id".into(), CanonicalJsonValue::String(id.into()));
	let raw: Box<serde_json::value::RawValue> = services
		.federation
		.format_pdu_into(event, Some(&version))
		.await;
	let wire: CanonicalJsonObject = serde_json::from_str(raw.get())?;
	assert!(!wire.contains_key("event_id"), "actual v9 formatter removes local event_id");
	Ok(wire)
}

fn response(events: &[CanonicalJsonObject]) -> Result<create_knock_event::v1::Response> {
	Ok(create_knock_event::v1::Response {
		knock_room_state: events
			.iter()
			.map(|event| serde_json::value::to_raw_value(event).map(RawStrippedState::Pdu))
			.collect::<std::result::Result<_, _>>()?,
	})
}

async fn exercise(services: &Services) -> Result {
	let room = room_id!("!knock-wire:localhost");
	let member = user_id!("@member:remote.invalid");
	let version = RoomVersionId::V9;
	let create = signed_wire(
		services,
		"m.room.create",
		"",
		json!({
			"creator": "@creator:localhost", "room_version": "9",
		}),
	)
	.await?;
	let join = signed_wire(
		services,
		"m.room.member",
		member.as_str(),
		json!({
			"membership": "join",
		}),
	)
	.await?;
	let member_id = gen_event_id(&join, &version)?;
	let outliers = &services.db["eventid_outlierpdu"];

	assert!(
		services
			.membership
			.ingest_send_knock_state(room, &response(&vec![create.clone(); 65])?, &version,)
			.await
			.is_err()
	);
	assert_eq!(outliers.count().await, 0);

	// All preflight failures preserve storage; a valid record preceding the bad
	// one cannot be persisted as partial state.
	let mut malformed = join.clone();
	malformed.remove("auth_events");
	assert!(
		services
			.membership
			.ingest_send_knock_state(room, &response(&[create.clone(), malformed])?, &version,)
			.await
			.is_err()
	);
	assert_eq!(outliers.count().await, 0);
	let mut wrong_room = join.clone();
	wrong_room.insert("room_id".into(), "!other:localhost".into());
	assert!(
		services
			.membership
			.ingest_send_knock_state(room, &response(&[create.clone(), wrong_room])?, &version,)
			.await
			.is_err()
	);
	assert_eq!(outliers.count().await, 0);
	#[expect(
		deprecated,
		reason = "negative control: a stripped summary is not canonical state"
	)]
	let stripped = create_knock_event::v1::Response {
		knock_room_state: vec![RawStrippedState::Stripped(Raw::from_json(
			serde_json::value::to_raw_value(&json!({
				"type": "m.room.member", "state_key": member,
				"sender": "@creator:localhost", "content": {"membership": "join"},
			}))?,
		))],
	};
	assert!(
		services
			.membership
			.ingest_send_knock_state(room, &stripped, &version)
			.await
			.is_err()
	);
	assert_eq!(outliers.count().await, 0);

	let state = services
		.membership
		.ingest_send_knock_state(room, &response(&[create, join.clone()])?, &version)
		.await?;
	let stored = services.timeline.get_pdu(&member_id).await?;
	assert_eq!(stored.event_id(), member_id);
	assert_eq!(stored.room_id(), room);
	assert_eq!(stored.state_key(), Some(member.as_str()));
	let stored_json = services.timeline.get_pdu_json(&member_id).await?;
	let roundtrip = services
		.federation
		.format_pdu_into(stored_json, Some(&version))
		.await;
	let roundtrip: CanonicalJsonObject = serde_json::from_str(roundtrip.get())?;
	assert_eq!(roundtrip, join, "normalization preserves the original signed wire object");
	services
		.short
		.get_or_create_shortroomid(room)
		.await?;
	let lock = services.state.mutex.lock(room).await;
	services
		.membership
		.apply_send_knock_state(room, &state, &lock)
		.await?;
	assert!(services.state_cache.is_joined(member, room).await);
	assert_eq!(
		services
			.state_cache
			.room_joined_count(room)
			.await?,
		1
	);
	Ok(())
}
