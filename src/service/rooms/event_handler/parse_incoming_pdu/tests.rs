use ruma::{CanonicalJsonObject, CanonicalJsonValue, RoomVersionId};
use serde_json::json;
use tuwunel_core::matrix::event::gen_event_id;

use super::room_id_of;

fn create_event(room_version: &str) -> CanonicalJsonObject {
	serde_json::from_value(json!({
		"type": "m.room.create",
		"state_key": "",
		"sender": "@creator:example.org",
		"origin_server_ts": 1,
		"depth": 1,
		"prev_events": [],
		"auth_events": [],
		"content": { "room_version": room_version },
		"hashes": { "sha256": "aGFzaA" },
		"signatures": {},
	}))
	.expect("a canonical create event")
}

#[test]
fn hashed_create_event_names_its_own_room() {
	let value = create_event("12");
	let event_id = gen_event_id(&value, &RoomVersionId::V12).expect("event id");

	let room_id = room_id_of(&value).expect("room derived from the create event");

	assert_eq!(room_id.as_str(), format!("!{}", event_id.localpart()));
}

#[test]
fn explicit_room_id_is_used_as_given() {
	let mut value = create_event("11");
	value.insert("room_id".into(), CanonicalJsonValue::String("!room:example.org".into()));

	let room_id = room_id_of(&value).expect("explicit room id");

	assert_eq!(room_id.as_str(), "!room:example.org");
}

#[test]
fn create_event_must_name_its_room_where_the_version_requires_it() {
	room_id_of(&create_event("11")).expect_err("a version 11 create event names its room");
}

#[test]
fn only_a_create_event_may_omit_its_room() {
	let mut value = create_event("12");
	value.insert("type".into(), CanonicalJsonValue::String("m.room.message".into()));

	room_id_of(&value).expect_err("only a create event may omit its room");
}
