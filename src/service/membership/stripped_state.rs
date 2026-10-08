use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, OwnedRoomId, RoomId, RoomVersionId,
	api::federation::membership::RawStrippedState,
	events::AnyStrippedStateEvent,
	room_version_rules::RoomIdFormatVersion,
	serde::{JsonObject, Raw},
};
use tuwunel_core::{Event, PduEvent, Result, implement, matrix::event::gen_event_id};

use super::Service;

/// MSC4311 verdict for the create event carried in federated stripped state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrippedCreateVerdict {
	/// A full create PDU bound to the room with valid signatures.
	Valid,

	/// No `m.room.create` event was present.
	Missing,

	/// A create event was present only in the legacy stripped form.
	NotPdu,

	/// A create PDU was present but does not bind to the room.
	WrongRoom,

	/// A create PDU was present but failed signature or hash checks.
	BadSignature,
}

/// Whether a non-`Valid` verdict warrants rejecting an invite or dropping the
/// event from knock state, given the room version and operator policy.
#[must_use]
pub fn enforce_stripped_create(
	verdict: StrippedCreateVerdict,
	v12_room_ids: bool,
	enforce: bool,
) -> bool {
	use StrippedCreateVerdict::*;

	match verdict {
		| Valid => false,
		// A complete create PDU bound to a different room must fail for v12+
		// rooms even during the migration window (MSC4311 Migration).
		| WrongRoom => v12_room_ids || enforce,
		| Missing | NotPdu | BadSignature => enforce,
	}
}

/// Whether the room version derives room ids from the create event hash
/// (MSC4291, room version 12 and above), which changes how a create event
/// binds to its room.
#[must_use]
pub fn v12_room_ids(room_version: &RoomVersionId) -> bool {
	room_version
		.rules()
		.is_some_and(|rules| matches!(rules.room_id_format, RoomIdFormatVersion::V2))
}

/// Down-convert a federation stripped-state entry to the 4-field client shape,
/// reducing a full PDU to content, sender, optional state_key, and type.
#[expect(
	deprecated,
	reason = "Matrix 1.16 still permits receiving the legacy stripped variant for backwards \
	          compatibility."
)]
#[must_use]
pub fn into_client_stripped(
	room_id: &RoomId,
	state: RawStrippedState,
) -> Option<Raw<AnyStrippedStateEvent>> {
	match state {
		| RawStrippedState::Stripped(raw) => Some(raw),
		| RawStrippedState::Pdu(raw) => {
			let mut event: JsonObject = serde_json::from_str(raw.get()).ok()?;

			// PduEvent requires event_id and room_id; a v12 create PDU federates
			// with neither, and to_format() drops both from the stripped shape.
			event.insert("event_id".into(), "$placeholder".into());
			event
				.entry("room_id")
				.or_insert_with(|| room_id.as_str().into());

			let pdu: PduEvent = serde_json::from_value(event.into()).ok()?;

			Some(pdu.to_format())
		},
	}
}

/// Validate the `m.room.create` event in a federated invite's or knock's
/// stripped state against the stated room (MSC4311). Decision-free: callers map
/// the verdict to their own reject-or-warn policy.
#[implement(Service)]
#[expect(
	deprecated,
	reason = "Matrix 1.16 still permits receiving the legacy stripped variant for backwards \
	          compatibility."
)]
#[tracing::instrument(level = "debug", skip_all, fields(%room_id))]
pub async fn validate_stripped_create(
	&self,
	state: &[RawStrippedState],
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
) -> Result<StrippedCreateVerdict> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let create = state.iter().find_map(|event| match event {
		| RawStrippedState::Pdu(raw) => serde_json::from_str::<CanonicalJsonObject>(raw.get())
			.ok()
			.filter(is_create),
		| RawStrippedState::Stripped(_) => None,
	});

	let Some(mut create) = create else {
		let stripped = state.iter().any(|event| match event {
			| RawStrippedState::Stripped(raw) =>
				serde_json::from_str::<CanonicalJsonObject>(raw.json().get())
					.is_ok_and(|json| is_create(&json)),
			| RawStrippedState::Pdu(_) => false,
		});

		return Ok(match stripped {
			| true => StrippedCreateVerdict::NotPdu,
			| false => StrippedCreateVerdict::Missing,
		});
	};

	create.remove("unsigned");

	// Room-id binding: v12+ rooms hash the create event (MSC4291); earlier
	// versions compare the create event's room_id field.
	let bound = if v12_room_ids(room_version_id) {
		gen_event_id(&create, room_version_id)
			.ok()
			.and_then(|event_id| OwnedRoomId::from_parts('!', event_id.localpart(), None).ok())
			.is_some_and(|expected| expected == room_id)
	} else {
		create
			.get("room_id")
			.and_then(CanonicalJsonValue::as_str)
			.is_some_and(|id| id == room_id.as_str())
	};

	if !bound {
		return Ok(StrippedCreateVerdict::WrongRoom);
	}

	if services_root
		.server_keys
		.verify_event(&create, Some(room_version_id))
		.await
		.is_err()
	{
		return Ok(StrippedCreateVerdict::BadSignature);
	}

	Ok(StrippedCreateVerdict::Valid)
}

fn is_create(json: &CanonicalJsonObject) -> bool {
	let field = |key| json.get(key).and_then(CanonicalJsonValue::as_str);

	field("type") == Some("m.room.create") && field("state_key") == Some("")
}
