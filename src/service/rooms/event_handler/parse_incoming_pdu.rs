use futures::{StreamExt, pin_mut};
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, OwnedEventId, OwnedRoomId, RoomId, RoomVersionId,
};
use serde_json::value::RawValue as RawJsonValue;
use tuwunel_core::{
	Result, err, implement,
	matrix::{event::gen_event_id, room_version},
	result::FlatOk,
};

#[cfg(test)]
mod tests;

type Parsed = (OwnedRoomId, OwnedEventId, CanonicalJsonObject);

#[implement(super::Service)]
#[tracing::instrument(
    name = "parse_incoming",
    level = "trace",
    skip_all,
    fields(
        len = pdu.get().len(),
    )
)]
pub async fn parse_incoming_pdu(&self, pdu: &RawJsonValue) -> Result<Parsed> {
	let value: CanonicalJsonObject = serde_json::from_str(pdu.get()).map_err(|e| {
		err!(BadServerResponse(debug_error!("Error parsing incoming event: {e} {pdu:#?}")))
	})?;

	let room_id = room_id_of(&value)?;

	let room_version_id = match self
		.services
		.state
		.get_room_version(&room_id)
		.await
	{
		| Ok(room_version_id) => room_version_id,
		// We may not be resident (e.g. a rescinded out-of-band invite); recover the
		// version from a locally-invited member's stored stripped state.
		| Err(_) => self
			.invited_room_version(&room_id)
			.await
			.ok_or_else(|| err!("Server is not in room {room_id}"))?,
	};

	gen_event_id(&value, &room_version_id)
		.map(move |event_id| (room_id, event_id, value))
		.map_err(|e| {
			err!(Request(InvalidParam("Could not convert event to canonical json: {e}")))
		})
}

/// The room an incoming PDU belongs to.
///
/// Where a room version takes the room ID from the hash of the create event
/// (MSC4291, room version 12), the create event carries no `room_id`: its room
/// derives from its own event ID, under the version its content declares. A
/// backfilled room start is exactly such an event, so refusing it left the
/// timeline without its create event and backfill asking forever.
fn room_id_of(value: &CanonicalJsonObject) -> Result<OwnedRoomId> {
	let invalid = || err!(Request(InvalidParam("Invalid room_id in pdu")));

	if let Some(room_id) = value.get("room_id") {
		return room_id
			.as_str()
			.map(OwnedRoomId::parse)
			.flat_ok_or(invalid());
	}

	let is_create = value
		.get("type")
		.and_then(CanonicalJsonValue::as_str)
		.is_some_and(|kind| kind == "m.room.create");

	let room_version: RoomVersionId = value
		.get("content")
		.and_then(CanonicalJsonValue::as_object)
		.and_then(|content| content.get("room_version"))
		.and_then(CanonicalJsonValue::as_str)
		.filter(|_| is_create)
		.map(RoomVersionId::try_from)
		.flat_ok_or(invalid())?;

	if room_version::rules(&room_version)?
		.event_format
		.require_room_create_room_id
	{
		return Err(invalid());
	}

	let event_id = gen_event_id(value, &room_version)?;

	OwnedRoomId::from_parts('!', event_id.localpart(), None).map_err(|_| invalid())
}

/// Recover a room's version from a locally-invited member's stored stripped
/// state, for a room we are not resident in (e.g. a rescinded out-of-band
/// invite). The create event in the stripped state carries the version.
#[implement(super::Service)]
async fn invited_room_version(&self, room_id: &RoomId) -> Option<RoomVersionId> {
	let invited = self
		.services
		.state_cache
		.room_members_invited(room_id)
		.map(ToOwned::to_owned);

	pin_mut!(invited);
	while let Some(user_id) = invited.next().await {
		if self.services.globals.user_is_local(&user_id)
			&& let Ok(stripped) = self
				.services
				.state_cache
				.invite_state(&user_id, room_id)
				.await && let Some(room_version) = super::room_version_of(&stripped)
		{
			return Some(room_version);
		}
	}

	None
}
