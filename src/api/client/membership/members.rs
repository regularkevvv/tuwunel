use std::collections::BTreeMap;

use axum::extract::State;
use futures::{StreamExt, pin_mut, stream};
use ruma::{
	UserId,
	api::{
		client::membership::{
			get_member_events::{self},
			joined_members::{self, v3::RoomMember},
		},
		error::{ErrorKind, LimitExceededErrorData},
	},
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEvent, RoomMemberEventContent},
	},
	serde::Raw,
};
use tuwunel_core::{
	Err, Error, Result,
	matrix::{Event, Pdu},
	utils::json::serialized_len,
};

use crate::Ruma;

const MAX_STATE_ENTRIES: usize = 4096;
const MAX_SOURCE_BYTES: usize = 512 * 1024;
const MAX_RESPONSE_BYTES: usize = 256 * 1024;
const RESPONSE_ENVELOPE_BYTES: usize = 64;

/// # `GET /_matrix/client/r0/rooms/{roomId}/members`
///
/// Lists complete, bounded membership state at the requested pagination
/// boundary, the caller's departure, or current state.
///
/// - Requires state visibility under the room history policy
pub(crate) async fn get_member_events_route(
	State(services): State<crate::State>,
	body: Ruma<get_member_events::v3::Request>,
) -> Result<get_member_events::v3::Response> {
	let snapshot = services
		.state_accessor
		.member_snapshot(&body.room_id, body.sender_user(), body.at.as_deref())
		.await?;

	let membership = body.membership.as_ref();
	let not_membership = body.not_membership.as_ref();
	let state = stream::iter(snapshot.hash).flat_map(|hash| {
		services
			.state_accessor
			.state_full_pdus_strict(hash)
	});
	pin_mut!(state);
	let mut source_bytes = 0_usize;
	let mut response_bytes = RESPONSE_ENVELOPE_BYTES;
	let mut chunk = Vec::new();
	let mut state_entries = 0_usize;
	let mut replaced_boundary = false;
	while let Some(entry) = state.next().await {
		let ((event_type, state_key), pdu) = entry?;
		state_entries = state_entries.saturating_add(1);
		if pdu.room_id().as_str() != body.room_id.as_str() {
			return Err(Error::bad_database("Mismatched membership snapshot room"));
		}
		charge_json(&mut source_bytes, pdu.as_pdu(), MAX_SOURCE_BYTES)?;
		let replaces = snapshot.boundary.as_ref().is_some_and(|event| {
			event.event_type().to_cow_str() == event_type.to_cow_str()
				&& event.state_key() == Some(state_key.as_str())
		});
		replaced_boundary |= replaces;
		if event_type != StateEventType::RoomMember {
			continue;
		}
		validate_member(&pdu)?;
		if replaces {
			continue;
		}
		append_member(&mut chunk, &mut response_bytes, pdu, membership, not_membership)?;
	}
	if let Some(pdu) = snapshot.boundary {
		if pdu.state_key().is_some() && !replaced_boundary && state_entries >= MAX_STATE_ENTRIES {
			return Err(Error::Request(
				ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
				"Historical membership state row limit reached".into(),
				http::StatusCode::TOO_MANY_REQUESTS,
			));
		}
		charge_json(&mut source_bytes, pdu.as_pdu(), MAX_SOURCE_BYTES)?;
		if pdu.event_type().to_cow_str() == "m.room.member" {
			append_member(&mut chunk, &mut response_bytes, pdu, membership, not_membership)?;
		}
	}

	Ok(get_member_events::v3::Response { chunk })
}

/// # `GET /_matrix/client/r0/rooms/{roomId}/joined_members`
///
/// Lists all members of a room.
///
/// - The sender user must be in the room
/// - An appservice needs at least one joined user in its local namespace
pub(crate) async fn joined_members_route(
	State(services): State<crate::State>,
	body: Ruma<joined_members::v3::Request>,
) -> Result<joined_members::v3::Response> {
	let mut allowed = services
		.state_cache
		.is_joined_checked(body.sender_user(), &body.room_id)
		.await?;
	if !allowed && let Some(appservice) = &body.appservice_info {
		allowed = services
			.state_cache
			.bounded_room_members(&body.room_id)
			.await?
			.iter()
			.any(|user| appservice.is_user_match(user));
	}
	if !allowed {
		return Err!(Request(Forbidden("You aren't a member of the room.")));
	}

	let state = services
		.state_accessor
		.room_state_full(&body.room_id);
	pin_mut!(state);
	let mut source_bytes = 0_usize;
	let mut response_bytes = RESPONSE_ENVELOPE_BYTES;
	let mut joined = BTreeMap::new();
	while let Some(entry) = state.next().await {
		let ((event_type, state_key), pdu) = entry?;
		charge_json(&mut source_bytes, pdu.as_pdu(), MAX_SOURCE_BYTES)?;
		if event_type != StateEventType::RoomMember {
			continue;
		}
		let user = UserId::parse(state_key.as_str())
			.map_err(|_| Error::bad_database("Invalid member state key"))?;

		let content = pdu
			.get_content::<RoomMemberEventContent>()
			.map_err(|_| Error::bad_database("Invalid membership state event"))?;
		if content.membership == MembershipState::Join {
			let member = RoomMember {
				display_name: content.displayname,
				avatar_url: content.avatar_url,
			};
			// A serialized tuple conservatively includes the escaped map key,
			// value, colon/separator and one extra byte of punctuation.
			charge_json(&mut response_bytes, &(&user, &member), MAX_RESPONSE_BYTES)?;
			joined.insert(user, member);
		}
	}

	Ok(joined_members::v3::Response { joined })
}

fn validate_member(pdu: &Pdu) -> Result<RoomMemberEventContent> {
	UserId::parse(
		pdu.state_key()
			.ok_or_else(|| Error::bad_database("Missing member state key"))?,
	)
	.map_err(|_| Error::bad_database("Invalid member state key"))?;
	pdu.get_content::<RoomMemberEventContent>()
		.map_err(|_| Error::bad_database("Invalid membership state event"))
}

fn append_member(
	chunk: &mut Vec<Raw<RoomMemberEvent>>,
	response_bytes: &mut usize,
	pdu: Pdu,
	membership: Option<&MembershipState>,
	not_membership: Option<&MembershipState>,
) -> Result {
	let content = validate_member(&pdu)?;
	let selected = match (membership, not_membership) {
		| (Some(included), Some(excluded)) =>
			content.membership == *included || content.membership != *excluded,
		| (Some(included), None) => content.membership == *included,
		| (None, Some(excluded)) => content.membership != *excluded,
		| (None, None) => true,
	};
	if selected {
		let event: Raw<RoomMemberEvent> = pdu.into_format();
		*response_bytes = response_bytes.saturating_add(1);
		charge_json(response_bytes, &event, MAX_RESPONSE_BYTES)?;
		chunk.push(event);
	}
	Ok(())
}

fn charge_json<T: serde::Serialize>(bytes: &mut usize, value: &T, limit: usize) -> Result {
	let size = serialized_len(value)
		.map_err(|_| Error::bad_database("Cannot serialize room membership state"))?;
	*bytes = bytes.saturating_add(size);
	if *bytes > limit {
		return Err(Error::Request(
			ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
			"Room membership response limit reached".into(),
			http::StatusCode::TOO_MANY_REQUESTS,
		));
	}
	Ok(())
}
