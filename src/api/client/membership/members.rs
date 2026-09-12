use std::collections::BTreeMap;

use axum::extract::State;
use futures::{TryStreamExt, pin_mut};
use ruma::{
	api::client::membership::{
		get_member_events::{self},
		joined_members::{self, v3::RoomMember},
	},
	events::{
		StateEventType,
		room::{
			history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
			member::{MembershipState, RoomMemberEventContent},
		},
	},
};
use tuwunel_core::{
	Err, Error, Result, is_equal_to, is_not_equal_to,
	matrix::Event,
	utils::future::{BoolExt, TryExtExt},
};

use crate::Ruma;

/// # `GET /_matrix/client/r0/rooms/{roomId}/members`
///
/// Lists all joined users in a room (TODO: at a specific point in time, with a
/// specific membership).
///
/// - Only works if the user is currently joined
pub(crate) async fn get_member_events_route(
	State(services): State<crate::State>,
	body: Ruma<get_member_events::v3::Request>,
) -> Result<get_member_events::v3::Response> {
	if !services
		.state_accessor
		.user_can_see_state_events(body.sender_user(), &body.room_id)
		.await
	{
		return Err!(Request(Forbidden(
			"You aren't a member of the room and weren't previously a member of the room."
		)));
	}

	let membership = body.membership.as_ref();
	let not_membership = body.not_membership.as_ref();
	let membership_filter = |content: &RoomMemberEventContent| {
		membership.is_none_or(is_equal_to!(&content.membership))
			&& not_membership.is_none_or(is_not_equal_to!(&content.membership))
	};
	let state = services
		.state_accessor
		.room_state_full(&body.room_id)
		.try_collect::<Vec<_>>()
		.await?;
	let mut chunk = Vec::new();
	for ((event_type, _), pdu) in state {
		if event_type != StateEventType::RoomMember {
			continue;
		}

		let content = pdu
			.get_content::<RoomMemberEventContent>()
			.map_err(|_| Error::bad_database("Invalid membership state event"))?;
		if membership_filter(&content) {
			chunk.push(pdu.into_format());
		}
	}

	Ok(get_member_events::v3::Response { chunk })
}

/// # `GET /_matrix/client/r0/rooms/{roomId}/joined_members`
///
/// Lists all members of a room.
///
/// - The sender user must be in the room
/// - TODO: An appservice just needs a puppet joined
pub(crate) async fn joined_members_route(
	State(services): State<crate::State>,
	body: Ruma<joined_members::v3::Request>,
) -> Result<joined_members::v3::Response> {
	let is_joined = services
		.state_cache
		.is_joined(body.sender_user(), &body.room_id);

	let is_world_readable = services
		.state_accessor
		.room_state_get_content(&body.room_id, &StateEventType::RoomHistoryVisibility, "")
		.map_ok_or(false, |c: RoomHistoryVisibilityEventContent| {
			c.history_visibility == HistoryVisibility::WorldReadable
		});

	pin_mut!(is_joined, is_world_readable);
	if !is_joined.or(is_world_readable).await {
		return Err!(Request(Forbidden("You aren't a member of the room.")));
	}

	let state = services
		.state_accessor
		.room_state_full(&body.room_id)
		.try_collect::<Vec<_>>()
		.await?;
	let mut joined = BTreeMap::new();
	for ((event_type, _), pdu) in state {
		if event_type != StateEventType::RoomMember {
			continue;
		}

		let content = pdu
			.get_content::<RoomMemberEventContent>()
			.map_err(|_| Error::bad_database("Invalid membership state event"))?;
		if content.membership == MembershipState::Join {
			joined.insert(pdu.sender().to_owned(), RoomMember {
				display_name: content.displayname,
				avatar_url: content.avatar_url,
			});
		}
	}

	Ok(joined_members::v3::Response { joined })
}
