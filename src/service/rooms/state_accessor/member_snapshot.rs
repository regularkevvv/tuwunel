use ruma::{
	RoomId, UserId,
	api::error::ErrorKind,
	events::{
		StateEventType,
		room::{
			history_visibility::HistoryVisibility,
			member::{MembershipState, RoomMemberEventContent},
		},
	},
};
use tuwunel_core::{
	Error, Result, err, implement,
	matrix::{Event, Pdu, PduCount},
};

use crate::rooms::short::ShortStateHash;

/// A fixed membership snapshot. Historical state is stored before its boundary
/// event; a state-event boundary replaces that one cell in the response.
pub struct MemberSnapshot {
	pub hash: Option<ShortStateHash>,
	pub boundary: Option<Pdu>,
}

#[implement(super::Service)]
pub async fn member_snapshot(
	&self,
	room_id: &RoomId,
	user_id: &UserId,
	at: Option<&str>,
) -> Result<MemberSnapshot> {
	let _state_lock = self.services.state.mutex.lock(room_id).await;
	let joined = self
		.services
		.state_cache
		.is_joined_checked(user_id, room_id)
		.await?;
	if let Some(token) = at {
		let count = token
			.parse::<PduCount>()
			.map_err(|_| err!(Request(InvalidParam("Invalid membership pagination token"))))?;
		let (position, event) = self
			.services
			.timeline
			.member_snapshot_boundary(room_id, count)
			.await?;
		let snapshot = self
			.member_snapshot_at_event(room_id, event)
			.await?;
		if !self
			.member_snapshot_visible(room_id, user_id, joined, position, &snapshot)
			.await?
		{
			return Err(err!(Request(Forbidden("Membership snapshot is not visible"))));
		}
		return Ok(snapshot);
	}

	// A former member retains the exact leave boundary, even if other members
	// subsequently change the room's state or history policy.
	let former = !joined
		&& self
			.services
			.state_cache
			.once_joined_checked(user_id, room_id)
			.await?;
	if former
		&& self
			.services
			.state_cache
			.is_left_checked(user_id, room_id)
			.await?
	{
		let count = self
			.services
			.state_cache
			.get_left_count(room_id, user_id)
			.await
			.map_err(|_| Error::bad_database("Missing former-member boundary"))?;
		if count == 0 || count > i64::MAX.cast_unsigned() {
			return Err(Error::bad_database("Invalid former-member boundary"));
		}
		let position = PduCount::Normal(count);
		let (stored_position, event) = self
			.services
			.timeline
			.member_snapshot_boundary(room_id, position)
			.await?;
		if stored_position != position
			|| event.event_type().to_cow_str() != "m.room.member"
			|| event.state_key() != Some(user_id.as_str())
		{
			return Err(Error::bad_database("Mismatched former-member boundary"));
		}
		let membership = event
			.get_content::<RoomMemberEventContent>()
			.map_err(|_| Error::bad_database("Invalid former-member event"))?
			.membership;
		if !matches!(membership, MembershipState::Leave | MembershipState::Ban) {
			return Err(Error::bad_database("Former-member boundary is not a departure"));
		}
		return self
			.member_snapshot_at_event(room_id, event)
			.await;
	}

	let hash = self
		.services
		.state
		.get_room_shortstatehash(room_id)
		.await
		.map_err(|error| {
			if error.kind() == ErrorKind::NotFound {
				if joined {
					Error::bad_database("Missing membership state")
				} else {
					err!(Request(Forbidden("Room membership state is not visible")))
				}
			} else {
				error
			}
		})?;
	if !joined {
		let allowed = match self.history_visibility_at(room_id, hash).await? {
			| HistoryVisibility::WorldReadable => true,
			| HistoryVisibility::Invited =>
				self.services
					.state_cache
					.is_invited_checked(user_id, room_id)
					.await?,
			| _ => false,
		};
		if !allowed {
			return Err(err!(Request(Forbidden("Room membership state is not visible"))));
		}
	}

	Ok(MemberSnapshot { hash: Some(hash), boundary: None })
}

#[implement(super::Service)]
async fn member_snapshot_at_event(&self, room_id: &RoomId, event: Pdu) -> Result<MemberSnapshot> {
	if event.room_id() != room_id {
		return Err(Error::bad_database("Mismatched membership boundary room"));
	}
	let hash = match self
		.services
		.state
		.pdu_shortstatehash(event.event_id())
		.await
	{
		| Ok(hash) => Some(hash),
		| Err(error)
			if error.kind() == ErrorKind::NotFound
				&& self
					.is_initial_room_create(room_id, event.event_id())
					.await =>
			None,
		| Err(error) if error.kind() == ErrorKind::NotFound =>
			return Err(Error::bad_database("Missing historical membership snapshot")),
		| Err(error) => return Err(error),
	};
	Ok(MemberSnapshot { hash, boundary: Some(event) })
}

#[implement(super::Service)]
async fn member_snapshot_visible(
	&self,
	room_id: &RoomId,
	user_id: &UserId,
	joined: bool,
	position: PduCount,
	snapshot: &MemberSnapshot,
) -> Result<bool> {
	let mut membership = MembershipState::Leave;
	let mut visibility = HistoryVisibility::Shared;
	if let Some(hash) = snapshot.hash {
		visibility = self.history_visibility_at(room_id, hash).await?;
		if let Some(event) = self
			.state_get_optional(hash, &StateEventType::RoomMember, user_id.as_str())
			.await?
		{
			if event.room_id() != room_id {
				return Err(Error::bad_database("Mismatched historical member room"));
			}
			membership = event
				.get_content::<RoomMemberEventContent>()
				.map_err(|_| Error::bad_database("Invalid historical member event"))?
				.membership;
		}
	}
	// Visibility is judged at the event's authorization state. In particular a
	// user can see their own leave event when joined immediately before it.
	if membership == MembershipState::Join {
		return Ok(true);
	}
	if let Some(event) = &snapshot.boundary
		&& event.event_type().to_cow_str() == "m.room.member"
		&& event.state_key() == Some(user_id.as_str())
	{
		let content = event
			.get_content::<RoomMemberEventContent>()
			.map_err(|_| Error::bad_database("Invalid boundary membership event"))?;
		if content.membership == MembershipState::Join
			|| content.membership == MembershipState::Invite
				&& visibility == HistoryVisibility::Invited
		{
			return Ok(true);
		}
	}
	match visibility {
		| HistoryVisibility::WorldReadable => Ok(true),
		| HistoryVisibility::Joined => Ok(false),
		| HistoryVisibility::Invited => Ok(membership == MembershipState::Invite),
		| _ if joined => Ok(true),
		| _ => {
			if !self
				.services
				.state_cache
				.once_joined_checked(user_id, room_id)
				.await?
			{
				return Ok(false);
			}
			let count = self
				.services
				.state_cache
				.get_left_count(room_id, user_id)
				.await
				.map_err(|_| Error::bad_database("Missing shared-history departure"))?;
			if count == 0 || count > i64::MAX.cast_unsigned() {
				return Err(Error::bad_database("Invalid shared-history departure"));
			}
			Ok(position <= PduCount::Normal(count))
		},
	}
}
