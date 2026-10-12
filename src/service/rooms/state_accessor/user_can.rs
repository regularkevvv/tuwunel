use futures::pin_mut;
use ruma::{
	EventId, RoomId, UserId,
	api::error::ErrorKind,
	events::{
		TimelineEventType,
		room::{
			history_visibility::HistoryVisibility,
			member::{MembershipState, RoomMemberEventContent},
			tombstone::RoomTombstoneEventContent,
		},
	},
};
use tuwunel_core::{
	Err, Error, Result, implement,
	matrix::{Event, PduCount, StateKey},
	pdu::PduBuilder,
	utils::FutureBoolExt,
};

use crate::rooms::{short::ShortStateHash, state::RoomMutexGuard};

/// Checks if a given user can redact a given event
///
/// If federation is true, it allows redaction events from any user of the
/// same server as the original event sender
#[implement(super::Service)]
pub async fn user_can_redact(
	&self,
	redacts: &EventId,
	sender: &UserId,
	room_id: &RoomId,
	federation: bool,
) -> Result<bool> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let redacting_event = match services_root.timeline.get_pdu(redacts).await {
		| Ok(pdu) => Some(pdu),
		| Err(error) if error.kind() == ErrorKind::NotFound => None,
		| Err(error) => return Err(error),
	};
	if redacting_event
		.as_ref()
		.is_some_and(|pdu| pdu.event_id() != redacts)
	{
		return Err(Error::bad_database("Mismatched redaction target"));
	}
	if redacting_event
		.as_ref()
		.is_some_and(|pdu| pdu.room_id() != room_id)
	{
		return Ok(false);
	}
	if redacting_event
		.as_ref()
		.is_some_and(|pdu| *pdu.kind() == TimelineEventType::RoomCreate)
	{
		return Err!(Request(Forbidden("Redacting m.room.create is not safe, forbidding.")));
	}
	if redacting_event
		.as_ref()
		.is_some_and(|pdu| *pdu.kind() == TimelineEventType::RoomServerAcl)
	{
		return Err!(Request(Forbidden(
			"Redacting m.room.server_acl will result in the room being inaccessible for \
			 everyone (empty allow key), forbidding."
		)));
	}

	let power_levels = self.get_power_levels(room_id).await?;
	Ok(power_levels.user_can_redact_event_of_other(sender)
		|| power_levels.user_can_redact_own_event(sender)
			&& redacting_event.as_ref().is_some_and(|event| {
				if federation {
					event.sender().server_name() == sender.server_name()
				} else {
					event.sender() == sender
				}
			}))
}

/// Whether a user is allowed to see an event, based on
/// the room's history_visibility at that event's state.
#[implement(super::Service)]
#[tracing::instrument(skip_all, level = "trace")]
pub async fn user_can_see_event(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	event_id: &EventId,
) -> bool {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let shortstatehash = match services_root
		.state
		.pdu_shortstatehash(event_id)
		.await
	{
		| Ok(shortstatehash) => Some(shortstatehash),
		| Err(error)
			if error.kind() == ErrorKind::NotFound
				&& self
					.is_initial_room_create(room_id, event_id)
					.await =>
			return true,
		| Err(error) if error.kind() == ErrorKind::NotFound =>
			self.snapshotless_state(room_id, event_id).await,
		| Err(_) => return false,
	};

	let Some(shortstatehash) = shortstatehash else {
		return false;
	};

	let Ok(history_visibility) = self
		.history_visibility_at(room_id, shortstatehash)
		.await
	else {
		return false;
	};

	match history_visibility {
		| HistoryVisibility::WorldReadable => true,

		// Allow if any member on requesting server was AT LEAST invited, else deny
		| HistoryVisibility::Invited =>
			self.user_was_invited(shortstatehash, user_id)
				.await,

		// Allow if any member on requested server was joined, else deny
		| HistoryVisibility::Joined =>
			self.user_was_joined(shortstatehash, user_id)
				.await,

		// An unrecognized value is treated as shared.
		| HistoryVisibility::Shared | _ =>
			self.user_shared_history(shortstatehash, room_id, event_id, user_id)
				.await,
	}
}

/// Whether a user may see an event under `shared` history visibility.
///
/// A current member sees the whole room, which the first check answers without
/// touching room state. A former member keeps events through their latest
/// leave, and lookup failures deny access.
#[implement(super::Service)]
async fn user_shared_history(
	&self,
	shortstatehash: ShortStateHash,
	room_id: &RoomId,
	event_id: &EventId,
	user_id: &UserId,
) -> bool {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let state_cache = &services_root.state_cache;

	if state_cache.is_joined(user_id, room_id).await
		|| self
			.user_was_joined(shortstatehash, user_id)
			.await
	{
		return true;
	}

	if !state_cache.once_joined(user_id, room_id).await {
		return false;
	}

	let Ok(left_count) = state_cache.get_left_count(room_id, user_id).await else {
		return false;
	};

	let Ok(event_count) = services_root
		.timeline
		.get_pdu_count(event_id)
		.await
	else {
		return false;
	};

	event_count <= PduCount::from_unsigned(left_count)
}

/// Whether a user is allowed to see an event, based on
/// the room's history_visibility at that event's state.
#[implement(super::Service)]
#[tracing::instrument(skip_all, level = "trace")]
pub async fn user_can_see_state_events(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	if services_root
		.state_cache
		.is_joined(user_id, room_id)
		.await
	{
		return true;
	}

	let Ok(shortstatehash) = services_root
		.state
		.get_room_shortstatehash(room_id)
		.await
	else {
		return false;
	};

	let Ok(history_visibility) = self
		.history_visibility_at(room_id, shortstatehash)
		.await
	else {
		return false;
	};

	match history_visibility {
		| HistoryVisibility::WorldReadable => true,

		| HistoryVisibility::Invited =>
			services_root
				.state_cache
				.is_invited(user_id, room_id)
				.await,

		| HistoryVisibility::Shared =>
			services_root
				.state_cache
				.once_joined(user_id, room_id)
				.await,

		| _ => false,
	}
}

/// The existing state-visibility policy with checked membership and snapshot
/// reads. Only a genuinely absent room becomes a negative authorization result.
#[implement(super::Service)]
pub async fn user_can_see_state_events_checked(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> Result<bool> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let cache = &services_root.state_cache;
	if cache.is_joined_checked(user_id, room_id).await? {
		return Ok(true);
	}
	let shortstatehash = match services_root
		.state
		.get_room_shortstatehash(room_id)
		.await
	{
		| Ok(hash) => hash,
		| Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
		| Err(error) => return Err(error),
	};
	match self
		.history_visibility_at(room_id, shortstatehash)
		.await?
	{
		| HistoryVisibility::WorldReadable => Ok(true),
		| HistoryVisibility::Invited => cache.is_invited_checked(user_id, room_id).await,
		| HistoryVisibility::Shared => cache.once_joined_checked(user_id, room_id).await,
		| _ => Ok(false),
	}
}

/// Whether a user may see a room: a current or prior membership (joined,
/// invited, left), or a world-readable room. Forgetting a room clears the
/// user's left-state, so a forgotten room is not visible.
#[implement(super::Service)]
pub async fn user_can_see_room(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let state_cache = &services_root.state_cache;
	let joined = state_cache.is_joined(user_id, room_id);
	let invited = state_cache.is_invited(user_id, room_id);
	let left = state_cache.is_left(user_id, room_id);
	let world_readable = self.is_world_readable(room_id);

	pin_mut!(joined, invited, left, world_readable);
	joined
		.or(invited)
		.or(left)
		.or(world_readable)
		.await
}

#[implement(super::Service)]
pub async fn user_can_invite(
	&self,
	room_id: &RoomId,
	sender: &UserId,
	target_user: &UserId,
	state_lock: &RoomMutexGuard,
) -> bool {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	services_root
		.timeline
		.create_hash_and_sign_event(
			PduBuilder::state(
				target_user.as_str(),
				&RoomMemberEventContent::new(MembershipState::Invite),
			),
			sender,
			room_id,
			state_lock,
		)
		.await
		.is_ok()
}

#[implement(super::Service)]
pub async fn user_can_tombstone(
	&self,
	room_id: &RoomId,
	user_id: &UserId,
	state_lock: &RoomMutexGuard,
) -> bool {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	if !services_root
		.state_cache
		.is_joined(user_id, room_id)
		.await
	{
		return false;
	}

	services_root
		.timeline
		.create_hash_and_sign_event(
			PduBuilder::state(StateKey::new(), &RoomTombstoneEventContent {
				replacement_room: room_id.into(), // placeholder,
				body: "Not a valid m.room.tombstone.".into(),
			}),
			user_id,
			room_id,
			state_lock,
		)
		.await
		.is_ok()
}
