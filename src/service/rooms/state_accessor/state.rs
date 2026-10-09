use std::{collections::BTreeSet, ops::Deref, sync::Arc};

use futures::{
	FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, future::try_join, pin_mut,
};
use ruma::{
	EventId, OwnedEventId, OwnedRoomId, OwnedServerName, RoomId, UserId,
	api::error::{ErrorKind, LimitExceededErrorData},
	events::{
		StateEventType, TimelineEventType,
		room::{
			history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
			member::{MembershipState, RoomMemberEventContent},
		},
	},
};
use serde::Deserialize;
use tuwunel_core::{
	Error, Result, at, err, implement,
	matrix::{Event, Pdu, PduCount, StateKey},
	pair_of,
	utils::{
		json::serialized_len,
		result::FlatOk,
		stream::{BroadbandExt, IterStream, ReadyExt, TryIgnore},
	},
};

use crate::rooms::{
	short::{ShortEventId, ShortStateHash, ShortStateKey},
	state_compressor::{CompressedState, compress_state_event, parse_compressed_state_event},
};

const MAX_STATE_MAPPING_BYTES: usize = 512 * 1024;

/// Freeze recipients from complete authoritative state, including the event
/// being admitted. Membership caches may lag a commit or a restart.
#[implement(super::Service)]
pub(crate) async fn federation_servers_for_append(
	&self,
	state: ShortStateHash,
	pending: &tuwunel_core::PduEvent,
) -> Result<Vec<OwnedServerName>> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();
	let mut servers = BTreeSet::new();
	let entries = self.state_full_shortids(state);
	let mut bytes = 0_usize;
	pin_mut!(entries);
	while let Some((shortkey, shortevent)) = entries.try_next().await? {
		let (kind, key) = services_root
			.short
			.get_statekey_from_short(shortkey)
			.await?;
		let event = services_root
			.short
			.get_eventid_from_short::<OwnedEventId>(shortevent)
			.await?;
		bytes = bytes
			.saturating_add(kind.to_cow_str().len())
			.saturating_add(key.as_str().len())
			.saturating_add(event.as_str().len());
		if bytes > MAX_STATE_MAPPING_BYTES {
			return Err(state_mapping_limit());
		}
		if services_root
			.short
			.get_shortstatekey(&kind, key.as_str())
			.await? != shortkey
			|| services_root
				.short
				.get_shorteventid(&event)
				.await? != shortevent
		{
			return Err(Error::bad_database("Federation state dictionaries disagree"));
		}
		if kind != StateEventType::RoomMember {
			continue;
		}
		let pdu = self
			.state_event_for_append(&event, Some(pending))
			.await?;
		if pdu.event_id() != event
			|| pdu.room_id() != pending.room_id
			|| pdu.event_type().to_cow_str() != kind.to_cow_str()
			|| pdu.state_key() != Some(key.as_str())
		{
			return Err(Error::bad_database("Federation membership binding is invalid"));
		}
		let user = UserId::parse(key.as_str())?;
		let member: RoomMemberEventContent = pdu.get_content()?;
		if member.membership == MembershipState::Join
			&& !services_root
				.globals
				.server_is_ours(user.server_name())
		{
			servers.insert(user.server_name().to_owned());
		}
	}
	// Departed, banned and invited targets still need the membership event.
	if pending.kind == TimelineEventType::RoomMember {
		let user = UserId::parse(pending.state_key.as_deref().ok_or_else(|| {
			Error::bad_database("Federation membership event lacks its state key")
		})?)?;
		if !services_root
			.globals
			.server_is_ours(user.server_name())
		{
			servers.insert(user.server_name().to_owned());
		}
	}
	Ok(servers.into_iter().collect())
}

fn state_mapping_limit() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Room state mapping byte limit reached".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}

/// The user was a joined member at this state (potentially in the past)
#[implement(super::Service)]
#[inline]
pub async fn user_was_joined(&self, shortstatehash: ShortStateHash, user_id: &UserId) -> bool {
	self.user_membership(shortstatehash, user_id)
		.await == MembershipState::Join
}

/// The user was an invited or joined room member at this state (potentially
/// in the past)
#[implement(super::Service)]
#[inline]
pub async fn user_was_invited(&self, shortstatehash: ShortStateHash, user_id: &UserId) -> bool {
	let s = self
		.user_membership(shortstatehash, user_id)
		.await;
	s == MembershipState::Join || s == MembershipState::Invite
}

/// Get membership for given user in state
#[implement(super::Service)]
pub async fn user_membership(
	&self,
	shortstatehash: ShortStateHash,
	user_id: &UserId,
) -> MembershipState {
	self.state_get_content(shortstatehash, &StateEventType::RoomMember, user_id.as_str())
		.await
		.map_or(MembershipState::Leave, |c: RoomMemberEventContent| c.membership)
}

/// MSC4115: the user's room membership "just after" the given PDU landed.
///
/// `pdu_shortstatehash` returns state-before-the-event, so a member event
/// targeting `user_id` overrides that lookup with its own content.
#[implement(super::Service)]
pub async fn user_membership_at_pdu(&self, user_id: &UserId, pdu: &Pdu) -> MembershipState {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	if pdu.kind() == &TimelineEventType::RoomMember
		&& pdu.state_key() == Some(user_id.as_str())
		&& let Ok(content) = pdu.get_content::<RoomMemberEventContent>()
	{
		return content.membership;
	}

	let Ok(shortstatehash) = services_root
		.state
		.pdu_shortstatehash(pdu.event_id())
		.await
	else {
		return MembershipState::Leave;
	};

	self.user_membership(shortstatehash, user_id)
		.await
}

/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`).
#[implement(super::Service)]
pub async fn state_get_content<T>(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<T>
where
	T: for<'de> Deserialize<'de> + Send,
{
	self.state_get(shortstatehash, event_type, state_key)
		.await
		.and_then(|event| event.get_content())
}

#[implement(super::Service)]
pub async fn state_contains(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> bool {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let Ok(shortstatekey) = services_root
		.short
		.get_shortstatekey(event_type, state_key)
		.await
	else {
		return false;
	};

	self.state_contains_shortstatekey(shortstatehash, shortstatekey)
		.await
}

#[implement(super::Service)]
pub async fn state_contains_type(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
) -> bool {
	let state_keys = self.state_keys(shortstatehash, event_type);

	pin_mut!(state_keys);
	state_keys.next().await.is_some()
}

#[implement(super::Service)]
pub async fn state_contains_shortstatekey(
	&self,
	shortstatehash: ShortStateHash,
	shortstatekey: ShortStateKey,
) -> bool {
	let start = compress_state_event(shortstatekey, 0);
	let end = compress_state_event(shortstatekey, u64::MAX);

	self.load_full_state(shortstatehash)
		.map_ok(|full_state| full_state.range(start..=end).next().copied())
		.await
		.flat_ok()
		.is_some()
}

/// Returns a single PDU from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
pub async fn state_get(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Pdu> {
	self.state_get_optional(shortstatehash, event_type, state_key)
		.await?
		.ok_or(err!(Request(NotFound("Not found in room state"))))
}

/// Returns the current-state PDU for one state cell, or `None` when a complete
/// snapshot proves the cell is absent. A missing or mismatched compact-key
/// mapping is not proof of absence, so it falls back to a strict snapshot
/// lookup.
#[implement(super::Service)]
pub async fn state_get_optional(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Option<Pdu>> {
	self.state_get_optional_for_append(shortstatehash, event_type, state_key, None)
		.await
}

/// The authenticated event being accepted may already be named by its
/// candidate state snapshot. Resolve only that exact ID from the caller's
/// event; every stored cell retains its normal strict binding checks.
#[implement(super::Service)]
pub(crate) async fn state_get_optional_for_append(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
	pending: Option<&Pdu>,
) -> Result<Option<Pdu>> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let direct_shortstatekey = match services_root
		.short
		.get_shortstatekey(event_type, state_key)
		.await
	{
		| Ok(shortstatekey) => services_root
			.short
			.get_statekey_from_short(shortstatekey)
			.await
			.ok()
			.is_some_and(|(candidate_type, candidate_key)| {
				candidate_type.eq(event_type) && candidate_key.as_str() == state_key
			})
			.then_some(shortstatekey),

		| Err(_) => None,
	};

	let direct_shorteventid = match direct_shortstatekey {
		| Some(shortstatekey) => {
			let start = compress_state_event(shortstatekey, 0);
			let end = compress_state_event(shortstatekey, u64::MAX);
			let full_state = self.load_full_state(shortstatehash).await?;
			let mut candidates = full_state.range(start..=end).copied();
			let shorteventid = candidates
				.next()
				.map(parse_compressed_state_event)
				.map(at!(1));
			if candidates.next().is_some() {
				return Err(Error::bad_database("Duplicate state key mapping"));
			}
			shorteventid
		},
		| None => None,
	};

	let shorteventid = match direct_shorteventid {
		| Some(shorteventid) => shorteventid,
		// Absence requires complete mappings bound to their stored events. A
		// corrupt reverse key can decode as another type and hide this cell.
		// Even a valid shortcut outside the snapshot cannot establish absence.
		| None => {
			let Some(shorteventid) = self
				.state_cell_from_snapshot(shortstatehash, event_type, state_key, pending)
				.await?
			else {
				return Ok(None);
			};
			shorteventid
		},
	};
	let event_id: OwnedEventId = services_root
		.short
		.get_eventid_from_short(shorteventid)
		.await
		.map_err(|_| Error::bad_database("Incomplete state event mapping"))?;

	let pdu = self
		.state_event_for_append(&event_id, pending)
		.await?;
	if pdu.event_id() != event_id
		|| pdu.event_type().to_cow_str() != event_type.to_cow_str()
		|| pdu.state_key() != Some(state_key)
	{
		return Err(Error::bad_database("Mismatched state event"));
	}

	Ok(Some(pdu))
}

/// Proves optional-state presence or absence from every bounded mapped cell.
#[implement(super::Service)]
async fn state_cell_from_snapshot(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
	pending: Option<&Pdu>,
) -> Result<Option<ShortEventId>> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let entries = self
		.state_full_shortids(shortstatehash)
		.try_collect::<Vec<_>>()
		.await?;
	let mut shorteventid = None;
	let mut bytes = 0_usize;
	let mut decoded = Vec::new();
	for (shortstatekey, candidate_event) in entries {
		let (candidate_type, candidate_key) = services_root
			.short
			.get_statekey_from_short(shortstatekey)
			.await
			.map_err(|_| Error::bad_database("Incomplete state key mapping"))?;
		bytes = bytes
			.saturating_add(candidate_type.to_cow_str().len())
			.saturating_add(candidate_key.as_str().len());
		if bytes > MAX_STATE_MAPPING_BYTES {
			return Err(state_mapping_limit());
		}
		let event_id: OwnedEventId = services_root
			.short
			.get_eventid_from_short(candidate_event)
			.await
			.map_err(|_| Error::bad_database("Incomplete state event mapping"))?;
		bytes = bytes.saturating_add(event_id.as_str().len());
		if bytes > MAX_STATE_MAPPING_BYTES {
			return Err(state_mapping_limit());
		}
		if candidate_type == *event_type
			&& candidate_key.as_str() == state_key
			&& shorteventid.replace(candidate_event).is_some()
		{
			return Err(Error::bad_database("Duplicate state key mapping"));
		}
		decoded.push((candidate_type, candidate_key, event_id));
	}
	let mut source_bytes = 0_usize;
	let mut snapshot_room: Option<OwnedRoomId> = None;
	for (candidate_type, candidate_key, event_id) in decoded {
		let pdu = self
			.state_event_for_append(&event_id, pending)
			.await?;
		source_bytes = source_bytes.saturating_add(
			serialized_len(pdu.as_pdu())
				.map_err(|_| Error::bad_database("Invalid state event serialization"))?,
		);
		if source_bytes > MAX_STATE_MAPPING_BYTES {
			return Err(state_mapping_limit());
		}
		if pdu.event_id() != event_id
			|| pdu.event_type().to_cow_str() != candidate_type.to_cow_str()
			|| pdu.state_key() != Some(candidate_key.as_str())
			|| snapshot_room
				.as_ref()
				.is_some_and(|room| room.as_str() != pdu.room_id().as_str())
		{
			return Err(Error::bad_database("Mismatched state event mapping"));
		}
		snapshot_room = Some(pdu.room_id().to_owned());
	}
	Ok(shorteventid)
}

#[implement(super::Service)]
async fn state_event_for_append(&self, event: &EventId, pending: Option<&Pdu>) -> Result<Pdu> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	if let Some(pdu) = pending.filter(|pdu| pdu.event_id() == event) {
		return Ok(pdu.clone());
	}
	services_root
		.timeline
		.get_pdu(event)
		.await
		.map_err(|error| {
			if error.is_not_found() {
				Error::bad_database("Incomplete state event")
			} else {
				error
			}
		})
}

/// Gets history visibility from an event's state without converting corrupt
/// state into the Matrix default of `shared` visibility.
#[implement(super::Service)]
pub async fn history_visibility_at(
	&self,
	room_id: &RoomId,
	shortstatehash: ShortStateHash,
) -> Result<HistoryVisibility> {
	let Some(pdu) = self
		.state_get_optional(shortstatehash, &StateEventType::RoomHistoryVisibility, "")
		.await?
	else {
		return Ok(HistoryVisibility::Shared);
	};

	if pdu.room_id() != room_id {
		return Err(Error::bad_database("Mismatched history visibility state event"));
	}

	pdu.get_content::<RoomHistoryVisibilityEventContent>()
		.map(|content| content.history_visibility)
		.map_err(|_| Error::bad_database("Invalid history visibility state event"))
}

/// The canonical first create event deliberately has no predecessor state
/// snapshot. It is the sole visibility fallback permitted when a PDU state
/// hash is absent.
#[implement(super::Service)]
pub async fn is_initial_room_create(&self, room_id: &RoomId, event_id: &EventId) -> bool {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let Ok(pdu) = services_root.timeline.get_pdu(event_id).await else {
		return false;
	};

	if pdu.event_id() != event_id
		|| pdu.room_id() != room_id
		|| *pdu.kind() != TimelineEventType::RoomCreate
		|| pdu.state_key() != Some("")
	{
		return false;
	}

	self.room_state_get(room_id, &StateEventType::RoomCreate, "")
		.await
		.is_ok_and(|current_create| {
			current_create.event_id() == event_id && current_create.room_id() == room_id
		})
}

/// The state to judge an event by when it has no state snapshot of its own.
///
/// Two kinds of stored event never had one: an outlier, which was stored but
/// never integrated, and an event backfilled into the timeline's negative
/// range. Both are judged by the room's current state. A normal timeline event
/// always carries a snapshot, so a missing one is incomplete state and yields
/// `None`, as does an unreadable or foreign event.
#[implement(super::Service)]
pub async fn snapshotless_state(
	&self,
	room_id: &RoomId,
	event_id: &EventId,
) -> Option<ShortStateHash> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let pdu = services_root
		.timeline
		.get_pdu(event_id)
		.await
		.ok()?;
	if pdu.event_id() != event_id || pdu.room_id() != room_id {
		return None;
	}

	match services_root
		.timeline
		.get_pdu_count(event_id)
		.await
	{
		| Ok(PduCount::Backfilled(_)) => {},
		| Err(error) if error.is_not_found() => {},
		| Ok(PduCount::Normal(_)) | Err(_) => return None,
	}

	services_root
		.state
		.get_room_shortstatehash(room_id)
		.await
		.ok()
}

/// Returns a single EventId from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
pub async fn state_get_id(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<OwnedEventId> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let shorteventid = self
		.state_get_shortid(shortstatehash, event_type, state_key)
		.await?;

	services_root
		.short
		.get_eventid_from_short(shorteventid)
		.await
}

/// Returns a single EventId from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
pub async fn state_get_shortid(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<ShortEventId> {
	self.state_get_shortid_optional(shortstatehash, event_type, state_key)
		.await?
		.ok_or(err!(Request(NotFound("Not found in room state"))))
}

/// Returns a compact event ID for one state cell, or `None` only when a
/// complete snapshot proves the cell is absent. A missing or mismatched
/// forward state-key mapping falls back to the reverse snapshot scan.
#[implement(super::Service)]
pub async fn state_get_shortid_optional(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Option<ShortEventId>> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let direct_shortstatekey = match services_root
		.short
		.get_shortstatekey(event_type, state_key)
		.await
	{
		| Ok(shortstatekey) => services_root
			.short
			.get_statekey_from_short(shortstatekey)
			.await
			.ok()
			.is_some_and(|(candidate_type, candidate_key)| {
				candidate_type.eq(event_type) && candidate_key.as_str() == state_key
			})
			.then_some(shortstatekey),

		| Err(_) => None,
	};

	if let Some(shortstatekey) = direct_shortstatekey {
		let start = compress_state_event(shortstatekey, 0);
		let end = compress_state_event(shortstatekey, u64::MAX);
		return Ok(self
			.load_full_state(shortstatehash)
			.await?
			.range(start..=end)
			.next()
			.copied()
			.map(parse_compressed_state_event)
			.map(at!(1)));
	}

	let full_state = self.load_full_state(shortstatehash).await?;
	for compressed in full_state.iter().copied() {
		let (candidate_shortstatekey, candidate_shorteventid) =
			parse_compressed_state_event(compressed);
		let (candidate_type, candidate_key) = services_root
			.short
			.get_statekey_from_short(candidate_shortstatekey)
			.await
			.map_err(|_| Error::bad_database("Incomplete state key mapping"))?;

		if candidate_type.eq(event_type) && candidate_key.as_str() == state_key {
			return Ok(Some(candidate_shorteventid));
		}
	}

	Ok(None)
}

/// Iterates the events for an event_type in the state.
#[implement(super::Service)]
pub fn state_type_pdus<'a>(
	&'a self,
	shortstatehash: ShortStateHash,
	event_type: &'a StateEventType,
) -> impl Stream<Item = impl Event> + Send + 'a {
	let services_guard = self.services.get();
	crate::once_services::services_stream!(services_guard, services_root, {
		self.state_keys_with_ids(shortstatehash, event_type)
			.map(at!(1))
			.broad_filter_map(move |event_id: OwnedEventId| async move {
				services_root
					.timeline
					.get_pdu(&event_id)
					.await
					.ok()
			})
	})
}

/// Iterates every current-state PDU of an event type without treating a
/// snapshot, dictionary, or PDU read failure as an omitted event.
#[implement(super::Service)]
pub fn state_type_pdus_strict<'a>(
	&'a self,
	shortstatehash: ShortStateHash,
	event_type: &'a StateEventType,
) -> impl Stream<Item = Result<Pdu>> + Send + 'a {
	self.state_full_pdus_strict(shortstatehash)
		.try_filter_map(async move |((event_type_, _), pdu)| {
			Ok(event_type_.eq(event_type).then_some(pdu))
		})
}

/// Iterates the state_keys for an event_type in the state; current state
/// event_id included.
#[implement(super::Service)]
pub fn state_keys_with_ids<'a>(
	&'a self,
	shortstatehash: ShortStateHash,
	event_type: &'a StateEventType,
) -> impl Stream<Item = (StateKey, OwnedEventId)> + Send + 'a {
	let services_guard = self.services.get();
	crate::once_services::services_stream!(services_guard, services_root, {
		self.state_keys_with_shortids(shortstatehash, event_type)
			.unzip()
			.map(|(state_keys, shorteventids): (Vec<_>, Vec<_>)| {
				services_root
					.short
					.multi_get_eventid_from_short(shorteventids.into_iter().stream())
					.zip(state_keys.into_iter().stream())
					.ready_filter_map(|(eid, sk)| eid.map(move |eid| (sk, eid)).ok())
			})
			.flatten_stream()
	})
}

/// Iterates current-state keys and IDs for an event type with complete
/// snapshot and reverse-dictionary reads.
#[implement(super::Service)]
pub fn state_keys_with_ids_strict<'a>(
	&'a self,
	shortstatehash: ShortStateHash,
	event_type: &'a StateEventType,
) -> impl Stream<Item = Result<(StateKey, OwnedEventId)>> + Send + 'a {
	self.state_full_entries_strict(shortstatehash)
		.try_filter_map(async move |((event_type_, state_key), event_id)| {
			Ok(event_type_
				.eq(event_type)
				.then_some((state_key, event_id)))
		})
}

/// Iterates the state_keys for an event_type in the state; current state
/// event_id included.
#[implement(super::Service)]
pub fn state_keys_with_shortids<'a>(
	&'a self,
	shortstatehash: ShortStateHash,
	event_type: &'a StateEventType,
) -> impl Stream<Item = (StateKey, ShortEventId)> + Send + 'a {
	let services_guard = self.services.get();
	crate::once_services::services_stream!(services_guard, services_root, {
		self.state_full_shortids(shortstatehash)
			.ignore_err()
			.unzip()
			.map(move |(shortstatekeys, shorteventids): (Vec<_>, Vec<_>)| {
				services_root
					.short
					.multi_get_statekey_from_short(shortstatekeys.into_iter().stream())
					.zip(shorteventids.into_iter().stream())
					.ready_filter_map(|(res, id)| res.map(|res| (res, id)).ok())
					.ready_filter_map(move |((event_type_, state_key), event_id)| {
						event_type_
							.eq(event_type)
							.then_some((state_key, event_id))
					})
			})
			.flatten_stream()
	})
}

/// Iterates the state_keys for an event_type in the state
#[implement(super::Service)]
pub fn state_keys<'a>(
	&'a self,
	shortstatehash: ShortStateHash,
	event_type: &'a StateEventType,
) -> impl Stream<Item = StateKey> + Send + 'a {
	let services_guard = self.services.get();
	crate::once_services::services_stream!(services_guard, services_root, {
		let short_ids = self
			.state_full_shortids(shortstatehash)
			.ignore_err()
			.map(at!(0));

		services_root
			.short
			.multi_get_statekey_from_short(short_ids)
			.ready_filter_map(Result::ok)
			.ready_filter_map(move |(event_type_, state_key)| {
				event_type_.eq(event_type).then_some(state_key)
			})
	})
}

/// Iterates current-state keys for an event type with complete snapshot and
/// reverse-dictionary reads.
#[implement(super::Service)]
pub fn state_keys_strict<'a>(
	&'a self,
	shortstatehash: ShortStateHash,
	event_type: &'a StateEventType,
) -> impl Stream<Item = Result<StateKey>> + Send + 'a {
	self.state_keys_with_ids_strict(shortstatehash, event_type)
		.map_ok(at!(0))
}

/// Returns the state events removed between the interval (present in .0 but
/// not in .1)
#[implement(super::Service)]
#[inline]
pub fn state_removed(
	&self,
	shortstatehash: pair_of!(ShortStateHash),
) -> impl Stream<Item = (ShortStateKey, ShortEventId)> + Send + '_ {
	self.state_added((shortstatehash.1, shortstatehash.0))
}

/// Returns the state events added between the interval (present in .1 but
/// not in .0)
#[implement(super::Service)]
pub fn state_added(
	&self,
	shortstatehash: pair_of!(ShortStateHash),
) -> impl Stream<Item = (ShortStateKey, ShortEventId)> + Send + '_ {
	let a = self.load_full_state(shortstatehash.0);
	let b = self.load_full_state(shortstatehash.1);
	try_join(a, b)
		.map_ok(|(a, b)| b.difference(&a).copied().collect::<Vec<_>>())
		.map_ok(IterStream::try_stream)
		.try_flatten_stream()
		.ignore_err()
		.map(parse_compressed_state_event)
}

/// Returns the state events added between the interval (present in .1 but
/// not in .0), failing instead of yielding an incomplete delta when either
/// snapshot cannot be reconstructed.
#[implement(super::Service)]
pub fn state_added_strict(
	&self,
	shortstatehash: pair_of!(ShortStateHash),
) -> impl Stream<Item = Result<(ShortStateKey, ShortEventId)>> + Send + '_ {
	let a = self.load_full_state(shortstatehash.0);
	let b = self.load_full_state(shortstatehash.1);
	try_join(a, b)
		.map_ok(|(a, b)| b.difference(&a).copied().collect::<Vec<_>>())
		.map_ok(IterStream::try_stream)
		.try_flatten_stream()
		.map_ok(parse_compressed_state_event)
}

#[implement(super::Service)]
pub fn state_full(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = ((StateEventType, StateKey), impl Event)> + Send + '_ {
	self.state_full_pdus(shortstatehash)
		.ready_filter_map(|pdu| {
			Some(((pdu.kind().to_cow_str().into(), pdu.state_key()?.into()), pdu))
		})
}

#[implement(super::Service)]
pub fn state_full_pdus(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = impl Event> + Send + '_ {
	let services_guard = self.services.get();
	crate::once_services::services_stream!(services_guard, services_root, {
		let short_ids = self
			.state_full_shortids(shortstatehash)
			.ignore_err()
			.map(at!(1));

		services_root
			.short
			.multi_get_eventid_from_short(short_ids)
			.ready_filter_map(Result::ok)
			.broad_filter_map(move |event_id: OwnedEventId| async move {
				services_root
					.timeline
					.get_pdu(&event_id)
					.await
					.ok()
			})
	})
}

/// Builds complete current-state entries from the snapshot. Both directions of
/// the compact-ID dictionaries are required, so no corrupt cell can disappear
/// from a response merely because its reverse mapping could not be read.
#[implement(super::Service)]
pub fn state_full_entries_strict(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = Result<((StateEventType, StateKey), OwnedEventId)>> + Send + '_ {
	let services_guard = self.services.get();
	async_stream::try_stream! {
		let services_root = services_guard.as_ref();
		let entries = self.state_full_ids_strict(shortstatehash).try_collect::<Vec<_>>().await?;
		let mut decoded = Vec::new();
		let mut bytes = 0_usize;
		for (shortstatekey, event_id) in entries {
			let state_key = services_root
				.short
				.get_statekey_from_short(shortstatekey)
				.await
				.map_err(|_| Error::bad_database("Incomplete state key mapping"))?;
			bytes = bytes
				.saturating_add(state_key.0.to_cow_str().len())
				.saturating_add(state_key.1.as_str().len())
				.saturating_add(event_id.as_str().len());
			if bytes > MAX_STATE_MAPPING_BYTES {
				Err(state_mapping_limit())?;
			}
			decoded.push((state_key, event_id));
		}
		for item in decoded { yield item; }
	}
}

/// Iterates complete current-state PDUs. Every PDU must bind to its snapshot
/// event ID, type, and state key before a caller can emit it.
#[implement(super::Service)]
pub fn state_full_pdus_strict(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = Result<((StateEventType, StateKey), Pdu)>> + Send + '_ {
	let services_guard = self.services.get();
	crate::once_services::services_stream!(services_guard, services_root, {
		self.state_full_entries_strict(shortstatehash)
			.and_then(async |(state_key, event_id)| {
				let pdu = services_root
					.timeline
					.get_pdu(&event_id)
					.await
					.map_err(|error| {
						if error.kind() == ErrorKind::NotFound {
							Error::bad_database("Incomplete state event")
						} else {
							error
						}
					})?;
				if pdu.event_id() != event_id
					|| pdu.event_type().to_cow_str() != state_key.0.to_cow_str()
					|| pdu.state_key() != Some(state_key.1.as_str())
				{
					return Err(Error::bad_database("Mismatched state event"));
				}
				Ok((state_key, pdu))
			})
	})
}

/// Builds a StateMap by iterating over all keys that start
/// with state_hash, this gives the full state for the given state_hash.
#[implement(super::Service)]
pub fn state_full_ids(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = (ShortStateKey, OwnedEventId)> + Send + '_ {
	let services_guard = self.services.get();
	crate::once_services::services_stream!(services_guard, services_root, {
		self.state_full_shortids(shortstatehash)
			.ignore_err()
			.unzip()
			.map(|(shortstatekeys, shorteventids): (Vec<_>, Vec<_>)| {
				services_root
					.short
					.multi_get_eventid_from_short(shorteventids.into_iter().stream())
					.zip(shortstatekeys.into_iter().stream())
					.ready_filter_map(|(eid, ssk)| eid.ok().map(|eid| (ssk, eid)))
			})
			.flatten_stream()
	})
}

/// Builds a complete StateMap for the given state hash.
///
/// Snapshot and reverse-mapping failures are returned without yielding a
/// partial map.
#[implement(super::Service)]
pub fn state_full_ids_strict(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = Result<(ShortStateKey, OwnedEventId)>> + Send + '_ {
	let services_guard = self.services.get();
	async_stream::try_stream! {
		let services_root = services_guard.as_ref();
		let entries = self.state_full_shortids(shortstatehash).try_collect::<Vec<_>>().await?;
		let mut decoded = Vec::new();
		let mut bytes = 0_usize;
		for (shortstatekey, shorteventid) in entries {
			let event_id = services_root
				.short
				.get_eventid_from_short::<OwnedEventId>(shorteventid)
				.await
				.map_err(|_| Error::bad_database("Incomplete state event mapping"))?;
			bytes = bytes.saturating_add(event_id.as_str().len());
			if bytes > MAX_STATE_MAPPING_BYTES {
				Err(state_mapping_limit())?;
			}
			decoded.push((shortstatekey, event_id));
		}
		for item in decoded { yield item; }
	}
}

#[implement(super::Service)]
pub fn state_full_shortids(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = Result<(ShortStateKey, ShortEventId)>> + Send + '_ {
	self.load_full_state(shortstatehash)
		.map_ok(|full_state| {
			full_state
				.deref()
				.iter()
				.copied()
				.map(parse_compressed_state_event)
				.collect()
		})
		.map_ok(Vec::into_iter)
		.map_ok(IterStream::try_stream)
		.try_flatten_stream()
}

#[implement(super::Service)]
#[tracing::instrument(name = "load", level = "debug", skip(self))]
async fn load_full_state(&self, shortstatehash: ShortStateHash) -> Result<Arc<CompressedState>> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	services_root
		.state_compressor
		.load_shortstatehash_info(shortstatehash)
		.map_err(|error| {
			if error.is_not_found() {
				err!(Database("Missing state IDs"))
			} else {
				error
			}
		})
		.map_ok(|vec| {
			vec.last()
				.expect("at least one layer")
				.full_state
				.clone()
		})
		.await
}
