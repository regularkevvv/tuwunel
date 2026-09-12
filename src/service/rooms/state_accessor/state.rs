use std::{ops::Deref, sync::Arc};

use futures::{
	FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, future::try_join, pin_mut,
};
use ruma::{
	EventId, OwnedEventId, RoomId, UserId,
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
	matrix::{Event, Pdu, StateKey},
	pair_of,
	utils::{
		result::FlatOk,
		stream::{BroadbandExt, IterStream, ReadyExt, TryBroadbandExt, TryIgnore},
	},
};

use crate::rooms::{
	short::{ShortEventId, ShortStateHash, ShortStateKey},
	state_compressor::{CompressedState, compress_state_event, parse_compressed_state_event},
};

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
	if pdu.kind() == &TimelineEventType::RoomMember
		&& pdu.state_key() == Some(user_id.as_str())
		&& let Ok(content) = pdu.get_content::<RoomMemberEventContent>()
	{
		return content.membership;
	}

	let Ok(shortstatehash) = self
		.services
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
	let Ok(shortstatekey) = self
		.services
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
	let direct_shortstatekey = match self
		.services
		.short
		.get_shortstatekey(event_type, state_key)
		.await
	{
		| Ok(shortstatekey) => self
			.services
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

	let event_id: OwnedEventId = match direct_shortstatekey {
		| Some(shortstatekey) => {
			let start = compress_state_event(shortstatekey, 0);
			let end = compress_state_event(shortstatekey, u64::MAX);

			let shorteventid = self
				.load_full_state(shortstatehash)
				.await?
				.range(start..=end)
				.next()
				.copied()
				.map(parse_compressed_state_event)
				.map(at!(1));
			let Some(shorteventid) = shorteventid else {
				return Ok(None);
			};

			self.services
				.short
				.get_eventid_from_short(shorteventid)
				.await
				.map_err(|_| Error::bad_database("Incomplete state event mapping"))?
		},

		| None => {
			let event_id = self
				.state_full_entries_strict(shortstatehash)
				.boxed()
				.try_collect::<Vec<_>>()
				.await?
				.into_iter()
				.find(|((candidate_type, candidate_key), _)| {
					candidate_type == event_type && candidate_key.as_str() == state_key
				})
				.map(at!(1));
			let Some(event_id) = event_id else {
				return Ok(None);
			};

			event_id
		},
	};

	let pdu = self
		.services
		.timeline
		.get_pdu(&event_id)
		.await
		.map_err(|_| Error::bad_database("Incomplete state event"))?;
	if pdu.event_id() != event_id
		|| pdu.event_type().to_cow_str() != event_type.to_cow_str()
		|| pdu.state_key() != Some(state_key)
	{
		return Err(Error::bad_database("Mismatched state event"));
	}

	Ok(Some(pdu))
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
	let Ok(pdu) = self.services.timeline.get_pdu(event_id).await else {
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

/// Returns a single EventId from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
pub async fn state_get_id(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<OwnedEventId> {
	let shorteventid = self
		.state_get_shortid(shortstatehash, event_type, state_key)
		.await?;

	self.services
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
	let direct_shortstatekey = match self
		.services
		.short
		.get_shortstatekey(event_type, state_key)
		.await
	{
		| Ok(shortstatekey) => self
			.services
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
		let (candidate_type, candidate_key) = self
			.services
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
	self.state_keys_with_ids(shortstatehash, event_type)
		.map(at!(1))
		.broad_filter_map(move |event_id: OwnedEventId| async move {
			self.services
				.timeline
				.get_pdu(&event_id)
				.await
				.ok()
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
	self.state_keys_with_shortids(shortstatehash, event_type)
		.unzip()
		.map(|(state_keys, shorteventids): (Vec<_>, Vec<_>)| {
			self.services
				.short
				.multi_get_eventid_from_short(shorteventids.into_iter().stream())
				.zip(state_keys.into_iter().stream())
				.ready_filter_map(|(eid, sk)| eid.map(move |eid| (sk, eid)).ok())
		})
		.flatten_stream()
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
	self.state_full_shortids(shortstatehash)
		.ignore_err()
		.unzip()
		.map(move |(shortstatekeys, shorteventids): (Vec<_>, Vec<_>)| {
			self.services
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
}

/// Iterates the state_keys for an event_type in the state
#[implement(super::Service)]
pub fn state_keys<'a>(
	&'a self,
	shortstatehash: ShortStateHash,
	event_type: &'a StateEventType,
) -> impl Stream<Item = StateKey> + Send + 'a {
	let short_ids = self
		.state_full_shortids(shortstatehash)
		.ignore_err()
		.map(at!(0));

	self.services
		.short
		.multi_get_statekey_from_short(short_ids)
		.ready_filter_map(Result::ok)
		.ready_filter_map(move |(event_type_, state_key)| {
			event_type_.eq(event_type).then_some(state_key)
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
	let short_ids = self
		.state_full_shortids(shortstatehash)
		.ignore_err()
		.map(at!(1));

	self.services
		.short
		.multi_get_eventid_from_short(short_ids)
		.ready_filter_map(Result::ok)
		.broad_filter_map(move |event_id: OwnedEventId| async move {
			self.services
				.timeline
				.get_pdu(&event_id)
				.await
				.ok()
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
	self.state_full_ids_strict(shortstatehash)
		.try_collect::<Vec<_>>()
		.and_then(async move |entries| {
			let (shortstatekeys, event_ids): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
			self.services
				.short
				.multi_get_statekey_from_short(shortstatekeys.into_iter().stream())
				.zip(event_ids.into_iter().stream())
				.map(|(state_key, event_id)| {
					state_key
						.map(|state_key| (state_key, event_id))
						.map_err(|_| Error::bad_database("Incomplete state key mapping"))
				})
				.try_collect::<Vec<_>>()
				.await
		})
		.map_ok(Vec::into_iter)
		.map_ok(IterStream::try_stream)
		.try_flatten_stream()
}

/// Iterates complete current-state PDUs. Every PDU must bind to its snapshot
/// event ID, type, and state key before a caller can emit it.
#[implement(super::Service)]
pub fn state_full_pdus_strict(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = Result<((StateEventType, StateKey), Pdu)>> + Send + '_ {
	self.state_full_entries_strict(shortstatehash)
		.broad_and_then(async |(state_key, event_id)| {
			let pdu = self
				.services
				.timeline
				.get_pdu(&event_id)
				.await
				.map_err(|_| Error::bad_database("Incomplete state event"))?;
			if pdu.event_id() != event_id
				|| pdu.event_type().to_cow_str() != state_key.0.to_cow_str()
				|| pdu.state_key() != Some(state_key.1.as_str())
			{
				return Err(Error::bad_database("Mismatched state event"));
			}
			Ok((state_key, pdu))
		})
}

/// Builds a StateMap by iterating over all keys that start
/// with state_hash, this gives the full state for the given state_hash.
#[implement(super::Service)]
pub fn state_full_ids(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = (ShortStateKey, OwnedEventId)> + Send + '_ {
	self.state_full_shortids(shortstatehash)
		.ignore_err()
		.unzip()
		.map(|(shortstatekeys, shorteventids): (Vec<_>, Vec<_>)| {
			self.services
				.short
				.multi_get_eventid_from_short(shorteventids.into_iter().stream())
				.zip(shortstatekeys.into_iter().stream())
				.ready_filter_map(|(eid, ssk)| eid.ok().map(|eid| (ssk, eid)))
		})
		.flatten_stream()
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
	self.state_full_shortids(shortstatehash)
		.try_fold(
			(Vec::new(), Vec::new()),
			async |(mut shortstatekeys, mut shorteventids), (shortstatekey, shorteventid)| {
				shortstatekeys.push(shortstatekey);
				shorteventids.push(shorteventid);

				Ok((shortstatekeys, shorteventids))
			},
		)
		.and_then(async move |(shortstatekeys, shorteventids)| {
			self.services
				.short
				.multi_get_eventid_from_short(shorteventids.into_iter().stream())
				.zip(shortstatekeys.into_iter().stream())
				.map(|(event_id, shortstatekey)| {
					event_id
						.map(|event_id| (shortstatekey, event_id))
						.map_err(|_| Error::bad_database("Incomplete state event mapping"))
				})
				.try_collect::<Vec<_>>()
				.await
		})
		.map_ok(Vec::into_iter)
		.map_ok(IterStream::try_stream)
		.try_flatten_stream()
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
	self.services
		.state_compressor
		.load_shortstatehash_info(shortstatehash)
		.map_err(|e| err!(Database("Missing state IDs: {e}")))
		.map_ok(|vec| {
			vec.last()
				.expect("at least one layer")
				.full_state
				.clone()
		})
		.await
}
