use futures::{Stream, StreamExt, TryFutureExt};
use ruma::{OwnedEventId, RoomId, events::StateEventType};
use serde::Deserialize;
use tuwunel_core::{
	Result, err, implement,
	matrix::{Event, Pdu, StateKey},
};

/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`).
#[implement(super::Service)]
pub async fn room_state_get_content<T>(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<T>
where
	T: for<'de> Deserialize<'de> + Send,
{
	self.room_state_get(room_id, event_type, state_key)
		.await
		.and_then(|event| event.get_content())
}

/// Returns the room state events for a specific type.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_state_type_pdus<'a>(
	&'a self,
	room_id: &'a RoomId,
	event_type: &'a StateEventType,
) -> impl Stream<Item = Result<impl Event>> + Send + 'a {
	self.services
		.state
		.get_room_shortstatehash(room_id)
		.map_ok(move |shortstatehash| {
			self.state_type_pdus_strict(shortstatehash, event_type)
				.map(move |result| {
					result.and_then(|pdu| {
						(pdu.room_id() == room_id)
							.then_some(pdu)
							.ok_or_else(|| err!(Database("Mismatched room state event")))
					})
				})
		})
		.map_err(move |e| err!(Database("Missing state for {room_id:?}: {e:?}")))
		.try_flatten_stream()
		.boxed()
}

/// Returns the full room state.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_state_full<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = Result<((StateEventType, StateKey), impl Event)>> + Send + 'a {
	self.services
		.state
		.get_room_shortstatehash(room_id)
		.map_ok(move |shortstatehash| {
			self.state_full_pdus_strict(shortstatehash)
				.map(move |result| {
					result.and_then(|(state_key, pdu)| {
						(pdu.room_id() == room_id)
							.then_some((state_key, pdu))
							.ok_or_else(|| err!(Database("Mismatched room state event")))
					})
				})
		})
		.map_err(move |e| err!(Database("Missing state for {room_id:?}: {e:?}")))
		.try_flatten_stream()
		.boxed()
}

/// Returns the full room state pdus
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_state_full_pdus<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = Result<impl Event>> + Send + 'a {
	self.services
		.state
		.get_room_shortstatehash(room_id)
		.map_ok(move |shortstatehash| {
			self.state_full_pdus_strict(shortstatehash)
				.map(move |result| {
					result.and_then(|(_, pdu)| {
						(pdu.room_id() == room_id)
							.then_some(pdu)
							.ok_or_else(|| err!(Database("Mismatched room state event")))
					})
				})
		})
		.map_err(move |e| err!(Database("Missing state for {room_id:?}: {e:?}")))
		.try_flatten_stream()
		.boxed()
}

/// Returns a single EventId from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn room_state_get_id(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<OwnedEventId> {
	self.services
		.state
		.get_room_shortstatehash(room_id)
		.and_then(|shortstatehash| self.state_get_id(shortstatehash, event_type, state_key))
		.await
}

/// Iterates the state_keys for an event_type in the state joined by the
/// `event_id` from the current state.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_state_keys_with_ids<'a>(
	&'a self,
	room_id: &'a RoomId,
	event_type: &'a StateEventType,
) -> impl Stream<Item = Result<(StateKey, OwnedEventId)>> + Send + 'a {
	self.services
		.state
		.get_room_shortstatehash(room_id)
		.map_ok(|shortstatehash| self.state_keys_with_ids_strict(shortstatehash, event_type))
		.map_err(move |e| err!(Database("Missing state for {room_id:?}: {e:?}")))
		.try_flatten_stream()
		.boxed()
}

/// Iterates the state_keys for an event_type in the state
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_state_keys<'a>(
	&'a self,
	room_id: &'a RoomId,
	event_type: &'a StateEventType,
) -> impl Stream<Item = Result<StateKey>> + Send + 'a {
	self.services
		.state
		.get_room_shortstatehash(room_id)
		.map_ok(|shortstatehash| self.state_keys_strict(shortstatehash, event_type))
		.map_err(move |e| err!(Database("Missing state for {room_id:?}: {e:?}")))
		.try_flatten_stream()
		.boxed()
}

/// Returns a single PDU from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn room_state_get(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Pdu> {
	self.services
		.state
		.get_room_shortstatehash(room_id)
		.and_then(|shortstatehash| self.state_get(shortstatehash, event_type, state_key))
		.await
}
