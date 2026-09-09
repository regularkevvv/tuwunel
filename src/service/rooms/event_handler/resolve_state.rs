use std::{
	borrow::Borrow,
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use ruma::{OwnedEventId, RoomId, RoomVersionId};
use tuwunel_core::{
	Error, Result, err, implement,
	matrix::room_version,
	trace,
	utils::stream::{IterStream, ReadyExt, WidebandExt},
};

use crate::rooms::{
	state_compressor::CompressedState,
	state_res::{self, AuthSet, StateMap},
};

#[implement(super::Service)]
#[tracing::instrument(
	name = "state",
	level = "debug",
	skip_all,
	fields(
		incoming = ?incoming_state.len()
	),
)]
pub async fn resolve_state(
	&self,
	room_id: &RoomId,
	room_version: &RoomVersionId,
	incoming_state: HashMap<u64, OwnedEventId>,
) -> Result<Arc<CompressedState>> {
	trace!("Loading current room state ids");
	let current_sstatehash = self
		.services
		.state
		.get_room_shortstatehash(room_id)
		.map_err(|e| err!(Database(error!("No state for {room_id:?}: {e:?}"))))
		.await?;

	let current_state_ids: HashMap<_, _> = self
		.services
		.state_accessor
		.state_full_ids_strict(current_sstatehash)
		.try_collect()
		.await?;

	trace!("Loading fork states");
	let fork_states = [current_state_ids, incoming_state];
	let chain_complete = AtomicBool::new(true);
	let auth_chains = fork_states
		.iter()
		.stream()
		.wide_then(|state| {
			self.fork_chain_strict(
				room_id,
				room_version,
				state.values().map(Borrow::borrow),
				&chain_complete,
			)
		})
		.ready_filter_map(Result::ok);

	let fork_states: Vec<_> = fork_states
		.iter()
		.stream()
		.wide_then(|fork_state| {
			self.fork_state(
				fork_state
					.iter()
					.map(|(key, event_id)| (*key, event_id)),
			)
		})
		.try_collect()
		.await?;

	trace!("Resolving state");
	let state = self
		.state_resolution(room_id, room_version, fork_states.into_iter().stream(), auth_chains)
		.await?;

	// Unconflicted state need not poll auth chains. Any chain that resolution
	// did consume must be complete before its result can be compressed or used.
	if !chain_complete.load(Ordering::Relaxed) {
		return Err(Error::bad_database(
			"Incomplete auth chain during incoming state resolution",
		));
	}

	trace!("State resolution done.");
	let state_events: Vec<_> = state
		.iter()
		.stream()
		.wide_then(|((event_type, state_key), event_id)| {
			self.services
				.short
				.get_or_create_shortstatekey(event_type, state_key)
				.map(move |shortstatekey| (shortstatekey, event_id))
		})
		.collect()
		.await;

	trace!("Compressing state...");
	let new_room_state: CompressedState = self
		.services
		.state_compressor
		.compress_state_events(
			state_events
				.iter()
				.map(|(ssk, eid)| (ssk, (*eid).borrow())),
		)
		.collect()
		.await;

	Ok(Arc::new(new_room_state))
}

#[implement(super::Service)]
#[tracing::instrument(name = "resolve", level = "debug", skip_all)]
pub(super) async fn state_resolution<StateSets, AuthSets>(
	&self,
	_room_id: &RoomId,
	room_version: &RoomVersionId,
	state_sets: StateSets,
	auth_chains: AuthSets,
) -> Result<StateMap<OwnedEventId>>
where
	StateSets: Stream<Item = StateMap<OwnedEventId>> + Send,
	AuthSets: Stream<Item = AuthSet<OwnedEventId>> + Send,
{
	state_res::resolve(
		&room_version::rules(room_version)?,
		state_sets,
		auth_chains,
		&async |event_id: OwnedEventId| self.event_fetch(&event_id).await,
		&async |event_id: OwnedEventId| self.event_exists(&event_id).await,
		self.services.server.config.hydra_backports,
	)
	.map_err(|e| err!(error!("State resolution failed: {e:?}")))
	.await
}
