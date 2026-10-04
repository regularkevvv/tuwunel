mod prune;

use std::{fmt::Write, iter::once, pin::pin, sync::Arc};

use async_trait::async_trait;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, future::join_all};
pub(crate) use prune::prune_goal;
pub use prune::{PruneSummary, Trigger};
use ruma::{
	CanonicalJsonObject, EventId, OwnedEventId, OwnedRoomId, RoomId, RoomVersionId, UserId,
	api::error::{ErrorKind, LimitExceededErrorData},
	events::{
		AnyStrippedStateEvent, StateEventType, TimelineEventType,
		room::power_levels::RoomPowerLevelsEventContent,
	},
	room_version_rules::AuthorizationRules,
	serde::Raw,
};
use serde_json::value::RawValue as RawJsonValue;
use tuwunel_core::{
	Error, Event, PduEvent, Result,
	error::inspect_debug_log,
	implement,
	matrix::{PduCount, RoomVersionRules, StateKey, room_version},
	result::{AndThenRef, FlatOk},
	smallvec::SmallVec,
	trace,
	utils::{
		IterStream, MutexMap, MutexMapGuard, calculate_hash,
		json::serialized_len,
		mutex_map::Guard,
		stream::{TryIgnore, WidebandExt},
	},
	warn,
};
use tuwunel_database::{Ignore, Interfix, Map, Txn, serialize_key};

use crate::{
	rooms::{
		short::{ShortEventId, ShortStateHash},
		state_cache::MembershipUpdate,
		state_compressor::{CompressedState, parse_compressed_state_event},
		state_res::{StateMap, auth_types_for_event},
	},
	services::OnceServices,
};

pub struct Service {
	/// Serializes room state as the middle per-room operation.
	///
	/// Acquire it after federation and before timeline insertion when those
	/// mutexes share a room. Never acquire the federation mutex while holding
	/// this guard.
	pub mutex: RoomMutexMap,
	services: Arc<OnceServices>,
	db: Data,
}

struct Data {
	shorteventid_shortstatehash: Arc<Map>,
	roomid_shortstatehash: Arc<Map>,
	roomid_pduleaves: Arc<Map>,
}

type RoomMutexMap = MutexMap<OwnedRoomId, ()>;
pub type RoomMutexGuard = MutexMapGuard<OwnedRoomId, ()>;
type ForwardExtremities = SmallVec<[OwnedEventId; 1]>;

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			mutex: RoomMutexMap::new(),
			services: args.services.clone(),
			db: Data {
				shorteventid_shortstatehash: args.db["shorteventid_shortstatehash"].clone(),
				roomid_shortstatehash: args.db["roomid_shortstatehash"].clone(),
				roomid_pduleaves: args.db["roomid_pduleaves"].clone(),
			},
		}))
	}

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let mutex = self.mutex.len();
		writeln!(out, "- state_mutex: {mutex}")?;

		Ok(())
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Set the room to the given statehash and update caches.
#[implement(Service)]
#[tracing::instrument(
	name = "force",
	level = "debug",
	skip_all,
	fields(
		count = ?self.services.globals.pending_count(),
		%shortstatehash,
	)
)]
pub async fn force_state(
	&self,
	room_id: &RoomId,
	shortstatehash: u64,
	statediffnew: Arc<CompressedState>,
	_statediffremoved: Arc<CompressedState>,
	state_lock: &RoomMutexGuard,
) -> Result {
	statediffnew
		.iter()
		.stream()
		.map(|&new| parse_compressed_state_event(new).1)
		.wide_filter_map(async |shorteventid| {
			let event_id: OwnedEventId = self
				.services
				.short
				.get_eventid_from_short(shorteventid)
				.inspect_err(inspect_debug_log)
				.await
				.ok()?;

			self.services
				.timeline
				.get_pdu(&event_id)
				.await
				.ok()
		})
		.map(Ok)
		.try_for_each(async move |pdu| match pdu.kind {
			| TimelineEventType::RoomMember => {
				let Some(user_id) = pdu
					.state_key
					.as_ref()
					.map(UserId::parse)
					.flat_ok()
				else {
					return Ok(());
				};

				let Ok(membership_event) = pdu.get_content() else {
					return Ok(());
				};

				let count = self.services.globals.next_count().await?;
				self.services
					.state_cache
					.update_membership(MembershipUpdate {
						room_id,
						user_id: &user_id,
						membership_event,
						sender: &pdu.sender,
						last_state: None,
						invite_via: None,
						update_joined_count: false,
						count: PduCount::Normal(*count),
					})
					.await
			},
			| _ => Ok(()),
		})
		.boxed()
		.await?;

	self.services
		.state_cache
		.update_joined_count(room_id)
		.await?;

	self.set_room_state(room_id, shortstatehash, state_lock)
		.await?;

	// Forced state may change this room's cached hierarchy summary.
	self.services.spaces.cache_evict(room_id).await?;

	Ok(())
}

/// Generates a new StateHash and associates it with the incoming event.
///
/// This adds all current state events (not including the incoming event)
/// to `stateid_pduid` and adds the incoming event to `eventid_statehash`.
#[implement(Service)]
#[tracing::instrument(
	name = "set",
	level = "debug",
	skip(self, state_ids_compressed),
	fields(
		count = ?self.services.globals.pending_count(),
	)
)]
pub async fn set_event_state(
	&self,
	event_id: &EventId,
	room_id: &RoomId,
	state_ids_compressed: Arc<CompressedState>,
) -> Result<ShortStateHash> {
	const KEY_LEN: usize = size_of::<ShortEventId>();
	const VAL_LEN: usize = size_of::<ShortStateHash>();

	let shorteventid = self
		.services
		.short
		.get_or_create_shorteventid(event_id)
		.await?;

	let state_hash = calculate_hash(state_ids_compressed.iter().map(|s| &s[..]));

	if let Ok(shortstatehash) = self
		.services
		.short
		.get_shortstatehash(&state_hash)
		.await
	{
		self.db
			.shorteventid_shortstatehash
			.aput::<KEY_LEN, VAL_LEN, _, _>(shorteventid, shortstatehash)
			.await?;

		return Ok(shortstatehash);
	}

	let previous_shortstatehash = self.get_room_shortstatehash(room_id).await;
	let states_parents = match previous_shortstatehash {
		| Ok(p) =>
			self.services
				.state_compressor
				.load_shortstatehash_info(p)
				.await?,
		| _ => Vec::new(),
	};

	let (statediffnew, statediffremoved) = if let Some(parent_stateinfo) = states_parents.last() {
		let statediffnew: CompressedState = state_ids_compressed
			.difference(&parent_stateinfo.full_state)
			.copied()
			.collect();

		let statediffremoved: CompressedState = parent_stateinfo
			.full_state
			.difference(&state_ids_compressed)
			.copied()
			.collect();

		(Arc::new(statediffnew), Arc::new(statediffremoved))
	} else {
		(state_ids_compressed, Arc::new(CompressedState::new()))
	};

	let save_statediff = |txn: &mut Txn, shortstatehash| {
		self.services
			.state_compressor
			.save_state_from_diff(
				txn,
				shortstatehash,
				statediffnew,
				statediffremoved,
				1_000_000, // high number because no state will be based on this one
				states_parents,
			)
	};

	let (shortstatehash, _) = self
		.services
		.short
		.get_or_create_shortstatehash(&state_hash, save_statediff)
		.await?;

	self.db
		.shorteventid_shortstatehash
		.aput::<KEY_LEN, VAL_LEN, _, _>(shorteventid, shortstatehash)
		.await?;

	Ok(shortstatehash)
}

/// Generates a new StateHash and associates it with the incoming event.
///
/// This adds all current state events (not including the incoming event)
/// to `stateid_pduid` and adds the incoming event to `eventid_statehash`.
/// The event's short id is allocated here if absent, which is the only
/// allocation of it on the local append path.
#[implement(Service)]
#[tracing::instrument(
	name = "set",
	level = "debug",
	skip(self, new_pdu),
	fields(
		count = ?self.services.globals.pending_count(),
	)
)]
pub async fn append_to_state(&self, new_pdu: &PduEvent) -> Result<u64> {
	const KEY_LEN: usize = size_of::<ShortEventId>();
	const VAL_LEN: usize = size_of::<ShortStateHash>();

	let shorteventid = self
		.services
		.short
		.get_or_create_shorteventid(&new_pdu.event_id)
		.await?;

	let previous_shortstatehash = self
		.get_room_shortstatehash(&new_pdu.room_id)
		.await;

	if let Ok(p) = previous_shortstatehash {
		self.db
			.shorteventid_shortstatehash
			.aput::<KEY_LEN, VAL_LEN, _, _>(shorteventid, p)
			.await?;
	}

	match &new_pdu.state_key {
		| Some(state_key) => {
			let states_parents = match previous_shortstatehash {
				| Ok(p) =>
					self.services
						.state_compressor
						.load_shortstatehash_info(p)
						.await?,
				| _ => Vec::new(),
			};

			let shortstatekey = self
				.services
				.short
				.get_or_create_shortstatekey(&new_pdu.kind.to_string().into(), state_key)
				.await;

			let new = self
				.services
				.state_compressor
				.compress_state_event(shortstatekey, &new_pdu.event_id)
				.await?;

			let replaces = states_parents
				.last()
				.map(|info| {
					info.full_state
						.iter()
						.find(|bytes| bytes.starts_with(&shortstatekey.to_be_bytes()))
				})
				.unwrap_or_default();

			if Some(&new) == replaces {
				return Ok(previous_shortstatehash.expect("must exist"));
			}

			// TODO: statehash with deterministic inputs
			let shortstatehash = self.services.globals.next_count().await?;
			let mut txn = self.services.db.txn();

			let mut statediffnew = CompressedState::new();
			statediffnew.insert(new);

			let mut statediffremoved = CompressedState::new();
			if let Some(replaces) = replaces {
				statediffremoved.insert(*replaces);
			}

			self.services
				.state_compressor
				.save_state_from_diff(
					&mut txn,
					*shortstatehash,
					Arc::new(statediffnew),
					Arc::new(statediffremoved),
					2,
					states_parents,
				)?;

			txn.execute().await?;

			Ok(*shortstatehash)
		},
		| _ => Ok(previous_shortstatehash.expect("first event in room must be a state event")),
	}
}

/// Set the state hash to a new version, but does not update state_cache.
#[implement(Service)]
#[tracing::instrument(skip(self, _mutex_lock), level = "debug")]
pub async fn set_room_state(
	&self,
	room_id: &RoomId,
	shortstatehash: u64,
	// Take mutex guard to make sure users get the room state mutex
	_mutex_lock: &RoomMutexGuard,
) -> Result {
	const BUFSIZE: usize = size_of::<u64>();

	self.db
		.roomid_shortstatehash
		.raw_aput::<BUFSIZE, _, _>(room_id, shortstatehash)
		.await?;

	Ok(())
}

/// Queues `shortstatehash` as the room's current state in `txn`, so it
/// becomes current in the same commit as the writes it accompanies. Like
/// [`Self::set_room_state`], it does not update state_cache.
#[implement(Service)]
pub fn set_room_state_txn(
	&self,
	txn: &mut Txn,
	room_id: &RoomId,
	shortstatehash: ShortStateHash,
	_mutex_lock: &RoomMutexGuard,
) {
	txn.raw_put(&self.db.roomid_shortstatehash, room_id, shortstatehash);
}

/// This fetches auth events from the current state.
#[implement(Service)]
#[expect(clippy::too_many_arguments)]
#[tracing::instrument(skip(self, content), level = "debug")]
pub async fn get_auth_events(
	&self,
	room_id: &RoomId,
	kind: &TimelineEventType,
	sender: &UserId,
	state_key: Option<&str>,
	content: &serde_json::value::RawValue,
	auth_rules: &AuthorizationRules,
	include_create: bool,
) -> Result<StateMap<PduEvent>>
where
	StateEventType: Send + Sync,
	StateKey: Send + Sync,
{
	let shortstatehash = match self.get_room_shortstatehash(room_id).await {
		| Ok(hash) => hash,
		| Err(error) if error.is_not_found() && *kind == TimelineEventType::RoomCreate =>
			return Ok(StateMap::new()),
		| Err(error) => return Err(error),
	};

	let auth_types =
		auth_types_for_event(kind, sender, state_key, content, auth_rules, include_create)?;
	// Derive optional auth cells from the complete immutable snapshot. Forward
	// shortcuts cannot prove absence, and no malformed stored cell may disappear
	// before auth checking or be blamed on the incoming event.
	let mut state = pin!(
		self.services
			.state_accessor
			.state_full_pdus_strict(shortstatehash)
	);
	let mut auth_events = StateMap::new();
	let mut source_bytes = 0_usize;
	while let Some((key, pdu)) = state.try_next().await? {
		source_bytes = source_bytes.saturating_add(
			serialized_len(pdu.as_pdu())
				.map_err(|_| Error::bad_database("Invalid auth state serialization"))?,
		);
		if source_bytes > 512 * 1024 {
			return Err(Error::Request(
				ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
				"Auth state byte limit reached".into(),
				http::StatusCode::TOO_MANY_REQUESTS,
			));
		}
		if pdu.room_id() != room_id {
			return Err(Error::bad_database("Mismatched current-state auth room"));
		}
		if !auth_types.contains(&key) {
			continue;
		}
		if key.0 == StateEventType::RoomPowerLevels {
			pdu.get_content::<RoomPowerLevelsEventContent>()
				.map_err(|_| Error::bad_database("Invalid stored auth power levels"))?;
		}
		if auth_events
			.insert(key, pdu.as_pdu().clone())
			.is_some()
		{
			return Err(Error::bad_database("Duplicate current-state auth key"));
		}
	}
	Ok(auth_events)
}

#[implement(Service)]
#[tracing::instrument(skip_all, level = "debug")]
pub async fn summary_stripped<Pdu: Event>(&self, event: &Pdu) -> Vec<Raw<AnyStrippedStateEvent>> {
	let cells = [
		(&StateEventType::RoomCreate, ""),
		(&StateEventType::RoomJoinRules, ""),
		(&StateEventType::RoomCanonicalAlias, ""),
		(&StateEventType::RoomName, ""),
		(&StateEventType::RoomAvatar, ""),
		(&StateEventType::RoomMember, event.sender().as_str()), // Add recommended events
		(&StateEventType::RoomEncryption, ""),
		(&StateEventType::RoomTopic, ""),
	];

	let fetches = cells.into_iter().map(|(event_type, state_key)| {
		self.services
			.state_accessor
			.room_state_get(event.room_id(), event_type, state_key)
	});

	join_all(fetches)
		.await
		.into_iter()
		.filter_map(Result::ok)
		.map(Event::into_format)
		.chain(once(event.to_format()))
		.collect()
}

/// Like `summary_stripped`, but formats each event as a full federation PDU
/// per the room version's event format (MSC4311). The membership `event` is
/// formatted from its `event_json`; the recommended state cells are fetched
/// from stored room state.
#[implement(Service)]
#[tracing::instrument(skip_all, level = "debug")]
pub async fn summary_pdus<Pdu: Event>(
	&self,
	event: &Pdu,
	event_json: &CanonicalJsonObject,
	room_version: &RoomVersionId,
) -> Vec<Box<RawJsonValue>> {
	let cells = [
		(&StateEventType::RoomCreate, ""),
		(&StateEventType::RoomJoinRules, ""),
		(&StateEventType::RoomCanonicalAlias, ""),
		(&StateEventType::RoomName, ""),
		(&StateEventType::RoomAvatar, ""),
		(&StateEventType::RoomMember, event.sender().as_str()),
		(&StateEventType::RoomEncryption, ""),
		(&StateEventType::RoomTopic, ""),
	];

	let membership = self
		.services
		.federation
		.format_pdu_into(event_json.clone(), Some(room_version))
		.boxed() // query-depth firewall
		.await;

	cells
		.into_iter()
		.stream()
		.wide_filter_map(async |(event_type, state_key)| {
			let pdu = self
				.services
				.state_accessor
				.room_state_get(event.room_id(), event_type, state_key)
				.await
				.ok()?;

			let pdu_json = self
				.services
				.timeline
				.get_pdu_json(pdu.event_id())
				.await
				.ok()?;

			Some(
				self.services
					.federation
					.format_pdu_into(pdu_json, Some(room_version))
					.await,
			)
		})
		.chain(once(membership).stream())
		.collect()
		.await
}

/// Returns the room's version rules
#[implement(Service)]
#[inline]
pub async fn get_room_version_rules(&self, room_id: &RoomId) -> Result<RoomVersionRules> {
	self.get_room_version(room_id)
		.await
		.and_then_ref(room_version::rules)
}

/// Returns the room's version.
#[implement(Service)]
#[tracing::instrument(
	level = "debug"
	skip(self),
	ret(level = "trace"),
)]
pub async fn get_room_version(&self, room_id: &RoomId) -> Result<RoomVersionId> {
	let hash = self.get_room_shortstatehash(room_id).await?;
	let create = self
		.services
		.state_accessor
		.state_get_optional(hash, &StateEventType::RoomCreate, "")
		.await?
		.ok_or_else(|| Error::bad_database("Missing known room create event"))?;
	if create.room_id() != room_id {
		return Err(Error::bad_database("Mismatched room create event"));
	}
	room_version::from_create_event(&create)
		.map_err(|_| Error::bad_database("Invalid room create content"))
}

#[implement(Service)]
#[tracing::instrument(
	level = "debug"
	skip(self),
	ret(level = "trace"),
)]
pub async fn get_room_shortstatehash(&self, room_id: &RoomId) -> Result<ShortStateHash> {
	let value = self.db.roomid_shortstatehash.get(room_id).await?;
	tuwunel_core::utils::bytes::u64_from_bytes(value.as_ref())
		.map_err(|_| Error::bad_database("Invalid room state hash"))
}

/// Returns the state hash at this event.
#[implement(Service)]
pub async fn pdu_shortstatehash(&self, event_id: &EventId) -> Result<ShortStateHash> {
	self.services
		.short
		.get_shorteventid(event_id)
		.and_then(|shorteventid| self.get_shortstatehash(shorteventid))
		.await
}

/// Returns the state hash at this event.
#[implement(Service)]
#[tracing::instrument(
	level = "debug"
	skip(self),
	ret(level = "trace"),
)]
pub async fn get_shortstatehash(&self, shorteventid: ShortEventId) -> Result<ShortStateHash> {
	const BUFSIZE: usize = size_of::<ShortEventId>();
	let value = self
		.db
		.shorteventid_shortstatehash
		.aqry::<BUFSIZE, _>(&shorteventid)
		.await?;
	tuwunel_core::utils::bytes::u64_from_bytes(value.as_ref())
		.map_err(|_| Error::bad_database("Invalid historical state hash"))
}

#[implement(Service)]
pub(super) async fn delete_room_shortstatehash(
	&self,
	room_id: &RoomId,
	_mutex_lock: &Guard<OwnedRoomId, ()>,
) -> Result {
	self.db
		.roomid_shortstatehash
		.remove(room_id)
		.await?;

	Ok(())
}

/// Collapses the room to a single forward extremity, keeping the one furthest
/// along in stream order, and returns the number removed.
#[implement(Service)]
#[tracing::instrument(
	level = "debug"
	skip_all,
	fields(%room_id),
)]
pub async fn collapse_forward_extremities(
	&self,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
) -> usize {
	let extremities: ForwardExtremities = self
		.get_forward_extremities(room_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	if extremities.len() <= 1 {
		return 0;
	}

	let survivor = join_all(extremities.iter().map(async |event_id| {
		self.services
			.timeline
			.get_pdu_count(event_id)
			.await
			.ok()
			.map(|count| (count, event_id))
	}))
	.await
	.into_iter()
	.flatten()
	.max_by_key(|(count, _)| *count)
	.map(|(_, event_id)| event_id);

	let Some(survivor) = survivor else {
		return 0;
	};

	self.set_forward_extremities(room_id, once(&**survivor), state_lock)
		.await;

	extremities.len().saturating_sub(1)
}

#[implement(Service)]
#[tracing::instrument(
	level = "trace"
	skip(self),
)]
pub fn get_forward_extremities<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = &EventId> + Send + '_ {
	let prefix = (room_id, Interfix);

	self.db
		.roomid_pduleaves
		.keys_prefix(&prefix)
		.map_ok(|(_, event_id): (Ignore, &EventId)| event_id)
		.ignore_err()
}

#[implement(Service)]
#[tracing::instrument(
	level = "debug"
	skip_all,
	fields(%room_id),
)]
pub async fn set_forward_extremities<'a, I>(
	&'a self,
	room_id: &'a RoomId,
	event_ids: I,
	_state_lock: &'a RoomMutexGuard,
) where
	I: Iterator<Item = &'a EventId> + Send + 'a,
{
	let prefix = (room_id, Interfix);
	self.db
		.roomid_pduleaves
		.keys_prefix_raw(&prefix)
		.ignore_err()
		.for_each(|key| async move {
			self.db
				.roomid_pduleaves
				.remove(key)
				.await
				.expect("database remove error");
		})
		.await;

	for event_id in event_ids {
		let key = (room_id, event_id);
		self.db
			.roomid_pduleaves
			.put_raw(key, event_id)
			.await
			.expect("database write error");
	}
}

/// Queues `event_ids` as the room's forward extremities into `txn`, so the
/// frontier moves in the same commit as the event that forms it.
///
/// A current leaf not in `event_ids` is deleted, and each of `event_ids` is
/// written once, so no key is both deleted and written in one batch.
#[implement(Service)]
pub async fn set_forward_extremities_txn<'a, I>(
	&'a self,
	txn: &mut Txn,
	room_id: &'a RoomId,
	event_ids: I,
	_state_lock: &'a RoomMutexGuard,
) where
	I: Iterator<Item = &'a EventId> + Send + 'a,
{
	let leaves: Vec<_> = event_ids
		.map(|event_id| {
			let key = serialize_key((room_id, event_id)).expect("failed to serialize leaf key");

			(key, event_id)
		})
		.collect();

	let prefix = (room_id, Interfix);
	let current: Vec<Vec<u8>> = self
		.db
		.roomid_pduleaves
		.keys_prefix_raw(&prefix)
		.ignore_err()
		.map(<[u8]>::to_vec)
		.collect()
		.await;

	for key in current.iter().filter(|key| {
		!leaves
			.iter()
			.any(|(leaf, _)| leaf.as_ref() == key.as_slice())
	}) {
		txn.del_raw(&self.db.roomid_pduleaves, key);
	}

	for (key, event_id) in &leaves {
		txn.insert_raw(&self.db.roomid_pduleaves, key, event_id.as_bytes());
	}
}

#[implement(Service)]
pub(super) async fn delete_all_rooms_forward_extremities(&self, room_id: &RoomId) -> Result {
	let prefix = (room_id, Interfix);

	self.db
		.roomid_pduleaves
		.keys_prefix_raw(&prefix)
		.ignore_err()
		.for_each(|key| async move {
			trace!("Removing key: {key:?}");
			self.db
				.roomid_pduleaves
				.remove(key)
				.await
				.expect("database write error");
		})
		.await;

	Ok(())
}
