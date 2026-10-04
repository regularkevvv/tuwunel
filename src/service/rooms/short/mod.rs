use std::{borrow::Borrow, sync::Arc};

use futures::{
	Stream, StreamExt, TryFutureExt, TryStreamExt,
	future::{Either, ready},
	pin_mut,
};
use ruma::{
	EventId, OwnedEventId, OwnedRoomId, RoomId,
	api::error::{ErrorKind, LimitExceededErrorData},
	events::StateEventType,
};
use serde::Deserialize;
pub use tuwunel_core::matrix::{ShortEventId, ShortId, ShortRoomId, ShortStateKey};
use tuwunel_core::{
	Err, Error, Result, err, implement,
	matrix::StateKey,
	utils,
	utils::{IterStream, MutexMap, hash::sha256::Digest},
};
use tuwunel_database::{Deserialized, Get, Map, Qry, Txn, serialize_val};

pub struct Service {
	db: Data,
	creating: Creating,
	services: Arc<crate::services::OnceServices>,
}

struct Data {
	eventid_shorteventid: Arc<Map>,
	shorteventid_eventid: Arc<Map>,
	statekey_shortstatekey: Arc<Map>,
	shortstatekey_statekey: Arc<Map>,
	roomid_shortroomid: Arc<Map>,
	statehash_shortstatehash: Arc<Map>,
}

/// Serializes concurrent allocations so one identity maps to one short id.
///
/// A guard is held across both the re-read that detects a competing
/// allocation and the writes that publish this one. Entries exist only while
/// a caller holds or awaits one, so an uncontended allocation leaves nothing
/// behind.
#[derive(Default)]
struct Creating {
	shorteventid: MutexMap<OwnedEventId, ()>,
	shortstatekey: MutexMap<(StateEventType, StateKey), ()>,
	shortstatehash: MutexMap<Digest, ()>,
	shortroomid: MutexMap<OwnedRoomId, ()>,
}

pub type ShortStateHash = ShortId;

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				eventid_shorteventid: args.db["eventid_shorteventid"].clone(),
				shorteventid_eventid: args.db["shorteventid_eventid"].clone(),
				statekey_shortstatekey: args.db["statekey_shortstatekey"].clone(),
				shortstatekey_statekey: args.db["shortstatekey_statekey"].clone(),
				roomid_shortroomid: args.db["roomid_shortroomid"].clone(),
				statehash_shortstatehash: args.db["statehash_shortstatehash"].clone(),
			},
			creating: Creating::default(),
			services: args.services.clone(),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Complete bounded preflight before lazily allocating missing event IDs.
/// Existing mappings and reverse bindings must be valid before any creation.
/// Output stays in input order, including duplicates; allocation follows
/// demand.
#[implement(Service)]
pub fn multi_get_or_create_shorteventid<'a, I>(
	&'a self,
	event_ids: I,
) -> impl Stream<Item = Result<ShortEventId>> + Send + 'a
where
	I: Iterator<Item = &'a EventId> + Clone + Send + 'a,
{
	self.prepare_event_ids(event_ids)
		.map_ok(Vec::into_iter)
		.map_ok(IterStream::try_stream)
		.try_flatten_stream()
		.and_then(move |(event_id, known)| match known {
			| Some(short) => Either::Left(ready(Ok(short))),
			| None => Either::Right(self.create_shorteventid(event_id)),
		})
}

#[implement(Service)]
async fn prepare_event_ids<'a, I>(
	&self,
	event_ids: I,
) -> Result<Vec<(&'a EventId, Option<ShortEventId>)>>
where
	I: Iterator<Item = &'a EventId> + Send,
{
	let mut events = Vec::new();
	let mut bytes = 0_usize;
	for event_id in event_ids {
		bytes = bytes.saturating_add(event_id.as_str().len());
		if events.len() >= 4096 || bytes > 512 * 1024 {
			return Err(short_allocation_limit());
		}
		events.push(event_id);
	}
	let reads = events
		.iter()
		.copied()
		.stream()
		.get(&self.db.eventid_shorteventid);
	pin_mut!(reads);
	let mut prepared = Vec::new();
	for event_id in &events {
		let read = reads
			.next()
			.await
			.ok_or_else(|| Error::bad_database("Incomplete event ID lookup batch"))?;
		let known = match read {
			| Ok(value) => Some(
				utils::bytes::u64_from_bytes(value.as_ref())
					.map_err(|_| Error::bad_database("Invalid compact event ID"))?,
			),
			| Err(error) if error.kind() == ErrorKind::NotFound => None,
			| Err(error) => return Err(error),
		};
		prepared.push((*event_id, known));
	}
	let known: Vec<_> = prepared
		.iter()
		.filter_map(|(event_id, short)| short.map(|short| (*event_id, short)))
		.collect();
	let reverse = known
		.iter()
		.map(|(_, short)| short.to_be_bytes())
		.stream()
		.get(&self.db.shorteventid_eventid);
	pin_mut!(reverse);
	for (event_id, _) in &known {
		let value = reverse
			.next()
			.await
			.ok_or_else(|| Error::bad_database("Incomplete event ID reverse batch"))?
			.map_err(|error| {
				if error.kind() == ErrorKind::NotFound {
					Error::bad_database("Incomplete event ID reverse mapping")
				} else {
					error
				}
			})?;
		if value.as_ref() != event_id.as_bytes() {
			return Err(Error::bad_database("Mismatched event ID reverse mapping"));
		}
	}
	Ok(prepared)
}

#[implement(Service)]
pub async fn get_or_create_shorteventid(&self, event_id: &EventId) -> Result<ShortEventId> {
	match self.existing_event_id(event_id).await? {
		| Some(short) => Ok(short),
		| None => self.create_shorteventid(event_id).await,
	}
}

#[implement(Service)]
async fn existing_event_id(&self, event_id: &EventId) -> Result<Option<ShortEventId>> {
	let short = match self.get_shorteventid(event_id).await {
		| Ok(short) => short,
		| Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
		| Err(error) => return Err(error),
	};
	let reverse = self
		.db
		.shorteventid_eventid
		.get(&short.to_be_bytes())
		.await
		.map_err(|error| {
			if error.kind() == ErrorKind::NotFound {
				Error::bad_database("Incomplete event ID reverse mapping")
			} else {
				error
			}
		})?;
	if reverse.as_ref() != event_id.as_bytes() {
		return Err(Error::bad_database("Mismatched event ID reverse mapping"));
	}
	Ok(Some(short))
}

#[implement(Service)]
async fn create_shorteventid(&self, event_id: &EventId) -> Result<ShortEventId> {
	let _lock = self.creating.shorteventid.lock(event_id).await;
	if let Some(short) = self.existing_event_id(event_id).await? {
		return Ok(short);
	}
	let short = self.services.globals.next_count().await?;
	let mut txn = self.services.db.txn();
	txn.insert_raw(&self.db.shorteventid_eventid, (*short).to_be_bytes(), event_id);
	txn.insert_raw(&self.db.eventid_shorteventid, event_id, (*short).to_be_bytes());
	txn.execute().await?;
	Ok(*short)
}

fn short_allocation_limit() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Compact ID input limit reached".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}

#[implement(Service)]
pub async fn get_shorteventid(&self, event_id: &EventId) -> Result<ShortEventId> {
	let value = self.db.eventid_shorteventid.get(event_id).await?;
	utils::bytes::u64_from_bytes(value.as_ref())
		.map_err(|_| err!(Database("Invalid compact event ID")))
}

#[implement(Service)]
pub async fn get_or_create_shortstatekey(
	&self,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<ShortStateKey> {
	if let Some(short) = self
		.existing_state_key(event_type, state_key)
		.await?
	{
		return Ok(short);
	}

	self.create_shortstatekey(event_type, state_key)
		.await
}

#[implement(Service)]
async fn create_shortstatekey(
	&self,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<ShortStateKey> {
	let owned_key = (event_type.clone(), StateKey::from_str(state_key));
	let _lock = self.creating.shortstatekey.lock(&owned_key).await;

	if let Some(short) = self
		.existing_state_key(event_type, state_key)
		.await?
	{
		return Ok(short);
	}

	let key = (event_type, state_key);
	let shortstatekey = self.services.globals.next_count().await?;
	let mut txn = self.services.db.txn();

	txn.put(&self.db.shortstatekey_statekey, *shortstatekey, key);
	txn.put(&self.db.statekey_shortstatekey, key, *shortstatekey);
	txn.execute().await?;

	Ok(*shortstatekey)
}

#[implement(Service)]
async fn existing_state_key(
	&self,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Option<ShortStateKey>> {
	let short = match self
		.get_shortstatekey(event_type, state_key)
		.await
	{
		| Ok(short) => short,
		| Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
		| Err(error) => return Err(error),
	};
	let reverse = self
		.db
		.shortstatekey_statekey
		.get(&short.to_be_bytes())
		.await
		.map_err(|error| {
			if error.kind() == ErrorKind::NotFound {
				Error::bad_database("Incomplete state key reverse mapping")
			} else {
				error
			}
		})?;
	let expected = serialize_val((event_type, state_key))?;
	if reverse.as_ref() != expected.as_slice() {
		return Err(Error::bad_database("Mismatched state key reverse mapping"));
	}
	Ok(Some(short))
}

#[implement(Service)]
pub async fn get_shortstatekey(
	&self,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<ShortStateKey> {
	if event_type
		.to_cow_str()
		.len()
		.saturating_add(state_key.len())
		.saturating_add(1) // tuple separator in the encoded dictionary key
		> 512 * 1024
	{
		return Err(short_allocation_limit());
	}
	let key = (event_type, state_key);
	let value = self.db.statekey_shortstatekey.qry(&key).await?;
	utils::bytes::u64_from_bytes(value.as_ref())
		.map_err(|_| Error::bad_database("Invalid compact state key"))
}

#[implement(Service)]
pub async fn get_eventid_from_short<Id>(&self, shorteventid: ShortEventId) -> Result<Id>
where
	Id: for<'de> Deserialize<'de> + Send + Sized + ToOwned,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	const BUFSIZE: usize = size_of::<ShortEventId>();

	self.db
		.shorteventid_eventid
		.aqry::<BUFSIZE, _>(&shorteventid)
		.await
		.deserialized()
		.map_err(|e| err!(Database("Failed to find EventId from short {shorteventid:?}: {e:?}")))
}

#[implement(Service)]
pub fn multi_get_eventid_from_short<'a, Id, S>(
	&'a self,
	shorteventid: S,
) -> impl Stream<Item = Result<Id>> + Send + 'a
where
	S: Stream<Item = ShortEventId> + Send + 'a,
	Id: for<'de> Deserialize<'de> + Send + Sized + ToOwned + 'a,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	shorteventid
		.qry(&self.db.shorteventid_eventid)
		.map(Deserialized::deserialized)
}

#[implement(Service)]
pub async fn get_statekey_from_short(
	&self,
	shortstatekey: ShortStateKey,
) -> Result<(StateEventType, StateKey)> {
	const BUFSIZE: usize = size_of::<ShortStateKey>();

	self.db
		.shortstatekey_statekey
		.aqry::<BUFSIZE, _>(&shortstatekey)
		.await
		.deserialized()
		.map_err(|e| {
			err!(Database(
				"Failed to find (StateEventType, state_key) from short {shortstatekey:?}: {e:?}"
			))
		})
}

#[implement(Service)]
pub fn multi_get_statekey_from_short<'a, S>(
	&'a self,
	shortstatekey: S,
) -> impl Stream<Item = Result<(StateEventType, StateKey)>> + Send + 'a
where
	S: Stream<Item = ShortStateKey> + Send + 'a,
{
	shortstatekey
		.qry(&self.db.shortstatekey_statekey)
		.map(Deserialized::deserialized)
}

/// Returns (shortstatehash, already_existed)
#[implement(Service)]
pub async fn get_or_create_shortstatehash<F>(
	&self,
	state_hash: &Digest,
	write_statediff: F,
) -> Result<(ShortStateHash, bool)>
where
	F: FnOnce(&mut Txn, ShortStateHash) -> Result,
{
	match self.get_shortstatehash(state_hash).await {
		| Ok(shortstatehash) => return Ok((shortstatehash, true)),
		| Err(error) if error.kind() == ErrorKind::NotFound => {},
		| Err(error) => return Err(error),
	}

	self.create_shortstatehash(state_hash, write_statediff)
		.await
}

#[implement(Service)]
async fn create_shortstatehash<F>(
	&self,
	state_hash: &Digest,
	write_statediff: F,
) -> Result<(ShortStateHash, bool)>
where
	F: FnOnce(&mut Txn, ShortStateHash) -> Result,
{
	let _lock = self
		.creating
		.shortstatehash
		.lock(state_hash)
		.await;

	match self.get_shortstatehash(state_hash).await {
		| Ok(shortstatehash) => return Ok((shortstatehash, true)),
		| Err(error) if error.kind() == ErrorKind::NotFound => {},
		| Err(error) => return Err(error),
	}

	let shortstatehash = self.services.globals.next_count().await?;
	let mut txn = self.services.db.txn();

	txn.insert_raw(
		&self.db.statehash_shortstatehash,
		state_hash,
		(*shortstatehash).to_be_bytes(),
	);
	write_statediff(&mut txn, *shortstatehash)?;
	txn.execute().await?;

	Ok((*shortstatehash, false))
}

#[implement(Service)]
pub async fn get_shortstatehash(&self, state_hash: &Digest) -> Result<ShortStateHash> {
	let value = self
		.db
		.statehash_shortstatehash
		.get(state_hash)
		.await?;
	let shortstatehash = utils::bytes::u64_from_bytes(value.as_ref())
		.map_err(|_| Error::bad_database("Invalid compact state hash"))?;
	self.existing_state_hash(shortstatehash).await?;
	Ok(shortstatehash)
}

#[implement(Service)]
async fn existing_state_hash(&self, shortstatehash: ShortStateHash) -> Result {
	self.services
		.state_compressor
		.load_shortstatehash_info(shortstatehash)
		.await
		.map(|_| ())
		.map_err(|error| {
			if error.kind() == ErrorKind::NotFound {
				Error::bad_database("Incomplete allocated state hash")
			} else {
				error
			}
		})
}

#[implement(Service)]
pub async fn get_shortroomid(&self, room_id: &RoomId) -> Result<ShortRoomId> {
	let value = self.db.roomid_shortroomid.get(room_id).await?;
	utils::bytes::u64_from_bytes(value.as_ref())
		.map_err(|_| err!(Database("Invalid compact room ID")))
}

#[implement(Service)]
pub async fn get_roomid_from_short(&self, shortroomid: ShortRoomId) -> Result<OwnedRoomId> {
	let stream = self.db.roomid_shortroomid.raw_stream();
	pin_mut!(stream);
	let mut found = None;
	let mut rows = 0_usize;
	let mut bytes = 0_usize;
	while let Some((key, value)) = stream.try_next().await? {
		rows = rows.saturating_add(1);
		bytes = bytes
			.saturating_add(key.len())
			.saturating_add(value.len());
		if rows > 4096 || bytes > 512 * 1024 {
			return Err(short_allocation_limit());
		}
		let room = std::str::from_utf8(key)
			.ok()
			.and_then(|room| RoomId::parse(room).ok())
			.ok_or_else(|| Error::bad_database("Invalid compact room mapping key"))?;
		let short = utils::bytes::u64_from_bytes(value)
			.map_err(|_| Error::bad_database("Invalid compact room mapping value"))?;
		if short == shortroomid && found.replace(room).is_some() {
			return Err(Error::bad_database("Duplicate compact room mapping"));
		}
	}
	found.ok_or_else(|| Error::bad_database("Missing compact room mapping"))
}

#[implement(Service)]
pub async fn get_or_create_shortroomid(&self, room_id: &RoomId) -> Result<ShortRoomId> {
	match self.get_shortroomid(room_id).await {
		| Ok(shortroomid) => return Ok(shortroomid),
		| Err(error) if error.kind() == ErrorKind::NotFound => {},
		| Err(error) => return Err(error),
	}

	self.create_shortroomid(room_id).await
}

#[implement(Service)]
async fn create_shortroomid(&self, room_id: &RoomId) -> Result<ShortRoomId> {
	const BUFSIZE: usize = size_of::<ShortRoomId>();

	let _lock = self.creating.shortroomid.lock(room_id).await;

	match self.get_shortroomid(room_id).await {
		| Ok(shortroomid) => return Ok(shortroomid),
		| Err(error) if error.kind() == ErrorKind::NotFound => {},
		| Err(error) => return Err(error),
	}

	let short = self.services.globals.next_count().await?;

	debug_assert!(size_of_val(&*short) == BUFSIZE, "buffer requirement changed");

	self.db
		.roomid_shortroomid
		.raw_aput::<BUFSIZE, _, _>(room_id, *short)
		.await?;

	Ok(*short)
}

#[implement(Service)]
pub async fn delete_shortroomid(&self, room_id: &RoomId) -> Result {
	if self
		.db
		.roomid_shortroomid
		.exists(room_id)
		.await
		.is_ok()
	{
		self.db.roomid_shortroomid.remove(room_id).await?;
		Ok(())
	} else {
		Err!(Database("not found"))
	}
}
