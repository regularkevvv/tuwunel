use std::{fmt::Debug, sync::Arc};

#[cfg(test)]
mod tests;

use futures::{Stream, StreamExt, TryStreamExt, stream::iter};
use ruma::{OwnedServerName, ServerName, UserId};
use tokio::sync::Mutex;
use tuwunel_core::{Error, Result, at, err, utils, utils::ReadyExt};
use tuwunel_database::{Database, Deserialized, Get, Map, Txn};
#[cfg(test)]
use tuwunel_database::{Row, deserialize_from_slice};

mod ack;
mod active;
mod attempt;
mod backoff;
mod discovery;
pub(super) use ack::ActiveAcknowledgement;
pub(super) use attempt::{BODY_LIMIT, PreparedAttempt, appservice_owner};
pub(super) use backoff::PushBackoff;
pub(super) use discovery::{DISCOVERY_PAGE_LIMIT, RecoverySource};

use super::{
	Destination, EduBuf, SendingEvent, TAG_BADGE_REFRESH, TAG_DEVICE_LIST_CHANGED,
	TAG_FROZEN_PUSH, TAG_TO_DEVICE,
};

pub(super) type OutgoingItem = (Key, SendingEvent, Destination);
pub(super) type SendingItem = (Key, SendingEvent);
pub(super) type QueueItem = (Key, SendingEvent);
pub(super) type Key = Vec<u8>;
pub(super) const ACTIVE_PROMOTION_LIMIT: usize = 48;

pub struct Data {
	servercurrentevent_data: Arc<Map>,
	servernameevent_data: Arc<Map>,
	servername_educount: Arc<Map>,
	sendingtransaction_record: Arc<Map>,
	pub(super) db: Arc<Database>,
	services: Arc<crate::services::OnceServices>,
	active_write: Mutex<()>,
}

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		Self {
			servercurrentevent_data: db["servercurrentevent_data"].clone(),
			servernameevent_data: db["servernameevent_data"].clone(),
			servername_educount: db["servername_educount"].clone(),
			sendingtransaction_record: db["sendingtransaction_record"].clone(),
			db: args.db.clone(),
			services: args.services.clone(),
			active_write: Mutex::new(()),
		}
	}

	#[inline]
	pub(super) async fn delete_active_request(&self, key: &[u8]) -> Result {
		let _guard = self.active_write.lock().await;
		self.servercurrentevent_data.remove(key).await
	}

	pub(super) async fn delete_all_requests_for(&self, destination: &Destination) -> Result {
		let _guard = self.active_write.lock().await;
		self.cancel_attempt_and_requests(destination)
			.await
	}

	pub(super) async fn mark_as_active<'a, I>(&self, events: I) -> Result
	where
		I: Iterator<Item = &'a QueueItem> + Send,
	{
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let _guard = self.active_write.lock().await;
		let events = events
			.filter(|(key, _)| !key.is_empty())
			.take(ACTIVE_PROMOTION_LIMIT + 1)
			.collect::<Vec<_>>();
		if events.is_empty() {
			return Ok(());
		}
		if events.len() > ACTIVE_PROMOTION_LIMIT {
			return Err(Error::bad_database("Active promotion exceeds the batch limit"));
		}
		// Validate the entire proposed promotion before consuming an identity or
		// staging a mutation. Native storage must share the provider's limits.
		for (key, event) in &events {
			if key.len() > tuwunel_bridge::MAX_KEY_BYTES {
				return Err(Error::bad_database("Outgoing promotion key exceeds storage limit"));
			}
			active::validate_payload(event.value_bytes())?;
		}
		self.require_active_schema().await?;
		let existing = iter(events.iter().map(|(key, _)| key.as_slice()))
			.get(&self.servercurrentevent_data)
			.map(|value| value.map(|value| value.as_ref().to_vec()))
			.collect::<Vec<_>>()
			.await;
		if existing.len() != events.len() {
			return Err(existing
				.into_iter()
				.find_map(|value| value.err().filter(|error| !error.is_not_found()))
				.unwrap_or_else(|| Error::bad_database("Incomplete active promotion lookup")));
		}
		let mut txn = self.db.txn();
		let mut batch_identity = None;
		for ((key, event), existing) in events.into_iter().zip(existing) {
			match existing {
				| Ok(value) => {
					let (_, existing) = parse_servercurrentevent(key, &value)?;
					if &existing != event {
						return Err(Error::bad_database(
							"Promotion would replace an active delivery",
						));
					}
					// An uncertain promotion may already be durable. Keep its
					// identity and any independently re-admitted pending row.
					continue;
				},
				| Err(error) if error.is_not_found() => {},
				| Err(error) => return Err(error),
			}
			// Dispatch hints can outlive a completed destination cancellation.
			// Only a matching durable pending admission may become active.
			let pending = match self.servernameevent_data.get(key).await {
				| Ok(value) => value.as_ref().to_vec(),
				| Err(error) if error.is_not_found() => continue,
				| Err(error) => return Err(error),
			};
			let (_, admitted, _) = decode_queued(Ok((key.as_slice(), &pending)))?;
			if &admitted != event {
				return Err(Error::bad_database(
					"Dispatch hint differs from its durable admission",
				));
			}
			let identity = if let Some(identity) = batch_identity {
				identity
			} else {
				// A key scopes its incarnation. All new members in this
				// atomic promotion can share one persisted counter identity.
				let identity = *services_root.globals.next_count().await?;
				batch_identity = Some(identity);
				identity
			};
			let value = active::encode(event, identity)?;
			txn.insert_raw(&self.servercurrentevent_data, key, value);
			txn.del_raw(&self.servernameevent_data, key);
		}
		txn.execute().await
	}

	/// Persist the selected/overflow EDUs and their consumed source watermark
	/// together. A rejected or uncertain commit cannot acknowledge just the
	/// cursor.
	pub(super) async fn persist_edus(
		&self,
		server: &ServerName,
		active: &[EduBuf],
		queued: &[EduBuf],
		last_count: u64,
	) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let _guard = self.active_write.lock().await;
		self.require_active_schema().await?;
		let prefix = Destination::Federation(server.to_owned()).get_prefix();

		let mut txn = self.db.txn();
		let rows = active
			.iter()
			.map(|edu| (&self.servercurrentevent_data, edu))
			.chain(
				queued
					.iter()
					.map(|edu| (&self.servernameevent_data, edu)),
			);
		for (map, edu) in rows {
			let mut key = prefix.clone();
			// The permit retires at the end of this iteration, before the
			// batch executes; EDU counts never gate reader visibility.
			let count = services_root.globals.next_count().await?;
			key.extend(&count.to_be_bytes());

			if Arc::ptr_eq(map, &self.servercurrentevent_data) {
				let value = active::encode(&SendingEvent::Edu(edu.clone()), *count)?;
				txn.insert_raw(map, key, value);
			} else {
				txn.insert_raw(map, key, edu.as_slice());
			}
		}
		txn.raw_put(&self.servername_educount, server, last_count);

		txn.execute().await
	}

	#[inline]
	pub fn active_requests(&self) -> impl Stream<Item = Result<OutgoingItem>> + Send + '_ {
		self.servercurrentevent_data
			.raw_stream()
			.map(decode_outgoing)
	}

	#[inline]
	pub fn active_requests_for(
		&self,
		destination: &Destination,
	) -> impl Stream<Item = Result<SendingItem>> + Send + '_ + use<'_> {
		let prefix = destination.get_prefix();
		self.servercurrentevent_data
			.raw_stream_from(&prefix)
			.ready_take_while(move |row| within_prefix(row, &prefix))
			.map(|row| decode_outgoing(row).map(|(key, event, _)| (key, event)))
	}

	pub(super) fn stage_request(&self, txn: &mut Txn, key: &[u8], event: &SendingEvent) {
		txn.insert_raw(&self.servernameevent_data, key, event.value_bytes());
	}

	/// Bound fanout while destinations are still borrowed and share one event.
	/// The first refusal stops the source before collecting the rest of it.
	pub(super) async fn federation_destinations<'a, S>(
		&self,
		servers: S,
		event: &SendingEvent,
	) -> Result<Vec<Destination>>
	where
		S: Stream<Item = &'a ServerName> + Send + 'a,
	{
		let mut servers = std::pin::pin!(servers);
		let mut next = servers.next().await;
		if next.is_none() {
			return Ok(Vec::new());
		}
		active::validate_payload(event.value_bytes())?;
		let mut budget = self.servernameevent_data.put_batch_budget()?;
		let mut destinations = Vec::new();
		while let Some(server) = next {
			let prefix_len = server.as_bytes().len().saturating_add(1);
			let key_len = queue_key_len(event, prefix_len, false)?;
			budget
				.try_put(key_len, event.value_bytes())
				.map_err(|error| admission_error(&error))?;
			destinations.push(Destination::Federation(server.to_owned()));
			next = servers.next().await;
		}
		Ok(destinations)
	}

	pub(super) async fn queue_requests<'a, I>(&self, requests: I) -> Result<Vec<Vec<u8>>>
	where
		I: Iterator<Item = (&'a SendingEvent, &'a Destination)> + Clone + Debug + Send,
	{
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let _guard = self.active_write.lock().await;
		if requests.clone().next().is_none() {
			return Ok(Vec::new());
		}
		let mut budget = self.servernameevent_data.put_batch_budget()?;
		for (event, destination) in requests.clone() {
			active::validate_payload(event.value_bytes())?;
			let key_len = queue_key_len(
				event,
				destination.prefix_len(),
				matches!(destination, Destination::Push(..)),
			)?;
			budget
				.try_put(key_len, event.value_bytes())
				.map_err(|error| admission_error(&error))?;
		}
		for (_, destination) in requests.clone() {
			self.resume_cancellation(destination).await?;
		}
		let mut keys: Vec<Vec<u8>> = Vec::new();
		for (event, dest) in requests.clone() {
			keys.push(match event {
				| SendingEvent::Pdu(pdu_id) | SendingEvent::FrozenPush(pdu_id) =>
					dest.event_key(pdu_id),
				| _ => {
					let count = services_root.globals.next_count().await?;
					let count = count.to_be_bytes();
					let mut key = dest.get_prefix_with_capacity(count.len());

					key.extend_from_slice(&count);

					key
				},
			});
		}

		let items = keys
			.iter()
			.map(Vec::as_slice)
			.zip(requests.map(at!(0)))
			.map(|(key, event)| (key, event.value_bytes()));

		Txn::insert(&self.servernameevent_data, items)
			.execute()
			.await?;

		Ok(keys)
	}

	/// Yields only pending queue items.
	///
	/// Empty-key payload wakes always pass because they have no durable row. A
	/// wake can outlive its row after a completed drain delivered the event.
	pub(super) fn retain_queued<'a, I>(
		&'a self,
		events: I,
	) -> impl Stream<Item = Result<QueueItem>> + Send + 'a
	where
		I: IntoIterator<Item = QueueItem> + Send + 'a,
		I::IntoIter: Send,
	{
		iter(events).filter_map(async |item| {
			let key = &item.0;
			if key.is_empty() {
				return Some(Ok(item));
			}

			let exists = self.servernameevent_data.exists(key).await;
			retain_existing(item, exists)
		})
	}

	pub fn queued_requests(
		&self,
		destination: &Destination,
	) -> impl Stream<Item = Result<QueueItem>> + Send + '_ + use<'_> {
		let prefix = destination.get_prefix();
		self.servernameevent_data
			.raw_stream_from(&prefix)
			.ready_take_while(move |row| within_prefix(row, &prefix))
			.map(decode_sending)
	}

	/// Copy only a promotable prefix, reserving active-envelope bytes before
	/// decoding payloads. The borrowed scan closes before promotion may write.
	pub(super) async fn queued_batch(&self, destination: &Destination) -> Result<Vec<QueueItem>> {
		const KEY_BYTES: usize = 128 * 1024;
		if destination.prefix_len() > tuwunel_bridge::MAX_KEY_BYTES {
			return Err(Error::bad_database("Outgoing queue prefix exceeds storage limit"));
		}
		let prefix = destination.get_prefix();
		let mut queued = self
			.servernameevent_data
			.raw_stream_from(&prefix)
			.ready_take_while(move |row| within_prefix(row, &prefix))
			.take(ACTIVE_PROMOTION_LIMIT)
			.boxed();
		let mut items = Vec::new();
		let mut key_bytes = 0_usize;
		let mut value_bytes = 0_usize;
		while let Some((key, value)) = queued.try_next().await? {
			if key.len() > tuwunel_bridge::MAX_KEY_BYTES {
				return Err(Error::bad_database("Outgoing queued key exceeds storage limit"));
			}
			if active::identity(value)?.is_some() {
				return Err(Error::bad_database("Active identity envelope in the pending queue"));
			}
			if let Err(error) = active::validate_payload(value) {
				if items.is_empty() {
					return Err(error);
				}
				break;
			}
			let next_keys = key_bytes.saturating_add(key.len());
			let next_values = value_bytes
				.saturating_add(value.len())
				.saturating_add(active::HEADER);
			if next_keys > KEY_BYTES || next_values > BODY_LIMIT {
				break;
			}
			let (owned_key, event, owner) = decode_queued(Ok((key, value)))?;
			if &owner != destination {
				return Err(Error::bad_database("Outgoing queued destination mismatch"));
			}
			items.push((owned_key, event));
			key_bytes = next_keys;
			value_bytes = next_values;
		}
		Ok(items)
	}

	/// A bounded, checked page of a user's owed push destinations. This only
	/// schedules wakes; the sender and durable rows retain delivery ownership.
	pub(super) async fn push_destinations_for_user_after(
		&self,
		user: &UserId,
		active: bool,
		after: Option<&[u8]>,
	) -> Result<(Vec<Destination>, Option<Key>)> {
		let mut prefix = Vec::from(b"$");
		prefix.extend_from_slice(user.as_bytes());
		prefix.push(0xFF);
		let map = if active {
			&self.servercurrentevent_data
		} else {
			&self.servernameevent_data
		};
		let keys = map
			.raw_keys_prefix_after(&prefix, after, 64)
			.await?;
		let mut destinations = std::collections::BTreeSet::new();
		let mut bytes = 0_usize;
		for key in &keys {
			if key.len() > super::wakes::MAX_CURSOR_BYTES {
				return Err(Error::bad_database("Push wake cursor exceeds limit"));
			}
			let value = map.get(key).await?;
			bytes = bytes
				.saturating_add(key.len())
				.saturating_add(value.len());
			if bytes > 4 * 1024 * 1024 {
				return Err(Error::bad_database("Push wake page exceeds limit"));
			}
			let (_, _, destination) = if active {
				decode_outgoing(Ok((key, &value)))?
			} else {
				decode_queued(Ok((key, &value)))?
			};
			if !matches!(&destination, Destination::Push(owner, _) if owner == user) {
				return Err(Error::bad_database("Push wake owner mismatch"));
			}
			destinations.insert(destination);
		}
		let next = (keys.len() == 64).then(|| keys.last().expect("full page").clone());
		Ok((destinations.into_iter().collect(), next))
	}

	async fn require_active_schema(&self) -> Result {
		let version: u64 = self.db["global"]
			.get(b"version")
			.await
			.deserialized()?;
		if version != crate::migrations::DATABASE_VERSION {
			return Err(Error::bad_database(
				"Active delivery identities require the current schema",
			));
		}
		Ok(())
	}

	pub async fn get_latest_educount(&self, server_name: &ServerName) -> Result<u64> {
		missing_count_is_zero(
			self.servername_educount
				.get(server_name)
				.await
				.deserialized(),
		)
	}

	/// Reconstruct unfinished source-window wakes after process replacement:
	/// the destinations among at most `limit` EDU watermarks after the key
	/// `after`, and the key to pass next, `None` once the watermarks end. The
	/// read is closed before this returns.
	#[cfg(test)]
	pub(super) async fn pending_edu_destinations(
		&self,
		retired: u64,
		after: Option<&[u8]>,
		limit: usize,
	) -> Result<(Vec<Destination>, Option<Key>)> {
		let rows = self
			.servername_educount
			.raw_rows_after(after, limit)
			.await?;

		let next = next_cursor(&rows, limit);
		let mut destinations = Vec::new();
		for (key, value) in &rows {
			let server: &ServerName = deserialize_from_slice(key)?;
			let count: u64 = deserialize_from_slice(value)?;
			if count > retired {
				return Err(Error::bad_database(
					"Outgoing EDU watermark exceeds the retired counter",
				));
			}
			if count < retired {
				destinations.push(Destination::Federation(server.to_owned()));
			}
		}

		Ok((destinations, next))
	}
}

/// The key a batch of at most `limit` rows resumes after, `None` when the
/// batch was short and so reached the end of what it reads.
#[cfg(test)]
fn next_cursor(rows: &[Row], limit: usize) -> Option<Key> {
	rows.last()
		.filter(|_| rows.len() >= limit)
		.map(|(key, _)| key.clone())
}

// A scan error is an item, never an end-of-prefix marker or an absent row.
fn within_prefix(row: &Result<(&[u8], &[u8])>, prefix: &[u8]) -> bool {
	match row {
		| Ok((key, _)) => key.starts_with(prefix),
		| Err(_) => true,
	}
}

fn decode_outgoing(row: Result<(&[u8], &[u8])>) -> Result<OutgoingItem> {
	let (key, value) = row?;
	let (destination, event) = parse_servercurrentevent(key, value)?;
	Ok((key.to_vec(), event, destination))
}

fn decode_sending(row: Result<(&[u8], &[u8])>) -> Result<SendingItem> {
	decode_queued(row).map(|(key, event, _)| (key, event))
}

fn decode_queued(row: Result<(&[u8], &[u8])>) -> Result<OutgoingItem> {
	let (key, value) = row?;
	if active::identity(value)?.is_some() {
		return Err(Error::bad_database("Active identity envelope in the pending queue"));
	}
	decode_outgoing(Ok((key, value)))
}

fn queue_key_len(event: &SendingEvent, prefix_len: usize, push: bool) -> Result<usize> {
	let suffix = match event {
		| SendingEvent::Pdu(id) | SendingEvent::FrozenPush(id) => id.as_ref().len(),
		| _ => size_of::<u64>(),
	};
	// Non-push destinations also need room for the cancellation marker.
	let required_suffix = if push { suffix } else { suffix.max(17) };
	if prefix_len.saturating_add(required_suffix) > tuwunel_bridge::MAX_KEY_BYTES {
		return Err(err!(Request(TooLarge("Outgoing destination exceeds storage key width"))));
	}
	Ok(prefix_len.saturating_add(suffix))
}

fn admission_error(error: &tuwunel_bridge::Error) -> Error {
	err!(Request(TooLarge("Outgoing atomic batch: {error}")))
}

fn retain_existing(item: QueueItem, exists: Result) -> Option<Result<QueueItem>> {
	match exists {
		| Ok(()) => Some(Ok(item)),
		| Err(error) if error.is_not_found() => None,
		| Err(error) => Some(Err(error)),
	}
}

fn missing_count_is_zero(count: Result<u64>) -> Result<u64> {
	match count {
		| Err(error) if error.is_not_found() => Ok(0),
		| result => result,
	}
}

pub(crate) fn parse_servercurrentevent(
	key: &[u8],
	value: &[u8],
) -> Result<(Destination, SendingEvent)> {
	let value = active::payload(value)?;
	// Appservices start with a plus
	Ok::<_, Error>(if key.starts_with(b"+") {
		let mut parts = key[1..].splitn(2, |&b| b == 0xFF);

		let server = parts
			.next()
			.expect("splitn always returns one element");
		let event = parts
			.next()
			.ok_or_else(|| Error::bad_database("Invalid bytes in servercurrentpdus."))?;

		let server = utils::string_from_bytes(server).map_err(|_| {
			Error::bad_database("Invalid server bytes in server_currenttransaction")
		})?;

		let decoded = match value {
			| [] => SendingEvent::Pdu(super::RawPduId::from_bytes(event)?),
			| [TAG_TO_DEVICE, ..] => SendingEvent::ToDevice(value.into()),
			| [TAG_DEVICE_LIST_CHANGED, ..] => SendingEvent::DeviceListChanged(value.into()),
			| [TAG_FROZEN_PUSH, ..] => {
				return Err(Error::bad_database("Frozen push has a non-push destination"));
			},
			| _ => SendingEvent::Edu(value.into()),
		};

		(Destination::Appservice(server), decoded)
	} else if key.starts_with(b"$") {
		let mut parts = key[1..].splitn(3, |&b| b == 0xFF);

		let user = parts
			.next()
			.expect("splitn always returns one element");
		let user_string = utils::str_from_bytes(user)
			.map_err(|_| Error::bad_database("Invalid user string in servercurrentevent"))?;
		let user_id = UserId::parse(user_string)
			.map_err(|_| Error::bad_database("Invalid user id in servercurrentevent"))?;

		let pushkey = parts
			.next()
			.ok_or_else(|| Error::bad_database("Invalid bytes in servercurrentpdus."))?;
		let pushkey_string = utils::string_from_bytes(pushkey)
			.map_err(|_| Error::bad_database("Invalid pushkey in servercurrentevent"))?;

		let event = parts
			.next()
			.ok_or_else(|| Error::bad_database("Invalid bytes in servercurrentpdus."))?;

		(Destination::Push(user_id, pushkey_string), match value {
			| [] => SendingEvent::Pdu(super::RawPduId::from_bytes(event)?),
			| [tag] if *tag == TAG_BADGE_REFRESH => SendingEvent::BadgeRefresh,
			| [TAG_FROZEN_PUSH] => SendingEvent::FrozenPush(super::RawPduId::from_bytes(event)?),
			| [TAG_FROZEN_PUSH, ..] => {
				return Err(Error::bad_database("Invalid frozen push queue tag"));
			},
			| _ => SendingEvent::Edu(value.into()),
		})
	} else {
		if value.first() == Some(&TAG_FROZEN_PUSH) {
			return Err(Error::bad_database("Frozen push has a non-push destination"));
		}
		let mut parts = key.splitn(2, |&b| b == 0xFF);

		let server = parts
			.next()
			.expect("splitn always returns one element");
		let event = parts
			.next()
			.ok_or_else(|| Error::bad_database("Invalid bytes in servercurrentpdus."))?;

		let server = utils::string_from_bytes(server).map_err(|_| {
			Error::bad_database("Invalid server bytes in server_currenttransaction")
		})?;

		(
			Destination::Federation(OwnedServerName::parse(&server).map_err(|_| {
				Error::bad_database("Invalid server string in server_currenttransaction")
			})?),
			if value.is_empty() {
				SendingEvent::Pdu(super::RawPduId::from_bytes(event)?)
			} else {
				SendingEvent::Edu(value.into())
			},
		)
	})
}
