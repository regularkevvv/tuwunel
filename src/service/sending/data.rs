use std::{fmt::Debug, sync::Arc};

#[cfg(test)]
mod tests;

use futures::{Stream, StreamExt, TryStreamExt, stream::iter};
use ruma::{OwnedServerName, ServerName, UserId};
use tuwunel_core::{Error, Result, at, utils, utils::ReadyExt};
use tuwunel_database::{Database, Deserialized, Map, Row, Txn, deserialize_from_slice};

use super::{
	Destination, EduBuf, SendingEvent, TAG_BADGE_REFRESH, TAG_DEVICE_LIST_CHANGED,
	TAG_FROZEN_PUSH, TAG_TO_DEVICE,
};

pub(super) type OutgoingItem = (Key, SendingEvent, Destination);
pub(super) type SendingItem = (Key, SendingEvent);
pub(super) type QueueItem = (Key, SendingEvent);
pub(super) type Key = Vec<u8>;

pub struct Data {
	servercurrentevent_data: Arc<Map>,
	servernameevent_data: Arc<Map>,
	servername_educount: Arc<Map>,
	pub(super) db: Arc<Database>,
	services: Arc<crate::services::OnceServices>,
}

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		Self {
			servercurrentevent_data: db["servercurrentevent_data"].clone(),
			servernameevent_data: db["servernameevent_data"].clone(),
			servername_educount: db["servername_educount"].clone(),
			db: args.db.clone(),
			services: args.services.clone(),
		}
	}

	#[inline]
	pub(super) async fn delete_active_request(&self, key: &[u8]) -> Result {
		self.servercurrentevent_data.remove(key).await
	}

	pub(super) async fn delete_all_active_requests_for(
		&self,
		destination: &Destination,
	) -> Result {
		let prefix = destination.get_prefix();
		self.servercurrentevent_data
			.raw_keys_prefix(&prefix)
			.try_for_each(|key| async move { self.servercurrentevent_data.remove(key).await })
			.await
	}

	pub(super) async fn delete_all_requests_for(&self, destination: &Destination) -> Result {
		let prefix = destination.get_prefix();
		self.servercurrentevent_data
			.raw_keys_prefix(&prefix)
			.try_for_each(|key| async move { self.servercurrentevent_data.remove(key).await })
			.await?;

		self.servernameevent_data
			.raw_keys_prefix(&prefix)
			.try_for_each(|key| async move { self.servernameevent_data.remove(key).await })
			.await
	}

	pub(super) async fn mark_as_active<'a, I>(&self, events: I) -> Result
	where
		I: Iterator<Item = &'a QueueItem>,
	{
		events
			.filter(|(key, _)| !key.is_empty())
			.fold(self.db.txn(), |mut txn, (key, val)| {
				txn.insert_raw(&self.servercurrentevent_data, key, val.value_bytes());
				txn.del_raw(&self.servernameevent_data, key);
				txn
			})
			.execute()
			.await
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
			let count = self.services.globals.next_count().await?;
			key.extend(&count.to_be_bytes());

			txn.insert_raw(map, key, edu.as_slice());
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

	/// At most `limit` in-flight requests after the key `after`, or from the
	/// first, and the key to pass next, `None` once the queue ends. The read
	/// is closed before this returns, so the caller may delete what it got.
	pub(super) async fn active_requests_after(
		&self,
		after: Option<&[u8]>,
		limit: usize,
	) -> Result<(Vec<OutgoingItem>, Option<Key>)> {
		let rows = self
			.servercurrentevent_data
			.raw_rows_after(after, limit)
			.await?;

		let next = next_cursor(&rows, limit);
		let items = rows
			.iter()
			.map(|(key, value)| decode_outgoing(Ok((key.as_slice(), value.as_slice()))))
			.collect::<Result<_>>()?;

		Ok((items, next))
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
			.map(decode_sending)
	}

	pub(super) fn stage_request(&self, txn: &mut Txn, key: &[u8], event: &SendingEvent) {
		txn.insert_raw(&self.servernameevent_data, key, event.value_bytes());
	}

	pub(super) async fn queue_requests<'a, I>(&self, requests: I) -> Result<Vec<Vec<u8>>>
	where
		I: Iterator<Item = (&'a SendingEvent, &'a Destination)> + Clone + Debug + Send,
	{
		let mut keys: Vec<Vec<u8>> = Vec::new();
		for (event, dest) in requests.clone() {
			keys.push(match event {
				| SendingEvent::Pdu(pdu_id) | SendingEvent::FrozenPush(pdu_id) =>
					dest.event_key(pdu_id),
				| _ => {
					let count = self.services.globals.next_count().await?;
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
			let (_, _, destination) = decode_outgoing(Ok((key, &value)))?;
			if !matches!(&destination, Destination::Push(owner, _) if owner == user) {
				return Err(Error::bad_database("Push wake owner mismatch"));
			}
			destinations.insert(destination);
		}
		let next = (keys.len() == 64).then(|| keys.last().expect("full page").clone());
		Ok((destinations.into_iter().collect(), next))
	}

	/// Destinations owning durable pending rows, among at most `limit` rows
	/// after `after`. Validate every row before returning the page. The read
	/// closes here, before startup promotes any row for delivery.
	pub(super) async fn queued_destinations_after(
		&self,
		after: Option<&[u8]>,
		limit: usize,
	) -> Result<(Vec<Destination>, Option<Key>)> {
		let rows = self
			.servernameevent_data
			.raw_rows_after(after, limit)
			.await?;

		let next = next_cursor(&rows, limit);
		let destinations = rows
			.iter()
			.map(|(key, value)| parse_servercurrentevent(key, value).map(at!(0)))
			.collect::<Result<_>>()?;

		Ok((destinations, next))
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
	decode_outgoing(row).map(|(key, event, _)| (key, event))
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

pub(super) fn parse_servercurrentevent(
	key: &[u8],
	value: &[u8],
) -> Result<(Destination, SendingEvent)> {
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
			| [TAG_FROZEN_PUSH, ..] =>
				return Err(Error::bad_database("Frozen push has a non-push destination")),
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
			| [TAG_FROZEN_PUSH, ..] =>
				return Err(Error::bad_database("Invalid frozen push queue tag")),
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
