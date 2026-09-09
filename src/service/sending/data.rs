use std::{fmt::Debug, sync::Arc};

#[cfg(test)]
mod tests;

use futures::{Stream, StreamExt, TryStreamExt, stream::iter};
use ruma::{OwnedServerName, ServerName, UserId};
use tuwunel_core::{Error, Result, at, utils, utils::ReadyExt};
use tuwunel_database::{Database, Deserialized, Map, Txn};

use super::{
	Destination, EduBuf, SendingEvent, TAG_BADGE_REFRESH, TAG_DEVICE_LIST_CHANGED, TAG_TO_DEVICE,
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

	pub(super) async fn queue_requests<'a, I>(&self, requests: I) -> Result<Vec<Vec<u8>>>
	where
		I: Iterator<Item = (&'a SendingEvent, &'a Destination)> + Clone + Debug + Send,
	{
		let mut keys: Vec<Vec<u8>> = Vec::new();
		for (event, dest) in requests.clone() {
			keys.push(match event {
				| SendingEvent::Pdu(pdu_id) => dest.event_key(pdu_id),
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

	/// Streams queued push destinations with a pending badge refresh.
	///
	/// Returned destinations are owned and may safely cross cursor advances.
	pub(super) fn queued_badge_refresh_destinations(
		&self,
	) -> impl Stream<Item = Result<Destination>> + Send + '_ {
		self.servernameevent_data
			.raw_stream_from(b"$")
			.ready_take_while(|row| within_prefix(row, b"$"))
			.ready_filter_map(decode_badge_destination)
	}

	pub async fn get_latest_educount(&self, server_name: &ServerName) -> Result<u64> {
		missing_count_is_zero(
			self.servername_educount
				.get(server_name)
				.await
				.deserialized(),
		)
	}

	/// Reconstruct unfinished source-window wakes after process replacement.
	pub(super) fn pending_edu_destinations(
		&self,
		retired: u64,
	) -> impl Stream<Item = Result<Destination>> + Send + '_ {
		self.servername_educount
			.stream()
			.map(move |row: Result<(&ServerName, u64)>| {
				let (server, count) = row?;
				if count > retired {
					return Err(Error::bad_database(
						"Outgoing EDU watermark exceeds the retired counter",
					));
				}
				Ok((server, count))
			})
			.try_filter(move |(_, count): &(&ServerName, u64)| {
				futures::future::ready(*count < retired)
			})
			.map_ok(|(server, _)| Destination::Federation(server.to_owned()))
	}
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

fn decode_badge_destination(row: Result<(&[u8], &[u8])>) -> Option<Result<Destination>> {
	match row {
		| Ok((key, value)) if value == [TAG_BADGE_REFRESH] =>
			Some(parse_servercurrentevent(key, value).map(at!(0))),
		| Ok(_) => None,
		| Err(error) => Some(Err(error)),
	}
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
			| [] => SendingEvent::Pdu(event.into()),
			| [TAG_TO_DEVICE, ..] => SendingEvent::ToDevice(value.into()),
			| [TAG_DEVICE_LIST_CHANGED, ..] => SendingEvent::DeviceListChanged(value.into()),
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
			| [] => SendingEvent::Pdu(event.into()),
			| [tag] if *tag == TAG_BADGE_REFRESH => SendingEvent::BadgeRefresh,
			| _ => SendingEvent::Edu(value.into()),
		})
	} else {
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
				SendingEvent::Pdu(event.into())
			} else {
				SendingEvent::Edu(value.into())
			},
		)
	})
}
