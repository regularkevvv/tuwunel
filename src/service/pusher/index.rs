//! Paired notification lookup by room/user/count and by canonical PDU/user.
//! Index rows share the notification commit; erasure removes both directions.

use ruma::{RoomId, UserId};
use tuwunel_core::{Error, Result, implement, matrix::pdu::RawPduId};
use tuwunel_database::{Interfix, Txn, deserialize_from_slice as deserialize_key, serialize_key};

use super::notification::check_mutation;

const SCOPE: u8 = 0;
const EVENT: u8 = 1;

pub(super) fn scope_prefix(room: &RoomId, user: &UserId) -> Result<Vec<u8>> {
	let mut key = vec![SCOPE];
	key.extend(serialize_key((room, user, Interfix))?);
	Ok(key)
}

fn room_prefix(room: &RoomId) -> Result<Vec<u8>> {
	let mut key = vec![SCOPE];
	key.extend(serialize_key((room, Interfix))?);
	Ok(key)
}

pub(super) fn scope_key(room: &RoomId, user: &UserId, count: u64) -> Result<Vec<u8>> {
	let mut key = vec![SCOPE];
	key.extend(serialize_key((room, user, count))?);
	Ok(key)
}

fn event_prefix(raw: &RawPduId) -> Vec<u8> {
	let mut key = vec![EVENT];
	key.extend_from_slice(raw.as_ref());
	key.push(tuwunel_database::SEP);
	key
}

fn event_key(raw: &RawPduId, user: &UserId) -> Vec<u8> {
	let mut key = event_prefix(raw);
	key.extend_from_slice(user.as_bytes());
	key
}

pub(super) fn decode_scope(key: &[u8]) -> Result<(&RoomId, &UserId, u64)> {
	if key.first() != Some(&SCOPE) {
		return Err(Error::bad_database("Invalid notification scope index tag"));
	}
	let (room, user, count): (&RoomId, &UserId, u64) = deserialize_key(&key[1..])?;
	if scope_key(room, user, count)? != key || count == 0 || count > i64::MAX.unsigned_abs() {
		return Err(Error::bad_database("Invalid notification scope index key"));
	}
	Ok((room, user, count))
}

#[implement(super::Service)]
pub(crate) fn stage_notification_index(
	&self,
	txn: &mut Txn,
	raw: RawPduId,
	room: &RoomId,
	user: &UserId,
) -> Result {
	if !matches!(raw, RawPduId::Normal(_))
		|| raw.pdu_count().into_unsigned() == 0
		|| raw.pdu_count().into_unsigned() > i64::MAX.unsigned_abs()
	{
		return Err(Error::bad_database("Invalid notification index PDU count"));
	}
	let scope = scope_key(room, user, raw.pdu_count().into_unsigned())?;
	txn.insert_raw(&self.db.notificationid_index, &scope, raw.as_ref());
	txn.insert_raw(&self.db.notificationid_index, event_key(&raw, user), scope);
	check_mutation(txn)
}

/// A resumed upgrade may revisit a page. Existing directions must agree with
/// the canonical source; migration must not silently repair corrupt bindings.
#[implement(super::Service)]
pub(crate) async fn stage_notification_index_backfill(
	&self,
	txn: &mut Txn,
	raw: RawPduId,
	room: &RoomId,
	user: &UserId,
) -> Result {
	let scope = scope_key(room, user, raw.pdu_count().into_unsigned())?;
	let event = event_key(&raw, user);
	let forward = self.db.notificationid_index.get(&scope).await;
	let reverse = self.db.notificationid_index.get(&event).await;
	match (forward, reverse) {
		| (Ok(forward), Ok(reverse)) => {
			if forward.as_ref() != raw.as_ref() {
				return Err(Error::bad_database(
					"Notification migration forward binding mismatch",
				));
			}
			if reverse.as_ref() != scope.as_slice() {
				return Err(Error::bad_database(
					"Notification migration reverse binding mismatch",
				));
			}
		},
		| (Err(forward), Err(reverse)) => {
			if !forward.is_not_found() {
				return Err(forward);
			}
			if !reverse.is_not_found() {
				return Err(reverse);
			}
		},
		| (Err(error), _) | (_, Err(error)) =>
			return Err(if error.is_not_found() {
				Error::bad_database("Notification migration index direction missing")
			} else {
				error
			}),
	}
	self.stage_notification_index(txn, raw, room, user)
}

/// Validate the reciprocal binding before counting a scoped notification.
#[implement(super::Service)]
pub(super) async fn notification_index_raw(&self, key: &[u8]) -> Result<RawPduId> {
	let (_, user, count) = decode_scope(key)?;
	let value = self.db.notificationid_index.get(key).await?;
	let raw = RawPduId::from_bytes(&value)?;
	if !matches!(raw, RawPduId::Normal(_)) || raw.pdu_count().into_unsigned() != count {
		return Err(Error::bad_database("Notification scope index count mismatch"));
	}
	let reverse = self
		.db
		.notificationid_index
		.get(&event_key(&raw, user))
		.await?;
	if reverse.as_ref() != key {
		return Err(Error::bad_database("Notification index directions disagree"));
	}
	Ok(raw)
}

/// Delete a bounded first page. Successful pages remove their own source rows,
/// so a durable history job needs no second cursor for this derived index.
#[implement(super::Service)]
pub(crate) async fn stage_notification_index_erasure(
	&self,
	txn: &mut Txn,
	raw: &RawPduId,
	room: &RoomId,
	limit: usize,
) -> Result<bool> {
	let short = self.services.short.get_shortroomid(room).await?;
	if raw.shortroomid() != short.to_be_bytes() {
		return Err(Error::bad_database("Notification erasure room binding mismatch"));
	}
	let prefix = event_prefix(raw);
	let keys = self
		.db
		.notificationid_index
		.raw_keys_prefix_after(&prefix, None, limit.saturating_add(1))
		.await?;
	let done = keys.len() <= limit;
	for key in keys.into_iter().take(limit) {
		let encoded = key
			.strip_prefix(prefix.as_slice())
			.ok_or_else(|| Error::bad_database("Notification event index prefix mismatch"))?;
		let user = UserId::parse(
			std::str::from_utf8(encoded)
				.map_err(|_| Error::bad_database("Invalid notification event index user"))?,
		)?;
		let scope = self.db.notificationid_index.get(&key).await?;
		let (owner_room, owner, count) = decode_scope(&scope)?;
		if owner_room != room
			|| owner != user
			|| count != raw.pdu_count().into_unsigned()
			|| self
				.db
				.notificationid_index
				.get(&scope)
				.await?
				.as_ref() != raw.as_ref()
		{
			return Err(Error::bad_database("Notification erasure index directions disagree"));
		}
		self.validate_notification_index_primary(raw, &user)
			.await?;
		txn.del_raw(&self.db.notificationid_index, &key);
		txn.del_raw(&self.db.notificationid_index, &scope);
		txn.del(&self.db.useridcount_notification, (&user, count));
		check_mutation(txn)?;
	}
	Ok(done)
}

/// Room deletion validates both index directions before removing any data.
/// The complete storage transaction still owns its canonical room exclusion.
#[implement(super::Service)]
pub(crate) async fn stage_room_notification_index_erasure(
	&self,
	txn: &mut Txn,
	room: &RoomId,
) -> Result {
	let short = self.services.short.get_shortroomid(room).await?;
	let keys = self
		.db
		.notificationid_index
		.raw_keys_prefix_after(&room_prefix(room)?, None, 302)
		.await?;
	if keys.len() > 301 {
		return super::notification::notification_limit();
	}
	for key in keys {
		let (owner_room, user, count) = decode_scope(&key)?;
		if owner_room != room {
			return Err(Error::bad_database("Room notification index prefix mismatch"));
		}
		let raw = self.notification_index_raw(&key).await?;
		if raw.shortroomid() != short.to_be_bytes() {
			return Err(Error::bad_database("Room notification index PDU binding mismatch"));
		}
		self.validate_notification_index_primary(&raw, user)
			.await?;
		txn.del_raw(&self.db.notificationid_index, &key);
		txn.del_raw(&self.db.notificationid_index, event_key(&raw, user));
		txn.del(&self.db.useridcount_notification, (user, count));
		check_mutation(txn)?;
	}
	Ok(())
}

#[implement(super::Service)]
async fn validate_notification_index_primary(&self, raw: &RawPduId, user: &UserId) -> Result {
	let value = self
		.db
		.useridcount_notification
		.get(&serialize_key((user, raw.pdu_count().into_unsigned()))?)
		.await?;
	let notified = super::append::parse_notified(&value)?;
	if notified.sroomid.to_be_bytes() != raw.shortroomid() {
		return Err(Error::bad_database("Notification erasure metadata binding mismatch"));
	}
	Ok(())
}
