use std::{collections::BTreeMap, sync::Arc};

use futures::{StreamExt, TryStreamExt, pin_mut, stream::select};
use ruma::{
	EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
	events::receipt::ReceiptThread,
};
use tuwunel_core::{
	Error, Result, implement, trace,
	utils::{
		stream::{BroadbandExt, ReadyExt, TryIgnore},
		u64_from_u8,
	},
};
use tuwunel_database::{
	Deserialized, Ignore, IgnoreAll, Interfix, KeyBuf, Map, Txn,
	deserialize_from_slice as deserialize_key,
};

/// Per-thread unread counts: `(notification, highlight)` keyed by thread root.
type ThreadCounts = BTreeMap<OwnedEventId, (u64, u64)>;

/// Per-thread last-read counts keyed by thread root. Used by sync v3 to
/// gate emission of `unread_thread_notifications` to threads whose read
/// cursor advanced within the sync window.
type ThreadLastReads = BTreeMap<OwnedEventId, u64>;

pub(crate) struct NotificationGuard {
	user: OwnedUserId,
	room: OwnedRoomId,
	_guard: tuwunel_core::utils::mutex_map::Guard<(OwnedRoomId, OwnedUserId), ()>,
	_user_guard: tuwunel_core::utils::mutex_map::Guard<OwnedUserId, ()>,
}

#[implement(super::Service)]
pub(crate) async fn lock_notification(&self, user: &UserId, room: &RoomId) -> NotificationGuard {
	let user_guard = self.lock_notification_user(user).await;
	NotificationGuard {
		_user_guard: user_guard,
		user: user.to_owned(),
		room: room.to_owned(),
		_guard: self
			.notification_mutex
			.lock(&(room.to_owned(), user.to_owned()))
			.await,
	}
}

/// Reset the main count pair and its sync stamp in one transaction.
#[implement(super::Service)]
pub async fn reset_notification_counts(&self, user: &UserId, room: &RoomId) -> Result {
	self.reset_notification_counts_for_thread(user, room, &ReceiptThread::Main)
		.await
}

/// Reset a single thread's count pair and sync stamp atomically.
#[implement(super::Service)]
pub async fn reset_thread_notification_counts(
	&self,
	user: &UserId,
	room: &RoomId,
	root: &EventId,
) -> Result {
	self.reset_notification_counts_for_thread(user, room, &ReceiptThread::Thread(root.to_owned()))
		.await
}

/// Clear the thread rows together while preserving the main count pair.
#[implement(super::Service)]
pub async fn clear_all_thread_notification_counts(&self, user: &UserId, room: &RoomId) -> Result {
	let _lock = self
		.notification_mutex
		.lock(&(room.to_owned(), user.to_owned()))
		.await;
	let mut txn = self.db.db.txn();
	self.stage_thread_clear(&mut txn, user, room)
		.await?;
	check_mutation(&txn)?;
	txn.execute().await
}

/// Reset one thread context. An unthreaded reset includes every thread row
/// and the main count pair in the same bounded commit. Inventories complete
/// before mutation; storage/limit errors leave all notification maps intact.
#[implement(super::Service)]
pub async fn reset_notification_counts_for_thread(
	&self,
	user: &UserId,
	room: &RoomId,
	thread: &ReceiptThread,
) -> Result {
	let guard = self.lock_notification(user, room).await;
	let mut txn = self.db.db.txn();
	self.stage_notification_reset(&mut txn, &guard, thread, None)
		.await?;
	// The permit stays alive through execute so sync cannot pass this stamp.
	let count = self.services.globals.next_count().await?;
	self.stage_notification_read_stamp(&mut txn, &guard, thread, *count)?;
	self.stage_notification_cutoff(&mut txn, &guard, thread, *count)
		.await?;
	check_mutation(&txn)?;
	txn.execute().await?;
	self.notification_reset_committed(&guard, thread);
	Ok(())
}

/// The caller retains `guard` until its receipt/event transaction commits.
/// A supplied stamp lets the receipt share its existing sequence permit.
#[implement(super::Service)]
pub(crate) async fn stage_notification_reset(
	&self,
	txn: &mut Txn,
	guard: &NotificationGuard,
	thread: &ReceiptThread,
	stamp: Option<u64>,
) -> Result {
	let user = &*guard.user;
	let room = &*guard.room;
	match thread {
		| ReceiptThread::Unthreaded => self.stage_thread_clear(txn, user, room).await?,
		| ReceiptThread::Main | ReceiptThread::Thread(_) => {},
		| _ => return Err(tuwunel_core::err!(Request(InvalidParam("Unknown receipt thread")))),
	}
	match thread {
		| ReceiptThread::Thread(root) => {
			txn.put(&self.db.userroomid_notificationcount, (user, room, root), 0_u64);
			txn.put(&self.db.userroomid_highlightcount, (user, room, root), 0_u64);
		},
		| _ => {
			txn.put(&self.db.userroomid_notificationcount, (user, room), 0_u64);
			txn.put(&self.db.userroomid_highlightcount, (user, room), 0_u64);
		},
	}
	if let Some(stamp) = stamp {
		self.stage_notification_read_stamp(txn, guard, thread, stamp)?;
	}
	check_mutation(txn)
}

#[implement(super::Service)]
fn stage_notification_read_stamp(
	&self,
	txn: &mut Txn,
	guard: &NotificationGuard,
	thread: &ReceiptThread,
	stamp: u64,
) -> Result {
	match thread {
		| ReceiptThread::Thread(root) => txn.put(
			&self.db.roomuserid_lastnotificationread,
			(&*guard.room, &*guard.user, root),
			stamp,
		),
		| _ =>
			txn.put(&self.db.roomuserid_lastnotificationread, (&*guard.room, &*guard.user), stamp),
	}
	check_mutation(txn)
}

#[implement(super::Service)]
pub(crate) fn notification_reset_committed(
	&self,
	guard: &NotificationGuard,
	thread: &ReceiptThread,
) {
	if matches!(thread, ReceiptThread::Main | ReceiptThread::Unthreaded) {
		let removed = self.clear_suppressed_room(&guard.user, &guard.room);
		if removed > 0 {
			trace!(user = %guard.user, room = %guard.room, removed, "Cleared suppressed push events after read");
		}
	}
}

#[implement(super::Service)]
async fn stage_thread_clear(&self, txn: &mut Txn, user: &UserId, room: &RoomId) -> Result {
	for map in [&self.db.userroomid_notificationcount, &self.db.userroomid_highlightcount] {
		stage_thread_keys(map, txn, user, room, false).await?;
	}
	stage_thread_keys(&self.db.roomuserid_lastnotificationread, txn, user, room, true).await
}

async fn stage_thread_keys(
	map: &Arc<Map>,
	txn: &mut Txn,
	user: &UserId,
	room: &RoomId,
	room_first: bool,
) -> Result {
	let prefix = if room_first {
		tuwunel_database::serialize_key((room, user, Interfix))?
	} else {
		tuwunel_database::serialize_key((user, room, Interfix))?
	};
	// Main reset needs four operations including the read cutoff. The extra
	// key detects overflow before any part of the sweep commits.
	let stream = map.keys_prefix_raw_capped(&prefix, 897_usize.saturating_sub(txn.len()));
	pin_mut!(stream);
	while let Some(key) = stream.try_next().await? {
		if room_first {
			let (stored_room, stored_user, _): (&RoomId, &UserId, &EventId) =
				deserialize_key(key)?;
			if stored_room != room || stored_user != user {
				return Err(Error::bad_database("Notification read stamp binding mismatch"));
			}
		} else {
			let (stored_user, stored_room, _): (&UserId, &RoomId, &EventId) =
				deserialize_key(key)?;
			if stored_room != room || stored_user != user {
				return Err(Error::bad_database("Notification counter binding mismatch"));
			}
		}
		txn.del_raw(map, key);
		if txn.len() > 896 || txn.size_in_bytes() > 512 * 1024 - 4096 {
			return notification_limit();
		}
	}
	Ok(())
}

pub(super) fn check_mutation(txn: &Txn) -> Result {
	if txn.len() > 900 || txn.size_in_bytes() > 512 * 1024 {
		return notification_limit();
	}
	Ok(())
}

fn notification_limit<T>() -> Result<T> {
	use ruma::api::error::{ErrorKind, LimitExceededErrorData};
	Err(Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Notification mutation limit exceeded".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	))
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn notification_count(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (user_id, room_id);
	self.db
		.userroomid_notificationcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

/// Return the user's account-wide unread notification count.
///
/// Joined main and thread rows contribute to a saturating total.
#[implement(super::Service)]
#[tracing::instrument(level = "trace", skip(self), ret)]
pub async fn global_notification_count(&self, user_id: &UserId) -> u64 {
	self.db
		.userroomid_notificationcount
		.stream_prefix_raw(&(user_id, Interfix))
		.ignore_err()
		.ready_filter_map(|(key, count)| {
			let count = u64_from_u8(count);

			(count > 0).then(|| (KeyBuf::from(key), count))
		})
		.broad_filter_map(|(key, count)| async move {
			let (_, room_id, _): (Ignore, &RoomId, IgnoreAll) =
				deserialize_key(&key).expect("notification count key");

			self.services
				.state_cache
				.is_joined(user_id, room_id)
				.await
				.then_some(count)
		})
		.ready_fold(0_u64, u64::saturating_add)
		.await
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn highlight_count(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (user_id, room_id);
	self.db
		.userroomid_highlightcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

/// Per-thread `(notification, highlight)` counts for one room and user.
/// `Interfix` excludes the legacy 2-tuple main row from the scan; only
/// 3-tuple `(user, room, root)` rows match.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn thread_notification_counts(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> ThreadCounts {
	let prefix = (user_id, room_id, Interfix);
	let notifications = self
		.db
		.userroomid_notificationcount
		.stream_prefix(&prefix)
		.ignore_err()
		.map(notification_kv);

	let highlights = self
		.db
		.userroomid_highlightcount
		.stream_prefix(&prefix)
		.ignore_err()
		.map(highlight_kv);

	select(notifications, highlights)
		.ready_fold(ThreadCounts::default(), merge_thread_count)
		.await
}

fn notification_kv(
	(key, notifications): ((&UserId, &RoomId, OwnedEventId), u64),
) -> (OwnedEventId, (u64, u64)) {
	(key.2, (notifications, 0))
}

fn highlight_kv(
	(key, highlights): ((&UserId, &RoomId, OwnedEventId), u64),
) -> (OwnedEventId, (u64, u64)) {
	(key.2, (0, highlights))
}

fn merge_thread_count(
	mut counts: ThreadCounts,
	(root, (notifications, highlights)): (OwnedEventId, (u64, u64)),
) -> ThreadCounts {
	let entry = counts.entry(root).or_default();
	entry.0 = entry.0.saturating_add(notifications);
	entry.1 = entry.1.saturating_add(highlights);
	counts
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn last_notification_read(&self, user_id: &UserId, room_id: &RoomId) -> Result<u64> {
	let key = (room_id, user_id);
	self.db
		.roomuserid_lastnotificationread
		.qry(&key)
		.await
		.deserialized()
}

/// Per-thread last-read counts for one room and user. `Interfix` keeps the
/// scan to 3-tuple `(room, user, root)` rows; the legacy 2-tuple main row
/// is excluded by construction and lives behind `last_notification_read`.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn thread_last_notification_reads(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> ThreadLastReads {
	let prefix = (room_id, user_id, Interfix);
	self.db
		.roomuserid_lastnotificationread
		.stream_prefix(&prefix)
		.ignore_err()
		.map(|((_, _, root), count): ((Ignore, Ignore, OwnedEventId), u64)| (root, count))
		.collect()
		.await
}

/// Unlike the sync stamp, this is the actual addressed event position. Main
/// and thread cutoffs are independent; an unthreaded cutoff covers both.
#[implement(super::Service)]
pub(crate) async fn stage_notification_cutoff(
	&self,
	txn: &mut Txn,
	guard: &NotificationGuard,
	thread: &ReceiptThread,
	position: u64,
) -> Result {
	if position > i64::MAX.unsigned_abs() {
		return Err(Error::bad_database("Invalid notification cutoff position"));
	}
	let kind = match thread {
		| ReceiptThread::Main => "main",
		| ReceiptThread::Unthreaded => "",
		| ReceiptThread::Thread(root) => root.as_str(),
		| _ => return Err(Error::bad_database("Invalid notification cutoff scope")),
	};
	let key = tuwunel_database::serialize_key((&*guard.room, &*guard.user, kind))?;
	let previous = match self
		.db
		.roomuserid_notificationcutoff
		.get(&key)
		.await
	{
		| Ok(value) => decode_cutoff(&value)?,
		| Err(error) if error.is_not_found() => 0,
		| Err(error) => return Err(error),
	};
	if position > previous {
		txn.insert_raw(&self.db.roomuserid_notificationcutoff, key, position.to_be_bytes());
	}
	check_mutation(txn)
}

#[implement(super::Service)]
pub(super) async fn notification_already_read(
	&self,
	user: &UserId,
	room: &RoomId,
	thread: Option<&EventId>,
	position: u64,
) -> Result<bool> {
	let kind = thread.map_or("main", EventId::as_str);
	let mut read = false;
	for kind in ["", kind] {
		match self
			.db
			.roomuserid_notificationcutoff
			.qry(&(room, user, kind))
			.await
		{
			| Ok(value) => {
				let cutoff = decode_cutoff(&value)?;
				read |= position <= cutoff;
			},
			| Err(error) if error.is_not_found() => {},
			| Err(error) => return Err(error),
		}
	}
	Ok(read)
}

fn decode_cutoff(value: &[u8]) -> Result<u64> {
	let cutoff = u64::from_be_bytes(
		value
			.try_into()
			.map_err(|_| Error::bad_database("Invalid notification read cutoff"))?,
	);
	if cutoff > i64::MAX.unsigned_abs() {
		return Err(Error::bad_database("Invalid notification read cutoff position"));
	}
	Ok(cutoff)
}

#[cfg(test)]
mod tests {
	use super::decode_cutoff;
	#[test]
	fn read_cutoffs_require_a_valid_normal_timeline_position() {
		for position in [0, 1, i64::MAX.unsigned_abs()] {
			assert_eq!(decode_cutoff(&position.to_be_bytes()).expect("normal cutoff"), position);
		}
		for invalid in [vec![], vec![0; 7], vec![0; 9], u64::MAX.to_be_bytes().to_vec()] {
			decode_cutoff(&invalid)
				.expect_err("corruption cannot cancel all future notifications");
		}
	}
}
