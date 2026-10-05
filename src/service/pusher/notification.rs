use std::{collections::BTreeMap, sync::Arc};

use futures::{TryStreamExt, pin_mut};
use ruma::{
	EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UInt, UserId,
	events::receipt::ReceiptThread,
};
use tuwunel_core::{Error, Result, implement, matrix::Event, trace};
use tuwunel_database::{
	Interfix, Map, Txn, deserialize_from_slice as deserialize_key, serialize_key,
};

/// Per-thread unread counts: `(notification, highlight)` keyed by thread root.
pub(super) type ThreadCounts = BTreeMap<OwnedEventId, (u64, u64)>;

/// Per-thread sync change stamps. Read resets and late notification
/// completion both advance these stamps; they are not event read cutoffs.
pub(super) type ThreadLastReads = BTreeMap<OwnedEventId, u64>;

/// One checked snapshot, serialized with notification completions and resets.
/// Change stamps signal sync activity; they are not event read cutoffs.
#[derive(Debug, Default)]
pub struct NotificationState {
	pub notifications: u64,
	pub highlights: u64,
	pub threads: ThreadCounts,
	pub change: Option<u64>,
	pub thread_changes: ThreadLastReads,
}

impl NotificationState {
	pub fn totals(&self) -> Result<(UInt, UInt)> {
		let (notifications, highlights) = self.threads.values().try_fold(
			(self.notifications, self.highlights),
			|(notifications, highlights), &(n, h)| -> Result<_> {
				Ok((checked_add(notifications, n)?, checked_add(highlights, h)?))
			},
		)?;
		Ok((count_uint(notifications)?, count_uint(highlights)?))
	}
}

const READ_ROWS: usize = 4096;
const READ_BYTES: usize = 4 * 1024 * 1024;
const READ_PAGE: usize = 64;

#[derive(Default)]
struct ReadBudget {
	rows: usize,
	bytes: usize,
}

impl ReadBudget {
	fn charge(&mut self, key: &[u8], value: &[u8]) -> Result {
		self.rows = self.rows.saturating_add(1);
		self.bytes = self
			.bytes
			.saturating_add(key.len())
			.saturating_add(value.len());
		if self.rows > READ_ROWS || self.bytes > READ_BYTES {
			return notification_limit();
		}
		Ok(())
	}
}

pub fn count_uint(value: u64) -> Result<UInt> {
	UInt::new(value)
		.ok_or_else(|| Error::bad_database("Notification count exceeds Matrix integer range"))
}

pub fn checked_add(lhs: u64, rhs: u64) -> Result<u64> {
	let sum = lhs
		.checked_add(rhs)
		.ok_or_else(|| Error::bad_database("Notification count overflow"))?;
	count_uint(sum)?;
	Ok(sum)
}

fn decode_count(value: &[u8]) -> Result<u64> {
	let count = u64::from_be_bytes(
		value
			.try_into()
			.map_err(|_| Error::bad_database("Invalid notification count width"))?,
	);
	count_uint(count)?;
	Ok(count)
}

async fn optional_value(map: &Arc<Map>, key: &[u8], stamp: bool) -> Result<Option<u64>> {
	match map.get(&key).await {
		| Ok(value) => Ok(Some(if stamp {
			decode_cutoff(&value)?
		} else {
			decode_count(&value)?
		})),
		| Err(error) if error.is_not_found() => Ok(None),
		| Err(error) => Err(error),
	}
}

async fn thread_rows(
	map: &Arc<Map>,
	user: &UserId,
	room: &RoomId,
	room_first: bool,
	budget: &mut ReadBudget,
) -> Result<BTreeMap<OwnedEventId, u64>> {
	let prefix = if room_first {
		serialize_key((room, user, Interfix))?
	} else {
		serialize_key((user, room, Interfix))?
	};
	let mut after = None;
	let mut result = BTreeMap::new();
	loop {
		let keys = map
			.raw_keys_prefix_after(
				&prefix,
				after.as_deref(),
				READ_PAGE.min(
					READ_ROWS
						.saturating_sub(budget.rows)
						.saturating_add(1),
				),
			)
			.await?;
		for key in &keys {
			let root = if room_first {
				let (r, u, root): (&RoomId, &UserId, OwnedEventId) = deserialize_key(key)?;
				if r != room
					|| u != user || serialize_key((r, u, &root))?.as_slice() != key.as_slice()
				{
					return Err(Error::bad_database("Notification change key binding mismatch"));
				}
				root
			} else {
				let (u, r, root): (&UserId, &RoomId, OwnedEventId) = deserialize_key(key)?;
				if r != room
					|| u != user || serialize_key((u, r, &root))?.as_slice() != key.as_slice()
				{
					return Err(Error::bad_database("Notification count key binding mismatch"));
				}
				root
			};
			let value = map.get(key).await?;
			budget.charge(key, &value)?;
			let count = if room_first {
				decode_cutoff(&value)?
			} else {
				decode_count(&value)?
			};
			result.insert(root, count);
		}
		if keys.is_empty() {
			break;
		}
		after = keys.last().cloned();
	}
	Ok(result)
}

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
	let _lock = self.lock_notification(user, room).await;
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
		serialize_key((room, user, Interfix))?
	} else {
		serialize_key((user, room, Interfix))?
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
		"Notification storage limit exceeded".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	))
}

/// Read main/thread counts and change stamps under the same mutation guard.
/// Genuine absence means zero/none; corruption, incomplete scans and overflow
/// refuse.
#[implement(super::Service)]
pub async fn notification_state(
	&self,
	user: &UserId,
	room: &RoomId,
) -> Result<NotificationState> {
	let _guard = self.lock_notification(user, room).await;
	let main = serialize_key((user, room))?;
	let stamp = serialize_key((room, user))?;
	let notifications = optional_value(&self.db.userroomid_notificationcount, &main, false)
		.await?
		.unwrap_or(0);
	let highlights = optional_value(&self.db.userroomid_highlightcount, &main, false)
		.await?
		.unwrap_or(0);
	let change = optional_value(&self.db.roomuserid_lastnotificationread, &stamp, true).await?;
	let mut budget = ReadBudget::default();
	let mut threads = ThreadCounts::new();
	for (root, count) in
		thread_rows(&self.db.userroomid_notificationcount, user, room, false, &mut budget).await?
	{
		threads.entry(root).or_default().0 = count;
	}
	for (root, count) in
		thread_rows(&self.db.userroomid_highlightcount, user, room, false, &mut budget).await?
	{
		threads.entry(root).or_default().1 = count;
	}
	let thread_changes =
		thread_rows(&self.db.roomuserid_lastnotificationread, user, room, true, &mut budget)
			.await?;
	let state = NotificationState {
		notifications,
		highlights,
		threads,
		change,
		thread_changes,
	};
	// Validate even on a quiet round whose downstream gates omit counts.
	// Otherwise an unrepresentable total could acknowledge an empty sync.
	state.totals()?;
	Ok(state)
}

#[implement(super::Service)]
pub async fn notification_count(&self, user: &UserId, room: &RoomId) -> Result<u64> {
	let _guard = self.lock_notification(user, room).await;
	optional_value(&self.db.userroomid_notificationcount, &serialize_key((user, room))?, false)
		.await
		.map(|count| count.unwrap_or(0))
}

#[implement(super::Service)]
pub async fn highlight_count(&self, user: &UserId, room: &RoomId) -> Result<u64> {
	let _guard = self.lock_notification(user, room).await;
	optional_value(&self.db.userroomid_highlightcount, &serialize_key((user, room))?, false)
		.await
		.map(|count| count.unwrap_or(0))
}

/// Joined main and thread rows contribute to a checked account-wide total.
/// The user guard excludes notification completion, resets and lifecycle
/// changes.
#[implement(super::Service)]
pub async fn global_notification_count(&self, user: &UserId) -> Result<u64> {
	let _guard = self.lock_notification_user(user).await;
	let prefix = serialize_key((user, Interfix))?;
	let mut after = None;
	let mut budget = ReadBudget::default();
	let mut total = 0;
	loop {
		let keys = self
			.db
			.userroomid_notificationcount
			.raw_keys_prefix_after(
				&prefix,
				after.as_deref(),
				READ_PAGE.min(
					READ_ROWS
						.saturating_sub(budget.rows)
						.saturating_add(1),
				),
			)
			.await?;
		for key in &keys {
			let room = count_key_room(key, user)?;
			let value = self
				.db
				.userroomid_notificationcount
				.get(key)
				.await?;
			budget.charge(key, &value)?;
			let count = decode_count(&value)?;
			if self
				.services
				.state_cache
				.is_joined_checked(user, &room)
				.await?
			{
				total = checked_add(total, count)?;
			}
		}
		if keys.is_empty() {
			break;
		}
		after = keys.last().cloned();
	}
	Ok(total)
}

fn count_key_room(key: &[u8], user: &UserId) -> Result<OwnedRoomId> {
	if let Ok((u, r)) = deserialize_key::<(&UserId, &RoomId)>(key)
		&& u == user
		&& serialize_key((u, r))?.as_slice() == key
	{
		return Ok(r.to_owned());
	}
	let (u, r, root): (&UserId, &RoomId, &EventId) = deserialize_key(key)?;
	if u != user || serialize_key((u, r, root))?.as_slice() != key {
		return Err(Error::bad_database("Account notification key binding mismatch"));
	}
	Ok(r.to_owned())
}

#[implement(super::Service)]
pub async fn thread_notification_counts(
	&self,
	user: &UserId,
	room: &RoomId,
) -> Result<ThreadCounts> {
	Ok(self.notification_state(user, room).await?.threads)
}

/// Compatibility name: this is a sync change stamp, not the read cutoff.
#[implement(super::Service)]
pub async fn last_notification_read(&self, user: &UserId, room: &RoomId) -> Result<u64> {
	let _guard = self.lock_notification(user, room).await;
	optional_value(&self.db.roomuserid_lastnotificationread, &serialize_key((room, user))?, true)
		.await?
		.ok_or_else(|| tuwunel_core::err!(Request(NotFound("Notification change stamp absent"))))
}

#[implement(super::Service)]
pub async fn thread_last_notification_reads(
	&self,
	user: &UserId,
	room: &RoomId,
) -> Result<ThreadLastReads> {
	Ok(self
		.notification_state(user, room)
		.await?
		.thread_changes)
}

/// Sliding sync needs thread-only activity as well as main count changes.
#[implement(super::Service)]
pub async fn notification_update_count(&self, user: &UserId, room: &RoomId) -> Result<u64> {
	let state = self.notification_state(user, room).await?;
	state
		.change
		.into_iter()
		.chain(state.thread_changes.into_values())
		.max()
		.ok_or_else(|| tuwunel_core::err!(Request(NotFound("Notification change stamp absent"))))
}

/// Determine read status from actual event cutoffs, independently of sync
/// stamps.
#[implement(super::Service)]
pub async fn notification_is_read<E: Event>(
	&self,
	user: &UserId,
	event: &E,
	position: u64,
) -> Result<bool> {
	if position == 0 || position > i64::MAX.unsigned_abs() {
		return Err(Error::bad_database("Invalid notification event position"));
	}
	let thread = self
		.services
		.threads
		.get_thread_id_checked(event)
		.await?;
	let _guard = self
		.lock_notification(user, event.room_id())
		.await;
	self.notification_already_read(user, event.room_id(), thread.as_deref(), position)
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
	let key = serialize_key((&*guard.room, &*guard.user, kind))?;
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
