//! Frozen, bounded notification work accepted with its canonical event. A
//! recipient's counts, queue rows, receipt and plan cursor share one commit.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use futures::{TryStreamExt, pin_mut};
use ruma::{
	EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UInt, UserId,
	events::{GlobalAccountDataEventType, TimelineEventType, push_rules::PushRulesEvent},
	push::{Action, HighlightTweakValue, Ruleset, Tweak},
};
use serde::{Deserialize, Serialize};
use tokio::sync::OwnedMutexGuard;
use tuwunel_core::{
	Error, Result, error, implement,
	matrix::{
		Event,
		pdu::{Count, Pdu, PduId, RawPduId},
	},
	utils::{mutex_map::Guard, time::now_millis},
};
use tuwunel_database::{Interfix, Map, Txn, serialize_key};

use super::{Evaluate, Notified, notification::check_mutation};
use crate::rooms::{short::ShortStateHash, state::RoomMutexGuard};

const MAX_PENDING: usize = 64;
const MAX_PLAN_BYTES: usize = 64 * 1024;
const MAX_RECIPIENTS: usize = 1024;
const MAX_PUSHKEYS: usize = 128;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Plan {
	format: u8,
	raw_id: Vec<u8>,
	room: OwnedRoomId,
	event: OwnedEventId,
	thread: Option<OwnedEventId>,
	recipients: Vec<Recipient>,
	next: usize,
	created_ms: u64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Recipient {
	user: OwnedUserId,
	actions: Vec<Action>,
	pushkeys: Vec<String>,
	push_everything: bool,
}

/// This record freezes push actions after the pending plan is retired. It
/// contains no pushkeys or gateway credentials. The queue carries a distinct
/// frozen-event tag, so a missing receipt cannot fall back to current rules.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
	format: u8,
	room: OwnedRoomId,
	event: OwnedEventId,
	thread: Option<OwnedEventId>,
	actions: Vec<Action>,
	push_everything: bool,
	canceled: bool,
}

pub(crate) struct PreparedNotifications {
	_admission: OwnedMutexGuard<()>,
}

impl Plan {
	fn decode(key: &[u8], value: &[u8]) -> Result<Self> {
		if value.len() > MAX_PLAN_BYTES {
			return limit();
		}
		let plan: Self = serde_json::from_slice(value)
			.map_err(|_| Error::bad_database("Invalid notification plan"))?;
		let raw = RawPduId::from_bytes(key)?;
		if plan.format != 1
			|| plan.raw_id != key
			|| !matches!(raw.pdu_count(), Count::Normal(_))
			|| plan.recipients.is_empty()
			|| plan.recipients.len() > MAX_RECIPIENTS
			|| plan.next >= plan.recipients.len()
		{
			return Err(Error::bad_database("Invalid notification plan binding or cursor"));
		}
		let mut users = BTreeSet::new();
		for recipient in &plan.recipients {
			if !users.insert(&recipient.user)
				|| recipient.pushkeys.len() > MAX_PUSHKEYS
				|| recipient.actions.len() > 64
				|| recipient
					.pushkeys
					.iter()
					.any(|key| key.len() > 512)
				|| recipient
					.pushkeys
					.iter()
					.collect::<BTreeSet<_>>()
					.len() != recipient.pushkeys.len()
			{
				return Err(Error::bad_database("Invalid notification recipient manifest"));
			}
		}
		Ok(plan)
	}

	fn receipt(&self, recipient: &Recipient, canceled: bool) -> Receipt {
		Receipt {
			format: 1,
			room: self.room.clone(),
			event: self.event.clone(),
			thread: self.thread.clone(),
			actions: recipient.actions.clone(),
			push_everything: recipient.push_everything,
			canceled,
		}
	}
}

#[implement(super::Service)]
pub(crate) async fn lock_notification_user(&self, user: &UserId) -> Guard<OwnedUserId, ()> {
	self.notification_users.lock(user).await
}

/// Caller holds room state and keeps the returned admission guard through the
/// PDU commit. Another room cannot consume the last pending-plan slot
/// meanwhile.
#[implement(super::Service)]
pub(crate) async fn stage_notification_plan(
	&self,
	txn: &mut Txn,
	raw: RawPduId,
	pdu: &Pdu,
	state: Option<ShortStateHash>,
) -> Result<PreparedNotifications> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let admission = self
		.notification_admission
		.clone()
		.lock_owned()
		.await;
	let pending = self.notification_inventory().await?;
	let mut targets = BTreeSet::new();
	let members = services_root
		.state_cache
		.bounded_room_members(pdu.room_id())
		.await?;
	let joined_count = UInt::try_from(members.len())
		.map_err(|_| Error::bad_database("Invalid notification member inventory count"))?;
	for user in members {
		if user != pdu.sender()
			&& services_root
				.users
				.notification_recipient_active(&user)
				.await? && !services_root
			.users
			.user_is_ignored_checked(pdu.sender(), &user)
			.await?
		{
			targets.insert(user);
		}
	}
	if *pdu.kind() == TimelineEventType::RoomMember
		&& let Some(target) = pdu.state_key()
	{
		let target = UserId::parse(target)?;
		if services_root
			.users
			.notification_recipient_active(&target)
			.await?
		{
			targets.insert(target);
		}
	}
	if targets.is_empty() {
		return Ok(PreparedNotifications { _admission: admission });
	}
	let state = match state {
		| Some(state) => state,
		| None =>
			services_root
				.state
				.get_room_shortstatehash(pdu.room_id())
				.await?,
	};
	let power_levels = services_root
		.state_accessor
		.get_power_levels_at(pdu.room_id(), state, Some(pdu))
		.await?;
	let related_events = self.related_events_checked(pdu).await?;
	let thread = services_root
		.threads
		.get_thread_id_checked(pdu)
		.await?;
	let serialized = pdu.to_format();
	let mut recipients = Vec::new();
	for user in targets {
		let rules = match services_root
			.account_data
			.get_global::<PushRulesEvent>(&user, GlobalAccountDataEventType::PushRules)
			.await
		{
			| Ok(event) => event.content.global,
			| Err(error) if error.is_not_found() => Ruleset::server_default(&user),
			| Err(error) => return Err(error),
		};
		let actions = self
			.get_actions_with_count(
				Evaluate {
					user: &user,
					ruleset: &rules,
					power_levels: Some(&power_levels),
					pdu: &serialized,
					room_id: pdu.room_id(),
					related_events: related_events.as_ref(),
				},
				joined_count,
			)
			.await?
			.to_vec();
		let (notify, highlight) = decisions(&actions);
		if !notify && !highlight && !services_root.config.push_everything {
			continue;
		}
		let pushkeys = self.notification_pushkeys(&user).await?;
		recipients.push(Recipient {
			user,
			actions,
			pushkeys,
			push_everything: services_root.config.push_everything,
		});
	}
	if recipients.is_empty() {
		return Ok(PreparedNotifications { _admission: admission });
	}
	if pending.len() >= MAX_PENDING {
		return limit();
	}
	let plan = Plan {
		format: 1,
		raw_id: raw.as_ref().to_vec(),
		room: pdu.room_id().to_owned(),
		event: pdu.event_id().to_owned(),
		thread,
		recipients,
		next: 0,
		created_ms: now_millis(),
	};
	let value = serde_json::to_vec(&plan)?;
	Plan::decode(raw.as_ref(), &value)?;
	txn.insert_raw(&self.db.pduid_notificationplan, raw, value);
	check_mutation(txn)?;
	Ok(PreparedNotifications { _admission: admission })
}

#[implement(super::Service)]
async fn notification_pushkeys(&self, user: &UserId) -> Result<Vec<String>> {
	let keys = self
		.db
		.senderkey_pusher
		.keys_prefix_capped::<(&UserId, &str), _>(&(user, Interfix), MAX_PUSHKEYS + 1);
	pin_mut!(keys);
	let mut result = Vec::new();
	while let Some(key) = keys.try_next().await? {
		if result.len() == MAX_PUSHKEYS || key.0 != user || key.1.len() > 512 {
			return limit();
		}
		// Validate the complete record, not only a possibly corrupt key. A
		// missing pusher during preparation is an error, not silent omission.
		self.get_pusher(user, key.1).await?;
		result.push(key.1.to_owned());
	}
	Ok(result)
}

/// Complete inventory, including one overflow probe. Startup validates all
/// source bindings and processed receipts before applying any recovered work.
#[implement(super::Service)]
async fn notification_inventory(&self) -> Result<Vec<Plan>> {
	let rows = self
		.db
		.pduid_notificationplan
		.raw_rows_prefix_after(&[], None, MAX_PENDING + 1)
		.await?;
	if rows.len() > MAX_PENDING {
		return limit();
	}
	rows.into_iter()
		.map(|(key, value)| Plan::decode(&key, &value))
		.collect()
}

#[implement(super::Service)]
async fn validate_notification_source(&self, plan: &Plan) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let raw = RawPduId::from_bytes(&plan.raw_id)?;
	let pdu = services_root
		.timeline
		.get_pdu_from_id(&raw)
		.await
		.map_err(|error| {
			if error.is_not_found() {
				Error::bad_database("Notification source PDU is missing")
			} else {
				error
			}
		})?;
	let binding = services_root
		.timeline
		.get_pdu_id(&plan.event)
		.await?;
	if binding != raw
		|| pdu.event_id() != plan.event
		|| pdu.room_id() != plan.room
		|| services_root
			.short
			.get_shortroomid(&plan.room)
			.await?
			.to_be_bytes()
			!= raw.shortroomid()
	{
		return Err(Error::bad_database("Notification source indexes disagree"));
	}
	for (index, recipient) in plan.recipients.iter().enumerate() {
		if !services_root
			.globals
			.user_is_local(&recipient.user)
		{
			return Err(Error::bad_database("Nonlocal notification recipient"));
		}
		let key = receipt_key(&raw, &recipient.user);
		match self
			.db
			.notificationreceiptid_record
			.get(&key)
			.await
		{
			| Ok(value) if index < plan.next => {
				let receipt = decode_receipt(&value, &plan.room, &plan.event)?;
				let expected = plan.receipt(recipient, receipt.canceled);
				if serde_json::to_vec(&receipt)? != serde_json::to_vec(&expected)? {
					return Err(Error::bad_database(
						"Notification progress receipt differs from frozen decision",
					));
				}
			},
			| Err(error) if error.is_not_found() && index >= plan.next => {},
			| Err(error) if !error.is_not_found() => return Err(error),
			| _ => return Err(Error::bad_database("Notification cursor and receipts disagree")),
		}
	}
	Ok(())
}

#[implement(super::Service)]
pub(crate) async fn restore_notifications(&self) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let plans = self.notification_inventory().await?;
	for plan in &plans {
		self.validate_notification_source(plan).await?;
	}
	for plan in plans {
		let state = services_root.state.mutex.lock(&plan.room).await;
		self.finish_notification_plan(&plan.raw_id, &state)
			.await?;
	}
	Ok(())
}

/// Live append already holds room state. The retry worker acquires it before
/// calling this method; history/room deletion therefore cannot race replay.
#[implement(super::Service)]
pub(crate) async fn append_pdu(
	&self,
	raw: RawPduId,
	_pdu: &Pdu,
	state: &RoomMutexGuard,
) -> Result {
	self.finish_notification_plan(raw.as_ref(), state)
		.await
}

#[implement(super::Service)]
async fn finish_notification_plan(&self, key: &[u8], _state: &RoomMutexGuard) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let _plan = self.notification_plans.lock(&key.to_vec()).await;
	let mut validated = false;
	loop {
		let value = match self.db.pduid_notificationplan.get(key).await {
			| Ok(value) => value,
			| Err(error) if error.is_not_found() => return Ok(()),
			| Err(error) => return Err(error),
		};
		let mut plan = Plan::decode(key, &value)?;
		if !validated {
			self.validate_notification_source(&plan).await?;
			validated = true;
		}
		let raw = RawPduId::from_bytes(key)?;
		let recipient = plan.recipients[plan.next].clone();
		let guard = self
			.lock_notification(&recipient.user, &plan.room)
			.await;
		let canceled = !services_root
			.users
			.notification_recipient_active(&recipient.user)
			.await? || self
			.notification_already_read(
				&recipient.user,
				&plan.room,
				plan.thread.as_deref(),
				raw.pdu_count().into_unsigned(),
			)
			.await?;
		let mut txn = self.db.db.txn();
		let (notify, highlight) = decisions(&recipient.actions);
		let mut wakes = Vec::new();
		if !canceled {
			let count_key = match &plan.thread {
				| Some(thread) => serialize_key((&recipient.user, &plan.room, thread))?,
				| None => serialize_key((&recipient.user, &plan.room))?,
			};
			if notify {
				stage_increment(&mut txn, &self.db.userroomid_notificationcount, &count_key)
					.await?;
			}
			if highlight {
				stage_increment(&mut txn, &self.db.userroomid_highlightcount, &count_key).await?;
			}
			if notify || highlight {
				let id: PduId = raw.into();
				let notified = Notified {
					ts: plan.created_ms,
					sroomid: id.shortroomid,
					tag: None,
					actions: recipient.actions.clone().into(),
				};
				txn.put(
					&self.db.useridcount_notification,
					(&recipient.user, id.count.into_unsigned()),
					tuwunel_database::Json(&notified),
				);
				self.stage_notification_index(&mut txn, raw, &plan.room, &recipient.user)?;
			}
			for pushkey in &recipient.pushkeys {
				wakes.push(services_root.sending.stage_frozen_push(
					&mut txn,
					raw,
					&recipient.user,
					pushkey,
				));
			}
		}
		txn.insert_raw(
			&self.db.notificationreceiptid_record,
			receipt_key(&raw, &recipient.user),
			serde_json::to_vec(&plan.receipt(&recipient, canceled))?,
		);
		plan.next = plan
			.next
			.checked_add(1)
			.ok_or_else(|| Error::bad_database("Notification cursor overflow"))?;
		if plan.next == plan.recipients.len() {
			txn.del_raw(&self.db.pduid_notificationplan, key);
		} else {
			txn.insert_raw(&self.db.pduid_notificationplan, key, serde_json::to_vec(&plan)?);
		}
		// Late completion must also become visible to a sync whose timeline
		// already passed the event. Keep the change stamp's sequence permit
		// through the same recipient commit; read cutoffs live separately.
		let update = if !canceled && (notify || highlight) {
			let update = services_root.globals.next_count().await?;
			match &plan.thread {
				| Some(thread) => txn.put(
					&self.db.roomuserid_lastnotificationread,
					(&plan.room, &recipient.user, thread),
					*update,
				),
				| None => txn.put(
					&self.db.roomuserid_lastnotificationread,
					(&plan.room, &recipient.user),
					*update,
				),
			}
			Some(update)
		} else {
			None
		};
		check_mutation(&txn)?;
		#[cfg(all(feature = "notification_recovery_tests", debug_assertions))]
		self.wait_notification_commit_for_test(&raw).await;
		txn.execute().await?;
		drop(update);
		drop(guard);
		for wake in wakes {
			services_root.sending.wake_frozen_push(wake)?;
		}
	}
}

#[implement(super::Service)]
pub(super) async fn notification_worker(self: Arc<Self>) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let mut interval = tokio::time::interval(Duration::from_secs(1));
	interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
	loop {
		tokio::select! {
			() = self.notification_stop.notified() => return Ok(()),
			_ = interval.tick() => {},
		}
		#[cfg(all(feature = "notification_recovery_tests", debug_assertions))]
		if self
			.notification_retry_paused
			.load(std::sync::atomic::Ordering::Acquire)
		{
			continue;
		}
		let plans = match self.notification_inventory().await {
			| Ok(plans) => plans,
			| Err(error) => {
				error!("Notification recovery inventory failed: {error}");
				continue;
			},
		};
		for plan in plans {
			let state = tokio::select! {
				() = self.notification_stop.notified() => return Ok(()),
				state = services_root.state.mutex.lock(&plan.room) => state,
			};
			if let Err(error) = self
				.finish_notification_plan(&plan.raw_id, &state)
				.await
			{
				error!(event_id = %plan.event, "Notification recovery remains pending: {error}");
			}
		}
	}
}

#[implement(super::Service)]
pub(crate) async fn frozen_push_decision(
	&self,
	raw: &RawPduId,
	user: &UserId,
	event: &Pdu,
) -> Result<(Vec<Action>, bool, bool)> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let value = self
		.db
		.notificationreceiptid_record
		.get(&receipt_key(raw, user))
		.await
		.map_err(|error| {
			if error.is_not_found() {
				Error::bad_database("Frozen push receipt is missing")
			} else {
				error
			}
		})?;
	let receipt = decode_receipt(&value, event.room_id(), event.event_id())?;
	let _guard = self
		.lock_notification(user, event.room_id())
		.await;
	let canceled = receipt.canceled
		|| !services_root
			.users
			.notification_recipient_active(user)
			.await?
		|| self
			.notification_already_read(
				user,
				event.room_id(),
				receipt.thread.as_deref(),
				raw.pdu_count().into_unsigned(),
			)
			.await?;
	Ok((receipt.actions, receipt.push_everything, canceled))
}

/// Event erasure and pending-plan cancellation share the canonical deletion
/// transaction. The caller holds room state through commit.
#[implement(super::Service)]
pub(crate) async fn stage_notification_erasure(
	&self,
	txn: &mut Txn,
	raw: &RawPduId,
	room: &RoomId,
) -> Result {
	txn.del_raw(&self.db.pduid_notificationplan, raw);
	let mut prefix = raw.as_ref().to_vec();
	prefix.push(tuwunel_database::SEP);
	let keys = self
		.db
		.notificationreceiptid_record
		.keys_prefix_raw_capped(&prefix, 901);
	pin_mut!(keys);
	while let Some(key) = keys.try_next().await? {
		let user = std::str::from_utf8(&key[prefix.len()..])
			.map_err(|_| Error::bad_database("Invalid notification receipt key"))?;
		UserId::parse(user)
			.map_err(|_| Error::bad_database("Invalid notification receipt user"))?;
		txn.del_raw(&self.db.notificationreceiptid_record, key);
		check_mutation(txn)?;
	}
	if !self
		.stage_notification_index_erasure(txn, raw, room, 301)
		.await?
	{
		return limit();
	}
	Ok(())
}

#[cfg(all(feature = "notification_recovery_tests", debug_assertions))]
pub(super) struct CommitPause {
	raw: RawPduId,
	entered: tokio::sync::oneshot::Sender<RawPduId>,
	release: Arc<tokio::sync::Notify>,
}

#[cfg(all(feature = "notification_recovery_tests", debug_assertions))]
pub struct NotificationCommitPause {
	entered: tokio::sync::oneshot::Receiver<RawPduId>,
	release: Arc<tokio::sync::Notify>,
}

#[cfg(all(feature = "notification_recovery_tests", debug_assertions))]
impl NotificationCommitPause {
	pub async fn entered(&mut self) -> Result<RawPduId> {
		(&mut self.entered)
			.await
			.map_err(|_| Error::bad_database("Notification commit pause was abandoned"))
	}
}

#[cfg(all(feature = "notification_recovery_tests", debug_assertions))]
impl Drop for NotificationCommitPause {
	fn drop(&mut self) { self.release.notify_one(); }
}

#[cfg(all(feature = "notification_recovery_tests", debug_assertions))]
impl super::Service {
	/// Holds one actual recipient commit after preparation. Only debug test
	/// builds expose this owned, automatically released pause.
	pub fn pause_notification_commit_for_test(&self, raw: RawPduId) -> NotificationCommitPause {
		let (entered, receiver) = tokio::sync::oneshot::channel();
		let release = Arc::new(tokio::sync::Notify::new());
		let mut gate = self
			.notification_commit_pause
			.lock()
			.expect("locked");
		assert!(gate.is_none(), "one owned notification commit pause");
		*gate = Some(CommitPause { raw, entered, release: release.clone() });
		NotificationCommitPause { entered: receiver, release }
	}

	async fn wait_notification_commit_for_test(&self, raw: &RawPduId) {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let pause = {
			let mut gate = self
				.notification_commit_pause
				.lock()
				.expect("locked");
			if gate.as_ref().is_some_and(|gate| &gate.raw == raw) {
				gate.take()
			} else {
				None
			}
		};
		if let Some(pause) = pause {
			pause.entered.send(*raw).ok();
			tokio::select! {
				() = pause.release.notified() => {},
				() = services_root.server.until_shutdown() => {},
			}
		}
	}

	/// Pauses only background retry. Immediate append still attempts its
	/// transaction, allowing deterministic pre-dispatch refusal fixtures.
	pub fn pause_notification_retry_for_test(&self, paused: bool) {
		self.notification_retry_paused
			.store(paused, std::sync::atomic::Ordering::Release);
	}

	pub async fn retry_notifications_for_test(&self) -> Result {
		self.restore_notifications().await
	}
}

fn receipt_key(raw: &RawPduId, user: &UserId) -> Vec<u8> {
	let mut key = raw.as_ref().to_vec();
	key.push(tuwunel_database::SEP);
	key.extend_from_slice(user.as_bytes());
	key
}

fn decode_receipt(value: &[u8], room: &RoomId, event: &EventId) -> Result<Receipt> {
	if value.len() > MAX_PLAN_BYTES {
		return limit();
	}
	let receipt: Receipt = serde_json::from_slice(value)
		.map_err(|_| Error::bad_database("Invalid frozen push receipt"))?;
	if receipt.format != 1
		|| receipt.room != room
		|| receipt.event != event
		|| receipt.actions.len() > 64
	{
		return Err(Error::bad_database("Frozen push receipt binding mismatch"));
	}
	Ok(receipt)
}

fn decisions(actions: &[Action]) -> (bool, bool) {
	(
		actions.iter().any(Action::should_notify),
		actions.iter().any(|action| {
			matches!(action, Action::SetTweak(Tweak::Highlight(HighlightTweakValue::Yes)))
		}),
	)
}

async fn stage_increment(txn: &mut Txn, map: &Arc<Map>, key: &[u8]) -> Result {
	let old = match map.get(key).await {
		| Ok(value) => u64::from_be_bytes(
			value
				.as_ref()
				.try_into()
				.map_err(|_| Error::bad_database("Invalid notification counter"))?,
		),
		| Err(error) if error.is_not_found() => 0,
		| Err(error) => return Err(error),
	};
	let new = super::notification::checked_add(old, 1)?;
	txn.insert_raw(map, key, new.to_be_bytes());
	Ok(())
}

fn limit<T>() -> Result<T> {
	use ruma::api::error::{ErrorKind, LimitExceededErrorData};
	Err(Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Notification recovery inventory limit exceeded".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	))
}

/// Typed history cleanup uses an exclusive receipt-key cursor, so more than
/// one recipient batch is supported without releasing the room exclusion.
#[implement(super::Service)]
pub(crate) async fn stage_notification_erasure_page(
	&self,
	txn: &mut Txn,
	raw: &RawPduId,
	room: &RoomId,
	after: Option<&[u8]>,
) -> Result<(Option<Vec<u8>>, bool)> {
	let mut prefix = raw.as_ref().to_vec();
	prefix.push(tuwunel_database::SEP);
	// Cancel owed work in the first cleanup page, before deleting any of its
	// completed receipts. A restart can then validate and resume the history
	// job without mistaking its legitimate receipt removals for corruption.
	txn.del_raw(&self.db.pduid_notificationplan, raw);
	let keys = self
		.db
		.notificationreceiptid_record
		.raw_keys_prefix_after(&prefix, after, 65)
		.await?;
	let receipts_done = keys.len() <= 64;
	let mut cursor = None;
	for key in keys.into_iter().take(64) {
		if !key.starts_with(&prefix) {
			return Err(Error::bad_database("Notification erasure key binding changed"));
		}
		let user = std::str::from_utf8(&key[prefix.len()..])
			.map_err(|_| Error::bad_database("Invalid notification receipt key"))?;
		UserId::parse(user)
			.map_err(|_| Error::bad_database("Invalid notification receipt user"))?;
		txn.del_raw(&self.db.notificationreceiptid_record, &key);
		cursor = Some(key);
	}
	let done = receipts_done
		&& self
			.stage_notification_index_erasure(txn, raw, room, 64)
			.await?;
	check_mutation(txn)?;
	Ok((cursor.or_else(|| after.map(<[u8]>::to_vec)), done))
}
