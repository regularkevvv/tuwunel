use std::{collections::HashSet, sync::Arc};

use futures::{FutureExt, StreamExt, future::join};
use ruma::{
	EventId, RoomId, UserId,
	api::client::push::ProfileTag,
	events::{
		AnySyncTimelineEvent, GlobalAccountDataEventType, TimelineEventType,
		push_rules::PushRulesEvent, room::power_levels::RoomPowerLevels,
	},
	push::{Action, Actions, HighlightTweakValue, Ruleset, Tweak},
	serde::Raw,
};
use serde::{Deserialize, Serialize};
use tracing::Level;
use tuwunel_core::{
	Error, Result, implement,
	matrix::{
		event::Event,
		pdu::{Count, Pdu, PduId, RawPduId},
	},
	trace,
	utils::{BoolExt, ReadyExt, future::TryExtExt, result::ErrLog, time::now_millis},
};
use tuwunel_database::{Map, Txn, serialize_key};

use super::{Evaluate, RelatedEvents};
use crate::rooms::{short::ShortRoomId, timeline::Effect};

/// Compact metadata stored for each notified event.
///
/// The database key supplies the user and PDU count. The stored `ShortRoomId`
/// combines with that count to reconstruct the event's `PduId`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Notified {
	/// Milliseconds time at which the event notification was sent.
	pub ts: u64,

	/// ShortRoomId
	pub sroomid: ShortRoomId,

	/// The profile tag of the rule that matched this event.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub tag: Option<ProfileTag>,

	/// Actions vector
	pub actions: Actions,
}

/// The values of one appended event shared by every recipient of it.
///
/// Each is resolved once per event and read once per recipient, so grouping
/// them keeps the per-recipient call from growing an argument per lookup.
#[derive(Clone, Copy)]
struct Appended<'a> {
	pdu_id: &'a RawPduId,
	pdu: &'a Pdu,
	power_levels: Option<&'a RoomPowerLevels>,
	serialized: &'a Raw<AnySyncTimelineEvent>,
	thread_root: Option<&'a EventId>,
	related_events: Option<&'a Arc<RelatedEvents>>,
}

/// Called by timeline append_pdu.
///
/// A write that fails for one recipient is logged, and the rest still run
/// (`Effect`).
#[implement(super::Service)]
#[tracing::instrument(name = "append", level = "debug", skip_all)]
pub(crate) async fn append_pdu(&self, pdu_id: RawPduId, pdu: &Pdu) -> Result {
	let push_target = self
		.services
		.state_cache
		.active_local_users_in_room(pdu.room_id())
		.map(ToOwned::to_owned)
		.ready_filter(|user| *user != pdu.sender())
		.filter_map(async |recipient_user| {
			self.services
				.users
				.user_is_ignored(pdu.sender(), &recipient_user)
				.await
				.is_false()
				.then_some(recipient_user)
		})
		.collect::<HashSet<_>>();

	let power_levels = self
		.services
		.state_accessor
		.get_power_levels(pdu.room_id())
		.ok();

	let (mut push_target, power_levels) = join(push_target, power_levels).boxed().await;

	if *pdu.kind() == TimelineEventType::RoomMember
		&& let Some(Ok(target_user_id)) = pdu.state_key().map(UserId::parse)
		&& self
			.services
			.users
			.is_active_local(&target_user_id)
			.await
	{
		push_target.insert(target_user_id);
	}

	if push_target.is_empty() {
		return Ok(());
	}

	let serialized = pdu.to_format();
	let (thread_root, related_events) =
		join(self.services.threads.get_thread_id(pdu), self.related_events(pdu)).await;

	let appended = Appended {
		pdu_id: &pdu_id,
		pdu,
		power_levels: power_levels.as_ref(),
		serialized: &serialized,
		thread_root: thread_root.as_deref(),
		related_events: related_events.as_ref(),
	};

	let _cork = self.db.db.cork();
	for user in &push_target {
		self.append_pdu_for_user(user, appended)
			.await
			.effect("push rule evaluation", pdu.event_id());
	}

	Ok(())
}

#[implement(super::Service)]
async fn append_pdu_for_user(
	&self,
	user: &UserId,
	Appended {
		pdu_id,
		pdu,
		power_levels,
		serialized,
		thread_root,
		related_events,
	}: Appended<'_>,
) -> Result {
	let rules_for_user = self
		.services
		.account_data
		.get_global(user, GlobalAccountDataEventType::PushRules)
		.await
		.log_err(Level::TRACE)
		.map_or_else(|_| Ruleset::server_default(user), |ev: PushRulesEvent| ev.content.global);

	let actions = self
		.get_actions(Evaluate {
			user,
			ruleset: &rules_for_user,
			power_levels,
			pdu: serialized,
			room_id: pdu.room_id(),
			related_events,
		})
		.await?;

	let notify = actions.iter().any(Action::should_notify);

	let highlight = actions.iter().any(|action| {
		matches!(action, Action::SetTweak(Tweak::Highlight(HighlightTweakValue::Yes)))
	});

	trace!(
		%user,
		event_id = %pdu.event_id(),
		actions = %actions.len(),
		notify,
		highlight,
		"Push rules evaluated",
	);

	if notify || highlight {
		let id: PduId = (*pdu_id).into();
		let notified = Notified {
			ts: now_millis(),
			sroomid: id.shortroomid,
			tag: None,
			actions: actions.into(),
		};

		self.commit_notification(user, pdu.room_id(), thread_root, id, &notified)
			.await
			.effect("notification row", pdu.event_id());
	}

	if notify || highlight || self.services.config.push_everything {
		self.get_pushkeys(user)
			.map(ToOwned::to_owned)
			.for_each(|push_key| async move {
				self.services
					.sending
					.send_pdu_push(pdu_id, user, push_key)
					.await
					.effect("push notification", pdu.event_id());
			})
			.await;
	}
	Ok(())
}

/// Commit one recipient's notification entry and its mutually exclusive
/// main/thread count pair together. Resets use the same room/user lock.
#[implement(super::Service)]
async fn commit_notification(
	&self,
	user: &UserId,
	room: &RoomId,
	thread: Option<&EventId>,
	id: PduId,
	notified: &Notified,
) -> Result {
	let _lock = self
		.notification_mutex
		.lock(&(room.to_owned(), user.to_owned()))
		.await;
	let notify = notified.actions.iter().any(Action::should_notify);
	let highlight = notified.actions.iter().any(|action| {
		matches!(action, Action::SetTweak(Tweak::Highlight(HighlightTweakValue::Yes)))
	});
	let key = match thread {
		| Some(root) => serialize_key((user, room, root))?,
		| None => serialize_key((user, room))?,
	};
	let mut txn = self.db.db.txn();
	if notify {
		stage_increment(&mut txn, &self.db.userroomid_notificationcount, &key).await?;
	}
	if highlight {
		stage_increment(&mut txn, &self.db.userroomid_highlightcount, &key).await?;
	}
	if matches!(id.count, Count::Normal(_)) {
		let key = serialize_key((user, id.count.into_unsigned()))?;
		let value = serde_json::to_vec(notified)?;
		txn.insert_raw(&self.db.useridcount_notification, key, value);
	}
	super::notification::check_mutation(&txn)?;
	txn.execute().await
}

async fn stage_increment(txn: &mut Txn, map: &Arc<Map>, key: &[u8]) -> Result {
	let old = match map.get(key).await {
		| Ok(value) => {
			let bytes: [u8; 8] = value
				.as_ref()
				.try_into()
				.map_err(|_| Error::bad_database("Invalid notification counter"))?;
			u64::from_be_bytes(bytes)
		},
		| Err(error) if error.is_not_found() => 0,
		| Err(error) => return Err(error),
	};
	let new = old
		.checked_add(1)
		.ok_or_else(|| Error::bad_database("Notification counter overflow"))?;
	txn.insert_raw(map, key, new.to_be_bytes());
	Ok(())
}
