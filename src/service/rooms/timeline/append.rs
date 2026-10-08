use std::{collections::BTreeMap, sync::Arc};

use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, EventId, UserId,
	events::{
		TimelineEventType,
		receipt::ReceiptThread,
		relation::RelationType,
		room::{
			encrypted::Relation,
			member::{MembershipState, RoomMemberEventContent},
		},
	},
};
use tuwunel_core::{
	Result, err, error, implement,
	matrix::{
		event::Event,
		pdu::{PduCount, PduEvent, PduId, RawPduId},
		room_version,
	},
	smallvec::SmallVec,
	utils,
};
use tuwunel_database::{Json, Txn};

use super::{ExtractBody, ExtractRelatesTo, ExtractRelatesToEventId, RoomMutexGuard, bias_count};
use crate::rooms::{
	read_receipt::PrivateRead,
	short::{ShortRoomId, ShortStateHash},
	state_accessor::plain_text_topic,
	state_cache::MembershipUpdate,
	state_compressor::CompressedState,
};

type Band<'a> = SmallVec<[&'a EventId; 1]>;

/// The outcome of one effect of a durable pdu, logged when it failed.
///
/// A pdu's effects follow the commit that stores it, each on its own: push
/// counts, notification rows and pushes, membership, the search index,
/// relations and threads, appservice delivery, and, for a local pdu, the queue
/// to the room's other servers. One that fails is logged at error level under
/// its name, with the pdu's event id and never its content, and the rest still
/// run. The pdu stays sent: its sender is told so, and a retry of the sender's
/// transaction finds the record and runs nothing again.
///
/// So each effect runs at most once per live process. Nothing records an
/// effect as owed, so one that failed, or one a crash cut short, is not
/// replayed.
pub(crate) trait Effect {
	fn effect(self, name: &'static str, event_id: &EventId);
}

impl Effect for Result {
	fn effect(self, name: &'static str, event_id: &EventId) {
		if let Err(e) = self {
			error!(%event_id, effect = name, "An effect of a stored event failed: {e}");
		}
	}
}

/// Append the incoming event setting the state snapshot to the state from
/// the server that sent the event.
#[implement(super::Service)]
#[tracing::instrument(
	name = "append_incoming",
	level = "debug",
	skip_all,
	ret(Debug)
)]
pub(crate) async fn append_incoming_pdu<'a, Leafs>(
	&'a self,
	pdu: &'a PduEvent,
	pdu_json: CanonicalJsonObject,
	new_room_leafs: Leafs,
	state_ids_compressed: Arc<CompressedState>,
	soft_fail: bool,
	state_lock: &'a RoomMutexGuard,
) -> Result<Option<RawPduId>>
where
	Leafs: Iterator<Item = &'a EventId> + Send + 'a,
{
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	// We append to state before appending the pdu, so we don't have a moment in
	// time with the pdu without it's state. This is okay because append_pdu can't
	// fail.
	services_root
		.state
		.set_event_state(&pdu.event_id, &pdu.room_id, state_ids_compressed)
		.await?;

	if soft_fail {
		services_root
			.pdu_metadata
			.mark_as_referenced(&pdu.room_id, pdu.prev_events.iter().map(AsRef::as_ref))
			.await?;

		// Keep the previous band rather than let a soft-failed event empty it; a
		// later accepted event self-chains and heals it.
		if let Some(new_room_leafs) = nonempty_band(new_room_leafs) {
			services_root
				.state
				.set_forward_extremities(&pdu.room_id, new_room_leafs.into_iter(), state_lock)
				.await;
		}

		return Ok(None);
	}

	// The event handler made the resolved state current before this call.
	let pdu_id = self
		.append_pdu(pdu, pdu_json, new_room_leafs, None, state_lock)
		.await?;

	Ok(Some(pdu_id))
}

fn nonempty_band<'a, Leafs>(leafs: Leafs) -> Option<Band<'a>>
where
	Leafs: Iterator<Item = &'a EventId>,
{
	let leafs: Band<'_> = leafs.collect();

	(!leafs.is_empty()).then_some(leafs)
}

/// Creates a new persisted data unit and adds it to a room.
///
/// By this point the incoming event should be fully authenticated, no auth
/// happens in `append_pdu`.
///
/// `room_state` is the room state after the event, for a caller that has not
/// made it current yet. It becomes current in the commit that stores the
/// event, before the event's count retires. Sync bounds its timeline by the
/// retired count while `required_state` and `/members` read current state, so
/// the state has to be current by the time any sync can deliver the event. A
/// caller that set the state after this returned let a sync pair a membership
/// change in its timeline with the membership it replaced. A state commit
/// that followed the event's, and failed, did the same: the count still
/// retired, and the stale pointer stayed across restarts.
///
/// Returns the pdu id, or an error only when the pdu did not commit. The pdu's
/// effects follow its commit, and one that fails is logged, not returned
/// (`Effect`).
#[implement(super::Service)]
#[inline]
pub async fn append_pdu<'a, Leafs>(
	&'a self,
	pdu: &'a PduEvent,
	pdu_json: CanonicalJsonObject,
	leafs: Leafs,
	room_state: Option<ShortStateHash>,
	state_lock: &'a RoomMutexGuard,
) -> Result<RawPduId>
where
	Leafs: Iterator<Item = &'a EventId> + Send + 'a,
{
	self.append_pdu_with_txnid(pdu, pdu_json, leafs, None, room_state, state_lock)
		.await
}

/// [`Self::append_pdu`], also recording a client transaction id in the same
/// commit as the event.
///
/// `txnid` is a `userdevicetxnid_response` key from
/// [`crate::transaction_ids::key`], written with the event id as its value.
#[implement(super::Service)]
#[tracing::instrument(name = "append", level = "debug", skip_all, ret(Debug))]
pub async fn append_pdu_with_txnid<'a, Leafs>(
	&'a self,
	pdu: &'a PduEvent,
	mut pdu_json: CanonicalJsonObject,
	leafs: Leafs,
	txnid: Option<&'a [u8]>,
	room_state: Option<ShortStateHash>,
	state_lock: &'a RoomMutexGuard,
) -> Result<RawPduId>
where
	Leafs: Iterator<Item = &'a EventId> + Send + 'a,
{
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	// Coalesce database writes for the remainder of this scope.
	let _cork = self.db.db.cork_and_flush();

	let shortroomid = services_root
		.short
		.get_shortroomid(pdu.room_id())
		.await
		.map_err(|_| err!(Database("Room does not exist")))?;

	// Make unsigned fields correct. This is not properly documented in the spec,
	// but state events need to have previous content in the unsigned field, so
	// clients can easily interpret things like membership changes
	if let Some(state_key) = pdu.state_key() {
		if let CanonicalJsonValue::Object(unsigned) = pdu_json
			.entry("unsigned".into())
			.or_insert_with(|| CanonicalJsonValue::Object(BTreeMap::default()))
		{
			if let Ok(shortstatehash) = services_root
				.state
				.pdu_shortstatehash(pdu.event_id())
				.await && let Ok(prev_state) = services_root
				.state_accessor
				.state_get(shortstatehash, &pdu.kind().to_string().into(), state_key)
				.await
			{
				unsigned.insert(
					"prev_content".into(),
					CanonicalJsonValue::Object(
						utils::to_canonical_object(prev_state.get_content_as_value()).map_err(
							|e| {
								err!(Database(error!(
									"Failed to convert prev_state to canonical JSON: {e}",
								)))
							},
						)?,
					),
				);
				unsigned.insert(
					"prev_sender".into(),
					CanonicalJsonValue::String(prev_state.sender().to_string()),
				);
				unsigned.insert(
					"replaces_state".into(),
					CanonicalJsonValue::String(prev_state.event_id().to_string()),
				);
			}
		} else {
			error!("Invalid unsigned type in pdu.");
		}
	}

	let insert_lock = self.mutex_insert.lock(pdu.room_id()).await;
	let next_count = services_root.globals.next_count().await?;

	let count = PduCount::Normal(*next_count);
	let pdu_id: RawPduId = PduId { shortroomid, count }.into();

	// One commit stores the pdu, marks the events it references, and makes it
	// the room's frontier. When `room_state` is given, the same commit makes
	// that state current. A failed commit leaves none of them, so the pdu is
	// never visible beside the state it replaced, and the frontier never names
	// a pdu that was not stored.
	let mut txn = self.append_pdu_txn(
		&pdu_id,
		pdu,
		&pdu_json,
		txnid,
		room_state.map(|room_state| (room_state, state_lock)),
	);

	// We must keep track of all events that have been referenced.
	services_root.pdu_metadata.mark_as_referenced_txn(
		&mut txn,
		pdu.room_id(),
		pdu.prev_events().map(AsRef::as_ref),
	);

	services_root
		.state
		.set_forward_extremities_txn(&mut txn, pdu.room_id(), leafs, state_lock)
		.await;

	let notifications = services_root
		.pusher
		.stage_notification_plan(&mut txn, pdu_id, pdu, room_state)
		.await?;

	let sender_read = services_root
		.read_receipt
		.stage_private_read(
			PrivateRead {
				room_id: pdu.room_id(),
				user_id: pdu.sender(),
				count: *next_count,
				ts: pdu.origin_server_ts(),
				thread: &ReceiptThread::Unthreaded,
				announce: false,
			},
			&mut txn,
			state_lock,
		)
		.await?;

	txn.execute().await?;
	drop(notifications);

	if let Some(sender_read) = sender_read {
		sender_read.committed(&services_root.pusher);
	}

	drop(insert_lock);

	// The pdu is durable. Nothing below returns an error: an effect that fails
	// is logged and the rest still run (`Effect`).
	let event_id = pdu.event_id();

	// Only local senders can own pushers.
	if services_root.globals.user_is_local(pdu.sender()) {
		services_root
			.sending
			.refresh_push_badge(pdu.sender())
			.await
			.effect("push badge", event_id);
	}

	services_root
		.pusher
		.append_pdu(pdu_id, pdu, state_lock)
		.await
		.effect("push", event_id);

	self.append_pdu_effects(pdu_id, pdu, shortroomid, count, state_lock)
		.await;

	// A recount that failed to commit, an earlier event's or this event's own,
	// is retried here, before this event goes to the room's servers.
	services_root
		.state_cache
		.repair_joined_count(pdu.room_id())
		.await
		.effect("joined count repair", event_id);

	drop(next_count);

	services_root
		.appservice
		.append_pdu(pdu_id, pdu)
		.await
		.effect("appservice delivery", event_id);

	Ok(pdu_id)
}

/// The effects a durable pdu's type and relations call for.
///
/// Each runs whether or not the ones before it failed (`Effect`).
#[implement(super::Service)]
async fn append_pdu_effects(
	&self,
	pdu_id: RawPduId,
	pdu: &PduEvent,
	shortroomid: ShortRoomId,
	count: PduCount,
	state_lock: &RoomMutexGuard,
) {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let event_id = pdu.event_id();

	match *pdu.kind() {
		| TimelineEventType::RoomRedaction => self
			.append_redaction_effects(pdu, shortroomid, state_lock)
			.await
			.effect("redaction", event_id),
		| TimelineEventType::RoomMember => self
			.append_member_effects(pdu, count)
			.await
			.effect("membership", event_id),
		| TimelineEventType::RoomMessage => {
			// A body that is not a string leaves nothing to index.
			let body = pdu
				.get_content::<ExtractBody>()
				.ok()
				.and_then(|content| content.body);

			if let Some(body) = body {
				services_root
					.search
					.index_pdu(shortroomid, &pdu_id, &body)
					.await
					.effect("search index", event_id);

				if services_root
					.admin
					.is_admin_command(pdu, &body)
					.await
				{
					services_root
						.admin
						.command(body, Some(event_id.into()))
						.await
						.effect("admin command", event_id);
				}
			}
		},
		| TimelineEventType::RoomTopic =>
			if let Some(topic) = pdu.get_content().ok().and_then(plain_text_topic) {
				services_root
					.search
					.index_pdu(shortroomid, &pdu_id, &topic)
					.await
					.effect("search index", event_id);
			},
		| _ => {},
	}

	// The cached hierarchy summary projects room state; evict on any state change.
	if pdu.state_key().is_some() {
		services_root
			.spaces
			.cache_evict(pdu.room_id())
			.await
			.effect("space hierarchy cache", event_id);
	}

	if let Ok(content) = pdu.get_content::<ExtractRelatesToEventId>()
		&& let Ok(related_pducount) = self
			.get_pdu_count(&content.relates_to.event_id)
			.await
	{
		services_root
			.pdu_metadata
			.add_relation(count, related_pducount)
			.await
			.effect("relation", event_id);
	}

	if let Ok(content) = pdu.get_content::<ExtractRelatesTo>() {
		match content.relates_to {
			| Relation::Reply(ruma::events::relation::Reply { in_reply_to }) => {
				// We need to do it again here, because replies don't have
				// event_id as a top level field
				if let Ok(related_pducount) = self.get_pdu_count(&in_reply_to.event_id).await {
					services_root
						.pdu_metadata
						.add_relation(count, related_pducount)
						.await
						.effect("relation", event_id);
				}
			},
			| Relation::Thread(thread) => {
				services_root
					.threads
					.add_to_thread(&thread.event_id, pdu_id, pdu)
					.await
					.effect("thread", event_id);
			},
			| Relation::Replacement(replacement) => {
				services_root
					.pdu_metadata
					.add_typed_relation(
						shortroomid,
						count,
						&replacement.event_id,
						pdu,
						RelationType::Replacement,
					)
					.await
					.effect("typed relation", event_id);
			},
			| Relation::Reference(reference) => {
				services_root
					.pdu_metadata
					.add_typed_relation(
						shortroomid,
						count,
						&reference.event_id,
						pdu,
						RelationType::Reference,
					)
					.await
					.effect("typed relation", event_id);
			},
			| _ => {}, // TODO: Aggregate other types
		}
	}
}

/// Redacts the event a redaction names, when its sender may redact it.
#[implement(super::Service)]
async fn append_redaction_effects(
	&self,
	pdu: &PduEvent,
	shortroomid: ShortRoomId,
	state_lock: &RoomMutexGuard,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let room_version = services_root
		.state
		.get_room_version(pdu.room_id())
		.await?;

	let room_rules = room_version::rules(&room_version)?;

	let redacts_id = pdu.redacts_id(&room_rules);

	if let Some(redacts_id) = &redacts_id
		&& services_root
			.state_accessor
			.user_can_redact(redacts_id, pdu.sender(), pdu.room_id(), false)
			.await?
	{
		self.redact_pdu(redacts_id, pdu, shortroomid, state_lock)
			.await?;
	}

	Ok(())
}

/// Record the membership transition an `m.room.member` event carries.
///
/// The cache is written here rather than off the resolved state so that a
/// user who is invited or knocked and leaves immediately still leaves the
/// earlier event on record for auth.
#[implement(super::Service)]
async fn append_member_effects(&self, pdu: &PduEvent, count: PduCount) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let Some(state_key) = pdu.state_key() else {
		return Ok(());
	};

	let user_id = UserId::parse(state_key).expect("This state_key was previously validated");
	// The parse error would quote the content, which the log must not carry.
	let content: RoomMemberEventContent = pdu
		.get_content()
		.map_err(|_| err!("the member event's content does not parse"))?;
	let is_invite = content.membership == MembershipState::Invite;
	let is_direct = content.is_direct;

	let stripped_state = match content.membership {
		| MembershipState::Invite | MembershipState::Knock => services_root
			.state
			.summary_stripped(pdu)
			.await
			.into(),
		| _ => None,
	};

	services_root
		.state_cache
		.update_membership(MembershipUpdate {
			room_id: pdu.room_id(),
			user_id: &user_id,
			membership_event: content,
			sender: pdu.sender(),
			last_state: stripped_state,
			invite_via: None,
			update_joined_count: true,
			count,
		})
		.await?;

	if is_invite {
		services_root
			.membership
			.auto_accept(pdu.room_id(), &user_id, pdu.sender(), is_direct);
	}

	Ok(())
}

/// The single commit that makes an accepted event durable.
///
/// It carries the event, its id and timestamp indexes, and the outlier
/// removal. When `txnid` is given, it also carries the sending client's
/// transaction record, with the event id as its value. When `room_state` is
/// given, it makes that state the room's current state, under the room's
/// state lock. Returned unexecuted so the commit's contents can be inspected.
/// [`Self::append_pdu`] adds the events the pdu references and the room's new
/// frontier, then executes it.
#[implement(super::Service)]
pub fn append_pdu_txn(
	&self,
	pdu_id: &RawPduId,
	pdu: &PduEvent,
	json: &CanonicalJsonObject,
	txnid: Option<&[u8]>,
	room_state: Option<(ShortStateHash, &RoomMutexGuard)>,
) -> Txn {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	debug_assert!(matches!(pdu_id.pdu_count(), PduCount::Normal(_)), "PduCount not Normal");

	let mut txn = self.db.db.txn();

	txn.raw_put(&self.db.pduid_pdu, pdu_id, Json(json));
	txn.insert_raw(&self.db.eventid_pduid, pdu.event_id.as_bytes(), pdu_id);
	txn.del_raw(&self.db.eventid_outlierpdu, pdu.event_id.as_bytes());

	let count_key = bias_count(pdu_id.count());
	let ts = u64::from(pdu.origin_server_ts);
	let key = (pdu.room_id(), ts, count_key);
	txn.put_raw(&self.db.roomid_tscount_pducount, key, pdu_id.count());

	if let Some(txnid) = txnid {
		txn.insert_raw(&self.db.userdevicetxnid_response, txnid, pdu.event_id.as_bytes());
	}

	if let Some((room_state, state_lock)) = room_state {
		services_root
			.state
			.set_room_state_txn(&mut txn, pdu.room_id(), room_state, state_lock);
	}

	txn
}

#[cfg(test)]
mod tests {
	use std::iter::empty;

	use ruma::event_id;

	use super::*;

	#[test]
	fn empty_band_is_skipped() {
		assert!(nonempty_band(empty::<&EventId>()).is_none());
	}

	#[test]
	fn nonempty_band_preserves_all_leaves() {
		let leaves = [event_id!("$a:test.local"), event_id!("$b:test.local")];

		let kept: Vec<&EventId> = nonempty_band(leaves.iter().copied())
			.expect("non-empty band retained")
			.into_iter()
			.collect();

		assert_eq!(kept, leaves);
	}
}
