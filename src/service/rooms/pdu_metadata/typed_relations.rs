use futures::{StreamExt, TryStreamExt, pin_mut};
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, EventId, OwnedEventId, RoomId,
	api::error::ErrorKind,
	events::{relation::RelationType, room::encrypted::Relation},
};
use tuwunel_core::{
	Error, Result,
	arrayvec::ArrayVec,
	implement,
	matrix::{Event, Pdu, PduCount, PduId, RawPduId},
	utils::{bytes::u64_from_bytes, stream::TryIgnore, u64_from_u8},
};

use super::{ExtractRelatesTo, RelationReadBudget, Service};
use crate::rooms::short::ShortRoomId;

/// `relatesto_typed` key buffer, sized to the writer's fixed length.
///
/// A key that fails the slice conversion cannot be a relation row, the
/// writer emitting no other length.
pub(crate) type Key = ArrayVec<u8, KEY_LEN>;

type Prefix = ArrayVec<u8, PREFIX_LEN>;

/// `relatesto_typed` rel_type discriminant, occupying one key byte between the
/// parent `RawPduId` and the child's ts. Stable on-disk format; the explicit
/// discriminants are permanent and must stay distinct.
#[derive(Clone, Copy)]
pub(super) enum Tag {
	Replace = 0x01,
	Reference = 0x02,
}

impl From<Tag> for u8 {
	#[inline]
	fn from(tag: Tag) -> Self {
		match tag {
			| Tag::Replace => 0x01,
			| Tag::Reference => 0x02,
		}
	}
}

/// `relatesto_typed` seek prefix: `shortroomid || parent_count || tag`.
pub(super) const PREFIX_LEN: usize = size_of::<u64>() * 2 + size_of::<u8>();

/// `relatesto_typed` key: the prefix followed by `child_ts || child_count`.
pub(super) const KEY_LEN: usize = PREFIX_LEN + size_of::<u64>() * 2;

/// `relatesto_typed` key: byte offset of the child `PduCount` (the key tail).
pub(super) const CHILD_COUNT_OFFSET: usize = KEY_LEN - size_of::<u64>();

/// Complete typed inventory before selecting an edit or capped references.
/// Only genuine purged-child absence is omitted. Every typed row, loaded PDU
/// and compact binding contributes to the caller's shared read budget.
#[implement(Service)]
pub(super) async fn typed_children(
	&self,
	parent: &Pdu,
	tag: Tag,
	budget: &mut RelationReadBudget,
) -> Result<Vec<Pdu>> {
	let parent_id: PduId = match self
		.services
		.timeline
		.get_pdu_id(parent.event_id())
		.await
	{
		| Ok(id) => id.into(),
		| Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
		| Err(error) => return Err(error),
	};
	if matches!(parent_id.count, PduCount::Backfilled(_)) {
		return Ok(Vec::new());
	}
	let stored = self
		.relation_pdu(&parent_id.into(), budget)
		.await?
		.ok_or_else(|| Error::bad_database("Missing typed relation parent"))?;
	if stored.event_id() != parent.event_id() || stored.room_id() != parent.room_id() {
		return Err(Error::bad_database("Mismatched typed relation parent"));
	}
	let prefix = prefix(parent_id.shortroomid, parent_id.count, tag);
	let mut prepared = Vec::new();
	{
		let rows = self
			.db
			.relatesto_typed
			.raw_stream_prefix(prefix.as_slice());
		pin_mut!(rows);
		while let Some((key, value)) = rows.try_next().await? {
			budget.charge(1, key.len().saturating_add(value.len()))?;
			if key.len() != KEY_LEN || value.len() != size_of::<u64>() {
				return Err(Error::bad_database("Invalid typed relation index record"));
			}
			let ts = u64_from_bytes(&key[PREFIX_LEN..CHILD_COUNT_OFFSET])?;
			let count = PduCount::from_unsigned(u64_from_bytes(&key[CHILD_COUNT_OFFSET..])?);
			if !matches!(count, PduCount::Normal(value) if value > 0) {
				return Err(Error::bad_database("Invalid typed relation child count"));
			}
			prepared.push((ts, count, u64_from_bytes(value)?));
		}
	}
	let mut children = Vec::new();
	for (ts, count, short) in prepared {
		let event_id = self.typed_child_event_id(short, budget).await?;
		let child_id: RawPduId = PduId {
			shortroomid: parent_id.shortroomid,
			count,
		}
		.into();
		let Some(child) = self.relation_pdu(&child_id, budget).await? else {
			match self.services.timeline.get_pdu_id(&event_id).await {
				| Err(error) if error.kind() == ErrorKind::NotFound => continue,
				| Err(error) => return Err(error),
				| Ok(_) =>
					return Err(Error::bad_database("Mismatched missing typed relation child")),
			}
		};
		if child.event_id().as_str() != event_id.as_str()
			|| child.room_id() != parent.room_id()
			|| u64::from(child.origin_server_ts().get()) != ts
		{
			return Err(Error::bad_database("Mismatched typed relation child"));
		}
		if child.is_redacted() {
			continue;
		}
		let content = child
			.get_content::<ExtractRelatesTo>()
			.map_err(|_| Error::bad_database("Invalid typed relation content"))?;
		let target = match (tag, content.relates_to) {
			| (Tag::Replace, Relation::Replacement(relation)) => relation.event_id,
			| (Tag::Reference, Relation::Reference(relation)) => relation.event_id,
			| _ => return Err(Error::bad_database("Mismatched typed relation kind")),
		};
		if target.as_str() != parent.event_id().as_str() {
			return Err(Error::bad_database("Mismatched typed relation target"));
		}
		children.push(child);
	}
	Ok(children)
}

#[implement(Service)]
async fn typed_child_event_id(
	&self,
	short: u64,
	budget: &mut RelationReadBudget,
) -> Result<OwnedEventId> {
	if short == 0 {
		return Err(Error::bad_database("Invalid zero typed child compact ID"));
	}
	let value = self.services.db["shorteventid_eventid"]
		.get(&short.to_be_bytes())
		.await
		.map_err(|error| {
			if error.kind() == ErrorKind::NotFound {
				Error::bad_database("Missing typed child reverse mapping")
			} else {
				error
			}
		})?;
	budget.charge(0, value.len())?;
	let event_id = std::str::from_utf8(&value)
		.map_err(|_| Error::bad_database("Invalid typed child event encoding"))?;
	let event_id = EventId::parse(event_id)
		.map_err(|_| Error::bad_database("Invalid typed child event ID"))?;
	let forward = self.services.db["eventid_shorteventid"]
		.get(&event_id)
		.await
		.map_err(|error| {
			if error.kind() == ErrorKind::NotFound {
				Error::bad_database("Missing typed child forward mapping")
			} else {
				error
			}
		})?;
	budget.charge(0, forward.len())?;
	if u64_from_bytes(&forward)
		.map_err(|_| Error::bad_database("Invalid typed child compact ID"))?
		!= short
	{
		return Err(Error::bad_database("Mismatched typed child compact ID"));
	}
	Ok(event_id)
}

/// Maintain the `rel_type`-aware relation index for an `m.replace` or
/// `m.reference` child of `parent`. The row is keyed by the parent so a serve
/// of `parent` seeks its newest edit (or its references) without loading
/// non-matching children. Indexed unconditionally; only the read fold is gated.
#[implement(Service)]
#[tracing::instrument(skip(self, child), level = "debug")]
pub async fn add_typed_relation<E: Event>(
	&self,
	shortroomid: ShortRoomId,
	child_count: PduCount,
	parent: &EventId,
	child: &E,
	rel_type: RelationType,
) -> Result {
	let Some(tag) = tag(&rel_type) else {
		return Ok(());
	};

	let parent_id: PduId = match self.services.timeline.get_pdu_id(parent).await {
		| Ok(id) => id.into(),
		| Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
		| Err(error) => return Err(error),
	};
	if parent_id.shortroomid != shortroomid {
		return Err(Error::bad_database("Mismatched typed relation parent room"));
	}
	let parent_count = parent_id.count;

	let (PduCount::Normal(_), PduCount::Normal(_)) = (parent_count, child_count) else {
		return Ok(()); // backfilled relations are not indexed
	};

	let child_short = self
		.services
		.short
		.get_or_create_shorteventid(child.event_id())
		.await?;

	let child_ts = u64::from(child.origin_server_ts().get());
	let key = key(shortroomid, parent_count, tag, child_ts, child_count);

	self.db
		.relatesto_typed
		.aput_raw::<KEY_LEN, _, _>(key.as_slice(), child_short.to_be_bytes())
		.await
}

fn tag(rel_type: &RelationType) -> Option<Tag> {
	match rel_type {
		| RelationType::Replacement => Some(Tag::Replace),
		| RelationType::Reference => Some(Tag::Reference),
		| _ => None,
	}
}

pub(super) fn key(
	shortroomid: ShortRoomId,
	parent: PduCount,
	tag: Tag,
	child_ts: u64,
	child: PduCount,
) -> Key {
	let mut key = ArrayVec::new();

	key.extend(shortroomid.to_be_bytes());
	key.extend(parent.to_be_bytes());
	key.push(u8::from(tag));
	key.extend(child_ts.to_be_bytes());
	key.extend(child.to_be_bytes());
	key
}

/// Remove the `relatesto_typed` row for a redacted `m.replace` or `m.reference`
/// child. Storage hygiene for edits and references; checked reads also reject
/// inconsistent retained children. Call before the
/// child's content is stripped, while its relation fields are still readable.
#[implement(Service)]
#[tracing::instrument(skip_all, level = "debug")]
pub async fn delete_typed_relation(&self, child_id: &RawPduId, child: &CanonicalJsonObject) {
	let Some(relates_to) = child
		.get("content")
		.and_then(CanonicalJsonValue::as_object)
		.and_then(|content| content.get("m.relates_to"))
		.and_then(CanonicalJsonValue::as_object)
	else {
		return;
	};

	let tag = match relates_to
		.get("rel_type")
		.and_then(CanonicalJsonValue::as_str)
	{
		| Some("m.replace") => Tag::Replace,
		| Some("m.reference") => Tag::Reference,
		| _ => return,
	};

	let Some(parent) = relates_to
		.get("event_id")
		.and_then(CanonicalJsonValue::as_str)
		.and_then(|parent| EventId::parse(parent).ok())
	else {
		return;
	};

	let Some(child_ts) = child
		.get("origin_server_ts")
		.and_then(CanonicalJsonValue::as_integer)
		.and_then(|ts| u64::try_from(i64::from(ts)).ok())
	else {
		return;
	};

	let child_count = child_id.pdu_count();
	let shortroomid = u64_from_u8(&child_id.shortroomid());

	let Ok(parent_count) = self
		.services
		.timeline
		.get_pdu_count(&parent)
		.await
	else {
		return;
	};

	let (PduCount::Normal(_), PduCount::Normal(_)) = (parent_count, child_count) else {
		return;
	};

	let key = key(shortroomid, parent_count, tag, child_ts, child_count);

	self.db
		.relatesto_typed
		.remove(key.as_slice())
		.await
		.expect("database write error");
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn delete_all_relatesto_typed_for_room(&self, room_id: &RoomId) -> Result {
	let Ok(shortroomid) = self.services.short.get_shortroomid(room_id).await else {
		return Ok(());
	};

	self.db
		.relatesto_typed
		.keys_prefix_raw(&shortroomid)
		.ignore_err()
		.for_each(|key| async move {
			self.db
				.relatesto_typed
				.remove(key)
				.await
				.expect("database write error");
		})
		.await;

	Ok(())
}

pub(super) fn prefix(shortroomid: ShortRoomId, parent: PduCount, tag: Tag) -> Prefix {
	let mut prefix = ArrayVec::new();

	prefix.extend(shortroomid.to_be_bytes());
	prefix.extend(parent.to_be_bytes());
	prefix.push(u8::from(tag));
	prefix
}
