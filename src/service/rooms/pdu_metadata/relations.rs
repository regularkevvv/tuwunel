use futures::{TryStreamExt, pin_mut};
use ruma::{
	EventId, OwnedUserId, UserId,
	api::{
		Direction,
		error::{ErrorKind, LimitExceededErrorData},
	},
	events::{reaction::ReactionEventContent, relation::RelationType},
};
use tuwunel_core::{
	Error, PduId, Result, implement, is_equal_to,
	matrix::{Event, Pdu, PduCount, RawPduId, event::RelationTypeEqual},
	utils::bytes::u64_from_bytes,
};

use super::Service;
use crate::rooms::short::ShortRoomId;

/// Shared examined-row and encoded-byte budget for one relation query,
/// including recursively fetched parents and rows whose children were purged.
#[derive(Clone, Copy, Default)]
pub struct RelationReadBudget {
	rows: usize,
	bytes: usize,
}

impl RelationReadBudget {
	pub(super) fn charge(&mut self, rows: usize, bytes: usize) -> Result {
		self.rows = self.rows.saturating_add(rows);
		self.bytes = self.bytes.saturating_add(bytes);
		if self.rows > 4096 || self.bytes > 512 * 1024 {
			return Err(Error::Request(
				ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
				"Relation read limit reached".into(),
				http::StatusCode::TOO_MANY_REQUESTS,
			));
		}
		Ok(())
	}
}

#[implement(Service)]
#[tracing::instrument(skip(self, from, to), level = "debug")]
pub async fn add_relation(&self, from: PduCount, to: PduCount) -> Result {
	const BUFSIZE: usize = size_of::<u64>() * 2;

	match (from, to) {
		| (PduCount::Normal(from), PduCount::Normal(to)) => {
			let key: &[u64] = &[to, from];

			self.db
				.tofrom_relation
				.aput_raw::<BUFSIZE, _, _>(key, [])
				.await?;
		},
		| _ => {}, // TODO: Relations with backfilled pdus
	}

	Ok(())
}

/// Query relations of an event to determine if matching any of the trailing
/// arguments. When all criteria are None the mere presence of a relation causes
/// this function to return true.
#[implement(Service)]
pub async fn event_has_relation(
	&self,
	event_id: &EventId,
	user_id: Option<&UserId>,
	rel_type: Option<&RelationType>,
	key: Option<&str>,
) -> Result<bool> {
	let pdu_id = match self.services.timeline.get_pdu_id(event_id).await {
		| Ok(pdu_id) => pdu_id,
		| Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
		| Err(error) => return Err(error),
	};

	self.has_relation(pdu_id.into(), user_id, rel_type, key)
		.await
}

/// Query relations of an event by PduId to determine if matching any of the
/// trailing arguments. When all criteria are None the mere presence of a
/// relation causes this function to return true.
#[implement(Service)]
pub async fn has_relation(
	&self,
	target: PduId,
	user_id: Option<&UserId>,
	rel_type: Option<&RelationType>,
	key: Option<&str>,
) -> Result<bool> {
	let relations = self
		.get_relations(target.shortroomid, target.count, None, Direction::Forward, None)
		.await?;
	Ok(relations.into_iter().any(|(_, pdu)| {
		if user_id.is_some_and(|user| user != pdu.sender()) {
			return false;
		}
		debug_assert!(
			key.is_none() || rel_type.is_none_or(is_equal_to!(&RelationType::Annotation)),
			"key argument only applies to Annotation type relations."
		);

		// A supplied annotation key avoids parsing the content twice.
		(key.is_some() || rel_type.is_none_or(|rel_type| rel_type.relation_type_equal(&pdu)))
			&& key.is_none_or(|key| {
				pdu.get_content()
					.map(|content: ReactionEventContent| content.relates_to.key == key)
					.unwrap_or(false)
			})
	}))
}

/// MSC3440 `related_by_*`: whether any event relates to `target` with a
/// `rel_type` in `rel_types` and a `sender` in `senders`. An empty list is
/// unconstrained on that axis; a single relating event must satisfy both.
#[implement(Service)]
pub async fn has_incoming_relation(
	&self,
	target: PduId,
	senders: &[OwnedUserId],
	rel_types: &[RelationType],
) -> Result<bool> {
	let relations = self
		.get_relations(target.shortroomid, target.count, None, Direction::Forward, None)
		.await?;
	Ok(relations.into_iter().any(|(_, pdu)| {
		let sender_matches = senders.is_empty() || senders.iter().any(is_equal_to!(pdu.sender()));

		let rel_type_matches = rel_types.is_empty()
			|| rel_types
				.iter()
				.any(|rel_type| rel_type.relation_type_equal(&pdu));

		sender_matches && rel_type_matches
	}))
}

#[implement(Service)]
pub async fn get_relations(
	&self,
	shortroomid: ShortRoomId,
	target: PduCount,
	from: Option<PduCount>,
	dir: Direction,
	user_id: Option<&UserId>,
) -> Result<Vec<(PduCount, Pdu)>> {
	self.get_relations_bounded(
		shortroomid,
		target,
		from,
		dir,
		user_id,
		&mut RelationReadBudget::default(),
	)
	.await
}

/// Validate the complete parent index before returning any selected children.
/// Only genuinely missing child rows are omitted, matching history purge's
/// documented dangling-index policy. Every other read/encoding/binding error
/// refuses the query, including errors after an earlier valid match.
#[implement(Service)]
pub async fn get_relations_bounded(
	&self,
	shortroomid: ShortRoomId,
	target: PduCount,
	from: Option<PduCount>,
	dir: Direction,
	user_id: Option<&UserId>,
	budget: &mut RelationReadBudget,
) -> Result<Vec<(PduCount, Pdu)>> {
	if matches!(target, PduCount::Backfilled(_)) {
		return Ok(Vec::new());
	}
	let parent_id: RawPduId = PduId { shortroomid, count: target }.into();
	let parent = self
		.relation_pdu(&parent_id, budget)
		.await?
		.ok_or_else(|| Error::bad_database("Missing relation parent event"))?;
	let target = target.to_be_bytes();

	let mut counts = Vec::new();
	{
		let rows = self.db.tofrom_relation.raw_stream_prefix(&target);
		pin_mut!(rows);
		while let Some((key, value)) = rows.try_next().await? {
			budget.charge(1, key.len().saturating_add(value.len()))?;
			if key.len() != 16 || !value.is_empty() {
				return Err(Error::bad_database("Invalid relation index record"));
			}
			let count = u64_from_bytes(&key[8..])
				.map(PduCount::from_unsigned)
				.map_err(|_| Error::bad_database("Invalid relation child count"))?;
			if !matches!(count, PduCount::Normal(_)) {
				return Err(Error::bad_database("Invalid relation child count"));
			}
			if from.is_none_or(|from| match dir {
				| Direction::Forward => count > from,
				| Direction::Backward => count < from,
			}) {
				counts.push(count);
			}
		}
	}
	if dir == Direction::Backward {
		counts.reverse();
	}
	let mut children = Vec::new();
	for count in counts {
		let id: RawPduId = PduId { shortroomid, count }.into();
		let Some(mut pdu) = self.relation_pdu(&id, budget).await? else {
			continue;
		};
		if pdu.room_id() != parent.room_id() {
			return Err(Error::bad_database("Mismatched relation room"));
		}
		if !pdu.is_redacted() {
			let content = pdu
				.get_content::<serde_json::Value>()
				.map_err(|_| Error::bad_database("Invalid relation event content"))?;
			let relation = &content["m.relates_to"];
			let relates = relation["event_id"]
				.as_str()
				.or_else(|| relation["m.in_reply_to"]["event_id"].as_str());
			if relates != Some(parent.event_id().as_str()) {
				return Err(Error::bad_database("Mismatched relation target"));
			}
		}
		pdu.remove_transaction_id_unless_sender(user_id);
		children.push((count, pdu));
	}
	Ok(children)
}

#[implement(Service)]
pub(super) async fn relation_pdu(
	&self,
	id: &RawPduId,
	budget: &mut RelationReadBudget,
) -> Result<Option<Pdu>> {
	let value = match self.services.db["pduid_pdu"].get(id).await {
		| Ok(value) => value,
		| Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
		| Err(error) => return Err(error),
	};
	budget.charge(0, value.len())?;
	let pdu = serde_json::from_slice::<Pdu>(&value)
		.map_err(|_| Error::bad_database("Invalid stored relation event"))?;
	let canonical = self
		.services
		.timeline
		.get_pdu_id(pdu.event_id())
		.await
		.map_err(|error| {
			if error.kind() == ErrorKind::NotFound {
				Error::bad_database("Missing relation event reverse mapping")
			} else {
				error
			}
		})?;
	let room = self
		.services
		.short
		.get_shortroomid(pdu.room_id())
		.await
		.map_err(|error| {
			if error.kind() == ErrorKind::NotFound {
				Error::bad_database("Missing relation room mapping")
			} else {
				error
			}
		})?;
	if canonical != *id || room != u64_from_bytes(&id.shortroomid())? {
		return Err(Error::bad_database("Mismatched relation event binding"));
	}
	Ok(Some(pdu))
}
