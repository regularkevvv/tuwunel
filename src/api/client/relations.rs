use std::collections::{HashSet, VecDeque};

use axum::extract::State;
use futures::{FutureExt, future::try_join3};
use ruma::{
	EventId, RoomId, UInt, UserId,
	api::{
		Direction,
		client::relations::{
			get_relating_events, get_relating_events_with_rel_type,
			get_relating_events_with_rel_type_and_event_type,
		},
		error::{ErrorKind, LimitExceededErrorData},
	},
	events::{TimelineEventType, relation::RelationType},
};
use tuwunel_core::{
	Err, Error, Result, at, err,
	matrix::{
		event::{Event, RelationTypeEqual},
		pdu::{PduCount, PduEvent, PduId},
	},
	utils::{BoolExt, json::serialized_len, result::FlatOk},
};
use tuwunel_service::{Services, rooms::pdu_metadata::RelationReadBudget};

use crate::{Ruma, client::is_ignored_pdu};

/// # `GET /_matrix/client/r0/rooms/{roomId}/relations/{eventId}/{relType}/{eventType}`
pub(crate) async fn get_relating_events_with_rel_type_and_event_type_route(
	State(services): State<crate::State>,
	body: Ruma<get_relating_events_with_rel_type_and_event_type::v1::Request>,
) -> Result<get_relating_events_with_rel_type_and_event_type::v1::Response> {
	paginate_relations_with_filter(
		&services,
		body.sender_user(),
		&body.room_id,
		&body.event_id,
		body.event_type.clone().into(),
		body.rel_type.clone().into(),
		body.from.as_deref(),
		body.to.as_deref(),
		body.limit,
		body.recurse,
		body.dir,
	)
	.await
	.map(|res| get_relating_events_with_rel_type_and_event_type::v1::Response {
		chunk: res.chunk,
		next_batch: res.next_batch,
		prev_batch: res.prev_batch,
		recursion_depth: res.recursion_depth,
	})
}

/// # `GET /_matrix/client/r0/rooms/{roomId}/relations/{eventId}/{relType}`
pub(crate) async fn get_relating_events_with_rel_type_route(
	State(services): State<crate::State>,
	body: Ruma<get_relating_events_with_rel_type::v1::Request>,
) -> Result<get_relating_events_with_rel_type::v1::Response> {
	paginate_relations_with_filter(
		&services,
		body.sender_user(),
		&body.room_id,
		&body.event_id,
		None,
		body.rel_type.clone().into(),
		body.from.as_deref(),
		body.to.as_deref(),
		body.limit,
		body.recurse,
		body.dir,
	)
	.await
	.map(|res| get_relating_events_with_rel_type::v1::Response {
		chunk: res.chunk,
		next_batch: res.next_batch,
		prev_batch: res.prev_batch,
		recursion_depth: res.recursion_depth,
	})
}

/// # `GET /_matrix/client/r0/rooms/{roomId}/relations/{eventId}`
pub(crate) async fn get_relating_events_route(
	State(services): State<crate::State>,
	body: Ruma<get_relating_events::v1::Request>,
) -> Result<get_relating_events::v1::Response> {
	paginate_relations_with_filter(
		&services,
		body.sender_user(),
		&body.room_id,
		&body.event_id,
		None,
		None,
		body.from.as_deref(),
		body.to.as_deref(),
		body.limit,
		body.recurse,
		body.dir,
	)
	.await
}

#[expect(clippy::too_many_arguments)]
#[tracing::instrument(
	name = "relations",
	level = "debug",
	skip_all,
	fields(room_id, target, from, to, dir, limit, recurse)
)]
async fn paginate_relations_with_filter(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	target: &EventId,
	filter_event_type: Option<TimelineEventType>,
	filter_rel_type: Option<RelationType>,
	from: Option<&str>,
	to: Option<&str>,
	limit: Option<UInt>,
	recurse: bool,
	dir: Direction,
) -> Result<get_relating_events::v1::Response> {
	let from: Option<PduCount> = from.map(str::parse).transpose()?;

	let to: Option<PduCount> = to.map(str::parse).transpose()?;

	// Spec (v1.10) recommends depth of at least 3
	let max_depth: usize = if recurse { 3 } else { 0 };

	let limit: usize = limit
		.map(TryInto::try_into)
		.flat_ok()
		.unwrap_or(30)
		.min(100);

	let target_event_id: &EventId = target;

	let target = services
		.timeline
		.get_pdu_id(target)
		.map(|result| match result {
			| Ok(id) => Ok(Some(PduId::from(id))),
			| Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
			| Err(error) => Err(error),
		});

	let visible = services
		.state_accessor
		.user_can_see_state_events(sender_user, room_id)
		.map(|visible| {
			visible
				.into_option()
				.ok_or_else(|| err!(Request(Forbidden("You cannot view this room."))))
		});

	let shortroomid = services.short.get_shortroomid(room_id);

	let (shortroomid, target, ()) = try_join3(shortroomid, target, visible).await?;

	let Some(target) = target else {
		return Ok(get_relating_events::v1::Response::new(Vec::new()));
	};

	if shortroomid != target.shortroomid {
		return Err!(Request(NotFound("Event not found in room.")));
	}

	if let PduCount::Backfilled(_) = target.count {
		return Ok(get_relating_events::v1::Response::new(Vec::new()));
	}

	let target_pdu = services.timeline.get_pdu(target_event_id).await?;
	if target_pdu.room_id() != room_id || target_pdu.event_id() != target_event_id {
		return Err(Error::bad_database("Mismatched relation parent event"));
	}
	if is_ignored_pdu(services, &target_pdu, sender_user).await {
		return Err!(HttpJson(NOT_FOUND, {
			"errcode": "M_SENDER_IGNORED",
			"error": "You have ignored the user that sent this event",
			"sender": target_pdu.sender().as_str(),
		}));
	}

	let events = collect_relations(
		RelationQuery {
			services,
			sender_user,
			shortroomid,
			from,
			to,
			dir,
			limit,
			max_depth,
			filter_event_type: filter_event_type.as_ref(),
			filter_rel_type: filter_rel_type.as_ref(),
		},
		target.count,
	)
	.await?;

	Ok(get_relating_events::v1::Response {
		recursion_depth: max_depth
			.gt(&0)
			.then(|| events.iter().map(at!(0)))
			.into_iter()
			.flatten()
			.max()
			.map(TryInto::try_into)
			.transpose()?,

		next_batch: events
			.last()
			.map(at!(1))
			.as_ref()
			.map(ToString::to_string),

		prev_batch: events
			.first()
			.map(at!(1))
			.or(from)
			.as_ref()
			.map(ToString::to_string),

		chunk: events
			.into_iter()
			.map(at!(2))
			.map(Event::into_format)
			.collect(),
	})
}

#[derive(Clone, Copy)]
struct RelationQuery<'a> {
	services: &'a Services,
	sender_user: &'a UserId,
	shortroomid: u64,
	from: Option<PduCount>,
	to: Option<PduCount>,
	dir: Direction,
	limit: usize,
	max_depth: usize,
	filter_event_type: Option<&'a TimelineEventType>,
	filter_rel_type: Option<&'a RelationType>,
}

async fn collect_relations(
	query: RelationQuery<'_>,
	target: PduCount,
) -> Result<Vec<(usize, PduCount, PduEvent)>> {
	let mut budget = RelationReadBudget::default();
	let mut queue = VecDeque::from([(0_usize, target)]);
	let mut visited = HashSet::from([target]);
	let mut examined = Vec::new();
	while let Some((depth, parent)) = queue.pop_front() {
		let children = query
			.services
			.pdu_metadata
			.get_relations_bounded(
				query.shortroomid,
				parent,
				query.from,
				query.dir,
				Some(query.sender_user),
				&mut budget,
			)
			.await?;
		for (count, pdu) in children {
			if !visited.insert(count) {
				continue;
			}
			if depth < query.max_depth {
				queue.push_back((depth.saturating_add(1), count));
			}
			examined.push((depth, count, pdu));
		}
	}
	// Order every recursion level together before taking the requested page.
	examined.sort_by_key(|(_, count, _)| *count);
	if query.dir == Direction::Backward {
		examined.reverse();
	}
	let mut events = Vec::new();
	let mut bytes = 64_usize;
	for (depth, count, pdu) in examined {
		if Some(count) == query.to || events.len() == query.limit {
			break;
		}
		if query
			.filter_event_type
			.is_some_and(|kind| kind != pdu.kind())
			|| query
				.filter_rel_type
				.is_some_and(|kind| !kind.relation_type_equal(&pdu))
			|| !query
				.services
				.state_accessor
				.user_can_see_event(query.sender_user, pdu.room_id(), pdu.event_id())
				.await
		{
			continue;
		}
		let pdu = query
			.services
			.pdu_metadata
			.bundle_aggregations(query.sender_user, pdu)
			.await;
		bytes = bytes.saturating_add(1).saturating_add(
			serialized_len(pdu.as_pdu())
				.map_err(|_| Error::bad_database("Invalid relation response event"))?,
		);
		if bytes > 256 * 1024 {
			return Err(Error::Request(
				ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
				"Relation response byte limit reached".into(),
				http::StatusCode::TOO_MANY_REQUESTS,
			));
		}
		events.push((depth, count, pdu));
	}
	Ok(events)
}
