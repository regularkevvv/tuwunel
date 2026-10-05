use futures::{Stream, StreamExt};
use ruma::{EventId, OwnedEventId, RoomId};
use tuwunel_core::{
	Result, implement,
	matrix::{Event, Pdu},
	utils::stream::TryIgnore,
};
use tuwunel_database::Txn;

use super::{RelationReadBudget, Service, typed_relations::Tag};

/// Cap on the `m.reference` bundle chunk; /relations is the paginated fallback.
const BUNDLE_MAX: usize = 100;

/// MSC2675/MSC3267: the event ids of `parent`'s `m.reference` children, oldest
/// first, from the typed index, capped at `BUNDLE_MAX`. Empty when
/// `parent` is redacted or unreferenced. Every index identity and existing
/// child record is checked before capping the chunk. The chunk is filtered
/// for neither ignored users nor history visibility. The ignored-user posture
/// matches the /relations endpoint, which also does not filter relation
/// children by ignored sender; the history-visibility posture matches the
/// thread and edit bundles and is less strict than /relations, which does
/// filter children by visibility.
#[implement(Service)]
#[tracing::instrument(skip_all, level = "trace")]
pub(super) async fn references(
	&self,
	parent: &Pdu,
	budget: &mut RelationReadBudget,
) -> Result<Vec<OwnedEventId>> {
	if parent.is_redacted() {
		return Ok(Vec::new());
	}
	Ok(self
		.typed_children(parent, Tag::Reference, budget)
		.await?
		.into_iter()
		.take(BUNDLE_MAX)
		.map(|pdu| pdu.event_id)
		.collect())
}

#[implement(Service)]
#[tracing::instrument(skip_all, level = "debug")]
pub async fn mark_as_referenced<'a, I>(&self, room_id: &RoomId, event_ids: I) -> Result
where
	I: Iterator<Item = &'a EventId>,
{
	for event_id in event_ids {
		let key = (room_id, event_id);

		self.db.referencedevents.put_raw(key, []).await?;
	}

	Ok(())
}

/// Queues the marks [`Self::mark_as_referenced`] writes into `txn`, so they
/// commit with the event that references them.
#[implement(Service)]
pub fn mark_as_referenced_txn<'a, I>(&self, txn: &mut Txn, room_id: &RoomId, event_ids: I)
where
	I: Iterator<Item = &'a EventId>,
{
	for event_id in event_ids {
		txn.put_raw(&self.db.referencedevents, (room_id, event_id), []);
	}
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug", ret)]
pub async fn is_event_referenced(&self, room_id: &RoomId, event_id: &EventId) -> bool {
	let key = (room_id, event_id);

	self.db.referencedevents.qry(&key).await.is_ok()
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn mark_event_soft_failed(&self, event_id: &EventId) -> Result {
	self.db
		.softfailedeventids
		.insert(event_id, [])
		.await
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug", ret)]
pub async fn is_event_soft_failed(&self, event_id: &EventId) -> bool {
	self.db
		.softfailedeventids
		.get(event_id)
		.await
		.is_ok()
}

/// Streams owned event IDs with soft-fail markers.
///
/// Each ID is copied before the database cursor advances.
#[implement(Service)]
pub fn soft_failed_event_ids(&self) -> impl Stream<Item = OwnedEventId> + Send + '_ {
	self.db
		.softfailedeventids
		.keys()
		.ignore_err()
		.map(|event_id: &EventId| event_id.to_owned())
}

/// Clears one event's soft-fail marker.
///
/// A later processing attempt can evaluate the event again.
#[implement(Service)]
pub async fn clear_event_soft_failed(&self, event_id: &EventId) -> Result {
	self.db.softfailedeventids.remove(event_id).await
}
