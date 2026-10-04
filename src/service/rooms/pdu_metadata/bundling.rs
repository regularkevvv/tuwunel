use std::collections::BTreeSet;

use ruma::{OwnedUserId, UserId, api::Direction, events::room::encrypted::Relation};
use tuwunel_core::{
	Error, PduId, Result, implement,
	matrix::{Event, Pdu, PduCount},
	utils::json::serialized_len,
};

use super::{
	ExtractRelatesTo, IgnoredThreadView,
	IgnoredThreadView::{Adjusted, Omitted, Unchanged},
	RelationReadBudget, Service,
	typed_relations::Tag,
};

/// Fold requester-specific thread/edit/reference bundles into a served clone.
/// Complete typed inventories share one row/byte budget and propagate errors;
/// disabled bundles and genuinely absent relations retain their normal forms.
#[implement(Service)]
#[tracing::instrument(skip_all, level = "trace")]
pub async fn bundle_aggregations(&self, sender_user: &UserId, mut pdu: Pdu) -> Result<Pdu> {
	let mut budget = RelationReadBudget::default();
	budget.charge(
		0,
		serialized_len(pdu.as_pdu())
			.map_err(|_| Error::bad_database("Invalid stored bundle event"))?,
	)?;
	if let Some(pruned) = self
		.services
		.state_accessor
		.erased_view(sender_user, &pdu)
		.await
	{
		return Ok(pruned);
	}
	if pdu.thread_latest_event()?.is_some() {
		let participated = self
			.services
			.threads
			.user_participated(pdu.event_id(), sender_user)
			.await?;
		pdu.set_thread_participated(participated)?;
		self.erase_thread_latest(sender_user, &mut pdu, &mut budget)
			.await?;
		if self.services.server.config.bundle_edit_relations {
			self.bundle_thread_latest_edit(sender_user, &mut pdu, &mut budget)
				.await?;
		}
	}
	if self.services.server.config.bundle_edit_relations {
		if let Some(mut replacement) = self.newest_replacement(&pdu, &mut budget).await?
			&& !self
				.services
				.state_accessor
				.erased_for(sender_user, &replacement)
				.await
		{
			replacement.remove_transaction_id_unless_sender(Some(sender_user));
			pdu.set_replacement_bundle(&replacement.into_format())?;
		}
	}
	if self
		.services
		.server
		.config
		.bundle_reference_relations
	{
		let references = self.references(&pdu, &mut budget).await?;
		if !references.is_empty() {
			pdu.set_reference_bundle(&references)?;
		}
	}
	Ok(pdu)
}

/// The bundle identity must agree with the retained canonical event before
/// erasure or edit processing can project its content for this requester.
#[implement(Service)]
async fn load_thread_latest(
	&self,
	pdu: &Pdu,
	budget: &mut RelationReadBudget,
) -> Result<Option<Pdu>> {
	let Some((event_id, sender)) = pdu.thread_latest_event()? else {
		return Ok(None);
	};
	let latest_id = self
		.services
		.timeline
		.get_pdu_id(&event_id)
		.await
		.map_err(|error| {
			if error.kind() == ruma::api::error::ErrorKind::NotFound {
				Error::bad_database("Missing bundled thread latest event")
			} else {
				error
			}
		})?;
	let latest = self
		.relation_pdu(&latest_id, budget)
		.await?
		.ok_or_else(|| Error::bad_database("Missing bundled thread latest record"))?;
	if latest.event_id().as_str() != event_id.as_str()
		|| latest.sender().as_str() != sender.as_str()
		|| latest.room_id() != pdu.room_id()
	{
		return Err(Error::bad_database("Mismatched bundled thread latest event"));
	}
	Ok(Some(latest))
}

#[implement(Service)]
async fn erase_thread_latest(
	&self,
	sender_user: &UserId,
	pdu: &mut Pdu,
	budget: &mut RelationReadBudget,
) -> Result {
	let Some(latest) = self.load_thread_latest(pdu, budget).await? else {
		return Ok(());
	};
	if !self
		.services
		.users
		.is_erased(latest.sender())
		.await
	{
		return Ok(());
	}
	if let Some(pruned) = self
		.services
		.state_accessor
		.erased_view(sender_user, &latest)
		.await
	{
		pdu.set_thread_latest_event(&pruned.into_format())?;
	}
	Ok(())
}

#[implement(Service)]
async fn bundle_thread_latest_edit(
	&self,
	sender_user: &UserId,
	pdu: &mut Pdu,
	budget: &mut RelationReadBudget,
) -> Result {
	let Some(mut latest) = self.load_thread_latest(pdu, budget).await? else {
		return Ok(());
	};
	if self
		.services
		.state_accessor
		.erased_for(sender_user, &latest)
		.await
	{
		return Ok(());
	}
	let Some(mut replacement) = self.newest_replacement(&latest, budget).await? else {
		return Ok(());
	};
	if self
		.services
		.state_accessor
		.erased_for(sender_user, &replacement)
		.await
	{
		return Ok(());
	}
	replacement.remove_transaction_id_unless_sender(Some(sender_user));
	latest.remove_transaction_id_unless_sender(Some(sender_user));
	latest.set_replacement_bundle(&replacement.into_format())?;
	pdu.set_thread_latest_event(&latest.into_format())?;
	Ok(())
}

/// Newest valid edit by the typed index's timestamp/count order. Matrix edits
/// by another sender or of another type remain non-applicable; their records
/// are still fully checked before selecting an eligible edit.
#[implement(Service)]
async fn newest_replacement(
	&self,
	parent: &Pdu,
	budget: &mut RelationReadBudget,
) -> Result<Option<Pdu>> {
	if parent.is_redacted() {
		return Ok(None);
	}
	Ok(self
		.typed_children(parent, Tag::Replace, budget)
		.await?
		.into_iter()
		.rev()
		.find(|child| child.sender() == parent.sender() && child.kind() == parent.kind()))
}

/// MSC3856: evaluate one served thread root against the requester's ignore
/// list. A cheap participant intersection gates the reply walk; one walk then
/// yields the replacement `latest_event`, the ignored-aware `count`, and the
/// omit-when-every-reply-is-ignored verdict. A root whose replies are not
/// indexed (backfilled history) adjusts nothing beyond its own redacted form.
#[implement(Service)]
#[tracing::instrument(skip_all, level = "trace")]
pub async fn ignored_thread_view(
	&self,
	sender_user: &UserId,
	ignored: &BTreeSet<OwnedUserId>,
	root: &Pdu,
) -> Result<IgnoredThreadView> {
	let root_id = self
		.services
		.timeline
		.get_pdu_id(root.event_id())
		.await
		.map_err(|error| {
			if error.kind() == ruma::api::error::ErrorKind::NotFound {
				Error::bad_database("Missing ignored thread root mapping")
			} else {
				error
			}
		})?;
	if matches!(root_id.pdu_count(), PduCount::Backfilled(_)) {
		return Ok(Unchanged);
	}

	let participants = self
		.services
		.threads
		.get_participants(&root_id)
		.await
		.map_err(|error| {
			if error.kind() == ruma::api::error::ErrorKind::NotFound {
				Error::bad_database("Missing ignored thread participants")
			} else {
				error
			}
		})?;

	if !participants
		.iter()
		.any(|user| ignored.contains(user))
	{
		return Ok(Unchanged);
	}

	let root_pid: PduId = root_id.into();
	let replies = self
		.get_relations(
			root_pid.shortroomid,
			root_pid.count,
			None,
			Direction::Backward,
			Some(sender_user),
		)
		.await?
		.into_iter()
		.filter_map(|(_, pdu)| {
			pdu.get_content()
				.is_ok_and(|content: ExtractRelatesTo| {
					matches!(content.relates_to, Relation::Thread(_))
				})
				.then_some(pdu)
		});

	let fold = |(total, unignored, latest): (usize, usize, Option<Pdu>), pdu: Pdu| match ignored
		.contains(pdu.sender())
	{
		| true => (total.saturating_add(1), unignored, latest),
		| false => (total.saturating_add(1), unignored.saturating_add(1), latest.or(Some(pdu))),
	};

	let (total, unignored, latest) = replies.fold((0, 0, None), fold);

	if total == 0 {
		return Ok(match self.redacted_root(ignored, root).await? {
			| None => Unchanged,
			| root => Adjusted { root, count: None, latest: None },
		});
	}

	if unignored == 0 {
		return Ok(Omitted);
	}

	let swap = root
		.thread_latest_event()?
		.is_some_and(|(_, sender)| ignored.contains(&sender));

	let latest = match swap.then_some(latest).flatten() {
		| None => None,
		| Some(reply) => {
			// MSC4025: the swapped-in reply must not reopen the erased-sender
			// seam the bundle pass gates on the stored latest.
			let reply = self
				.services
				.state_accessor
				.erased_view(sender_user, &reply)
				.await
				.unwrap_or(reply);

			Some(reply.into_format())
		},
	};

	let count = unignored.ne(&total).then_some(unignored);

	let root = self.redacted_root(ignored, root).await?;

	if root.is_none() && count.is_none() && latest.is_none() {
		return Ok(Unchanged);
	}

	Ok(Adjusted { root, count, latest })
}

/// Redact an ignored root or propagate failed stored room/event reads.
#[implement(Service)]
#[tracing::instrument(skip_all, level = "trace")]
async fn redacted_root(
	&self,
	ignored: &BTreeSet<OwnedUserId>,
	root: &Pdu,
) -> Result<Option<Box<Pdu>>> {
	if !ignored.contains(root.sender()) {
		return Ok(None);
	}
	let rules = self
		.services
		.state
		.get_room_version_rules(root.room_id())
		.await?;
	let root = root
		.redacted(&rules.redaction)
		.map_err(|_| Error::bad_database("Invalid ignored thread root redaction"))?;
	Ok(Some(Box::new(root)))
}
