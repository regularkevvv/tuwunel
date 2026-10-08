use futures::{FutureExt, TryFutureExt, TryStreamExt};
use ruma::{
	RoomId, api::federation::space::SpaceHierarchyParentSummary as ParentSummary,
	events::space::child::HierarchySpaceChildEvent, room::RoomSummary, serde::Raw,
};
use tuwunel_core::{
	Err, Error, Event, Result, debug, error, implement,
	utils::{future::TryExtExt, timepoint_has_passed},
};

use super::{Accessibility, Cached, Identifier};

/// Gets the summary of a space using solely local information.
#[implement(super::Service)]
#[tracing::instrument(name = "local", level = "debug", skip_all)]
pub(super) async fn get_summary_and_children_local(
	&self,
	current_room: &RoomId,
	sender: &Identifier<'_>,
) -> Result<Accessibility> {
	use Accessibility::{Accessible, Inaccessible};

	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	match self.cache_get(current_room).await {
		| Err(e) if !e.is_not_found() => {
			error!(?current_room, "cache error: {e}");
			return Err(e);
		},
		| Ok(Cached { expires, summary: Some(mut cached) }) if !timepoint_has_passed(expires) => {
			debug!(?current_room, ?expires, "cache hit");
			if !self
				.is_accessible_child(current_room, &cached.summary.join_rule, sender)
				.await
			{
				return Ok(Inaccessible);
			}
			cached.summary.num_joined_members = services_root
				.state_cache
				.room_joined_count_uint(current_room)
				.await?;
			return Ok(Accessible(cached));
		},
		| Ok(Cached { expires, summary: None }) if !timepoint_has_passed(expires) => {
			// Cache negative: try local computation below.
			debug!(?current_room, ?expires, "negative cache hit");
		},
		| _ => {
			// Cache miss, expired, or negative: try local computation below.
			debug!(?current_room, "no usable cache entry");
		},
	}

	if !services_root
		.state_cache
		.server_in_room(services_root.server.name.as_ref(), current_room)
		.await
	{
		debug!(?current_room, "no local membership; defer to federation");
		return Err!(Request(NotFound("Space room not found locally.")));
	}

	let children_state = self
		.get_space_child_events(current_room)
		.map_ok(Event::into_format)
		.try_collect()
		.await?;

	let summary = self
		.get_room_summary(current_room, children_state, sender)
		.boxed()
		.await;

	match summary {
		| Ok(Inaccessible) => self.cache_put(current_room, None).await?,
		| Ok(Accessible(ref summary)) =>
			self.cache_put(current_room, Some(summary))
				.await?,
		| _ => (),
	}

	summary
}

#[implement(super::Service)]
pub(super) async fn get_room_summary(
	&self,
	room_id: &RoomId,
	children_state: Vec<Raw<HierarchySpaceChildEvent>>,
	sender: &Identifier<'_>,
) -> Result<Accessibility, Error> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let join_rule = services_root
		.state_accessor
		.get_join_rules(room_id)
		.await;

	let is_accessible_child = self
		.is_accessible_child(room_id, &join_rule.clone().into(), sender)
		.await;

	if !is_accessible_child {
		return Ok(Accessibility::Inaccessible);
	}

	let name = services_root
		.state_accessor
		.get_name(room_id)
		.ok();

	let topic = services_root
		.state_accessor
		.get_room_topic(room_id)
		.ok();

	let room_type = services_root
		.state_accessor
		.get_room_type(room_id)
		.ok();

	let world_readable = services_root
		.state_accessor
		.is_world_readable(room_id);

	let guest_can_join = services_root
		.state_accessor
		.guest_can_join(room_id);

	let num_joined_members = services_root
		.state_cache
		.room_joined_count_uint(room_id);

	let canonical_alias = services_root
		.state_accessor
		.get_canonical_alias(room_id)
		.ok();

	let avatar_url = services_root
		.state_accessor
		.get_avatar(room_id)
		.map_ok(|content| content.url)
		.ok();

	let room_version = services_root.state.get_room_version(room_id).ok();

	let encryption = services_root
		.state_accessor
		.get_room_encryption(room_id)
		.ok();

	let (
		canonical_alias,
		name,
		num_joined_members,
		topic,
		world_readable,
		guest_can_join,
		avatar_url,
		room_type,
		room_version,
		encryption,
	) = futures::join!(
		canonical_alias,
		name,
		num_joined_members,
		topic,
		world_readable,
		guest_can_join,
		avatar_url,
		room_type,
		room_version,
		encryption,
	);

	let summary = ParentSummary {
		children_state,
		summary: RoomSummary {
			avatar_url: avatar_url.flatten(),
			canonical_alias,
			name,
			topic,
			world_readable,
			guest_can_join,
			room_type,
			encryption,
			room_version,
			room_id: room_id.to_owned(),
			num_joined_members: num_joined_members?,
			join_rule: join_rule.clone().into(),
		},
	};

	Ok(Accessibility::Accessible(summary))
}
