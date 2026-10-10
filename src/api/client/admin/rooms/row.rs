use ruma::{
	RoomId, UInt,
	api::error::{ErrorKind, LimitExceededErrorData},
	events::{
		StateEventType, TimelineEventType,
		room::{
			canonical_alias::RoomCanonicalAliasEventContent,
			create::RoomCreateEventContent,
			encryption::RoomEncryptionEventContent,
			guest_access::RoomGuestAccessEventContent,
			history_visibility::RoomHistoryVisibilityEventContent,
			join_rules::{JoinRule, RoomJoinRulesEventContent},
			name::RoomNameEventContent,
		},
	},
};
use serde::de::DeserializeOwned;
use synapse_admin_api::rooms::list_rooms::v1::RoomDetails;
use tuwunel_core::{
	Error, Result, err,
	matrix::{Event, Pdu},
};
use tuwunel_service::{Services, rooms::state_res::events::RoomCreateEvent};

/// Aggregate work across a complete room list, also used for a single summary.
pub(super) struct RoomRowBudget {
	state_cells: usize,
	state_bytes: usize,
	member_rows: usize,
	member_bytes: usize,
	detail_bytes: usize,
}

impl Default for RoomRowBudget {
	fn default() -> Self {
		Self {
			state_cells: 4096,
			state_bytes: 256 * 1024,
			member_rows: 4096,
			member_bytes: 256 * 1024,
			detail_bytes: 256 * 1024,
		}
	}
}

/// Assemble complete room details without converting failed reads into
/// defaults. State fields use one immutable hash; the directory and membership
/// reads have their own snapshots. Charge examined inputs before filters or
/// pagination.
pub(super) async fn room_row(
	services: &Services,
	room: &RoomId,
	budget: &mut RoomRowBudget,
) -> Result<RoomDetails> {
	let hash = services
		.state
		.get_room_shortstatehash(room)
		.await
		.map_err(|error| {
			if error.is_not_found() {
				err!(Database("Missing known room state"))
			} else {
				error
			}
		})?;
	let stack = services
		.state_compressor
		.load_shortstatehash_info(hash)
		.await?;
	let state_cells = stack
		.last()
		.ok_or_else(|| err!(Database("Missing room state layer")))?
		.full_state
		.len();
	charge(&mut budget.state_cells, state_cells)?;
	let state_events = state_cells
		.try_into()
		.map_err(|_| err!(Database("Invalid state count")))?;
	let raw_create = state_event(services, room, hash, &StateEventType::RoomCreate, budget)
		.await?
		.ok_or_else(|| err!(Database("Missing room create event")))?;
	let create_content: RoomCreateEventContent = decode(&raw_create)?;
	let create = RoomCreateEvent::new(raw_create);
	let version = Some(
		create
			.room_version()
			.map_err(|_| err!(Database("Invalid room version")))?
			.to_string(),
	);
	let federatable = create
		.federate()
		.map_err(|_| err!(Database("Invalid room federation flag")))?;
	let creator = Some(create.sender().to_owned());
	let name =
		content::<RoomNameEventContent>(services, room, hash, &StateEventType::RoomName, budget)
			.await?
			.map(|content| content.name)
			.filter(|name| !name.is_empty());
	let canonical_alias = content::<RoomCanonicalAliasEventContent>(
		services,
		room,
		hash,
		&StateEventType::RoomCanonicalAlias,
		budget,
	)
	.await?
	.and_then(|content| content.alias);
	let encryption = content::<RoomEncryptionEventContent>(
		services,
		room,
		hash,
		&StateEventType::RoomEncryption,
		budget,
	)
	.await?
	.map(|content| content.algorithm.to_string());
	let join_rules = Some(
		content::<RoomJoinRulesEventContent>(
			services,
			room,
			hash,
			&StateEventType::RoomJoinRules,
			budget,
		)
		.await?
		.map_or(JoinRule::Invite, |content| content.join_rule)
		.kind(),
	);
	let guest_access = content::<RoomGuestAccessEventContent>(
		services,
		room,
		hash,
		&StateEventType::RoomGuestAccess,
		budget,
	)
	.await?
	.map(|content| content.guest_access);
	let history_visibility = content::<RoomHistoryVisibilityEventContent>(
		services,
		room,
		hash,
		&StateEventType::RoomHistoryVisibility,
		budget,
	)
	.await?
	.map(|content| content.history_visibility);
	let public = services
		.directory
		.is_public_room_checked(room)
		.await?;
	let joined_count = services
		.state_cache
		.room_joined_count(room)
		.await
		.map_err(|error| {
			if error.is_not_found() {
				err!(Database("Missing known room member count"))
			} else {
				error
			}
		})?;
	let joined_members = UInt::try_from(joined_count)
		.map_err(|_| err!(Database("Invalid joined-member count")))?;
	let members = services
		.state_cache
		.bounded_local_member_count(room, budget.member_rows, budget.member_bytes)
		.await?;
	charge(&mut budget.member_rows, members.examined)?;
	charge(&mut budget.member_bytes, members.user_id_bytes)?;
	let joined_local_members = members
		.local_members
		.try_into()
		.map_err(|_| err!(Database("Invalid local-member count")))?;
	let row = RoomDetails {
		room_id: room.to_owned(),
		name,
		canonical_alias,
		joined_members,
		joined_local_members,
		version,
		creator,
		encryption,
		federatable,
		public,
		join_rules,
		guest_access,
		history_visibility,
		state_events,
		room_type: create_content.room_type,
	};
	charge(&mut budget.detail_bytes, serde_json::to_vec(&row)?.len())?;
	Ok(row)
}

async fn content<T: DeserializeOwned>(
	services: &Services,
	room: &RoomId,
	hash: u64,
	kind: &StateEventType,
	budget: &mut RoomRowBudget,
) -> Result<Option<T>> {
	state_event(services, room, hash, kind, budget)
		.await?
		.as_ref()
		.map(decode)
		.transpose()
}

async fn state_event(
	services: &Services,
	room: &RoomId,
	hash: u64,
	kind: &StateEventType,
	budget: &mut RoomRowBudget,
) -> Result<Option<Pdu>> {
	let event = services
		.state_accessor
		.state_get_optional(hash, kind, "")
		.await?;
	if let Some(event) = &event {
		if event.room_id() != room
			|| event.kind() != &TimelineEventType::from(kind.clone())
			|| event.state_key() != Some("")
		{
			return Err(err!(Database("Mismatched room summary event")));
		}
		charge(&mut budget.state_bytes, event.content().get().len())?;
	}
	Ok(event)
}

fn decode<T: DeserializeOwned>(event: &Pdu) -> Result<T> {
	event
		.get_content()
		.map_err(|_| err!(Database("Invalid stored room summary content")))
}

fn charge(remaining: &mut usize, amount: usize) -> Result {
	*remaining = remaining.checked_sub(amount).ok_or_else(|| {
		Error::Request(
			ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
			"Room summary inventory limit reached".into(),
			http::StatusCode::TOO_MANY_REQUESTS,
		)
	})?;
	Ok(())
}
