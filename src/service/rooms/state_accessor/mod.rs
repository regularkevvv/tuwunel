mod erased;
mod member_snapshot;
mod room_state;
mod server_can;
mod state;
mod user_can;

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::try_join;
pub use member_snapshot::MemberSnapshot;
use ruma::{
	EventEncryptionAlgorithm, OwnedRoomAliasId, RoomId, UserId,
	api::error::ErrorKind,
	events::{
		StateEventType,
		room::{
			avatar::RoomAvatarEventContent,
			canonical_alias::RoomCanonicalAliasEventContent,
			create::RoomCreateEventContent,
			encryption::RoomEncryptionEventContent,
			guest_access::{GuestAccess, RoomGuestAccessEventContent},
			history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
			join_rules::{JoinRule, RoomJoinRulesEventContent},
			member::RoomMemberEventContent,
			name::RoomNameEventContent,
			power_levels::{RoomPowerLevels, RoomPowerLevelsEventContent},
			topic::RoomTopicEventContent,
		},
	},
	room::RoomType,
};
use tuwunel_core::{
	Error, Result, err,
	matrix::{Event, Pdu, room_version},
	utils::BoolExt,
};

use crate::rooms::state_res::events::RoomCreateEvent;

pub struct Service {
	services: Arc<crate::services::OnceServices>,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self { services: args.services.clone() }))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Gets the effective power levels of a room, regardless of if there is an
	/// `m.room.power_levels` state. Defaults require proven absence in a
	/// complete snapshot; corruption and failed reads remain errors.
	pub async fn get_power_levels(&self, room_id: &RoomId) -> Result<RoomPowerLevels> {
		let snapshot = self
			.services
			.state
			.get_room_shortstatehash(room_id)
			.await
			.map_err(|error| {
				if error.kind() == ErrorKind::NotFound {
					Error::bad_database("Missing room permission state")
				} else {
					error
				}
			})?;
		self.get_power_levels_at(room_id, snapshot, None)
			.await
	}

	/// Evaluate permissions in the candidate state accepted with a new event.
	pub(crate) async fn get_power_levels_at(
		&self,
		room_id: &RoomId,
		snapshot: crate::rooms::short::ShortStateHash,
		pending: Option<&Pdu>,
	) -> Result<RoomPowerLevels> {
		let create = async {
			self.state_get_optional_for_append(snapshot, &StateEventType::RoomCreate, "", pending)
				.await?
				.ok_or_else(|| Error::bad_database("Missing room creation event"))
		};
		let power_levels = self.state_get_optional_for_append(
			snapshot,
			&StateEventType::RoomPowerLevels,
			"",
			pending,
		);
		let (create, power_levels) = try_join(create, power_levels)
			.await
			.map_err(|error| {
				if error.kind() == ErrorKind::NotFound {
					Error::bad_database("Missing room creation event")
				} else {
					error
				}
			})?;
		if create.room_id() != room_id {
			return Err(Error::bad_database("Mismatched room creation event"));
		}
		let create = RoomCreateEvent::new(create);
		let power_levels = power_levels
			.map(|pdu| {
				if pdu.room_id() != room_id {
					return Err(Error::bad_database("Mismatched power level event"));
				}
				pdu.get_content::<RoomPowerLevelsEventContent>()
					.map_err(|_| Error::bad_database("Invalid power level event"))
			})
			.transpose()?;

		let room_version = create.room_version()?;
		let rules = room_version::rules(&room_version)?;
		let creators = create.creators(&rules.authorization)?;

		Ok(RoomPowerLevels::new(power_levels.into(), &rules.authorization, creators))
	}

	pub async fn get_create(&self, room_id: &RoomId) -> Result<RoomCreateEvent<Pdu>> {
		self.room_state_get(room_id, &StateEventType::RoomCreate, "")
			.await
			.map(RoomCreateEvent::new)
	}

	pub async fn get_name(&self, room_id: &RoomId) -> Result<String> {
		self.room_state_get_content(room_id, &StateEventType::RoomName, "")
			.await
			.and_then(|c: RoomNameEventContent| {
				c.name
					.is_empty()
					.is_false()
					.then_some(c.name)
					.ok_or_else(|| err!(Request(NotFound("Empty name found in event content."))))
			})
	}

	pub async fn get_avatar(&self, room_id: &RoomId) -> Result<RoomAvatarEventContent> {
		self.room_state_get_content(room_id, &StateEventType::RoomAvatar, "")
			.await
	}

	pub async fn get_member(
		&self,
		room_id: &RoomId,
		user_id: &UserId,
	) -> Result<RoomMemberEventContent> {
		self.room_state_get_content(room_id, &StateEventType::RoomMember, user_id.as_str())
			.await
	}

	/// Checks if guests are able to view room content without joining
	pub async fn is_world_readable(&self, room_id: &RoomId) -> bool {
		self.room_state_get_content(room_id, &StateEventType::RoomHistoryVisibility, "")
			.await
			.map(|c: RoomHistoryVisibilityEventContent| {
				c.history_visibility == HistoryVisibility::WorldReadable
			})
			.unwrap_or(false)
	}

	/// Checks if guests are able to join a given room
	pub async fn guest_can_join(&self, room_id: &RoomId) -> bool {
		self.room_state_get_content(room_id, &StateEventType::RoomGuestAccess, "")
			.await
			.map(|c: RoomGuestAccessEventContent| c.guest_access == GuestAccess::CanJoin)
			.unwrap_or(false)
	}

	/// Gets the primary alias from canonical alias event
	pub async fn get_canonical_alias(&self, room_id: &RoomId) -> Result<OwnedRoomAliasId> {
		self.room_state_get_content(room_id, &StateEventType::RoomCanonicalAlias, "")
			.await
			.and_then(|c: RoomCanonicalAliasEventContent| {
				c.alias
					.ok_or_else(|| err!(Request(NotFound("No alias found in event content."))))
			})
	}

	/// Gets the room topic
	pub async fn get_room_topic(&self, room_id: &RoomId) -> Result<String> {
		self.room_state_get_content(room_id, &StateEventType::RoomTopic, "")
			.await
			.and_then(|content: RoomTopicEventContent| {
				plain_text_topic(content)
					.ok_or_else(|| err!(Request(NotFound("Empty topic found in event content."))))
			})
	}

	/// Returns the join rules for a given room (`JoinRule` type). Will default
	/// to Invite if doesnt exist or invalid
	pub async fn get_join_rules(&self, room_id: &RoomId) -> JoinRule {
		self.room_state_get_content(room_id, &StateEventType::RoomJoinRules, "")
			.await
			.map_or(JoinRule::Invite, |c: RoomJoinRulesEventContent| c.join_rule)
	}

	pub async fn get_room_type(&self, room_id: &RoomId) -> Result<RoomType> {
		self.room_state_get_content(room_id, &StateEventType::RoomCreate, "")
			.await
			.and_then(|content: RoomCreateEventContent| {
				content
					.room_type
					.ok_or_else(|| err!(Request(NotFound("No type found in event content"))))
			})
	}

	/// Gets the room's encryption algorithm if `m.room.encryption` state event
	/// is found
	pub async fn get_room_encryption(
		&self,
		room_id: &RoomId,
	) -> Result<EventEncryptionAlgorithm> {
		self.room_state_get_content(room_id, &StateEventType::RoomEncryption, "")
			.await
			.map(|content: RoomEncryptionEventContent| content.algorithm)
	}

	pub async fn is_encrypted_room(&self, room_id: &RoomId) -> bool {
		self.room_state_get(room_id, &StateEventType::RoomEncryption, "")
			.await
			.is_ok()
	}

	/// Whether the current state proves that this room is encrypted.
	///
	/// Unlike [`Self::is_encrypted_room`], this keeps an unreadable state event
	/// distinct from a normally absent one.
	pub async fn is_encrypted_room_strict(&self, room_id: &RoomId) -> Result<bool> {
		match self
			.room_state_get(room_id, &StateEventType::RoomEncryption, "")
			.await
		{
			| Ok(_) => Ok(true),
			| Err(error) if error.is_not_found() => Ok(false),
			| Err(error) => Err(error),
		}
	}
}

/// Resolves an `m.room.topic` to its plain-text rendering: the `m.topic`
/// block's `text/plain` representation when present (MSC3765), else the legacy
/// `topic` field; `None` when neither yields a non-empty string.
pub(crate) fn plain_text_topic(content: RoomTopicEventContent) -> Option<String> {
	let topic = content
		.topic_block
		.text
		.find_plain()
		.map(ToOwned::to_owned)
		.unwrap_or(content.topic);

	topic.is_empty().is_false().then_some(topic)
}
