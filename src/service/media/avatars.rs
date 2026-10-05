use std::collections::HashSet;

use ruma::{
	OwnedMxcUri, RoomId,
	api::error::{ErrorKind, LimitExceededErrorData},
	events::{StateEventType, room::avatar::RoomAvatarEventContent},
};
use tuwunel_core::{Error, Result, err, matrix::Event};

use super::Service;

impl Service {
	/// Prepare every registered profile and room avatar before deleting media.
	/// Account/room inventories each have a 1,024-row / 128 KiB ID budget; all
	/// observed avatar URI bytes share 128 KiB, including repeated references.
	/// Room state resolution remains subject to its own snapshot
	/// implementation.
	pub(super) async fn avatar_mxcs(&self) -> Result<HashSet<OwnedMxcUri>> {
		let users = self
			.services
			.users
			.bounded_registered_users()
			.await?;
		let rooms = self.services.metadata.bounded_room_ids().await?;
		let mut spared = HashSet::new();
		let mut bytes = 128 * 1024_usize;
		for user in users {
			match self.services.profile.avatar_url(&user).await {
				| Ok(avatar) => retain_avatar(&mut spared, &mut bytes, avatar)?,
				| Err(error) if error.is_not_found() => {},
				| Err(error) => return Err(error),
			}
		}
		for room in rooms {
			if let Some(avatar) = self.room_avatar(&room).await? {
				retain_avatar(&mut spared, &mut bytes, avatar)?;
			}
		}
		Ok(spared)
	}

	async fn room_avatar(&self, room: &RoomId) -> Result<Option<OwnedMxcUri>> {
		// Only a complete state snapshot can prove an avatar absent. A missing
		// room hash or referenced event must never become an empty spare-set.
		let hash = self
			.services
			.state
			.get_room_shortstatehash(room)
			.await?;
		let Some(event) = self
			.services
			.state_accessor
			.state_get_optional(hash, &StateEventType::RoomAvatar, "")
			.await?
		else {
			return Ok(None);
		};
		if event.room_id() != room {
			return Err(err!(Database("Mismatched room avatar event")));
		}
		let content: RoomAvatarEventContent = event.get_content()?;
		Ok(content.url)
	}
}

fn retain_avatar(
	spared: &mut HashSet<OwnedMxcUri>,
	bytes: &mut usize,
	avatar: OwnedMxcUri,
) -> Result {
	if !avatar.is_valid() {
		return Err(err!(Database("Invalid stored avatar URI")));
	}
	*bytes = bytes
		.checked_sub(avatar.as_bytes().len())
		.ok_or_else(|| {
			Error::Request(
				ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
				"Avatar inventory byte limit reached".into(),
				http::StatusCode::TOO_MANY_REQUESTS,
			)
		})?;
	spared.insert(avatar);
	Ok(())
}
