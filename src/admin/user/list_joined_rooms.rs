use futures::{StreamExt, TryStreamExt};
use ruma::OwnedRoomId;
use tuwunel_core::{Err, Result};

use crate::{admin_command, get_room_info, utils::parse_local_user_id};

#[admin_command]
pub(super) async fn list_joined_rooms(&self, user_id: String) -> Result {
	// Validate user id
	let user_id = parse_local_user_id(self.services, &user_id)?;

	let mut rooms: Vec<(OwnedRoomId, u64, String)> = self
		.services
		.state_cache
		.rooms_joined(&user_id)
		.then(|room_id| get_room_info(self.services, room_id))
		.try_collect()
		.await?;

	if rooms.is_empty() {
		return Err!("User is not in any rooms.");
	}

	rooms.sort_by_key(|r| r.1);
	rooms.reverse();

	write!(self, "Rooms {user_id} Joined ({}):\n```\n", rooms.len()).await?;
	for (id, members, name) in &rooms {
		writeln!(self, "{id}\tMembers: {members}\tName: {name}").await?;
	}
	write!(self, "```").await
}
