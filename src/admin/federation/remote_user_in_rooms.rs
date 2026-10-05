use futures::{StreamExt, TryStreamExt};
use ruma::{OwnedRoomId, OwnedUserId};
use tuwunel_core::{Err, Result};

use crate::{admin_command, get_room_info};

#[admin_command]
pub(super) async fn remote_user_in_rooms(&self, user_id: OwnedUserId) -> Result {
	if user_id.server_name() == self.services.server.name {
		return Err!(
			"User belongs to our server, please use `list-joined-rooms` user admin command \
			 instead.",
		);
	}

	if !self.services.users.exists(&user_id).await {
		return Err!("Remote user does not exist in our database.",);
	}

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

	let num = rooms.len();
	write!(self, "Rooms {user_id} shares with us ({num}):\n```\n").await?;
	for (id, members, name) in &rooms {
		writeln!(self, "{id} | Members: {members} | Name: {name}").await?;
	}
	write!(self, "```").await
}
