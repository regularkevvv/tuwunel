use futures::{StreamExt, TryStreamExt};
use ruma::OwnedRoomId;
use tuwunel_core::{Err, Result, utils::IterStream};

use crate::{admin_command, get_room_info};

#[admin_command]
pub(super) async fn list_banned_rooms(&self, no_details: bool) -> Result {
	let room_ids: Vec<OwnedRoomId> = self
		.services
		.metadata
		.list_banned_rooms()
		.map(Into::into)
		.collect()
		.await;

	if room_ids.is_empty() {
		return Err!("No rooms are banned.");
	}

	let mut rooms = room_ids
		.iter()
		.stream()
		.then(|room_id| get_room_info(self.services, room_id))
		.try_collect::<Vec<_>>()
		.await?;

	rooms.sort_by_key(|r| r.1);
	rooms.reverse();

	let num = rooms.len();

	write!(self, "Rooms Banned ({num}):\n```\n").await?;
	for (id, members, name) in &rooms {
		if no_details {
			writeln!(self, "{id}").await?;
		} else {
			writeln!(self, "{id}\tMembers: {members}\tName: {name}").await?;
		}
	}
	write!(self, "```").await
}
