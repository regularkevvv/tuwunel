use tuwunel_core::{Err, Result};

use crate::{PAGE_SIZE, admin_command, utils::bounded_room_listing};

#[admin_command]
pub(super) async fn room_list(
	&self,
	page: Option<usize>,
	exclude_disabled: bool,
	exclude_banned: bool,
	no_details: bool,
) -> Result {
	// TODO: i know there's a way to do this with clap, but i can't seem to find it
	let page = page.unwrap_or(1);
	let mut rooms =
		bounded_room_listing(self.services, false, exclude_disabled, exclude_banned).await?;

	rooms.sort_by_key(|r| r.1);
	rooms.reverse();

	let rooms = rooms
		.into_iter()
		.skip(page.saturating_sub(1).saturating_mul(PAGE_SIZE))
		.take(PAGE_SIZE)
		.collect::<Vec<_>>();

	if rooms.is_empty() {
		return Err!("No more rooms.");
	}

	write!(self, "Rooms ({}):\n```\n", rooms.len()).await?;
	for (id, members, name) in &rooms {
		if no_details {
			writeln!(self, "{id}").await?;
		} else {
			writeln!(self, "{id}\tMembers: {members}\tName: {name}").await?;
		}
	}
	write!(self, "```").await
}
