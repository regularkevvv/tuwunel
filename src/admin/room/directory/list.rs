use tuwunel_core::Result;

use crate::{PAGE_SIZE, admin_command, utils::bounded_room_listing};

#[admin_command]
pub(super) async fn directory_list(&self, page: Option<usize>) -> Result {
	let page = page.unwrap_or(1);
	let mut rooms = bounded_room_listing(self.services, true, false, false).await?;

	rooms.sort_by_key(|r| r.1);
	rooms.reverse();

	let rooms: Vec<_> = rooms
		.into_iter()
		.skip(page.saturating_sub(1).saturating_mul(PAGE_SIZE))
		.take(PAGE_SIZE)
		.collect();

	if rooms.is_empty() {
		self.write_str("No rooms are published.").await?;

		return Ok(());
	}

	write!(self, "Rooms (page {page}):\n```\n").await?;
	for (id, members, name) in &rooms {
		writeln!(self, "{id} | Members: {members} | Name: {name}").await?;
	}
	write!(self, "```").await
}
