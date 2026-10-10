use regex::Regex;
use ruma::{OwnedUserId, RoomId};
use tuwunel_core::{Error, Result};
use tuwunel_service::Services;

use crate::{admin_command, get_room_info, utils::parse_user_id};

enum Target {
	User(OwnedUserId),
	Pattern(Regex),
}

impl Target {
	fn matches(&self, members: &[OwnedUserId], sole_member: bool) -> bool {
		(!sole_member || members.len() == 1)
			&& members.iter().any(|member| match self {
				| Self::User(user) => member == user,
				| Self::Pattern(pattern) => pattern.is_match(member.as_str()),
			})
	}
}

#[admin_command]
pub(super) async fn room_purge_user(
	&self,
	user_id: String,
	regex: bool,
	sole_member: bool,
	dry_run: bool,
) -> Result {
	let services = self.services;
	// Prove which room is protected; a failed admin lookup cannot mean false.
	let admin_room = services.admin.get_admin_room().await?;
	let target = if regex {
		Target::Pattern(Regex::new(&user_id)?)
	} else {
		Target::User(parse_user_id(services, &user_id)?)
	};
	let rooms = match &target {
		| Target::Pattern(_) => services.metadata.bounded_room_ids().await?,
		| Target::User(user) =>
			services
				.state_cache
				.bounded_rooms_joined(user)
				.await?,
	};
	let mut selected = Vec::new();
	let mut rows = 4096_usize;
	let mut bytes = 256_usize * 1024;
	// Complete every source and member inventory before any deletion. Keys of
	// unmatched rooms consume the shared budget too; no partial candidate list.
	for room in rooms {
		if room == admin_room {
			continue;
		}
		let _state_lock = services.state.mutex.lock(&room).await;
		let members = members(services, &room, rows, bytes).await?;
		rows = charge(rows, members.len())?;
		bytes = charge(
			bytes,
			members
				.iter()
				.map(|user| user.as_bytes().len())
				.sum(),
		)?;
		if target.matches(&members, sole_member) {
			selected.push(room);
		}
	}

	if dry_run {
		let mut details = Vec::new();
		for room in &selected {
			details.push(get_room_info(services, room).await?);
		}
		self.write_str("Matching rooms:\n```\n").await?;
		for (id, members, name) in details {
			writeln!(self, "{id}\tMembers: {members}\tName: {name}").await?;
		}
		return write!(self, "```\nMatched {} rooms.", selected.len()).await;
	}

	let mut deleted = 0_usize;
	let mut rows = 4096_usize;
	let mut bytes = 256_usize * 1024;
	for room in selected {
		let state_lock = services.state.mutex.lock(&room).await;
		// Membership may change after preflight. Recheck under the deletion's
		// state lock, with a second bounded pass. This is not cross-room atomic.
		let members = members(services, &room, rows, bytes).await?;
		rows = charge(rows, members.len())?;
		bytes = charge(
			bytes,
			members
				.iter()
				.map(|user| user.as_bytes().len())
				.sum(),
		)?;
		if !target.matches(&members, sole_member) {
			continue;
		}
		services
			.delete
			.delete_room(&room, false, state_lock)
			.await?;
		deleted = deleted.saturating_add(1);
	}
	match deleted {
		| 0 => write!(self, "No rooms matched."),
		| _ => write!(self, "Deleted {deleted} rooms from our database."),
	}
	.await
}

async fn members(
	services: &Services,
	room: &RoomId,
	rows: usize,
	bytes: usize,
) -> Result<Vec<OwnedUserId>> {
	let count = services
		.state_cache
		.room_joined_count_uint(room)
		.await?;
	let members = services
		.state_cache
		.bounded_room_members_with_budget(room, rows, bytes)
		.await?;
	if u64::from(count) != u64::try_from(members.len())? {
		return Err(Error::bad_database("Joined-member count disagrees with inventory"));
	}
	Ok(members)
}

fn charge(remaining: usize, used: usize) -> Result<usize> {
	remaining
		.checked_sub(used)
		.ok_or_else(|| Error::bad_database("Invalid member purge inventory budget"))
}
