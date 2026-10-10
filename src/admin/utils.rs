use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId};
use tuwunel_core::{Err, Result, err};
use tuwunel_service::Services;

pub(crate) async fn get_room_info(
	services: &Services,
	room_id: &RoomId,
) -> Result<(OwnedRoomId, u64, String)> {
	let join_count = services
		.state_cache
		.room_joined_count_uint(room_id)
		.await?;
	let join_count = u64::from(join_count);

	let name = match services.state_accessor.get_name(room_id).await {
		| Ok(name) => name,
		| Err(error) if !error.is_not_found() => return Err(error),
		| Err(_) if join_count == 2 => services
			.state_cache
			.bounded_room_members(room_id)
			.await?
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<_>>()
			.join(", "),
		| Err(_) => room_id.to_string(),
	};

	Ok((room_id.into(), join_count, name))
}

/// Parses user ID
pub(crate) fn parse_user_id(services: &Services, user_id: &str) -> Result<OwnedUserId> {
	UserId::parse_with_server_name(user_id.to_lowercase(), services.globals.server_name())
		.map_err(|e| err!("The supplied username is not a valid username: {e}"))
}

/// Parses user ID as our local user
pub(crate) fn parse_local_user_id(services: &Services, user_id: &str) -> Result<OwnedUserId> {
	let user_id = parse_user_id(services, user_id)?;

	if !services.globals.user_is_local(&user_id) {
		return Err!("User {user_id:?} does not belong to our server.");
	}

	Ok(user_id)
}

/// Parses user ID that is an active (not guest or deactivated) local user
pub(crate) async fn parse_active_local_user_id(
	services: &Services,
	user_id: &str,
) -> Result<OwnedUserId> {
	let user_id = parse_local_user_id(services, user_id)?;

	if !services.users.exists(&user_id).await {
		return Err!("User {user_id:?} does not exist on this server.");
	}

	if services.users.is_deactivated(&user_id).await? {
		return Err!("User {user_id:?} is deactivated.");
	}

	Ok(user_id)
}

/// Complete room inventory and details before sorting/pagination. Unmatched
/// rooms consume the source cap; read errors and oversized retained names
/// refuse without a partial administrative list.
pub(crate) async fn bounded_room_listing(
	services: &Services,
	published_only: bool,
	exclude_disabled: bool,
	exclude_banned: bool,
) -> Result<Vec<(OwnedRoomId, u64, String)>> {
	let inventory = services.metadata.bounded_room_ids().await?;
	let mut rows = Vec::new();
	let mut bytes = 0_usize;
	for room in inventory {
		if published_only
			&& !services
				.directory
				.is_public_room_checked(&room)
				.await?
		{
			continue;
		}
		if exclude_disabled
			&& services.db["disabledroomids"]
				.contains_checked(&(&room,))
				.await?
		{
			continue;
		}
		if exclude_banned
			&& services.db["bannedroomids"]
				.contains_checked(&(&room,))
				.await?
		{
			continue;
		}
		let row = get_room_info(services, &room).await?;
		bytes = bytes
			.saturating_add(row.0.as_str().len())
			.saturating_add(row.2.len())
			.saturating_add(size_of::<(OwnedRoomId, u64, String)>());
		if bytes > 256 * 1024 {
			return Err!("Room listing retained details exceed the supported bound.");
		}
		rows.push(row);
	}
	Ok(rows)
}
