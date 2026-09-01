use ruma::OwnedRoomId;
use tuwunel_core::{Err, Result, err};

use super::parse_alias_from_localpart;
use crate::admin_command;

#[admin_command]
pub(super) async fn alias_set(
	&self,
	force: bool,
	room_id: OwnedRoomId,
	room_alias_localpart: String,
) -> Result {
	let room_alias = parse_alias_from_localpart(self.services, &room_alias_localpart)?;

	match self
		.services
		.alias
		.resolve_local_alias(&room_alias)
		.await
	{
		| Ok(id) => {
			if !force {
				return Err!(
					"Refusing to overwrite in use alias for {id}, use -f or --force to overwrite"
				);
			}

			self.services
				.alias
				.set_alias(&room_alias, &room_id)
				.await
				.map_err(|err| err!("Failed to remove alias: {err}"))?;

			write!(self, "Successfully overwrote alias (formerly {id})").await
		},
		| _ => {
			self.services
				.alias
				.set_alias(&room_alias, &room_id)
				.await
				.map_err(|err| err!("Failed to remove alias: {err}"))?;

			self.write_str("Successfully set alias").await
		},
	}
}
