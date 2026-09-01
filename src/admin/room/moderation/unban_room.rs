use ruma::OwnedRoomOrAliasId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn unban_room(&self, room: OwnedRoomOrAliasId) -> Result {
	let room_id = self.services.alias.maybe_resolve(&room).await?;

	self.services
		.metadata
		.unban_room(&room_id)
		.await?;
	self.services
		.metadata
		.enable_room(&room_id)
		.await?;
	self.write_str("Room unbanned and federation re-enabled.")
		.await
}
