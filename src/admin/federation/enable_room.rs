use ruma::OwnedRoomId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn enable_room(&self, room_id: OwnedRoomId) -> Result {
	self.services
		.metadata
		.enable_room(&room_id)
		.await?;
	self.write_str("Room enabled.").await
}
