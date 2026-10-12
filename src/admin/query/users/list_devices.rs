use ruma::OwnedUserId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn list_devices(&self, user_id: OwnedUserId) -> Result {
	let query = self.services.users.bounded_device_ids(&user_id);

	self.write_timed_query_try(query).await
}
