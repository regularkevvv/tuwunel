use ruma::OwnedUserId;
use tuwunel_core::Result;
use tuwunel_service::users::{MAX_ADMIN_DEVICE_BYTES, MAX_ADMIN_DEVICE_ROWS};

use crate::admin_command;

#[admin_command]
pub(super) async fn list_devices_metadata(&self, user_id: OwnedUserId) -> Result {
	let query = self.services.users.bounded_devices_metadata(
		&user_id,
		MAX_ADMIN_DEVICE_ROWS,
		MAX_ADMIN_DEVICE_BYTES,
	);

	self.write_timed_query_try(query).await
}
