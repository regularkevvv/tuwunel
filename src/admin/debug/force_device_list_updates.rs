use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn force_device_list_updates(&self) -> Result {
	// Complete checked source inventory (1,024 rows / 128 KiB raw key+value
	// bytes) before the first marker. Individual user updates retain their
	// atomic commit and error semantics; this is not an all-user transaction.
	let users = self
		.services
		.users
		.bounded_registered_users()
		.await?;
	for user in users {
		self.services
			.users
			.mark_device_key_update(&user)
			.await?;
	}

	write!(self, "Marked all devices for all users as having new keys to update").await
}
