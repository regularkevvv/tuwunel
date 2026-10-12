use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn room_prune_empty(&self, force: bool) -> Result {
	let rooms = self
		.services
		.delete
		.bounded_empty_local_rooms()
		.await?;

	let mut deleted = 0_usize;
	for room_id in &rooms {
		let state_lock = self.services.state.mutex.lock(room_id).await;
		// Membership may have changed since the complete preflight.
		if self
			.services
			.state_cache
			.has_local_membership_checked(room_id)
			.await?
		{
			continue;
		}

		self.services
			.delete
			.delete_room(room_id, force, state_lock)
			.await?;
		deleted = deleted.saturating_add(1);
	}

	write!(self, "Successfully deleted {deleted} rooms from our database.").await?;

	Ok(())
}
