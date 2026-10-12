use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn last_active(&self, limit: u16) -> Result {
	let activity = self
		.services
		.users
		.recent_local_activity(usize::from(limit))
		.await?;
	for item in activity {
		let ago = item.last_seen_ts;
		let user_id = item.user_id.localpart();
		let ip = item.last_seen_ip.as_deref().unwrap_or_default();
		write!(self, "{ago:?} {ip:<40} {user_id}\n").await?;
	}
	Ok(())
}
