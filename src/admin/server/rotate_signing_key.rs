use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn rotate_signing_key(&self) -> Result {
	let staged = self
		.services
		.server_keys
		.stage_signing_key()
		.await?;

	let active = self.services.server_keys.active_key_id();

	self.write_str(&format!(
		"Staged signing key {staged}. {active} stays active until the next start, which makes \
		 {staged} active and publishes {active} as an old verify key."
	))
	.await
}
