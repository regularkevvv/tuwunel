use tuwunel_core::{Result, warn};

use crate::Services;

/// Clears the ephemeral peer-status reachability rows once.
///
/// This prevents the new span-based backoff fold from misreading v1.8.1's
/// per-bucket failure rows.
pub(super) async fn clear_servername_status(services: &Services) -> Result {
	let db = &services.db;
	let servername_status = db["servername_status"].clone();

	warn!("Clearing federation peer-status reachability rows");
	servername_status.clear().await?;

	db["global"]
		.insert(b"clear_servername_status", [])
		.await?;
	servername_status.sort()
}
