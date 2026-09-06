use futures::StreamExt;
use ruma::OwnedUserId;
use tuwunel_core::Result;

use crate::admin_command;

/// Immediate operator revocation of every session belonging to a user.
///
/// ADR-0004 requires operator revocation to be immediate rather than to wait
/// for the next refresh-time policy re-check, so this removes the Matrix
/// devices outright — invalidating their access and refresh tokens — and then
/// drops the stored upstream grant, asking the provider to revoke it when one
/// publishes a revocation endpoint.
///
/// The account stays active and its `(iss, sub)` identity association is kept,
/// so the user can sign in again through the identity provider if its policy
/// still allows them. Use `users deactivate` when the account itself should
/// end.
#[admin_command]
pub(super) async fn oauth_revoke_sessions(&self, user_id: OwnedUserId) -> Result {
	let devices: usize = self
		.services
		.users
		.all_device_ids(&user_id)
		.count()
		.await;

	self.services
		.users
		.all_device_ids(&user_id)
		.for_each(|device_id| {
			self.services
				.users
				.remove_device(&user_id, device_id)
		})
		.await;

	self.services
		.oauth
		.clear_user_grants(&user_id)
		.await;

	self.write_str(&format!(
		"Revoked {devices} device session(s) for {user_id} and cleared the stored upstream \
		 grant. The account remains active."
	))
	.await
}
