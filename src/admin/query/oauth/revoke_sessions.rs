use ruma::OwnedUserId;
use tuwunel_core::Result;

use crate::admin_command;

/// Immediate operator revocation of every session belonging to a user.
///
/// ADR-0004 requires operator revocation to be immediate rather than to wait
/// for the next refresh-time policy re-check, so this removes the Matrix
/// devices outright — invalidating their access and refresh tokens — and then
/// drops every stored upstream grant, asking the provider to revoke each when
/// it publishes a revocation endpoint. It is the same revocation a refusal from
/// the identity provider triggers.
///
/// The account stays active and its `(iss, sub)` identity association is kept,
/// so the user can sign in again through the identity provider if its policy
/// still allows them. Use `users deactivate` when the account itself should
/// end.
#[admin_command]
pub(super) async fn oauth_revoke_sessions(&self, user_id: OwnedUserId) -> Result {
	let devices = self
		.services
		.oauth
		.revoke_user_sessions(&user_id)
		.await;

	self.write_str(&format!(
		"Revoked {devices} device session(s) for {user_id} and cleared every stored upstream \
		 grant. The account remains active."
	))
	.await
}
