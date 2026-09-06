use axum::extract::State;
use futures::StreamExt;
use ruma::{
	UserId,
	api::client::session::{logout, logout_all},
};
use tuwunel_core::Result;
use tuwunel_service::Services;

use crate::{ClientIp, Ruma};

/// Drop the upstream OIDC grant a user still has stored.
///
/// ADR-0004 requires logout to remove the session *and* the stored grant, so a
/// signed-out account leaves no refresh token behind that could re-open one.
/// The identity association survives: it maps `(iss, sub)` to this account, and
/// deleting it would make the next sign-in look like a new identity and
/// register a second account.
async fn clear_upstream_grant(services: &Services, user_id: &UserId) {
	services.oauth.clear_user_grants(user_id).await;
}

/// # `POST /_matrix/client/v3/logout`
///
/// Log out the current device.
///
/// - Invalidates access token
/// - Deletes device metadata (device id, device display name, last seen ip,
///   last seen ts)
/// - Forgets to-device events
/// - Triggers device list updates
#[tracing::instrument(skip_all, fields(%client), name = "logout")]
pub(crate) async fn logout_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<logout::v3::Request>,
) -> Result<logout::v3::Response> {
	services
		.users
		.remove_device(body.sender_user(), body.sender_device()?)
		.await;

	// Only the last device out clears the grant: another device of the same user
	// still needs it for its own refresh-time policy re-check.
	if services
		.users
		.all_device_ids(body.sender_user())
		.count()
		.await == 0
	{
		clear_upstream_grant(&services, body.sender_user()).await;
	}

	Ok(logout::v3::Response::new())
}

/// # `POST /_matrix/client/r0/logout/all`
///
/// Log out all devices of this user.
///
/// - Invalidates all access tokens
/// - Deletes all device metadata (device id, device display name, last seen ip,
///   last seen ts)
/// - Forgets all to-device events
/// - Triggers device list updates
///
/// Note: This is equivalent to calling [`GET
/// /_matrix/client/r0/logout`](fn.logout_route.html) from each device of this
/// user.
#[tracing::instrument(skip_all, fields(%client), name = "logout")]
pub(crate) async fn logout_all_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<logout_all::v3::Request>,
) -> Result<logout_all::v3::Response> {
	services
		.users
		.all_device_ids(body.sender_user())
		.for_each(|device_id| {
			services
				.users
				.remove_device(body.sender_user(), device_id)
		})
		.await;

	clear_upstream_grant(&services, body.sender_user()).await;

	Ok(logout_all::v3::Response::new())
}
