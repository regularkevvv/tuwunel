use axum::extract::State;
use futures::StreamExt;
use ruma::api::client::session::{logout, logout_all};
use tuwunel_core::{Result, info};

use crate::{ClientIp, Ruma};

/// # `POST /_matrix/client/v3/logout`
///
/// Log out the current device.
///
/// - Invalidates access token
/// - Deletes device metadata (device id, device display name, last seen ip,
///   last seen ts)
/// - Forgets to-device events
/// - Triggers device list updates
/// - Clears the upstream grant this device's sign-in obtained (ADR-0004: logout
///   removes the session *and* its stored grant)
///
/// Other devices hold grants of their own and keep them. The identity
/// association survives: it maps `(iss, sub)` to this account, and deleting it
/// would make the next sign-in look like a new identity and register a second
/// account.
#[tracing::instrument(skip_all, fields(%client), name = "logout")]
pub(crate) async fn logout_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<logout::v3::Request>,
) -> Result<logout::v3::Response> {
	let user_id = body.sender_user();
	let device_id = body.sender_device()?;

	services
		.users
		.remove_device(user_id, device_id)
		.await;

	services
		.oauth
		.clear_device_grants(user_id, device_id)
		.await;

	// A grant obtained before grants were bound to devices backs every device
	// that has none of its own, so only the last device out clears it.
	if services
		.users
		.all_device_ids(user_id)
		.count()
		.await == 0
	{
		services.oauth.clear_user_grants(user_id).await;
	}

	info!(audit = "logout", %user_id, %device_id, "Device signed out.");

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
/// - Clears every stored upstream grant of the account
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
	let user_id = body.sender_user();

	services
		.users
		.all_device_ids(user_id)
		.for_each(|device_id| services.users.remove_device(user_id, device_id))
		.await;

	services.oauth.clear_user_grants(user_id).await;

	info!(audit = "logout_all", %user_id, "Every device of the account signed out.");

	Ok(logout_all::v3::Response::new())
}
