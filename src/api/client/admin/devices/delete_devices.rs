use axum::extract::State;
use synapse_admin_api::devices::delete_devices::v1 as delete_devices;
use tuwunel_core::Result;

use super::require_local_user;
use crate::{Ruma, client::admin::require_admin};

/// # `POST /_synapse/admin/v2/users/{user_id}/delete_devices`
///
/// Unknown device IDs are silently ignored.
pub(crate) async fn admin_delete_devices_route(
	State(services): State<crate::State>,
	body: Ruma<delete_devices::Request>,
) -> Result<delete_devices::Response> {
	require_admin(&services, body.sender_user()).await?;

	let user_id = &body.user_id;
	require_local_user(&services, user_id).await?;

	services
		.users
		.remove_devices(user_id, &body.devices)
		.await?;

	Ok(delete_devices::Response {})
}
