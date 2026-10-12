use axum::extract::State;
use futures::TryStreamExt;
use ruma::OwnedDeviceId;
use synapse_admin_api::mas::sync_devices::{Request, Response};
use tuwunel_core::{
	Result,
	utils::stream::{IterStream, TryBroadbandExt},
};

use super::{Mas, existing_user};
use crate::Ruma;

/// # `POST /_synapse/mas/sync_devices`
pub(crate) async fn sync_devices_route(
	_mas: Mas,
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let user_id = existing_user(services, &body.localpart).await?;

	let current = services
		.users
		.bounded_device_ids(&user_id)
		.await?;
	let removed: Vec<OwnedDeviceId> = current
		.iter()
		.filter(|device_id| !body.devices.contains(*device_id))
		.cloned()
		.collect();
	services
		.users
		.remove_devices(&user_id, &removed)
		.await?;

	body.devices
		.iter()
		.filter(|device_id| !current.contains(*device_id))
		.try_stream()
		.broad_and_then(async |device_id| {
			services
				.users
				.create_device(&user_id, Some(device_id.as_ref()), (None, None), None, None, None)
				.await
				.map(|_| ())
		})
		.try_collect::<()>()
		.await?;

	Ok(Response::new())
}
