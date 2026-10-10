use ruma::{
	OwnedDeviceId, UserId,
	api::client::dehydrated_device::{
		DehydratedDeviceData, put_dehydrated_device::unstable::Request,
	},
	serde::Raw,
};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Err, Result, implement, trace};
use tuwunel_database::{Deserialized, Json};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DehydratedDevice {
	/// Unique ID of the device.
	pub device_id: OwnedDeviceId,

	/// Contains serialized and encrypted private data.
	pub device_data: Raw<DehydratedDeviceData>,
}

/// Creates or recreates the user's dehydrated device.
#[implement(super::Service)]
#[tracing::instrument(
	level = "info",
	skip_all,
	fields(
		%user_id,
		device_id = %request.device_id,
		display_name = ?request.initial_device_display_name,
	)
)]
pub async fn set_dehydrated_device(&self, user_id: &UserId, request: Request) -> Result {
	assert!(
		self.exists(user_id).await,
		"Tried to create dehydrated device for non-existent user"
	);

	let existing_id = self.get_dehydrated_device_id(user_id).await;

	if existing_id.is_err()
		&& self
			.device_exists(user_id, &request.device_id)
			.await
	{
		return Err!("A hydrated device already exists with that ID.");
	}

	if let Ok(existing_id) = existing_id {
		self.remove_device(user_id, &existing_id).await?;
	}

	let device_id = self
		.create_device(
			user_id,
			Some(&request.device_id),
			(None, None),
			None,
			request.initial_device_display_name.as_deref(),
			None,
		)
		.await?;

	let guard = self.device_mutation_locks.lock(user_id).await;
	self.db
		.userdeviceid_metadata
		.qry(&(user_id, &*device_id))
		.await?;
	trace!(device_data = ?request.device_data);
	self.db
		.userid_dehydrateddevice
		.raw_put(
			user_id,
			Json(&DehydratedDevice {
				device_id: device_id.clone(),
				device_data: request.device_data,
			}),
		)
		.await?;
	drop(guard);

	trace!(device_keys = ?request.device_keys);
	self.add_device_keys(user_id, &device_id, &request.device_keys)
		.await?;

	trace!(one_time_keys = ?request.one_time_keys);
	self.add_one_time_keys(
		user_id,
		&device_id,
		&request.one_time_keys,
		request.one_time_keys.len(),
	)
	.await?;

	// MSC3814: dehydrated device MUST be cross-signed and have a fallback key.
	trace!(fallback_keys = ?request.fallback_keys);
	self.add_fallback_keys(
		user_id,
		&device_id,
		request
			.fallback_keys
			.iter()
			.map(|(id, key)| (id.as_ref(), key)),
	)
	.await?;

	Ok(())
}

/// Get the device_id of the user's dehydrated device.
#[implement(super::Service)]
#[tracing::instrument(
	level = "debug",
	skip_all,
	fields(%user_id)
)]
pub async fn get_dehydrated_device_id(&self, user_id: &UserId) -> Result<OwnedDeviceId> {
	self.get_dehydrated_device(user_id)
		.await
		.map(|device| device.device_id)
}

/// Get the dehydrated device private data
#[implement(super::Service)]
#[tracing::instrument(
	level = "debug",
	skip_all,
	fields(%user_id),
	ret,
)]
pub async fn get_dehydrated_device(&self, user_id: &UserId) -> Result<DehydratedDevice> {
	self.db
		.userid_dehydrateddevice
		.get(user_id)
		.await
		.deserialized::<String>()
		.and_then(|raw| serde_json::from_str(&raw).map_err(Into::into))
}
