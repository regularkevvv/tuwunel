use axum::extract::State;
use ruma::api::client::backup::{create_backup_version, get_latest_backup_info};
use tuwunel_core::{Result, err};

use super::{get_count_etag, validate_algorithm_shape};
use crate::Ruma;

/// # `POST /_matrix/client/r0/room_keys/version`
///
/// Creates a new backup.
pub(crate) async fn create_backup_version_route(
	State(services): State<crate::State>,
	body: Ruma<create_backup_version::v3::Request>,
) -> Result<create_backup_version::v3::Response> {
	validate_algorithm_shape(&body.algorithm)
		.map_err(|e| err!(Request(BadJson("Invalid backup metadata: {e}"))))?;

	let version = services
		.key_backups
		.create_backup(body.sender_user(), &body.algorithm)
		.await?;

	Ok(create_backup_version::v3::Response { version })
}

/// # `GET /_matrix/client/r0/room_keys/version`
///
/// Get information about the latest backup version.
pub(crate) async fn get_latest_backup_info_route(
	State(services): State<crate::State>,
	body: Ruma<get_latest_backup_info::v3::Request>,
) -> Result<get_latest_backup_info::v3::Response> {
	let (version, algorithm) = services
		.key_backups
		.get_latest_backup(body.sender_user())
		.await
		.map_err(|_| err!(Request(NotFound("Key backup does not exist."))))?;

	validate_algorithm_shape(&algorithm)
		.map_err(|e| err!(Request(NotFound("Key backup does not exist: {e}"))))?;

	let (count, etag) = get_count_etag(&services, body.sender_user(), &version).await?;

	Ok(get_latest_backup_info::v3::Response { algorithm, count, etag, version })
}
