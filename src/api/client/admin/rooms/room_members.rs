use axum::extract::State;
use ruma::api::error::{ErrorKind, LimitExceededErrorData};
use synapse_admin_api::rooms::room_members::v1::{Request, Response};
use tuwunel_core::{Err, Error, Result};

use super::usize_to_uint;
use crate::{Ruma, client::admin::require_admin};

/// # `GET /_synapse/admin/v1/rooms/{room_id}/members`
///
/// Lists the joined members of a room, local and remote. No pagination.
pub(crate) async fn admin_room_members_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	require_admin(&services, body.sender_user()).await?;

	if !services
		.metadata
		.exists_checked(&body.room_id)
		.await?
	{
		return Err!(Request(NotFound("Room not found")));
	}

	let members = services
		.state_cache
		.bounded_room_members(&body.room_id)
		.await?;
	// Include a conservative envelope for object fields, total and commas.
	let mut response_bytes = 64_usize;
	for member in &members {
		response_bytes = response_bytes
			.saturating_add(serde_json::to_vec(member)?.len())
			.saturating_add(1);
		if response_bytes > 256 * 1024 {
			return Err(Error::Request(
				ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
				"Room member response limit reached".into(),
				http::StatusCode::TOO_MANY_REQUESTS,
			));
		}
	}

	let total = usize_to_uint(members.len());

	Ok(Response { members, total })
}
