use axum::extract::State;
use ruma::api::client::account::check_registration_token_validity;
use tuwunel_core::{Err, Result};

use crate::Ruma;

/// # `GET /_matrix/client/v1/register/m.login.registration_token/validity`
///
/// Checks if the provided registration token is valid at the time of checking
///
/// Currently does not have any ratelimiting, and this isn't very practical as
/// there is only one registration token allowed.
pub(crate) async fn check_registration_token_validity(
	State(services): State<crate::State>,
	body: Ruma<check_registration_token_validity::v1::Request>,
) -> Result<check_registration_token_validity::v1::Response> {
	if !services.registration_tokens.is_enabled().await? {
		return Err!(Request(Forbidden("Server does not allow token registration")));
	}

	let valid = match services
		.registration_tokens
		.is_token_valid(&body.token)
		.await
	{
		| Ok(()) => true,
		| Err(error) if error.kind() == ruma::api::error::ErrorKind::forbidden() => false,
		| Err(error) => return Err(error),
	};

	Ok(check_registration_token_validity::v1::Response { valid })
}
