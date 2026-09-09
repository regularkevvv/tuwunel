//! A token use and its durable UIAA completion marker share one transaction.
use ruma::{
	DeviceId, UserId,
	api::client::uiaa::{AuthType, UiaaInfo},
};
use tuwunel_core::{Result, err, implement};

use super::Service;

/// The caller holds the UIAA session lock. The token service acquires its
/// token lock, then prepares this progress without acquiring another session
/// lock. Both remain held until the atomic commit resolves. Preparation or
/// token validation failure leaves both durable records unchanged.
#[implement(Service)]
pub(super) async fn complete_registration_token(
	&self,
	user: &UserId,
	device: &DeviceId,
	info: &UiaaInfo,
	token: &str,
) -> Result<UiaaInfo> {
	let session = info
		.session
		.as_deref()
		.ok_or_else(|| err!(Request(Forbidden("Missing UIAA session identifier."))))?;
	let mut progress = info.clone();
	progress
		.completed
		.push(AuthType::RegistrationToken);
	self.services
		.registration_tokens
		.consume_with(token, || self.prepare_progress(user, device, session, &progress))
		.await?;
	Ok(progress)
}
