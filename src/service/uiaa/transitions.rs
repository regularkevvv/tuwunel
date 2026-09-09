use std::fmt;

use ruma::{
	DeviceId, UserId,
	api::client::uiaa::{AuthData, AuthType, UiaaInfo},
};
use tuwunel_core::{Err, Result, err, implement};

use super::Service;

/// MutexMap instruments keys. Never expose a UIAA capability to tracing.
#[derive(Clone, Eq, Hash, PartialEq)]
pub(super) struct SessionKey(String);

impl SessionKey {
	pub(super) fn new(session: &str) -> Self { Self(session.to_owned()) }
}

impl fmt::Debug for SessionKey {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("[redacted UIAA session]")
	}
}

/// Called with the transition lock held. Stored state must refer to the
/// same session capability; malformed state never starts a replacement flow.
#[implement(Service)]
pub(super) async fn load_session(
	&self,
	user: &UserId,
	device: &DeviceId,
	auth: &AuthData,
	template: &UiaaInfo,
	new_session: String,
) -> Result<UiaaInfo> {
	if let Some(session) = auth.session() {
		self.get_uiaa_session(user, device, session).await
	} else {
		let mut info = template.clone();
		info.session = Some(new_session.clone());
		// Reserve durable capacity before any stage consumes a registration
		// token or claims an email proof. A failed first attempt also needs a
		// real session behind the identifier returned to the client.
		self.save_progress(user, device, &new_session, &info, true)
			.await?;
		Ok(info)
	}
}

/// Finish SSO only while the existing, exact-owner session is live. Holding
/// the transition lock across the read and write prevents a delayed callback
/// from resurrecting a session concurrently consumed by the client.
#[implement(Service)]
pub async fn complete_sso(&self, user_id: &UserId, session: &str) -> Result {
	let _transition = self
		.transitions
		.lock(&SessionKey::new(session))
		.await;
	let (owner, device, mut info) = self
		.get_uiaa_session_by_session_id(session)
		.await
		.filter(|(owner, ..)| owner == user_id)
		.ok_or_else(|| err!(Request(Forbidden("UIAA session not found."))))?;

	let has_stage = |stage: &AuthType| {
		info.flows
			.iter()
			.any(|flow| flow.stages.contains(stage))
	};
	let oauth = has_stage(&AuthType::OAuth);
	let sso = has_stage(&AuthType::Sso);
	if !oauth && !sso {
		return Err!(Request(Forbidden("UIAA session does not offer SSO.")));
	}

	if oauth && !info.completed.contains(&AuthType::OAuth) {
		self.services
			.users
			.allow_cross_signing_replacement(&owner)
			.await;
		info.completed.push(AuthType::OAuth);
	}
	if sso && !info.completed.contains(&AuthType::Sso) {
		info.completed.push(AuthType::Sso);
	}

	self.update_uiaa_session(&owner, &device, session, Some(&info))
		.await
}

/// Registration releases its retained session after redeeming the durable
/// email claim. Use the same exclusion as authentication and SSO completion.
#[implement(Service)]
pub async fn delete_session(&self, user: &UserId, device: &DeviceId, session: &str) -> Result {
	let _transition = self
		.transitions
		.lock(&SessionKey::new(session))
		.await;
	self.update_uiaa_session(user, device, session, None)
		.await
}

#[cfg(test)]
mod tests {
	use super::SessionKey;

	#[test]
	fn session_lock_debug_is_redacted() {
		assert_eq!(
			format!("{:?}", SessionKey::new("secret-capability")),
			"[redacted UIAA session]"
		);
	}
}
