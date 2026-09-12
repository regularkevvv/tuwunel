use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use ruma::{
	OwnedDeviceId,
	events::{
		GlobalAccountDataEvent, GlobalAccountDataEventType, push_rules::PushRulesEventContent,
	},
	push::Ruleset,
};
use tuwunel_core::{Result, debug_warn, error, warn};

pub struct Service {
	services: Arc<crate::services::OnceServices>,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self { services: args.services.clone() }))
	}

	async fn worker(self: Arc<Self>) -> Result {
		let unset = self
			.services
			.config
			.emergency_password
			.as_ref()
			.is_none_or(String::is_empty);

		if self.services.globals.is_read_only() {
			if !unset {
				debug_warn!("emergency password feature ignored in read_only mode.");
			}
			return Ok(());
		}

		if unset {
			return self
				.seal_emergency_access()
				.await
				.inspect_err(|e| {
					error!("Failed to seal the server user after emergency access: {e}");
				});
		}

		if self.services.config.ldap.enable {
			warn!("emergency password feature not available with LDAP enabled.");
			return Ok(());
		}

		self.set_emergency_access()
			.await
			.inspect_err(|e| {
				error!("Failed to set the emergency password for the server user: {e}");
			})
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Seals the server user when no emergency password is configured: clears
	/// its password and signs out every session, so the access a break-glass
	/// release granted ends with the release that withdraws it
	/// (docs/runbooks/break-glass.md). Nothing is written when there is nothing
	/// to seal.
	async fn seal_emergency_access(&self) -> Result {
		let server_user = &self.services.globals.server_user;
		let has_password = self
			.services
			.users
			.password_hash(server_user)
			.await
			.is_ok_and(|hash| !hash.is_empty());

		let devices: Vec<OwnedDeviceId> = self
			.services
			.users
			.all_device_ids(server_user)
			.map(ToOwned::to_owned)
			.collect()
			.await;

		if !has_password && devices.is_empty() {
			return Ok(());
		}

		warn!("The emergency password is unset: signing the server account out and clearing it.");
		self.services
			.users
			.set_password(server_user, None)
			.await?;

		for device in &devices {
			self.services
				.users
				.remove_device(server_user, device)
				.await;
		}

		Ok(())
	}

	/// Sets the emergency password and push rules for the server user account
	/// in case emergency password is set
	async fn set_emergency_access(&self) -> Result {
		let server_user = &self.services.globals.server_user;

		self.services
			.users
			.set_password(server_user, self.services.config.emergency_password.as_deref())
			.await?;

		let (ruleset, pwd_set) = match self.services.config.emergency_password {
			| Some(_) => (Ruleset::server_default(server_user), true),
			| None => (Ruleset::new(), false),
		};

		self.services
			.account_data
			.update(
				None,
				server_user,
				GlobalAccountDataEventType::PushRules
					.to_string()
					.into(),
				&serde_json::to_value(&GlobalAccountDataEvent {
					content: PushRulesEventContent { global: ruleset },
				})
				.expect("to json value always works"),
			)
			.await?;

		if pwd_set {
			warn!(
				"The server account emergency password is set! Please unset it as soon as you \
				 finish admin account recovery! You will be logged out of the server service \
				 account when you finish."
			);
			Ok(())
		} else {
			// logs out any users still in the server service account and removes sessions
			self.services
				.users
				.deactivate_account(server_user)
				.await
		}
	}
}
