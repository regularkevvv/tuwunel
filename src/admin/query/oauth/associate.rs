use ruma::OwnedUserId;
use tuwunel_core::{Err, Result, apply, err, itertools::Itertools};

use crate::admin_command;

#[admin_command]
pub(super) async fn oauth_associate(
	&self,
	provider: String,
	user_id: OwnedUserId,
	claim: Vec<String>,
	force: bool,
) -> Result {
	if !self.services.globals.user_is_local(&user_id) {
		return Err!(Request(NotFound("User {user_id:?} does not belong to this server.")));
	}

	if !self.services.users.exists(&user_id).await {
		return Err!(Request(NotFound("User {user_id:?} is not registered")));
	}

	let provider = self
		.services
		.oauth
		.providers
		.get(&provider)
		.await?;

	let claim = claim
		.iter()
		.map(|kv| {
			let (key, val) = kv
				.split_once('=')
				.ok_or_else(|| err!("Missing '=' in --claim {kv}=???"))?;

			if !key.is_empty() && !val.is_empty() {
				Ok((key, val))
			} else {
				Err!("Missing key or value in --claim=key=value argument")
			}
		})
		.map_ok(apply!(2, ToOwned::to_owned))
		.collect::<Result<_>>()?;

	let (replaced, committed) = self
		.services
		.oauth
		.sessions
		.associate_user(provider.id(), &user_id, claim, force)
		.await?;

	let lead = match committed {
		| 0 => format!("Pending association {}", replaced.map_or("added", |_| "replaced")),
		| n => format!(
			"Replaced {n} committed session(s) across all providers and added pending \
			 association"
		),
	};

	writeln!(self, "{lead} for {user_id} on provider {}.", provider.id()).await
}
