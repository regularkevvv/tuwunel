use futures::StreamExt;
use ruma::OwnedUserId;
use tuwunel_core::{Err, Result, apply, err, itertools::Itertools, utils::stream::ReadyExt};

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

	let committed = self
		.services
		.oauth
		.user_sessions(&user_id)
		.ready_filter_map(Result::ok)
		.count()
		.await;

	if committed > 0 && !force {
		return Err!(
			"{user_id} already has {committed} committed OAuth session(s); the pending claim \
			 would be shadowed at login. Re-run with --force to replace existing sessions, or \
			 run `query oauth delete {user_id} --force` first."
		);
	}

	// The claim is durable before any session is removed: a restart between
	// the two leaves the claim, never an account with neither.
	let replaced = self
		.services
		.oauth
		.sessions
		.set_user_association_pending(provider.id(), &user_id, claim)
		.await?;

	if committed > 0 {
		self.services
			.oauth
			.delete_user_sessions(&user_id)
			.await;
	}

	let lead = match committed {
		| 0 => format!("Pending association {}", replaced.map_or("added", |_| "replaced")),
		| n => format!(
			"Replaced {n} committed session(s) across all providers and added pending \
			 association"
		),
	};

	writeln!(self, "{lead} for {user_id} on provider {}.", provider.id()).await
}
