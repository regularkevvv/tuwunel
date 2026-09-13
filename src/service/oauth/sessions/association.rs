//! Pending account associations set by `query oauth associate`.
//!
//! Each records which local account a sign-in whose claims all match
//! reaches. They live in D1 (`oauthidpuserid_pendingclaims`, keyed by
//! provider and user) because the admin command can remove the account's
//! existing sessions, and a claim held only in memory would then be lost with
//! the process.

use std::collections::BTreeMap;

use futures::{StreamExt, pin_mut};
use ruma::{OwnedUserId, UserId};
use serde_json::Value;
use tuwunel_core::{Result, debug, implement, trace, utils::string_from_bytes};
use tuwunel_database::{Deserialized, Interfix, Json};

use super::{Sessions, UserInfo};

pub type Claims = BTreeMap<String, String>;

/// Records the claims a sign-in must present to reach `user_id`, returning
/// the claims it replaces.
#[implement(Sessions)]
pub async fn set_user_association_pending(
	&self,
	idp_id: &str,
	user_id: &UserId,
	claims: Claims,
) -> Result<Option<Claims>> {
	let key = (idp_id, user_id);
	let replaced = self
		.db
		.oauthidpuserid_pendingclaims
		.qry(&key)
		.await
		.deserialized::<Json<Claims>>()
		.map(|Json(claims)| claims)
		.ok();

	self.db
		.oauthidpuserid_pendingclaims
		.put(key, Json(&claims))
		.await?;

	Ok(replaced)
}

/// The account a sign-in with `userinfo` from `idp_id` was associated with,
/// if every recorded claim matches.
#[implement(Sessions)]
pub async fn find_user_association_pending(
	&self,
	idp_id: &str,
	userinfo: &UserInfo,
) -> Option<OwnedUserId> {
	let claiming = serde_json::to_value(userinfo)
		.expect("Failed to transform user_info into serde_json::Value");

	let claiming = claiming
		.as_object()
		.expect("Failed to interpret user_info as object");

	assert!(
		!claiming.is_empty(),
		"Expecting at least one claim from user_info such as `sub`"
	);

	// Claim values carry personal data such as email addresses; only their
	// count is recorded.
	debug!(?idp_id, claims = claiming.len(), "finding pending association");

	let prefix = (idp_id, Interfix);
	let pending = self
		.db
		.oauthidpuserid_pendingclaims
		.stream_prefix_raw(&prefix);

	pin_mut!(pending);
	while let Some(entry) = pending.next().await {
		let Ok((key, val)) = entry else {
			continue;
		};

		let Some(user_id) = key
			.rsplit(|&b| b == 0xFF)
			.next()
			.and_then(|user| string_from_bytes(user).ok())
			.and_then(|user| OwnedUserId::try_from(user).ok())
		else {
			continue;
		};

		let Ok(claimant) = serde_json::from_slice::<Claims>(val) else {
			continue;
		};

		trace!(?user_id, claims = claimant.len(), "checking against pending association");

		// An empty claim set would match every sign-in; it is never recorded
		// by the admin command and never honoured here.
		if !claimant.is_empty()
			&& claimant
				.iter()
				.all(|(claim, value)| claiming.get(claim).and_then(Value::as_str) == Some(value))
		{
			return Some(user_id);
		}
	}

	None
}

#[implement(Sessions)]
pub async fn remove_provider_associations_pending(&self, idp_id: &str) -> Result {
	self.db
		.oauthidpuserid_pendingclaims
		.del_prefix(&(idp_id, Interfix))
		.await
}

#[implement(Sessions)]
pub async fn remove_user_association_pending(
	&self,
	user_id: &UserId,
	idp_id: Option<&str>,
) -> Result {
	let Some(idp_id) = idp_id else {
		return Ok(());
	};

	self.db
		.oauthidpuserid_pendingclaims
		.del((idp_id, user_id))
		.await
}

#[implement(Sessions)]
pub async fn is_user_association_pending(&self, user_id: &UserId) -> bool {
	let pending = self.db.oauthidpuserid_pendingclaims.raw_keys();

	pin_mut!(pending);
	while let Some(key) = pending.next().await {
		if key.is_ok_and(|key| key.rsplit(|&b| b == 0xFF).next() == Some(user_id.as_bytes())) {
			return true;
		}
	}

	false
}
