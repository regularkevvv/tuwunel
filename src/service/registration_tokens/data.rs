use std::{fmt, sync::Arc, time::SystemTime};

use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tuwunel_core::{
	Err, Error, Result,
	config::registration_tokens::{MAX_TOKEN_BYTES, valid_token},
	err,
	ruma::api::error::{ErrorKind, LimitExceededErrorData},
	utils::{self, MutexMap},
};
use tuwunel_database::{Database, Json, Map};

pub(super) struct Data {
	registrationtoken_info: Arc<Map>,
	transitions: MutexMap<TokenKey, ()>,
	admission: tokio::sync::Mutex<()>,
}

const MAX_RECORD_BYTES: usize = 1024;

pub(super) fn check_lookup_key(token: &str) -> Result {
	// Preserve exact admin lookup/revocation of legacy keys, but bound the
	// key copied into a transition lock or backend request.
	if token.len() > tuwunel_bridge::MAX_KEY_BYTES {
		return Err!(Request(TooLarge("Registration token lookup key exceeds storage limit")));
	}
	Ok(())
}

fn capacity_error() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Registration token inventory limit reached".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}

/// MutexMap instruments keys. A registration capability is never a trace field.
#[derive(Clone, Eq, Hash, PartialEq)]
struct TokenKey(String);

impl fmt::Debug for TokenKey {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("[redacted registration token]")
	}
}

/// Metadata of a registration token.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct DatabaseTokenInfo {
	/// The number of times this token has been used to create an account.
	pub uses: u64,
	/// When this token will expire, if it expires.
	pub expires: TokenExpires,
}

impl DatabaseTokenInfo {
	pub(super) fn new(expires: TokenExpires) -> Self { Self { uses: 0, expires } }

	/// Determine whether this token info represents a valid token, i.e. one
	/// that has not exhausted its `max_uses` or passed its `max_age`. When
	/// both `expires.max_uses` and `expires.max_age` are `None`, this always
	/// returns `true`.
	#[must_use]
	pub fn is_valid(&self) -> bool {
		if let Some(max_uses) = self.expires.max_uses
			&& self.uses >= max_uses
		{
			return false;
		}

		if let Some(max_age) = self.expires.max_age {
			let now = SystemTime::now();

			if now > max_age {
				return false;
			}
		}

		true
	}
}

impl fmt::Display for DatabaseTokenInfo {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Token used {} times. {}", self.uses, self.expires)?;

		Ok(())
	}
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct TokenExpires {
	pub max_uses: Option<u64>,
	pub max_age: Option<SystemTime>,
}

impl fmt::Display for TokenExpires {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut msgs = vec![];

		if let Some(max_uses) = self.max_uses {
			msgs.push(format!("after {max_uses} uses"));
		}

		if let Some(max_age) = self.max_age {
			let now = SystemTime::now();
			let expires_at = utils::time::format(max_age, "%F %T");

			match max_age.duration_since(now) {
				| Ok(duration) => {
					let expires_in = utils::time::pretty(duration);
					msgs.push(format!("in {expires_in} ({expires_at})"));
				},
				| Err(_) => {
					write!(f, "Expired at {expires_at}")?;
					return Ok(());
				},
			}
		}

		if !msgs.is_empty() {
			write!(f, "Expires {}.", msgs.join(" or "))?;
		} else {
			write!(f, "Never expires.")?;
		}

		Ok(())
	}
}

impl Data {
	pub(super) fn new(db: &Arc<Database>) -> Self {
		Self {
			registrationtoken_info: db["registrationtoken_info"].clone(),
			transitions: MutexMap::new(),
			admission: tokio::sync::Mutex::new(()),
		}
	}

	/// Associate a registration token with its metadata in the database.
	pub(super) async fn save_token(
		&self,
		token: &str,
		expires: TokenExpires,
	) -> Result<DatabaseTokenInfo> {
		if !valid_token(token) {
			return Err!(Request(InvalidParam("Invalid registration token identifier")));
		}
		let _transition = self
			.transitions
			.lock(&TokenKey(token.to_owned()))
			.await;
		let _admission = self.admission.lock().await;
		match self.registrationtoken_info.get(token).await {
			| Ok(_) => return Err!(Request(InvalidParam("Registration token already exists"))),
			| Err(error) if error.is_not_found() => {},
			| Err(error) => return Err(error),
		}
		if self.bounded_keys().await?.len() >= super::MAX_DATABASE_TOKENS {
			return Err(capacity_error());
		}
		let info = DatabaseTokenInfo::new(expires);
		self.registrationtoken_info
			.raw_put(token, Json(&info))
			.await?;
		Ok(info)
	}

	/// Delete a registration token.
	pub(super) async fn revoke_token(&self, token: &str) -> Result {
		check_lookup_key(token)?;
		let _transition = self
			.transitions
			.lock(&TokenKey(token.to_owned()))
			.await;
		match self.registrationtoken_info.get(token).await {
			| Ok(_) => self.registrationtoken_info.remove(token).await,
			| Err(error) if error.is_not_found() =>
				Err!(Request(NotFound("Registration token not found"))),
			| Err(error) => Err(error),
		}
	}

	/// Look up a registration token's metadata.
	pub(super) async fn check_token(&self, token: &str, consume: bool) -> Result<bool> {
		if !valid_token(token) {
			return Ok(false);
		}
		let _transition = self
			.transitions
			.lock(&TokenKey(token.to_owned()))
			.await;
		let mut info = match self.get_token_info(token).await {
			| Ok(info) => info,
			| Err(error) if error.is_not_found() => return Ok(false),
			| Err(error) => return Err(error),
		};

		if !info.is_valid() {
			self.registrationtoken_info.remove(token).await?;
			return Ok(false);
		}

		if consume {
			info.uses = info
				.uses
				.checked_add(1)
				.ok_or_else(|| err!("Registration token use counter overflow"))?;

			if info.is_valid() {
				self.registrationtoken_info
					.raw_put(token, Json(info))
					.await?;
			} else {
				self.registrationtoken_info.remove(token).await?;
			}
		}

		Ok(true)
	}

	/// Read current metadata. Absence, corruption and I/O failure stay
	/// distinct.
	pub(super) async fn get_token_info(&self, token: &str) -> Result<DatabaseTokenInfo> {
		check_lookup_key(token)?;
		let value = self.registrationtoken_info.get(token).await?;
		if value.len() > MAX_RECORD_BYTES {
			return Err!("Registration token metadata exceeds record-size limit");
		}
		serde_json::from_slice(value.as_ref())
			.map_err(|_| err!("Invalid registration token metadata"))
	}

	/// Replace a token's expiry while preserving its use counter.
	pub(super) async fn update_token(
		&self,
		token: &str,
		expires: TokenExpires,
	) -> Result<DatabaseTokenInfo> {
		check_lookup_key(token)?;
		let _transition = self
			.transitions
			.lock(&TokenKey(token.to_owned()))
			.await;
		let current = self.get_token_info(token).await?;

		let info = DatabaseTokenInfo { uses: current.uses, expires };

		self.registrationtoken_info
			.raw_put(token, Json(&info))
			.await?;

		Ok(info)
	}

	/// Collect all valid tokens, deleting expired ones on the way.
	pub(super) async fn iterate_and_clean_tokens(
		&self,
	) -> Result<Vec<(String, DatabaseTokenInfo)>> {
		// Close the scan before mutation. Re-read under the same lock as
		// consumption/revocation/update: an old expiry snapshot must not delete
		// a token that an operator has renewed in the meantime.
		let all = self.bounded_keys().await?;

		let mut valid = Vec::new();
		for token in all {
			let _transition = self
				.transitions
				.lock(&TokenKey(token.clone()))
				.await;
			let info = match self.get_token_info(&token).await {
				| Ok(info) => info,
				| Err(error) if error.is_not_found() => continue,
				| Err(error) => return Err(error),
			};
			if info.is_valid() {
				valid.push((token, info));
			} else {
				self.registrationtoken_info.remove(&token).await?;
			}
		}

		Ok(valid)
	}

	async fn bounded_keys(&self) -> Result<Vec<String>> {
		let keys: Vec<String> = self
			.registrationtoken_info
			.raw_keys()
			.take(super::MAX_DATABASE_TOKENS.saturating_add(1))
			.map(|key| {
				let key = key?;
				if key.len() > MAX_TOKEN_BYTES {
					return Err!("Registration token inventory contains an oversized legacy key");
				}
				let token = std::str::from_utf8(key)
					.map_err(|_| err!("Invalid registration token key"))?;
				if !valid_token(token) {
					return Err!(
						"Registration token inventory contains an unsupported legacy key"
					);
				}
				Ok(token.to_owned())
			})
			.try_collect()
			.await?;
		if keys.len() > super::MAX_DATABASE_TOKENS {
			return Err(capacity_error());
		}
		Ok(keys)
	}
}

#[cfg(test)]
mod tests {
	use super::TokenKey;

	#[test]
	fn token_lock_debug_is_redacted() {
		assert_eq!(
			format!("{:?}", TokenKey("secret-capability".into())),
			"[redacted registration token]"
		);
	}
}
