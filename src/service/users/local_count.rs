use futures::{StreamExt, pin_mut};
use ruma::{
	UserId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use tuwunel_core::{Error, Result};

use super::Service;

/// Total user-map rows examined by the private count diagnostic, including
/// inactive accounts. One additional row detects overflow without a short
/// count.
pub const MAX_LOCAL_USER_COUNT_ROWS: usize = 1024;

impl Service {
	/// Counts accounts with a nonempty stored password, preserving the existing
	/// diagnostic's sentinel/disabled semantics. Refuses larger inventories and
	/// propagates storage or deserialization errors rather than counting a
	/// prefix.
	pub async fn bounded_local_user_count(&self) -> Result<usize> {
		let rows = self
			.db
			.userid_password
			.stream_capped::<&UserId, &[u8]>(MAX_LOCAL_USER_COUNT_ROWS.saturating_add(1));
		pin_mut!(rows);
		let mut scanned = 0_usize;
		let mut count = 0_usize;
		while let Some(row) = rows.next().await {
			let (_, password) = row?;
			if scanned >= MAX_LOCAL_USER_COUNT_ROWS {
				return Err(Error::Request(
					ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
					"User count inventory limit reached; use paginated admin user listing".into(),
					http::StatusCode::TOO_MANY_REQUESTS,
				));
			}
			scanned = scanned.saturating_add(1);
			if !password.is_empty() {
				count = count.saturating_add(1);
			}
		}
		Ok(count)
	}
}
