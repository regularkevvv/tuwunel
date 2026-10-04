use futures::{StreamExt, pin_mut};
use ruma::{
	OwnedUserId, UserId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use tuwunel_core::{Error, Result};

use super::Service;

/// Total user-map rows examined by the private count diagnostic, including
/// inactive accounts. One additional row detects overflow without a short
/// count.
pub const MAX_LOCAL_USER_COUNT_ROWS: usize = 1024;
const MAX_LOCAL_USER_INVENTORY_BYTES: usize = 128 * 1024;

impl Service {
	/// A complete local-user inventory for administrative work. Examines at
	/// most 1,025 rows including disabled accounts, retains at most 128 KiB of
	/// user IDs, and closes the read before any caller starts changing users or
	/// rooms. Refuses an oversized or corrupt inventory without a partial list.
	pub async fn bounded_local_users(&self) -> Result<Vec<OwnedUserId>> {
		let rows = self
			.db
			.userid_password
			.stream_capped::<&UserId, &[u8]>(MAX_LOCAL_USER_COUNT_ROWS.saturating_add(1));
		pin_mut!(rows);
		let mut users = Vec::new();
		let mut scanned = 0_usize;
		let mut bytes = 0_usize;
		while let Some(row) = rows.next().await {
			let (user, password) = row?;
			if scanned >= MAX_LOCAL_USER_COUNT_ROWS {
				return Err(inventory_limit());
			}
			scanned = scanned.saturating_add(1);
			if password.is_empty() {
				continue;
			}
			bytes = bytes.saturating_add(user.as_bytes().len());
			if bytes > MAX_LOCAL_USER_INVENTORY_BYTES {
				return Err(inventory_limit());
			}
			users.push(user.to_owned());
		}
		Ok(users)
	}

	/// Counts accounts with a nonempty stored password, preserving the existing
	/// diagnostic's sentinel/disabled semantics. Refuses larger inventories and
	/// propagates storage or deserialization errors rather than counting a
	/// prefix.
	pub async fn bounded_local_user_count(&self) -> Result<usize> {
		self.count_user_inventory(true).await
	}

	/// Counts every registered inventory row, including inactive accounts,
	/// without collecting owned IDs. Refuses beyond 1,024 rows or on decoding
	/// errors; this is distinct from the active-local diagnostic's semantics.
	pub async fn bounded_user_count(&self) -> Result<usize> {
		self.count_user_inventory(false).await
	}

	async fn count_user_inventory(&self, local_only: bool) -> Result<usize> {
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
					"User count inventory limit reached".into(),
					http::StatusCode::TOO_MANY_REQUESTS,
				));
			}
			scanned = scanned.saturating_add(1);
			if !local_only || !password.is_empty() {
				count = count.saturating_add(1);
			}
		}
		Ok(count)
	}
}

fn inventory_limit() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"User inventory limit reached; use an explicit bounded user list".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}
