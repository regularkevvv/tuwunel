use futures::{StreamExt, pin_mut};
use ruma::{
	OwnedUserId, UserId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use tuwunel_core::{Error, Result};
use tuwunel_database::successor;

use super::Service;

/// Maximum inventory rows included in one admin listing page.
pub const MAX_LOCAL_USER_PAGE_ROWS: usize = 32;
const MAX_USER_ID_BYTES: usize = 4096;

/// A fresh bounded snapshot of one part of the user inventory. The cursor
/// refers to the last included row, even if that account is disabled.
#[derive(Debug)]
pub struct LocalUserPage {
	/// Accounts with nonempty stored passwords from the included rows.
	pub users: Vec<OwnedUserId>,
	/// Included rows plus a possible lookahead row, at most `limit + 1`.
	pub examined: usize,
	/// Last included inventory row when another page exists.
	pub next: Option<OwnedUserId>,
}

fn output_limit() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"User page exceeds the user-id response budget".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}

impl Service {
	/// Examines at most `limit + 1` rows and closes the read before returning.
	/// Accounts with empty stored passwords still consume the row budget. The
	/// lookahead detects another page; it is not skipped by the next cursor.
	/// Storage/decoding errors fail the whole page. Pages are separate
	/// snapshots, so an insertion behind a returned cursor is not included in
	/// later pages.
	pub async fn local_user_page(
		&self,
		after: Option<&UserId>,
		limit: usize,
	) -> Result<LocalUserPage> {
		if !(1..=MAX_LOCAL_USER_PAGE_ROWS).contains(&limit) {
			return Err(Error::BadRequest(
				ErrorKind::InvalidParam,
				"User page limit must be between 1 and 32 rows",
			));
		}
		let from = after.map(|user| successor(user.as_bytes()));
		let rows = self
			.db
			.userid_password
			.stream_capped_from::<&UserId, &[u8]>(from.as_deref(), limit.saturating_add(1));
		pin_mut!(rows);
		let mut users = Vec::new();
		let mut examined = 0_usize;
		let mut last = None;
		let mut more = false;
		let mut bytes = 0_usize;
		while let Some(row) = rows.next().await {
			let (user, password) = row?;
			examined = examined.saturating_add(1);
			if examined > limit {
				more = true;
				break;
			}
			last = Some(user.to_owned());
			if !password.is_empty() {
				bytes = bytes.saturating_add(user.as_bytes().len());
				if bytes > MAX_USER_ID_BYTES {
					return Err(output_limit());
				}
				users.push(user.to_owned());
			}
		}
		let next = if more { last } else { None };
		if bytes.saturating_add(
			next.as_ref()
				.map_or(0, |user| user.as_bytes().len()),
		) > MAX_USER_ID_BYTES
		{
			return Err(output_limit());
		}
		Ok(LocalUserPage { users, examined, next })
	}
}
