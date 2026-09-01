use std::sync::Arc;

use futures::StreamExt;
use ruma::{MilliSecondsSinceUnixEpoch, OwnedUserId, UserId};
use serde::Deserialize;
use tuwunel_core::{Result, debug_warn, err, utils::stream::TryIgnore, warn};
use tuwunel_database::{Json, Map};

use crate::{Services, users::Moderation};

/// Some databases store suspension and lock state under differently-named
/// columns and a richer value; tuwunel keeps only the forensic `when`/`by`,
/// with row presence as the flag. `suspended_by` is an unvalidated string in
/// the source, so it is parsed leniently when copied.
#[derive(Deserialize)]
struct ForeignModeration {
	suspended: bool,
	suspended_at: MilliSecondsSinceUnixEpoch,
	suspended_by: String,
}

/// Copies foreign suspension and lock columns into tuwunel's own. Runs every
/// boot and is non-destructive: the source columns are read, never removed, so
/// a database also opened by its origin server keeps working. A no-op when the
/// source columns are absent.
pub(super) async fn migrate_moderation(services: &Services) -> Result {
	copy_moderation(services, "userid_suspension", "userid_suspended").await?;
	copy_moderation(services, "userid_lock", "userid_locked").await?;

	Ok(())
}

async fn copy_moderation(
	services: &Services,
	source_name: &'static str,
	target_name: &'static str,
) -> Result {
	let db = &services.db;

	let Some(source) = db.open_cf(source_name)? else {
		return Ok(());
	};

	let target = &db[target_name];
	let server_user = services.globals.server_user.as_ref();

	let cork = db.cork_and_sync();
	let (copied, skipped) = source
		.raw_stream()
		.ignore_err()
		.fold((0_usize, 0_usize), |acc, (key, value)| async move {
			tally(acc, copy_one(target, server_user, key, value).await)
		})
		.await;

	drop(cork);

	if skipped > 0 {
		warn!(%copied, %skipped, source = %source_name, "Imported moderation entries; some skipped");
	}

	Ok(())
}

fn tally((copied, skipped): (usize, usize), result: Result<bool>) -> (usize, usize) {
	match result {
		| Ok(true) => (copied.saturating_add(1), skipped),
		| Ok(false) => (copied, skipped),
		| Err(e) => {
			debug_warn!(error = %e, "skipping unreadable moderation entry");
			(copied, skipped.saturating_add(1))
		},
	}
}

/// Writes one `Moderation` into the target column. A `false` return is a
/// cleared entry (`suspended == false`), which has no tuwunel representation.
async fn copy_one(
	target: &Arc<Map>,
	server_user: &UserId,
	key: &[u8],
	value: &[u8],
) -> Result<bool> {
	let entry: ForeignModeration = serde_json::from_slice(value)
		.map_err(|e| err!(Database("moderation entry is not JSON: {e}")))?;

	if !entry.suspended {
		return Ok(false);
	}

	let moderation = to_moderation(entry, server_user);

	target.raw_put(key, Json(moderation)).await?;

	Ok(true)
}

/// Maps a foreign entry to tuwunel's `Moderation`. The suspension is preserved
/// even when the recorded actor is unparsable, attributing it to the importing
/// server; the actor is forensic only, while the row's presence is the flag.
fn to_moderation(entry: ForeignModeration, fallback: &UserId) -> Moderation {
	Moderation {
		when: entry.suspended_at,
		by: OwnedUserId::try_from(entry.suspended_by).unwrap_or_else(|_| fallback.to_owned()),
	}
}

#[cfg(test)]
mod tests {
	use ruma::user_id;

	use super::{ForeignModeration, to_moderation};

	#[test]
	fn foreign_suspension_maps_to_moderation() {
		let json = br#"{"suspended":true,"suspended_at":1700000000000,"suspended_by":"@mod:example.org"}"#;

		let entry: ForeignModeration =
			serde_json::from_slice(json).expect("foreign moderation deserializes");
		assert!(entry.suspended);

		let moderation = to_moderation(entry, user_id!("@import:localhost"));

		assert_eq!(u64::from(moderation.when.get()), 1_700_000_000_000);
		assert_eq!(moderation.by.as_str(), "@mod:example.org");
	}

	#[test]
	fn unparsable_actor_falls_back_to_server() {
		let json = br#"{"suspended":true,"suspended_at":1,"suspended_by":"not-a-user-id"}"#;

		let entry: ForeignModeration =
			serde_json::from_slice(json).expect("foreign moderation deserializes");

		// The suspension is still carried; only the forensic actor falls back.
		let moderation = to_moderation(entry, user_id!("@import:localhost"));

		assert_eq!(moderation.by.as_str(), "@import:localhost");
	}

	#[test]
	fn cleared_entry_is_recognized() {
		let json = br#"{"suspended":false,"suspended_at":1,"suspended_by":"@a:b.c"}"#;

		let entry: ForeignModeration =
			serde_json::from_slice(json).expect("foreign moderation deserializes");

		assert!(!entry.suspended);
	}

	#[test]
	fn unknown_fields_are_ignored() {
		let json =
			br#"{"suspended":true,"suspended_at":1,"suspended_by":"@a:b.c","reason":"spam"}"#;

		let entry: ForeignModeration =
			serde_json::from_slice(json).expect("unknown fields are ignored");

		assert!(entry.suspended);
	}
}
