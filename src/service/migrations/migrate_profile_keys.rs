use futures::StreamExt;
use ruma::{UserId, profile::ProfileFieldName};
use serde::de::IgnoredAny;
use tuwunel_core::{Result, info, utils::stream::TryExpect, warn};
use tuwunel_database::Json;

use crate::Services;

/// Relocates the per-user displayname and avatar_url out of their dedicated
/// columns into the unified useridprofilekey_value store keyed by MSC4133 field
/// name, where the profile service now reads them.
///
/// The dedicated columns are left intact, so an older binary opening the same
/// database still resolves.
pub(super) async fn migrate_profile_keys(services: &Services) -> Result {
	let db = &services.db;
	let cork = db.cork_and_sync();

	let userid_displayname = db["userid_displayname"].clone();
	let userid_avatarurl = db["userid_avatarurl"].clone();
	let userid_blurhash = db["userid_blurhash"].clone();
	let useridprofilekey_value = db["useridprofilekey_value"].clone();

	warn!(
		"Relocating displaynames, avatar_urls and blurhashes into the unified profile-key store"
	);

	let mut displaynames = 0_usize;
	{
		let stream = userid_displayname.stream().expect_ok();
		futures::pin_mut!(stream);
		while let Some(item) = stream.next().await {
			let (user_id, displayname): (&UserId, &str) = item;
			let key = (user_id, ProfileFieldName::DisplayName.as_str());
			let value = displayname.to_owned();

			useridprofilekey_value
				.put(key, Json(value))
				.await?;

			displaynames = displaynames.saturating_add(1);
		}
	}

	let mut avatar_urls = 0_usize;
	{
		let stream = userid_avatarurl.stream().expect_ok();
		futures::pin_mut!(stream);
		while let Some(item) = stream.next().await {
			let (user_id, avatar_url): (&UserId, &str) = item;
			let key = (user_id, ProfileFieldName::AvatarUrl.as_str());
			let value = avatar_url.to_owned();

			useridprofilekey_value
				.put(key, Json(value))
				.await?;

			avatar_urls = avatar_urls.saturating_add(1);
		}
	}

	let mut blurhashes = 0_usize;
	{
		let stream = userid_blurhash.stream().expect_ok();
		futures::pin_mut!(stream);
		while let Some(item) = stream.next().await {
			let (user_id, blurhash): (&UserId, &str) = item;
			let key = (user_id, "xyz.amorgan.blurhash");
			let value = blurhash.to_owned();

			useridprofilekey_value
				.put(key, Json(value))
				.await?;

			blurhashes = blurhashes.saturating_add(1);
		}
	}

	let mut fixed_strings = 0_usize;
	{
		let stream = useridprofilekey_value.raw_stream().expect_ok();
		futures::pin_mut!(stream);
		while let Some((key, value)) = stream.next().await {
			if serde_json::from_slice::<IgnoredAny>(value).is_err() {
				let Ok(string) = str::from_utf8(value) else {
					warn!("Non-UTF8 data in profile value: {key:?} => {value:?}");
					useridprofilekey_value.remove(key).await?;
					continue;
				};
				useridprofilekey_value
					.raw_put(key, Json(string))
					.await?;
				fixed_strings = fixed_strings.saturating_add(1);
			}
		}
	}

	drop(cork);
	info!(%displaynames, %avatar_urls, %blurhashes, %fixed_strings, "Relocated profile keys into useridprofilekey_value");

	db["global"]
		.insert(b"migrate_profile_keys_to_useridprofilekey", [])
		.await?;
	useridprofilekey_value.sort()
}
