use futures::StreamExt;
use ruma::UserId;
use tuwunel_core::{Result, info, utils::stream::TryIgnore, warn};
use tuwunel_database::SEP;

use crate::Services;

pub(super) async fn upgrade_legacy_mediaid_user(services: &Services) -> Result {
	let db = &services.db;
	let cork = db.cork_and_sync();
	let mediaid_user = db["mediaid_user"].clone();

	warn!("Upgrading legacy mediaid_user keys to composite (mxc, user_id) layout");

	let (mut checked, mut upgraded, mut removed_invalid) = (0_usize, 0_usize, 0_usize);
	{
		let stream = mediaid_user.raw_stream().ignore_err();
		futures::pin_mut!(stream);
		while let Some((raw_key, raw_val)) = stream.next().await {
			checked = checked.saturating_add(1);

			let has_sep = raw_key.contains(&SEP);
			let user_id = str::from_utf8(raw_val)
				.ok()
				.and_then(|s| <&UserId>::try_from(s).ok());

			match (has_sep, user_id) {
				| (true, _) => {},
				| (false, None) => {
					warn!(?raw_key, ?raw_val, "Legacy entry has unparsable user_id, removing");

					mediaid_user.remove(raw_key).await?;
					removed_invalid = removed_invalid.saturating_add(1);
				},
				| (false, Some(user_id)) => {
					let mut new_key = raw_key.to_vec();

					new_key.push(SEP);
					new_key.extend_from_slice(user_id.as_bytes());

					mediaid_user
						.put_raw(new_key, user_id.as_str())
						.await?;
					mediaid_user.remove(raw_key).await?;

					upgraded = upgraded.saturating_add(1);
				},
			}
		}
	}

	drop(cork);
	info!(
		%checked,
		%upgraded,
		%removed_invalid,
		"Upgraded legacy mediaid_user keys"
	);

	db["global"]
		.insert(b"upgrade_legacy_mediaid_user", [])
		.await?;
	mediaid_user.sort()
}
