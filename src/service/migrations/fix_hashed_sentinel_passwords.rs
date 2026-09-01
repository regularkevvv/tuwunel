use futures::StreamExt;
use tuwunel_core::{
	Result, debug, err, info,
	utils::{
		hash::{password, verify_password},
		stream::TryExpect,
	},
	warn,
};

use crate::Services;

pub(super) async fn fix_hashed_sentinel_passwords(services: &Services) -> Result {
	const PASSWORD_SENTINEL: &str = "*";

	if services.config.identity_provider.is_empty() {
		debug!("Skipping sentinel password migration since no SSO IdP configured.");
		return Ok(());
	}

	let db = &services.db;
	let cork = db.cork_and_sync();
	let userid_password = db["userid_password"].clone();
	let hashed_sentinel = password(PASSWORD_SENTINEL).map_err(|e| {
		err!("Could not apply migration: failed to hash sentinel password: {e:?}")
	})?;

	warn!(
		"Fixing occurrences of password-hash {hashed_sentinel:?} generated from \
		 {PASSWORD_SENTINEL:?}"
	);

	let (mut checked, mut good, mut bad) = (0_usize, 0_usize, 0_usize);
	{
		let stream = userid_password.stream().expect_ok();
		futures::pin_mut!(stream);
		while let Some(item) = stream.next().await {
			let (key, val): (&str, &str) = item;
			let good_sentinel = val == PASSWORD_SENTINEL;
			let bad_sentinel = !val.is_empty()
				&& !good_sentinel
				&& verify_password(PASSWORD_SENTINEL, val).is_ok();

			checked = checked.saturating_add(usize::from(true));
			good = good.saturating_add(usize::from(good_sentinel));
			bad = bad.saturating_add(usize::from(bad_sentinel));

			if bad_sentinel {
				userid_password
					.insert(key, PASSWORD_SENTINEL)
					.await?;
			}
		}
	}

	drop(cork);
	info!(?checked, ?good, ?bad, "Fixed any occurrences of hashed sentinel passwords");

	db["global"]
		.insert(b"fix_hashed_sentinel_passwords", [])
		.await?;
	userid_password.sort()
}
