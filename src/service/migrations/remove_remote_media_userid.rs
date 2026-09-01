use futures::StreamExt;
use ruma::{MxcUri, UserId};
use tuwunel_core::{Result, info, utils::stream::TryExpect, warn};

use crate::Services;

pub(super) async fn remove_remote_media_userid(services: &Services) -> Result {
	let db = &services.db;
	let cork = db.cork_and_sync();
	let mediaid_user = db["mediaid_user"].clone();

	warn!("Removing stored user id for remote media");

	let (mut checked, mut removed_remote, mut removed_invalid) = (0_usize, 0_usize, 0_usize);
	{
		let stream = mediaid_user.keys().expect_ok();
		futures::pin_mut!(stream);
		while let Some(item) = stream.next().await {
			let (mxc_uri, user_id): (&MxcUri, &UserId) = item;
			checked = checked.saturating_add(1);

			let Ok(mxc) = mxc_uri.parts() else {
				warn!(?mxc_uri, "Invalid MXC URL, removing it");

				mediaid_user.del((mxc_uri, user_id)).await?;

				removed_invalid = removed_invalid.saturating_add(1);

				continue;
			};

			if !services.globals.server_is_ours(mxc.server_name) {
				mediaid_user.del((mxc_uri, user_id)).await?;

				removed_remote = removed_remote.saturating_add(1);
			}
		}
	}

	drop(cork);
	info!(
		%checked,
		%removed_remote,
		%removed_invalid,
		"Removed stored user id for remote media"
	);

	db["global"]
		.insert(b"remove_remote_media_userid", [])
		.await?;
	mediaid_user.sort()
}
