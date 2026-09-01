use futures::StreamExt;
use tuwunel_core::{
	Result, debug_info, debug_warn, info, itertools::Itertools, utils::stream::TryIgnore, warn,
};

use crate::Services;

pub(super) async fn fix_bad_double_separator_in_state_cache(services: &Services) -> Result {
	warn!("Fixing bad double separator in state_cache roomuserid_joined");

	let db = &services.db;
	let roomuserid_joined = &db["roomuserid_joined"];
	let _cork = db.cork_and_sync();

	let mut iter_count: usize = 0;

	{
		let stream = roomuserid_joined.raw_stream().ignore_err();
		futures::pin_mut!(stream);
		while let Some((key, value)) = stream.next().await {
			let mut key = key.to_vec();
			iter_count = iter_count.saturating_add(1);
			debug_info!(%iter_count);
			let Some(first_sep_index) = key.iter().position(|&i| i == 0xFF) else {
				debug_warn!(?key, "roomuserid_joined key has no 0xFF separator; skipping");
				continue;
			};

			if key
				.iter()
				.get(first_sep_index..=first_sep_index.saturating_add(1))
				.copied()
				.collect_vec()
				== vec![0xFF, 0xFF]
			{
				debug_warn!("Found bad key: {key:?}");
				roomuserid_joined.remove(&key).await?;

				key.remove(first_sep_index);
				debug_warn!("Fixed key: {key:?}");
				roomuserid_joined.insert(&key, value).await?;
			}
		}
	}

	info!("Finished fixing");

	db["global"]
		.insert(b"fix_bad_double_separator_in_state_cache", [])
		.await?;
	roomuserid_joined.sort()
}
