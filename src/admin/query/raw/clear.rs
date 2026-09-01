use futures::{FutureExt, StreamExt};
use tokio::time::Instant;
use tuwunel_core::{Err, Result, utils::stream::TryIgnore};

use crate::admin_command;

#[admin_command]
pub(super) async fn raw_clear(&self, map: String, confirm: bool) -> Result {
	let map = self.services.db.get(&map)?;

	if !confirm {
		return Err!("Are you really sure you want to clear all data? Add the --confirm option.");
	}

	let timer = Instant::now();
	let cork = self.services.db.cork();
	let count = map.for_clear().ignore_err().count().boxed().await;

	drop(cork);
	let query_time = timer.elapsed();
	write!(self, "Operation completed in {query_time:?}\n\nremoved {count} keys\n").await
}
