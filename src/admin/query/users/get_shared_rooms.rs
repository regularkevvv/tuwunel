use ruma::OwnedUserId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn get_shared_rooms(&self, user_a: OwnedUserId, user_b: OwnedUserId) -> Result {
	let query = async {
		// Prove both complete inventories before writing any query output.
		// Each exact-user inventory is at most 1,024 rooms / 128 KiB of IDs.
		let a = self
			.services
			.state_cache
			.bounded_rooms_joined(&user_a)
			.await?;
		let mut b = self
			.services
			.state_cache
			.bounded_rooms_joined(&user_b)
			.await?;
		b.sort_unstable();
		let shared = a
			.into_iter()
			.filter(|room| b.binary_search(room).is_ok())
			.collect::<Vec<_>>();
		Ok(shared)
	};

	self.write_timed_query_try(query).await
}
