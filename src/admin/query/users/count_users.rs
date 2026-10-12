use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn count_users(&self) -> Result {
	self.write_timed_query_try(self.services.users.bounded_user_count())
		.await
}
