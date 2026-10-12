use ruma::OwnedUserId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn list_users(&self, after: Option<OwnedUserId>, limit: u16) -> Result {
	let page = self
		.services
		.users
		.local_user_page(after.as_deref(), usize::from(limit))
		.await?;

	write!(
		self,
		"Page contains {} local user account(s); examined {} inventory rows:\n```\n",
		page.users.len(),
		page.examined
	)
	.await?;
	for user in &page.users {
		writeln!(self, "{user}").await?;
	}
	writeln!(self, "```").await?;
	if let Some(next) = page.next {
		writeln!(self, "Next page: `!admin users list-users --after {next} --limit {limit}`")
			.await?;
	}
	Ok(())
}
