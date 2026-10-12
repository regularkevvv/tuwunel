use ruma::OwnedUserId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn iter_users(
	&self,
	historical: bool,
	after: Option<OwnedUserId>,
	limit: u16,
) -> Result {
	let page = self
		.services
		.users
		.user_inventory_page(after.as_deref(), usize::from(limit), historical)
		.await?;
	write!(
		self,
		"Page contains {} user inventory row(s); examined {} rows:\n```\n",
		page.users.len(),
		page.examined
	)
	.await?;
	for user in page.users {
		writeln!(self, "{user}").await?;
	}
	writeln!(self, "```").await?;
	if let Some(next) = page.next {
		let filter = if historical { " --historical" } else { "" };
		writeln!(
			self,
			"Next page: `!admin query users iter-users --after {next} --limit {limit}{filter}`"
		)
		.await?;
	}
	Ok(())
}
