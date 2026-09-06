use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn raw_sequence(&self) -> Result {
	let sequence = self.services.db.engine()?.current_sequence();

	write!(self, "{sequence:#?}").await
}
