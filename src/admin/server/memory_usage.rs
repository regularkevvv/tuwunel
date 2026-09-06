use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn memory_usage(&self) -> Result {
	let services_usage = self.services.memory_usage().await?;
	let database_usage = self.services.db.engine()?.memory_usage()?;

	write!(self, "Services:\n{services_usage}\nDatabase:\n{database_usage}",).await
}
