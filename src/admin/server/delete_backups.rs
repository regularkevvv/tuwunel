use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn delete_backups(&self, keep: usize) -> Result {
	let count = self
		.blocking_db(move |db| {
			let engine = db.engine()?;
			engine.backup_purge(keep)?;
			engine.backup_count()
		})
		.await?;

	write!(self, "Done. Currently have {count} backups.").await
}
