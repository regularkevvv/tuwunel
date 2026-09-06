use tuwunel_core::{Err, Result, err};

use crate::admin_command;

#[admin_command]
pub(super) async fn resync_database(&self) -> Result {
	if !self.services.db.is_secondary() {
		return Err!("Not a secondary instance.");
	}

	self.services
		.db
		.engine()?
		.update()
		.map_err(|e| err!("Failed to update from primary: {e:?}"))
}
