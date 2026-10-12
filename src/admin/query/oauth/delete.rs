use tuwunel_core::{
	Err, Result,
	either::{Left, Right},
};

use super::SessionOrUserId;
use crate::admin_command;

#[admin_command]
pub(super) async fn oauth_delete(&self, id: SessionOrUserId, force: bool) -> Result {
	if !force {
		return Err!(
			"Deleting these records can cause registration conflicts. Use --force to be sure."
		);
	}

	match &id {
		| Left(sess_id) => {
			self.services
				.oauth
				.sessions
				.delete(sess_id)
				.await?;
		},
		| Right(user_id) => {
			self.services
				.oauth
				.delete_user_sessions(user_id)
				.await?;
		},
	}

	write!(self, "deleted any oauth state for {id}").await
}
