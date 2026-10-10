use std::sync::Arc;

use futures::FutureExt;
use ruma::{OwnedRoomAliasId, OwnedRoomId, OwnedUserId, RoomId};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Err, Result, debug, trace, warn};

use crate::rooms::timeline::RoomMutexGuard;

mod erasure;
mod inventory;

pub struct Service {
	services: Arc<crate::services::OnceServices>,
}

/// Records local-user eviction results and aliases targeted for removal.
///
/// Its serialized layout matches Synapse's `ShutdownRoom`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ShutdownRoom {
	pub kicked_users: Vec<OwnedUserId>,
	pub failed_to_kick_users: Vec<OwnedUserId>,
	pub local_aliases: Vec<OwnedRoomAliasId>,
	pub new_room_id: Option<OwnedRoomId>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self { services: args.services.clone() }))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	pub async fn delete_if_empty_local(&self, room_id: &RoomId, state_lock: RoomMutexGuard) {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		debug_assert!(
			services_root.config.delete_rooms_after_leave,
			"Caller must checking if delete_rooms_after_leave configured."
		);

		match services_root
			.state_cache
			.has_local_membership_checked(room_id)
			.await
		{
			| Ok(true) => {
				trace!(?room_id, "Not deleting with local joined or invited");
				return;
			},
			| Err(error) => {
				warn!(%error, "Not deleting room with incomplete membership inventory");
				return;
			},
			| Ok(false) => {},
		}

		debug!(?room_id, "Preparing to delete room...");

		if let Err(error) = services_root
			.delete
			.delete_room(room_id, false, state_lock)
			.boxed()
			.await
		{
			warn!(%error, %room_id, "Room cleanup refused");
		}
	}

	pub async fn delete_room(
		&self,
		room_id: &RoomId,
		force: bool,
		state_lock: RoomMutexGuard,
	) -> Result<ShutdownRoom> {
		self.require_unprotected_room(room_id).await?;
		self.preflight_erasure(room_id, force).await?;
		let summary = self.shutdown_room(room_id, &state_lock).await?;

		self.purge_room(room_id, force, &state_lock)
			.await?;

		debug!(?room_id, "Successfully deleted room from our database");

		Ok(summary)
	}

	/// Evicts every local user, strips the room's local aliases, and
	/// unpublishes it from the directory, returning the shutdown summary. The
	/// admin delete runs this phase alone when `purge` is false.
	pub async fn shutdown_room(
		&self,
		room_id: &RoomId,
		state_lock: &RoomMutexGuard,
	) -> Result<ShutdownRoom> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		self.require_unprotected_room(room_id).await?;
		// Validate every source before any eviction or alias mutation.
		let members = services_root
			.state_cache
			.bounded_room_members(room_id)
			.await?;
		services_root
			.alias
			.preflight_room_alias_shutdown(room_id)
			.await?;
		let (mut kicked_users, mut failed_to_kick_users) = (Vec::new(), Vec::new());
		debug!(?room_id, "Making all local users leave the room and forgetting it");
		for user_id in members
			.into_iter()
			.filter(|user| services_root.globals.user_is_local(user))
		{
			match services_root
				.membership
				.leave(&user_id, room_id, Some("Room Deleted".into()), true, state_lock)
				.await
			{
				| Ok(()) => kicked_users.push(user_id),
				| Err(e) => {
					warn!(%e, "Failed to leave room");
					failed_to_kick_users.push(user_id);
				},
			}
		}

		debug!("Deleting room aliases and directory publication");
		let local_aliases = services_root
			.alias
			.remove_room_aliases(room_id)
			.await?;

		Ok(ShutdownRoom {
			kicked_users,
			failed_to_kick_users,
			local_aliases,
			new_room_id: None,
		})
	}

	async fn preflight_erasure(&self, room_id: &RoomId, force: bool) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let _insert = services_root
			.timeline
			.mutex_insert
			.lock(room_id)
			.await;
		let _federation = services_root
			.sending
			.db
			.lock_federation_sources()
			.await;
		let txn = self.prepare_storage_erasure(room_id).await?;
		services_root
			.state_cache
			.preflight_room_storage_erasure(room_id, force, txn)
			.await
	}

	/// Wipes the room's storage. `force` widens the erasure of local users'
	/// left-state (it is not Synapse's `force_purge`).
	async fn purge_room(
		&self,
		room_id: &RoomId,
		force: bool,
		_state_lock: &RoomMutexGuard,
	) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let _insert = services_root
			.timeline
			.mutex_insert
			.lock(room_id)
			.await;
		let _federation = services_root
			.sending
			.db
			.lock_federation_sources()
			.await;
		let txn = self.prepare_storage_erasure(room_id).await?;
		services_root
			.state_cache
			.commit_room_storage_erasure(room_id, force, txn)
			.await
	}

	async fn require_unprotected_room(&self, room_id: &RoomId) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		// Unknown protection is a refusal, never permission to erase.
		let admin = services_root.admin.get_admin_room().await?;
		if admin == room_id {
			return Err!(Request(Forbidden("Cannot delete or shut down the admin room")));
		}
		Ok(())
	}
}
