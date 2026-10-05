use std::sync::Arc;

use futures::FutureExt;
use ruma::{OwnedRoomAliasId, OwnedRoomId, OwnedUserId, RoomId};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Result, debug, trace, warn};

use crate::rooms::timeline::RoomMutexGuard;

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
		debug_assert!(
			self.services.config.delete_rooms_after_leave,
			"Caller must checking if delete_rooms_after_leave configured."
		);

		match self
			.services
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

		if let Err(error) = self
			.services
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
		self.services
			.state_cache
			.preflight_room_erasure(room_id, force)
			.await?;
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
		// Validate every source before any eviction or alias mutation.
		let members = self
			.services
			.state_cache
			.bounded_room_members(room_id)
			.await?;
		self.services
			.alias
			.preflight_room_alias_shutdown(room_id)
			.await?;
		let (mut kicked_users, mut failed_to_kick_users) = (Vec::new(), Vec::new());
		debug!(?room_id, "Making all local users leave the room and forgetting it");
		for user_id in members
			.into_iter()
			.filter(|user| self.services.globals.user_is_local(user))
		{
			match self
				.services
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
		let local_aliases = self
			.services
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

	/// Wipes the room's storage. `force` widens the erasure of local users'
	/// left-state (it is not Synapse's `force_purge`).
	async fn purge_room(
		&self,
		room_id: &RoomId,
		force: bool,
		state_lock: &RoomMutexGuard,
	) -> Result {
		debug!("Deleting room's threads from database");
		self.services
			.threads
			.delete_all_rooms_threads(room_id)
			.await?;

		debug!("Deleting all the room's search token IDs from our database");
		self.services
			.search
			.delete_all_search_tokenids_for_room(room_id)
			.await?;

		debug!("Deleting all room's forward extremities from our database");
		self.services
			.state
			.delete_all_rooms_forward_extremities(room_id)
			.await?;

		debug!("Deleting all the room's event (PDU) references");
		self.services
			.pdu_metadata
			.delete_all_referenced_for_room(room_id)
			.await?;

		debug!("Deleting all the room's typed relation index entries");
		self.services
			.pdu_metadata
			.delete_all_relatesto_typed_for_room(room_id)
			.await?;

		debug!("Deleting all the room's member counts");
		self.services
			.state_cache
			.delete_room_join_counts(room_id, force)
			.await?;

		debug!("Deleting all the room's private read receipts");
		self.services
			.read_receipt
			.delete_all_read_receipts(room_id)
			.await?;

		debug!("Deleting the room's last notifications read.");
		self.services
			.pusher
			.delete_room_notification_read(room_id)
			.await?;

		debug!("Deleting room state hash from our database");
		self.services
			.state
			.delete_room_shortstatehash(room_id, state_lock)
			.await?;

		debug!("Deleting PDUs");
		self.services
			.timeline
			.delete_pdus(room_id)
			.await?;

		debug!("Deleting internal room ID from our database");
		self.services
			.short
			.delete_shortroomid(room_id)
			.await?;
		Ok(())
	}
}
