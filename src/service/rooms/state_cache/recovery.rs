use futures::{StreamExt, pin_mut};
use ruma::{
	OwnedRoomId, RoomId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use tuwunel_core::{Error, Result};
use tuwunel_database::{Ignore, Interfix};

use super::{Service, update::RECOUNT_PENDING};

const MAX_ROOMS: usize = 1024;
const MAX_BYTES: usize = 128 * 1024;

impl Service {
	/// Restore durable pending counts before normal service workers/readiness.
	/// A complete 1,024-room / 128-KiB inventory is validated before any
	/// repair; the scan reads at most one extra key and never projects marker
	/// values. Maintenance mode retains explicit operator/test control over
	/// repairs.
	pub(crate) async fn restore_pending_recounts(&self) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let rooms = self.pending_recount_rooms().await?;
		for room in &rooms {
			if !services_root
				.metadata
				.exists_checked(room)
				.await?
			{
				// A remote join/knock can publish room state before any local
				// timeline PDU. Both durable identifiers must already exist.
				services_root.short.get_shortroomid(room).await?;
				services_root
					.state
					.get_room_shortstatehash(room)
					.await?;
			}
			let marker = services_root.db["global"]
				.qry(&(RECOUNT_PENDING, room))
				.await?;
			if !marker.is_empty() {
				return Err(Error::bad_database("Invalid membership recount marker"));
			}
			self.recount_is_current(room).await?;
		}
		// Projection completion can rebuild counts and retire their markers.
		// Validate every existing recount marker before allowing that first write.
		self.restore_membership_projections().await?;
		for room in rooms {
			let _state_lock = services_root.state.mutex.lock(&room).await;
			self.repair_joined_count(&room).await?;
		}
		Ok(())
	}

	async fn pending_recount_rooms(&self) -> Result<Vec<OwnedRoomId>> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let prefix = (RECOUNT_PENDING, Interfix);
		let keys = services_root.db["global"]
			.keys_prefix_capped::<(Ignore, &RoomId), _>(&prefix, MAX_ROOMS.saturating_add(1));
		pin_mut!(keys);
		let mut rooms = Vec::new();
		let mut bytes = 0_usize;
		while let Some(key) = keys.next().await {
			let (_, room) = key?;
			bytes = bytes.saturating_add(room.as_bytes().len());
			if rooms.len() >= MAX_ROOMS || bytes > MAX_BYTES {
				return Err(Error::Request(
					ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
					"Pending recount inventory limit reached".into(),
					http::StatusCode::TOO_MANY_REQUESTS,
				));
			}
			rooms.push(room.to_owned());
		}
		Ok(rooms)
	}
}
