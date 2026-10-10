use ruma::OwnedRoomId;
use tuwunel_core::{Result, err};

use super::Service;

impl Service {
	/// Prepares a complete empty-room candidate list without changing rooms.
	/// The room source is bounded to 1,024 IDs / 128 KiB. Membership scans
	/// share 4,096 examined keys / 256 KiB user-ID bytes across the source.
	/// Protected rooms still consume the work required to exclude them.
	pub async fn bounded_empty_local_rooms(&self) -> Result<Vec<OwnedRoomId>> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let rooms = services_root.metadata.bounded_room_ids().await?;
		let mut rows = 4096_usize;
		let mut bytes = 256_usize * 1024;
		let mut empty = Vec::new();
		for room in rooms {
			let joined = services_root
				.state_cache
				.bounded_local_member_count(&room, rows, bytes)
				.await?;
			charge(&mut rows, joined.examined)?;
			charge(&mut bytes, joined.user_id_bytes)?;
			if joined.local_members > 0 {
				continue;
			}
			let invited = services_root
				.state_cache
				.bounded_local_invited_member_count(&room, rows, bytes)
				.await?;
			charge(&mut rows, invited.examined)?;
			charge(&mut bytes, invited.user_id_bytes)?;
			if invited.local_members == 0 {
				empty.push(room);
			}
		}
		Ok(empty)
	}
}

fn charge(remaining: &mut usize, examined: usize) -> Result {
	*remaining = remaining
		.checked_sub(examined)
		.ok_or_else(|| err!(Database("Invalid room prune inventory budget")))?;
	Ok(())
}
