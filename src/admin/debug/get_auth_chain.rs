use std::{iter::once, time::Instant};

use futures::{TryStreamExt, future::ready};
use ruma::{CanonicalJsonValue, OwnedEventId, RoomId};
use tuwunel_core::{Err, Result, err};

use crate::admin_command;

#[admin_command]
pub(super) async fn get_auth_chain(&self, event_id: OwnedEventId) -> Result {
	let Ok(event) = self
		.services
		.timeline
		.get_pdu_json(&event_id)
		.await
	else {
		return Err!("Event not found.");
	};

	let room_id_str = event
		.get("room_id")
		.and_then(CanonicalJsonValue::as_str)
		.ok_or_else(|| err!(Database("Invalid event in database")))?;

	let room_id = <&RoomId>::try_from(room_id_str)
		.map_err(|_| err!(Database("Invalid room id field in event in database")))?;

	let room_version = self
		.services
		.state
		.get_room_version(room_id)
		.await?;

	let start = Instant::now();
	let count = self
		.services
		.auth_chain
		.event_ids_iter(room_id, &room_version, once(event_id.as_ref()))
		.try_fold(0_usize, |count, _| ready(Ok(count.saturating_add(1))))
		.await?;

	let elapsed = start.elapsed();
	let out = format!("Loaded auth chain with length {count} in {elapsed:?}");

	self.write_str(&out).await
}
