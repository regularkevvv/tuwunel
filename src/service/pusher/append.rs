use ruma::{api::client::push::ProfileTag, push::Actions};
use serde::{Deserialize, Serialize};

use crate::rooms::short::ShortRoomId;

/// Compact metadata stored for each notified event.
///
/// The database key supplies the user and PDU count. The stored `ShortRoomId`
/// combines with that count to reconstruct the event's `PduId`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Notified {
	/// Milliseconds time at which the event notification was sent.
	pub ts: u64,

	/// ShortRoomId
	pub sroomid: ShortRoomId,

	/// The profile tag of the rule that matched this event.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub tag: Option<ProfileTag>,

	/// Actions vector
	pub actions: Actions,
}

/// Validate the stored notification metadata consistently for pages and resets.
pub(super) fn parse_notified(value: &[u8]) -> tuwunel_core::Result<Notified> {
	use tuwunel_core::Error;
	if value.len() > 64 * 1024 {
		return Err(Error::bad_database("Notification metadata exceeds limit"));
	}
	let notified: Notified = serde_json::from_slice(value)
		.map_err(|_| Error::bad_database("Invalid notification metadata"))?;
	if notified.actions.len() > 64
		|| ruma::UInt::new(notified.ts).is_none()
		|| notified.sroomid == 0
	{
		return Err(Error::bad_database("Invalid notification metadata fields"));
	}
	Ok(notified)
}
