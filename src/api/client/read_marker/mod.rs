mod read_markers;
mod receipt;

use ruma::{EventId, MilliSecondsSinceUnixEpoch, RoomId, UserId, events::receipt::ReceiptThread};
use tuwunel_core::{Err, PduCount, Result, err, utils::result::LogErr};
use tuwunel_service::{Services, rooms::read_receipt::PrivateRead};

pub(crate) use self::{read_markers::set_read_marker_route, receipt::create_receipt_route};

/// Resolves `event` to its timeline position and stores the private read
/// marker for `thread` there.
///
/// Returns whether the marker advanced. A backfilled event carries no forward
/// position, so it is rejected rather than stored.
async fn set_private_marker(
	services: &Services,
	room_id: &RoomId,
	user_id: &UserId,
	event: &EventId,
	thread: &ReceiptThread,
) -> Result<bool> {
	let state = services.state.mutex.lock(room_id).await;
	let raw = services
		.timeline
		.get_pdu_id(event)
		.await
		.map_err(|error| {
			if error.is_not_found() {
				err!(Request(NotFound("Event not found.")))
			} else {
				error
			}
		})?;
	let pdu = services.timeline.get_pdu_from_id(&raw).await?;
	if pdu.event_id != event {
		return Err!(Database("Private receipt event binding mismatch"));
	}
	if pdu.room_id != room_id {
		return Err!(Request(InvalidParam("Event does not belong to this room.")));
	}

	let PduCount::Normal(count) = raw.pdu_count() else {
		return Err!(Request(InvalidParam(
			"Event is a backfilled PDU and cannot be marked as read."
		)));
	};

	let advanced = services
		.read_receipt
		.private_read_set_with_state(
			PrivateRead {
				room_id,
				user_id,
				count,
				ts: MilliSecondsSinceUnixEpoch::now(),
				thread,
				announce: true,
			},
			&state,
		)
		.await?;

	Ok(advanced)
}

/// Refresh the badge after the receipt and counts commit together.
///
/// The refresh follows every advance because the gateway can hold a stale
/// badge while the stored count is already zero; only a delivery reconciles
/// it.
async fn refresh_badge(services: &Services, user_id: &UserId) -> Result {
	services
		.sending
		.refresh_push_badge(user_id)
		.await
		.log_err()
		.ok();

	Ok(())
}
