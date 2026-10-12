use axum::extract::State;
use ruma::{
	OwnedEventId, api::client::redact::redact_event,
	events::room::redaction::RoomRedactionEventContent,
};
use tuwunel_core::{Err, Result, err, matrix::pdu::PduBuilder, warn};
use tuwunel_service::transaction_ids;

use crate::{Ruma, client::utils::is_self_redaction};

/// # `PUT /_matrix/client/r0/rooms/{roomId}/redact/{eventId}/{txnId}`
///
/// Tries to send a redaction event into the room.
///
/// The transaction record commits with the redaction and its target mutation.
pub(crate) async fn redact_event_route(
	State(services): State<crate::State>,
	body: Ruma<redact_event::v3::Request>,
) -> Result<redact_event::v3::Response> {
	let sender_user = body.sender_user();

	if services.config.disable_local_redactions
		&& !services.admin.user_is_admin(sender_user).await
	{
		warn!(
			%sender_user,
			event_id = %body.event_id,
			"Local redactions are disabled, non-admin user attempted to redact an event"
		);
		return Err!(Request(Forbidden("Redactions are disabled on this server.")));
	}

	if services.users.is_suspended(sender_user).await
		&& !is_self_redaction(&services, sender_user, &body.event_id).await
	{
		return Err!(Request(UserSuspended("Account is suspended.")));
	}

	let sender_device = body.sender_device.as_deref();
	let txnid = transaction_ids::key(sender_user, sender_device, &body.txn_id);
	let _txnid_lock = services
		.transaction_ids
		.mutex
		.lock(txnid.as_slice())
		.await;
	let state_lock = services.state.mutex.lock(&body.room_id).await;

	match services
		.transaction_ids
		.existing_txnid(sender_user, sender_device, &body.txn_id)
		.await
	{
		| Ok(response) => {
			if response.is_empty() {
				return Err!(Request(InvalidParam(
					"Tried to use txn_id already used for an incompatible endpoint."
				)));
			}
			let event_id = std::str::from_utf8(&response)
				.ok()
				.and_then(|id| OwnedEventId::try_from(id).ok())
				.ok_or_else(|| {
					err!(Database("Invalid event_id in redaction transaction record"))
				})?;
			return Ok(redact_event::v3::Response { event_id });
		},
		| Err(error) if error.is_not_found() => {},
		| Err(error) => return Err(error),
	}

	let event_id = services
		.timeline
		.build_and_append_pdu_with_txnid(
			PduBuilder {
				redacts: Some(body.event_id.clone()),
				..PduBuilder::timeline(&RoomRedactionEventContent {
					redacts: Some(body.event_id.clone()),
					reason: body.reason.clone(),
				})
			},
			sender_user,
			&body.room_id,
			Some(&txnid),
			&state_lock,
		)
		.await?;

	drop(state_lock);

	Ok(redact_event::v3::Response { event_id })
}
