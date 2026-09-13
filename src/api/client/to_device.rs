use std::collections::BTreeMap;

use axum::extract::State;
use ruma::api::{
	client::to_device::send_event_to_device,
	error::ErrorKind,
	federation::{self, transactions::edu::DirectDeviceContent},
};
use tuwunel_core::{Error, Result};
use tuwunel_service::{sending::EduBuf, transaction_ids, users::ToDeviceTarget};

use crate::Ruma;

/// # `PUT /_matrix/client/r0/sendToDevice/{eventType}/{txnId}`
///
/// Send a to-device event to a set of client devices.
///
/// Every local delivery commits together with the transaction record, so an
/// interrupted request delivers to all local recipients or none, and its
/// retry is a no-op once it has. Each remote recipient's EDU carries a
/// `message_id` derived from the transaction id, so a retry that re-queues it
/// is dropped by the receiving server's dedupe.
pub(crate) async fn send_event_to_device_route(
	State(services): State<crate::State>,
	body: Ruma<send_event_to_device::v3::Request>,
) -> Result<send_event_to_device::v3::Response> {
	let sender_user = body.sender_user();
	let sender_device = body.sender_device.as_deref();
	let txnid = transaction_ids::key(sender_user, sender_device, &body.txn_id);

	// A concurrent repeat of this request waits here and then finds the record.
	let _txnid_lock = services
		.transaction_ids
		.mutex
		.lock(txnid.as_slice())
		.await;

	// Check if this is a new transaction id
	if services
		.transaction_ids
		.existing_txnid(sender_user, sender_device, &body.txn_id)
		.await
		.is_ok()
	{
		return Ok(send_event_to_device::v3::Response {});
	}

	// Validate every local message before anything is queued or written, so a
	// malformed one refuses the request without a partial delivery.
	let mut local = Vec::new();
	for (target_user_id, map) in &body.messages {
		if !services.globals.user_is_local(target_user_id) {
			continue;
		}

		for (target_device_id_maybe, event) in map {
			let content = event
				.deserialize_as()
				.map_err(|_| Error::BadRequest(ErrorKind::InvalidParam, "Event is invalid"))?;

			local.push(ToDeviceTarget {
				user_id: target_user_id.clone(),
				device: target_device_id_maybe.clone(),
				content,
			});
		}
	}

	// Remote EDUs are queued before the local commit: an interruption after
	// this point makes the retry re-queue them under the same message ids,
	// whereas one queued after the commit would be lost to a retry that finds
	// the transaction already recorded.
	for (target_user_id, map) in &body.messages {
		if services.globals.user_is_local(target_user_id) {
			continue;
		}

		for (target_device_id_maybe, event) in map {
			let mut map = BTreeMap::new();
			map.insert(target_device_id_maybe.clone(), event.clone());
			let mut messages = BTreeMap::new();
			messages.insert(target_user_id.clone(), map);

			let message_id = transaction_ids::to_device_message_id(
				sender_user,
				sender_device,
				&body.txn_id,
				target_user_id,
				target_device_id_maybe,
			);

			let mut buf = EduBuf::new();
			serde_json::to_writer(
				&mut buf,
				&federation::transactions::edu::Edu::DirectToDevice(DirectDeviceContent {
					sender: sender_user.to_owned(),
					ev_type: body.event_type.clone(),
					message_id: message_id.into(),
					messages,
				}),
			)
			.expect("DirectToDevice EDU can be serialized");

			services
				.sending
				.send_edu_server(target_user_id.server_name(), buf)
				.await?;
		}
	}

	services
		.users
		.deliver_to_device(sender_user, &body.event_type.to_string(), &local, Some(&txnid))
		.await?;

	Ok(send_event_to_device::v3::Response {})
}
