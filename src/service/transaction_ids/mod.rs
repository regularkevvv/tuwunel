use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ruma::{DeviceId, TransactionId, UserId, to_device::DeviceIdOrAllDevices};
use sha2::{Digest, Sha256};
use tuwunel_core::{Result, implement, utils::MutexMap};
use tuwunel_database::{Handle, Map};

pub struct Service {
	db: Data,
	/// Serializes the check-then-record of one transaction id, keyed by
	/// [`key`]. A retry racing the request it repeats (a federation
	/// transaction carrying a duplicate EDU is handled concurrently) must see
	/// the record the first one commits rather than deliver alongside it.
	pub mutex: MutexMap<Vec<u8>, ()>,
}

struct Data {
	userdevicetxnid_response: Arc<Map>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				userdevicetxnid_response: args.db["userdevicetxnid_response"].clone(),
			},
			mutex: MutexMap::new(),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Key of the `userdevicetxnid_response` record for one transaction id.
///
/// [`Service::add_txnid`] and the writers that fold the record into a larger
/// atomic commit (the timeline append, to-device delivery) all build it here,
/// so [`Service::existing_txnid`] finds a record whichever of them wrote it.
#[must_use]
pub fn key(user_id: &UserId, device_id: Option<&DeviceId>, txn_id: &TransactionId) -> Vec<u8> {
	let mut key = user_id.as_bytes().to_vec();
	key.push(0xFF);
	key.extend_from_slice(
		device_id
			.map(DeviceId::as_bytes)
			.unwrap_or_default(),
	);
	key.push(0xFF);
	key.extend_from_slice(txn_id.as_bytes());

	key
}

/// `message_id` of the DirectToDevice EDU one client to-device request sends
/// toward one remote recipient.
///
/// Derived from the request's transaction id rather than drawn fresh, so a
/// client retry after an interrupted request re-queues the same id and the
/// receiving server's `message_id` dedupe drops the repeat. The recipient is
/// part of the input because one request sends one EDU per addressed
/// (user, device) pair, and each needs its own id. The digest keeps the
/// sender's device and transaction id off the wire.
#[must_use]
pub fn to_device_message_id(
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
	txn_id: &TransactionId,
	target_user_id: &UserId,
	target_device: &DeviceIdOrAllDevices,
) -> String {
	let mut input = key(sender_user, sender_device, txn_id);
	input.push(0xFF);
	input.extend_from_slice(target_user_id.as_bytes());
	input.push(0xFF);
	input.extend_from_slice(target_device.to_string().as_bytes());

	URL_SAFE_NO_PAD.encode(Sha256::digest(&input))
}

#[implement(Service)]
pub async fn add_txnid(
	&self,
	user_id: &UserId,
	device_id: Option<&DeviceId>,
	txn_id: &TransactionId,
	data: &[u8],
) -> Result {
	self.db
		.userdevicetxnid_response
		.insert(&key(user_id, device_id, txn_id), data)
		.await?;

	Ok(())
}

// If there's no entry, this is a new transaction
#[implement(Service)]
pub async fn existing_txnid(
	&self,
	user_id: &UserId,
	device_id: Option<&DeviceId>,
	txn_id: &TransactionId,
) -> Result<Handle<'_>> {
	let key = (user_id, device_id, txn_id);
	self.db.userdevicetxnid_response.qry(&key).await
}

#[cfg(test)]
mod tests {
	use ruma::{TransactionId, device_id, to_device::DeviceIdOrAllDevices, user_id};
	use tuwunel_database::serialize_key;

	use super::{key, to_device_message_id};

	/// The raw key the atomic writers build is the one the codec lookup in
	/// `existing_txnid` probes, with and without a device.
	#[test]
	fn key_matches_the_lookup_encoding() {
		let user = user_id!("@alice:example.com");
		let device = device_id!("ALICEDEV");
		let txn: &TransactionId = "m1.1".into();

		for device in [Some(device), None] {
			let lookup = serialize_key((user, device, txn)).expect("serializes");

			assert_eq!(key(user, device, txn), lookup.as_slice());
		}
	}

	#[test]
	fn message_id_is_stable_across_retries() {
		let sender = user_id!("@alice:example.com");
		let device = Some(device_id!("ALICEDEV"));
		let txn: &TransactionId = "m1.1".into();
		let target = user_id!("@bob:remote.example");
		let all = DeviceIdOrAllDevices::AllDevices;

		assert_eq!(
			to_device_message_id(sender, device, txn, target, &all),
			to_device_message_id(sender, device, txn, target, &all),
		);
	}

	/// Every EDU one request sends, and every other request, gets its own id.
	#[test]
	fn message_id_separates_recipients_and_requests() {
		let sender = user_id!("@alice:example.com");
		let device = Some(device_id!("ALICEDEV"));
		let txn: &TransactionId = "m1.1".into();
		let bob = user_id!("@bob:remote.example");
		let carol = user_id!("@carol:other.example");
		let all = DeviceIdOrAllDevices::AllDevices;
		let one = DeviceIdOrAllDevices::DeviceId(device_id!("BOBDEV").to_owned());

		let base = to_device_message_id(sender, device, txn, bob, &all);
		let others = [
			to_device_message_id(sender, device, txn, bob, &one),
			to_device_message_id(sender, device, txn, carol, &all),
			to_device_message_id(sender, device, "m1.2".into(), bob, &all),
			to_device_message_id(sender, None, txn, bob, &all),
		];

		for other in others {
			assert_ne!(base, other);
		}
	}
}
