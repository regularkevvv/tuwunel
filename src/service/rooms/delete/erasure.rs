use futures::{TryStreamExt, pin_mut};
use ruma::{
	RoomId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use tuwunel_core::{
	Error, Result,
	matrix::pdu::{PduEvent, RawPduId},
};
use tuwunel_database::{Interfix, Txn};

use super::Service;

#[derive(Default)]
struct Budget {
	keys: usize,
	key_bytes: usize,
	value_bytes: usize,
}

impl Budget {
	fn cap(&self) -> usize {
		4096_usize
			.saturating_sub(self.keys)
			.saturating_add(1)
	}

	fn key(&mut self, key: &[u8]) -> Result {
		self.keys = self.keys.saturating_add(1);
		self.key_bytes = self.key_bytes.saturating_add(key.len());
		if self.keys > 4096
			|| self.key_bytes > 512 * 1024
			|| key.len() > tuwunel_bridge::MAX_KEY_BYTES
		{
			return Err(limit());
		}
		Ok(())
	}

	fn value(&mut self, value: &[u8]) -> Result {
		self.value_bytes = self.value_bytes.saturating_add(value.len());
		if self.value_bytes > 4 * 1024 * 1024 || value.len() > tuwunel_bridge::MAX_VALUE_BYTES {
			return Err(limit());
		}
		Ok(())
	}
}

impl Service {
	/// Prepare all previously erased room maps without performing a write.
	/// The caller owns state/timeline/source exclusion. Key-only scans share
	/// 4,096 keys / 512 KiB; inspected PDU values share 4 MiB. The full
	/// storage and membership transaction is checked against the 900-op bridge
	/// cap.
	pub(super) async fn prepare_storage_erasure(&self, room: &RoomId) -> Result<Txn> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let raw = services_root.db["roomid_shortroomid"]
			.get(room)
			.await?;
		let encoded: [u8; 8] = raw
			.as_ref()
			.try_into()
			.map_err(|_| Error::bad_database("Invalid room erasure short ID"))?;
		let short = u64::from_be_bytes(encoded);
		let mut budget = Budget::default();
		let mut txn = services_root.db.txn();
		for name in [
			"threadid_userids",
			"threadactivityid_rootid",
			"threadrootid_latestcount",
			"tokenids",
			"relatesto_typed",
			"pduid_notificationplan",
			"notificationreceiptid_record",
		] {
			let map = &services_root.db[name];
			let keys = map.keys_prefix_raw_capped(&short, budget.cap());
			pin_mut!(keys);
			loop {
				let next = keys.try_next().await?;
				let Some(key) = next else { break };
				budget.key(key)?;
				txn.del_raw(map, key);
			}
		}
		for name in [
			"roomid_pduleaves",
			"referencedevents",
			"roomuserid_privateread",
			"roomuserid_lastprivatereadupdate",
			"roomuserid_privatereadsync",
			"readreceiptid_readreceipt",
			"roomuserid_lastnotificationread",
			"roomuserid_notificationcutoff",
			"roomid_tscount_pducount",
		] {
			let map = &services_root.db[name];
			let keys = map.keys_prefix_raw_capped(&(room, Interfix), budget.cap());
			pin_mut!(keys);
			loop {
				let next = keys.try_next().await?;
				let Some(key) = next else { break };
				budget.key(key)?;
				txn.del_raw(map, key);
			}
		}
		services_root
			.pusher
			.stage_room_notification_index_erasure(&mut txn, room)
			.await?;
		self.stage_pdus(room, short, &mut budget, &mut txn)
			.await?;
		txn.del_raw(&services_root.db["roomid_shortstatehash"], room);
		txn.del_raw(&services_root.db["roomid_shortroomid"], room);
		Ok(txn)
	}

	async fn stage_pdus(
		&self,
		room: &RoomId,
		short: u64,
		budget: &mut Budget,
		txn: &mut Txn,
	) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let map = &services_root.db["pduid_pdu"];
		let keys = map.keys_prefix_raw_capped(&short, budget.cap());
		pin_mut!(keys);
		while let Some(key) = keys.try_next().await? {
			budget.key(key)?;
			let raw = RawPduId::from_bytes(key)?;
			let value = map.get(key).await?;
			budget.value(&value)?;
			let pdu: PduEvent = serde_json::from_slice(&value)
				.map_err(|_| Error::bad_database("Invalid stored erasure PDU"))?;
			let binding = match services_root.db["eventid_pduid"]
				.get(&pdu.event_id)
				.await
			{
				| Ok(binding) => binding,
				| Err(error) if error.is_not_found() =>
					return Err(Error::bad_database("Room erasure PDU binding is missing")),
				| Err(error) => return Err(error),
			};
			if pdu.room_id != room || binding.as_ref() != key {
				return Err(Error::bad_database("Room erasure PDU indexes disagree"));
			}
			services_root
				.sending
				.db
				.stage_federation_erasure(txn, &raw)
				.await?;
			txn.del_raw(map, key);
			txn.del_raw(&services_root.db["eventid_pduid"], &pdu.event_id);
			txn.del_raw(&services_root.db["eventid_outlierpdu"], &pdu.event_id);
		}
		Ok(())
	}
}

fn limit() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Room erasure inventory limit reached".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}
