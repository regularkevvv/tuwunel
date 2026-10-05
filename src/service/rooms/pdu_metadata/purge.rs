use futures::{StreamExt, TryStreamExt};
use ruma::{
	EventId, RoomId,
	events::{relation::RelationType, room::encrypted::Relation},
};
use tuwunel_core::{
	Error, PduId, Result,
	arrayvec::ArrayVec,
	implement,
	matrix::{Event, Pdu, PduCount, RawPduId},
	utils::{stream::automatic_width, u64_from_u8},
};

use super::{ExtractRelatesTo, Service};
use crate::rooms::short::ShortRoomId;

type Prefix = ArrayVec<u8, 16>;

/// Stage the metadata owned by a purged parent alongside the canonical
/// removal. Dangling child rows are retained, as in the existing reader
/// contract. Scans close before the caller commits; storage and encoding
/// errors refuse the entire event mutation.
#[implement(Service)]
pub(crate) async fn append_purge_event_relations(
	&self,
	txn: &mut tuwunel_database::Txn,
	shortroomid: ShortRoomId,
	parent: PduCount,
	room_id: &RoomId,
	event_id: &EventId,
) -> Result {
	let target = parent.to_be_bytes();
	self.append_relation_removals(txn, &self.db.tofrom_relation, &target, false)
		.await?;
	let mut prefix = Prefix::new();
	prefix.extend(shortroomid.to_be_bytes());
	prefix.extend(parent.to_be_bytes());
	self.append_relation_removals(txn, &self.db.relatesto_typed, &prefix, true)
		.await?;
	txn.del(&self.db.referencedevents, (room_id, event_id));
	txn.del_raw(&self.services.db["eventid_policysigstate"], event_id);
	txn.del_raw(&self.db.softfailedeventids, event_id);
	crate::rooms::timeline::check_purge_batch(txn)
}

#[implement(Service)]
async fn append_relation_removals(
	&self,
	txn: &mut tuwunel_database::Txn,
	map: &std::sync::Arc<tuwunel_database::Map>,
	prefix: &[u8],
	typed: bool,
) -> Result {
	let mut after = None;
	loop {
		let keys = map
			.raw_keys_prefix_after(prefix, after.as_deref(), 64)
			.await?;
		if keys.is_empty() {
			return Ok(());
		}
		for key in &keys {
			self.append_relation_key(txn, map, key, typed)
				.await?;
			crate::rooms::timeline::check_purge_batch(txn)?;
		}
		after = keys.last().cloned();
	}
}

/// Rebuild `relatesto_typed` from every stored PDU. Run once at startup behind
/// a `global` marker, and on demand from the admin command. Clears first so a
/// partial or stale index is replaced wholesale.
#[implement(Service)]
pub async fn rebuild_typed_relations(&self) -> Result {
	self.db.relatesto_typed.clear().await?;

	let pdus = self.services.db["pduid_pdu"].clone();

	pdus.raw_stream()
		.map(|row| {
			let (key, value) = row?;
			let raw_pdu_id = RawPduId::from_bytes(key)?;
			let pdu_id = PduId {
				shortroomid: u64_from_u8(&raw_pdu_id.shortroomid()),
				count: raw_pdu_id.pdu_count(),
			};
			let pdu = serde_json::from_slice::<Pdu>(value)
				.map_err(|_| Error::bad_database("Invalid stored relation event"))?;

			Ok((pdu_id, pdu))
		})
		.try_for_each_concurrent(automatic_width(), async |(pdu_id, pdu)| {
			self.index_pdu_relations(pdu_id, &pdu).await
		})
		.await
}

#[implement(Service)]
async fn index_pdu_relations(&self, pdu_id: PduId, pdu: &Pdu) -> Result {
	let Ok(content) = pdu.get_content::<ExtractRelatesTo>() else {
		return Ok(());
	};

	let (rel_type, parent) = match content.relates_to {
		| Relation::Replacement(replacement) => (RelationType::Replacement, replacement.event_id),
		| Relation::Reference(reference) => (RelationType::Reference, reference.event_id),
		| _ => return Ok(()),
	};

	self.add_typed_relation(pdu_id.shortroomid, pdu_id.count, &parent, pdu, rel_type)
		.await
}

#[implement(Service)]
async fn append_relation_key(
	&self,
	txn: &mut tuwunel_database::Txn,
	map: &std::sync::Arc<tuwunel_database::Map>,
	key: &[u8],
	typed: bool,
) -> Result {
	let value = map.get(key).await?;
	let valid = if typed {
		key.len() == super::typed_relations::KEY_LEN
			&& matches!(key[16], 1 | 2)
			&& value.len() == 8
	} else {
		key.len() == 16 && value.is_empty()
	};
	if !valid {
		return Err(Error::bad_database("Invalid purge relation row"));
	}
	txn.del_raw(map, key);
	Ok(())
}

#[implement(Service)]
pub(crate) async fn append_history_relation_page(
	&self,
	txn: &mut tuwunel_database::Txn,
	short: ShortRoomId,
	parent: PduCount,
	typed: bool,
	after: Option<&[u8]>,
) -> Result<(Option<Vec<u8>>, bool)> {
	let mut prefix = Vec::new();
	if typed {
		prefix.extend_from_slice(&short.to_be_bytes());
	}
	prefix.extend_from_slice(&parent.to_be_bytes());
	let map = if typed {
		&self.db.relatesto_typed
	} else {
		&self.db.tofrom_relation
	};
	let keys = map
		.raw_keys_prefix_after(&prefix, after, 64)
		.await?;
	for key in &keys {
		self.append_relation_key(txn, map, key, typed)
			.await?;
	}
	Ok((keys.last().cloned(), keys.len() < 64))
}

#[implement(Service)]
pub(crate) async fn append_history_points(
	&self,
	txn: &mut tuwunel_database::Txn,
	short: ShortRoomId,
	parent: PduCount,
	room: &RoomId,
	event: &EventId,
) -> Result {
	for typed in [false, true] {
		let mut prefix = Vec::new();
		if typed {
			prefix.extend_from_slice(&short.to_be_bytes());
		}
		prefix.extend_from_slice(&parent.to_be_bytes());
		let map = if typed {
			&self.db.relatesto_typed
		} else {
			&self.db.tofrom_relation
		};
		if !map
			.raw_keys_prefix_after(&prefix, None, 1)
			.await?
			.is_empty()
		{
			return Err(Error::bad_database("History relation cleanup is unfinished"));
		}
	}
	txn.del(&self.db.referencedevents, (room, event));
	txn.del_raw(&self.db.softfailedeventids, event);
	txn.del_raw(&self.services.db["eventid_policysigstate"], event);
	Ok(())
}
