//! A canonical commit owns its immutable federation recipients. Queue pages
//! transfer that obligation atomically; hints and post-commit caches do not.
use std::{sync::atomic::Ordering, time::Duration};

use ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedServerName, RoomId};
use sha2::{Digest, Sha256};
use tokio::sync::OwnedMutexGuard;
use tuwunel_core::{
	Error, Result,
	matrix::{PduCount, PduEvent, RawPduId},
	utils::hash::sha256::hash,
};
use tuwunel_database::Txn;

use super::{Data, Destination, parse_servercurrentevent};
use crate::{
	rooms::short::ShortStateHash,
	sending::{Msg, SendingEvent, Service},
};

const PREFIX: u8 = 0x08;
const ROLE_PREFIX: u8 = 0x09;
const ROLE_MAGIC: &[u8; 5] = b"MSFR\x01";
const MAGIC: &[u8] = b"MSFP\x01";
const MAX_PLAN_BYTES: usize = 1_500_000;
const MAX_RECIPIENTS: usize = 4097;
const MAX_PENDING: usize = 64;
const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;
const PAGE: usize = 64;

struct Plan {
	state: ShortStateHash,
	room: OwnedRoomId,
	event: OwnedEventId,
	next: usize,
	cancelled: Vec<usize>,
	servers: Vec<OwnedServerName>,
}

fn bad() -> Error { Error::bad_database("Invalid canonical federation plan") }

fn witness_key(raw: &RawPduId) -> Vec<u8> {
	let mut key = Vec::with_capacity(17);
	key.push(PREFIX);
	key.extend_from_slice(raw.as_ref());
	key
}

fn role_key(raw: &RawPduId) -> Vec<u8> {
	let mut key = witness_key(raw);
	key[0] = ROLE_PREFIX;
	key
}

// Hash borrowed identifiers without retaining another event body. UTF-8 cannot
// contain 0xff, so it separates the variable-width room and event
// unambiguously.
fn role_record(raw: &RawPduId, room: &RoomId, event: &EventId, owned: bool) -> [u8; 38] {
	let mut out = [0_u8; 38];
	out[..5].copy_from_slice(ROLE_MAGIC);
	out[5] = u8::from(owned);
	let mut digest = Sha256::new();
	digest.update(&out[..6]);
	digest.update(raw.as_ref());
	digest.update(room.as_bytes());
	digest.update([0xFF]);
	digest.update(event.as_bytes());
	out[6..].copy_from_slice(&digest.finalize());
	out
}

fn witness(value: &[u8]) -> [u8; 36] {
	let mut out = [0_u8; 36];
	out[..4].copy_from_slice(
		&u32::try_from(value.len())
			.expect("bounded plan")
			.to_be_bytes(),
	);
	out[4..].copy_from_slice(&hash(value));
	out
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result {
	let length = u16::try_from(value.len()).map_err(|_| bad())?;
	out.extend_from_slice(&length.to_be_bytes());
	out.extend_from_slice(value.as_bytes());
	Ok(())
}

fn take<'a>(bytes: &mut &'a [u8], count: usize) -> Result<&'a [u8]> {
	if bytes.len() < count {
		return Err(bad());
	}
	let (value, rest) = bytes.split_at(count);
	*bytes = rest;
	Ok(value)
}

fn number(bytes: &mut &[u8]) -> Result<usize> {
	Ok(usize::from(u16::from_be_bytes(take(bytes, 2)?.try_into().map_err(|_| bad())?)))
}

fn string<'a>(bytes: &mut &'a [u8]) -> Result<&'a str> {
	let length = number(bytes)?;
	std::str::from_utf8(take(bytes, length)?).map_err(|_| bad())
}

impl Plan {
	fn encode(&self) -> Result<Vec<u8>> {
		if self.servers.is_empty()
			|| self.servers.len() > MAX_RECIPIENTS
			|| self.next >= self.servers.len()
			|| self.state == 0
			|| self
				.servers
				.windows(2)
				.any(|pair| pair[0] >= pair[1])
		{
			return Err(bad());
		}
		if self
			.cancelled
			.iter()
			.any(|index| *index >= self.servers.len())
			|| self
				.cancelled
				.windows(2)
				.any(|pair| pair[0] >= pair[1])
		{
			return Err(bad());
		}
		let size = MAGIC
			.len()
			.saturating_add(18)
			.saturating_add(self.cancelled.len().saturating_mul(2))
			.saturating_add(self.room.as_str().len())
			.saturating_add(self.event.as_str().len())
			.saturating_add(self.servers.iter().fold(0_usize, |sum, server| {
				sum.saturating_add(2)
					.saturating_add(server.as_str().len())
			}));
		if size > MAX_PLAN_BYTES {
			return Err(bad());
		}
		let mut out = Vec::with_capacity(size);
		out.extend_from_slice(MAGIC);
		out.extend_from_slice(&self.state.to_be_bytes());
		put_string(&mut out, self.room.as_str())?;
		put_string(&mut out, self.event.as_str())?;
		out.extend_from_slice(
			&u16::try_from(self.next)
				.map_err(|_| bad())?
				.to_be_bytes(),
		);
		out.extend_from_slice(
			&u16::try_from(self.servers.len())
				.map_err(|_| bad())?
				.to_be_bytes(),
		);
		out.extend_from_slice(
			&u16::try_from(self.cancelled.len())
				.map_err(|_| bad())?
				.to_be_bytes(),
		);
		for index in &self.cancelled {
			out.extend_from_slice(
				&u16::try_from(*index)
					.map_err(|_| bad())?
					.to_be_bytes(),
			);
		}
		for server in &self.servers {
			put_string(&mut out, server.as_str())?;
		}
		Ok(out)
	}

	fn decode(value: &[u8]) -> Result<Self> {
		if value.len() > MAX_PLAN_BYTES {
			return Err(bad());
		}
		let mut bytes = value;
		if take(&mut bytes, MAGIC.len())? != MAGIC {
			return Err(bad());
		}
		let state = u64::from_be_bytes(
			take(&mut bytes, 8)?
				.try_into()
				.map_err(|_| bad())?,
		);
		let room = OwnedRoomId::try_from(string(&mut bytes)?).map_err(|_| bad())?;
		let event = OwnedEventId::try_from(string(&mut bytes)?).map_err(|_| bad())?;
		let next = number(&mut bytes)?;
		let count = number(&mut bytes)?;
		if state == 0 || count == 0 || count > MAX_RECIPIENTS || next >= count {
			return Err(bad());
		}
		let cancelled_count = number(&mut bytes)?;
		if cancelled_count > count {
			return Err(bad());
		}
		let mut cancelled = Vec::with_capacity(cancelled_count);
		for _ in 0..cancelled_count {
			let index = number(&mut bytes)?;
			if index >= count
				|| cancelled
					.last()
					.is_some_and(|last| *last >= index)
			{
				return Err(bad());
			}
			cancelled.push(index);
		}
		let mut servers = Vec::with_capacity(count);
		for _ in 0..count {
			let server = OwnedServerName::try_from(string(&mut bytes)?).map_err(|_| bad())?;
			if servers.last().is_some_and(|last| last >= &server) {
				return Err(bad());
			}
			servers.push(server);
		}
		if !bytes.is_empty() {
			return Err(bad());
		}
		Ok(Self {
			state,
			room,
			event,
			next,
			cancelled,
			servers,
		})
	}
}

impl Data {
	/// Also held through history's canonical erasure commit. Queue/ACK writers
	/// use the same exclusion, so a page cannot resurrect an erased source.
	pub(crate) async fn lock_federation_sources(&self) -> OwnedMutexGuard<()> {
		self.active_write.clone().lock_owned().await
	}

	async fn source_plan(&self, raw: &RawPduId) -> Result<Option<Plan>> {
		let stored_witness = match self.db["global"].get(&witness_key(raw)).await {
			| Ok(value) => Some(value),
			| Err(error) if error.is_not_found() => None,
			| Err(error) => return Err(error),
		};
		let value = match self.db["pduid_federationplan"]
			.get(raw.as_ref())
			.await
		{
			| Ok(value) => value,
			| Err(error) if error.is_not_found() && stored_witness.is_none() => return Ok(None),
			| Err(error) if error.is_not_found() => return Err(bad()),
			| Err(error) => return Err(error),
		};
		if value.len() > MAX_PLAN_BYTES
			|| stored_witness
				.as_ref()
				.is_none_or(|stored| stored.as_ref() != witness(value.as_ref()))
		{
			return Err(bad());
		}
		Plan::decode(value.as_ref()).map(Some)
	}

	// Caller owns active_write. Read fixed-width witnesses before any plan
	// decoding; complete inventories refuse corruption rather than skip it.
	async fn source_inventory(&self) -> Result<(Vec<RawPduId>, usize)> {
		let keys = self.db["global"]
			.raw_keys_prefix_after(&[PREFIX], None, MAX_PENDING + 1)
			.await?;
		let plans = self.db["pduid_federationplan"]
			.raw_keys_after(None, MAX_PENDING + 1)
			.await?;
		if keys.len() > MAX_PENDING || keys.len() != plans.len() {
			return Err(bad());
		}
		let mut raw_ids = Vec::with_capacity(keys.len());
		let mut total = 0_usize;
		for (key, plan) in keys.iter().zip(&plans) {
			if key.len() != 17 || &key[1..] != plan.as_slice() {
				return Err(bad());
			}
			let raw = RawPduId::from_bytes(plan)?;
			let value = self.db["global"].get(key).await?;
			if value.len() != 36 {
				return Err(bad());
			}
			let size =
				usize::try_from(u32::from_be_bytes(value[..4].try_into().map_err(|_| bad())?))
					.map_err(|_| bad())?;
			if size == 0 || size > MAX_PLAN_BYTES {
				return Err(bad());
			}
			total = total.checked_add(size).ok_or_else(bad)?;
			if total > MAX_PENDING_BYTES {
				return Err(bad());
			}
			raw_ids.push(raw);
		}
		Ok((raw_ids, total))
	}

	/// Every new canonical row records whether this server accepted its
	/// delivery role. Missing metadata cannot be interpreted as a new role
	/// after an ACK.
	pub(crate) fn stage_federation_role(
		&self,
		txn: &mut Txn,
		raw: &RawPduId,
		room: &RoomId,
		event: &EventId,
		owned: bool,
	) {
		txn.insert_raw(&self.db["global"], role_key(raw), role_record(raw, room, event, owned));
	}

	async fn federation_role_owned(&self, raw: &RawPduId, pdu: &PduEvent) -> Result<bool> {
		let value = self.db["global"]
			.get(&role_key(raw))
			.await
			.map_err(|error| {
				if error.is_not_found() {
					Error::bad_database(
						"Canonical federation delivery role is missing. Databases predating \
						 schema 24 did not retain historical forwarding receipts; do not infer \
						 a role or requeue this event. Preserve the database for explicit \
						 legacy recovery. On new data this indicates missing ownership metadata.",
					)
				} else {
					error
				}
			})?;
		if value.len() != 38 || value[5] > 1 {
			return Err(bad());
		}
		let owned = value[5] == 1;
		if value.as_ref() != role_record(raw, &pdu.room_id, &pdu.event_id, owned) {
			return Err(bad());
		}
		Ok(owned)
	}

	/// Caller holds the canonical room lock, so erasure and another handshake
	/// cannot race the role's first admission. Refusal leaves the received role
	/// unchanged; a completed owned role never creates a new source page.
	pub(crate) async fn admit_known_federation_role(
		&self,
		raw: RawPduId,
		pdu: &PduEvent,
		state: ShortStateHash,
	) -> Result {
		let _guard = self.lock_federation_sources().await;
		self.require_active_schema().await?;
		self.require_deliverable_pdu(&raw).await?;
		if self.federation_role_owned(&raw, pdu).await? {
			// A duplicate handshake still validates any outstanding obligation.
			// Completed roles have no plan; a corrupt retained plan is not an
			// acknowledged acceptance merely because its role is already owned.
			if let Some(plan) = self.source_plan(&raw).await?
				&& (plan.room != pdu.room_id || plan.event != pdu.event_id)
			{
				return Err(bad());
			}
			return Ok(());
		}
		let mut txn = self.db.txn();
		self.prepare_federation_plan(&mut txn, raw, pdu, state)
			.await?;
		self.stage_federation_role(&mut txn, &raw, &pdu.room_id, &pdu.event_id, true);
		txn.check_bridge_admission()?;
		txn.execute_flushed().await
	}

	pub(crate) async fn stage_federation_plan(
		&self,
		txn: &mut Txn,
		raw: RawPduId,
		pdu: &PduEvent,
		state: ShortStateHash,
	) -> Result<OwnedMutexGuard<()>> {
		let guard = self.lock_federation_sources().await;
		self.require_active_schema().await?;
		self.prepare_federation_plan(txn, raw, pdu, state)
			.await?;
		Ok(guard)
	}

	// Caller holds active_write through either the canonical commit or the
	// existing event's role commit. No hint or cursor can escape before it.
	async fn prepare_federation_plan(
		&self,
		txn: &mut Txn,
		raw: RawPduId,
		pdu: &PduEvent,
		state: ShortStateHash,
	) -> Result {
		if self.event_erasure_started(&raw).await? {
			return Err(Error::bad_database("Cannot admit federation for an erasing event"));
		}
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();
		let servers = services_root
			.state_accessor
			.federation_servers_for_append(state, pdu)
			.await?;

		for server in &servers {
			self.resume_cancellation(&Destination::Federation(server.clone()))
				.await?;
		}
		if servers.is_empty() {
			return Ok(());
		}
		let plan = Plan {
			state,
			room: pdu.room_id.clone(),
			event: pdu.event_id.clone(),
			next: 0,
			cancelled: Vec::new(),
			servers,
		};
		let value = plan.encode()?;
		let (pending, bytes) = self.source_inventory().await?;
		// Admission must verify existing bodies before trusting their quota
		// witnesses. Decode one bounded plan at a time; retain no second inventory.
		for raw in &pending {
			if self.source_plan(raw).await?.is_none() {
				return Err(bad());
			}
		}
		// Reserve every plan's possible cancellation bitmap growth now. Later
		// cancellation pages cannot exceed the admitted aggregate byte budget.
		let reserved = pending
			.len()
			.saturating_add(1)
			.saturating_mul(MAX_RECIPIENTS)
			.saturating_mul(2);
		if pending.len() >= MAX_PENDING
			|| bytes
				.saturating_add(value.len())
				.saturating_add(reserved)
				> MAX_PENDING_BYTES
		{
			return Err(Error::bad_database(
				"Canonical federation admission is full; retry the event",
			));
		}
		if self.source_plan(&raw).await?.is_some() {
			return Err(bad());
		}
		txn.insert_raw(&self.db["global"], witness_key(&raw), witness(&value));
		txn.insert_raw(&self.db["pduid_federationplan"], raw.as_ref(), value);
		Ok(())
	}

	/// One page transfers responsibility to the ordinary delivery queues. The
	/// committed cursor prevents replaying an earlier page after its ACK.
	pub(crate) async fn materialize_federation_page(&self, raw: RawPduId) -> Result<Vec<Msg>> {
		let _guard = self.lock_federation_sources().await;
		self.require_active_schema().await?;
		let Some(plan) = self.source_plan(&raw).await? else {
			return Ok(Vec::new());
		};
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();
		if let PduCount::Normal(count) = raw.pdu_count() {
			if count > services_root.globals.pending_count().end {
				return Err(bad());
			}
			if count > services_root.globals.current_count() {
				return Ok(Vec::new());
			}
		}
		let pdu = services_root
			.timeline
			.get_pdu_from_id(&raw)
			.await?;
		if !self.federation_role_owned(&raw, &pdu).await?
			|| pdu.event_id != plan.event
			|| pdu.room_id != plan.room
			|| services_root
				.timeline
				.get_pdu_id(&plan.event)
				.await? != raw
			|| services_root
				.short
				.get_shortroomid(&plan.room)
				.await?
				.to_be_bytes()
				!= raw.shortroomid()
			|| plan
				.servers
				.iter()
				.any(|server| services_root.globals.server_is_ours(server))
		{
			return Err(bad());
		}
		for server in &plan.servers[plan.next
			..plan
				.next
				.saturating_add(PAGE)
				.min(plan.servers.len())]
		{
			self.resume_cancellation(&Destination::Federation(server.clone()))
				.await?;
		}
		let Some(mut plan) = self.source_plan(&raw).await? else {
			return Ok(Vec::new());
		};
		let end = plan
			.next
			.saturating_add(PAGE)
			.min(plan.servers.len());
		let mut txn = self.db.txn();
		let mut messages = Vec::with_capacity(end.saturating_sub(plan.next));
		for index in plan.next..end {
			if plan.cancelled.binary_search(&index).is_ok() {
				continue;
			}
			let server = &plan.servers[index];
			let dest = Destination::Federation(server.clone());
			let queue_id = dest.event_key(&raw);
			let event = SendingEvent::Pdu(raw);
			let active = match self.servercurrentevent_data.get(&queue_id).await {
				| Ok(value) if parse_servercurrentevent(&queue_id, &value)?.1 != event =>
					return Err(bad()),
				| Ok(_) => true,
				| Err(error) if error.is_not_found() => false,
				| Err(error) => return Err(error),
			};
			match self.servernameevent_data.get(&queue_id).await {
				| Ok(value) if !value.is_empty() => return Err(bad()),
				| Ok(_) => {},
				| Err(error) if error.is_not_found() && !active =>
					self.stage_request(&mut txn, &queue_id, &event),
				| Err(error) if error.is_not_found() => {},
				| Err(error) => return Err(error),
			}
			messages.push(Msg { dest, event, queue_id });
		}
		plan.next = end;
		if end == plan.servers.len() {
			txn.del_raw(&self.db["pduid_federationplan"], raw.as_ref());
			txn.del_raw(&self.db["global"], witness_key(&raw));
		} else {
			let value = plan.encode()?;
			txn.insert_raw(&self.db["global"], witness_key(&raw), witness(&value));
			txn.insert_raw(&self.db["pduid_federationplan"], raw.as_ref(), value);
		}
		txn.check_bridge_admission()?;
		txn.execute_flushed().await?;
		Ok(messages)
	}

	pub(super) async fn cancel_federation_sources(&self, destination: &Destination) -> Result {
		let Destination::Federation(server) = destination else {
			return Ok(());
		};
		let (ids, _) = self.source_inventory().await?;
		for raw in ids {
			let mut plan = self.source_plan(&raw).await?.ok_or_else(bad)?;
			let Ok(index) = plan.servers.binary_search(server) else {
				continue;
			};
			if index < plan.next {
				continue;
			}
			let Err(position) = plan.cancelled.binary_search(&index) else {
				continue;
			};
			plan.cancelled.insert(position, index);
			let value = plan.encode()?;
			let mut txn = self.db.txn();
			txn.insert_raw(&self.db["global"], witness_key(&raw), witness(&value));
			txn.insert_raw(&self.db["pduid_federationplan"], raw.as_ref(), value);
			txn.check_bridge_admission()?;
			txn.execute_flushed().await?;
		}
		Ok(())
	}

	/// Caller holds lock_federation_sources through the canonical erase commit.
	pub(crate) async fn stage_federation_erasure(&self, txn: &mut Txn, raw: &RawPduId) -> Result {
		self.stage_event_queues_erasure(txn, raw).await?;
		self.stage_federation_role_erasure(txn, raw);
		self.stage_federation_source_erasure(txn, raw)
			.await
	}

	pub(crate) fn stage_federation_role_erasure(&self, txn: &mut Txn, raw: &RawPduId) {
		txn.del_raw(&self.db["global"], role_key(raw));
		self.stage_finish_event_erasure(txn, raw);
	}

	pub(super) async fn stage_federation_source_erasure(
		&self,
		txn: &mut Txn,
		raw: &RawPduId,
	) -> Result {
		if self.source_plan(raw).await?.is_some() {
			txn.del_raw(&self.db["pduid_federationplan"], raw.as_ref());
			txn.del_raw(&self.db["global"], witness_key(raw));
		}
		Ok(())
	}

	#[cfg(test)]
	pub(crate) async fn has_federation_plan(&self, raw: &RawPduId) -> Result<bool> {
		Ok(self.source_plan(raw).await?.is_some())
	}
}

impl Service {
	pub(crate) async fn resume_federation_source(&self, raw: RawPduId) -> Result {
		for message in self.db.materialize_federation_page(raw).await? {
			self.dispatch(message)?;
		}
		Ok(())
	}

	pub(crate) fn wake_federation_sources(&self) { self.federation_source_signal.notify_one(); }

	pub(in crate::sending) async fn federation_source_worker(&self) -> Result {
		let mut after = None;
		let mut turns = 0_usize;
		while self.server.is_running()
			&& !self
				.federation_source_stopped
				.load(Ordering::Acquire)
		{
			let (ids, _) = {
				let _guard = self.db.lock_federation_sources().await;
				self.db.source_inventory().await?
			};
			let next = ids.iter().find(|raw| {
				after
					.as_ref()
					.is_none_or(|after: &RawPduId| raw.as_ref() > after.as_ref())
			});
			if let Some(raw) = next.filter(|_| turns < MAX_PENDING) {
				turns = turns.saturating_add(1);
				after = Some(*raw);
				if let Err(error) = self.resume_federation_source(*raw).await {
					tuwunel_core::warn!(?error, "Canonical federation page remains owed");
				}
				tokio::task::yield_now().await;
				continue;
			}
			after = None;
			turns = 0;
			let delay = if ids.is_empty() {
				Duration::from_secs(5)
			} else {
				Duration::from_secs(1)
			};
			tokio::select! {
				() = self.federation_source_signal.notified() => {},
				() = tokio::time::sleep(delay) => {},
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::{MAX_RECIPIENTS, Plan};

	#[test]
	fn manifest_preserves_large_fanout_and_rejects_corruption() {
		let plan = Plan {
			state: 9,
			room: "!room:example.org".try_into().unwrap(),
			event: "$event".try_into().unwrap(),
			next: 900,
			cancelled: vec![901, 4096],
			servers: (0..MAX_RECIPIENTS)
				.map(|i| {
					format!("server{i:04}.invalid")
						.try_into()
						.unwrap()
				})
				.collect(),
		};
		let value = plan.encode().unwrap();
		let decoded = Plan::decode(&value).unwrap();
		assert_eq!(decoded.servers, plan.servers);
		assert_eq!(decoded.next, 900);
		for width in [0, 1, 4, 5, 12, 20, value.len() - 1] {
			assert!(Plan::decode(&value[..width]).is_err());
		}
		let mut extra = value;
		extra.push(0);
		assert!(Plan::decode(&extra).is_err());
	}
}
