//! Sender-owned immutable transactions. Membership and body chunks commit
//! before HTTP; acknowledgement retires them with the selected active rows.

use std::{collections::HashSet, sync::Arc};

use futures::{StreamExt, TryStreamExt, stream::iter};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tuwunel_bridge::MAX_KEY_BYTES;
use tuwunel_core::{Error, Result, utils::hash::sha256::hash};
use tuwunel_database::{Cbor, Get, Txn, deserialize_from_slice, serialize_to_vec};

use super::{ActiveAcknowledgement, Data, Destination, active, parse_servercurrentevent};

const MAGIC: &[u8] = b"MSTX\x02";
const HEADER_LIMIT: usize = 256 * 1024;
pub(in crate::sending) const BODY_LIMIT: usize = 3 * 1024 * 1024;
const CHUNK_SIZE: usize = 128 * 1024;
const MEMBER_LIMIT: usize = 512;
const CANCEL_PAGE: usize = 64;
// The global map also owns the required schema/counter. Keeping the witness
// outside the body map distinguishes a lost journal from unattempted rows;
// losing the entire global map already fails the schema/counter checks.
const WITNESS_PREFIX: u8 = 0x06;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(in crate::sending) enum AttemptKind {
	Appservice,
	Federation,
	Cancelled,
}

impl AttemptKind {
	fn for_destination(destination: &Destination) -> Option<Self> {
		match destination {
			| Destination::Appservice(_) => Some(Self::Appservice),
			| Destination::Federation(_) => Some(Self::Federation),
			| Destination::Push(..) => None,
		}
	}
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Member {
	key: ByteBuf,
	identity: Option<u64>,
	digest: [u8; 32],
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Record {
	owner: ByteBuf,
	generation: u64,
	kind: AttemptKind,
	recipient: Option<[u8; 32]>,
	#[serde(deserialize_with = "deserialize_members")]
	members: Vec<Member>,
	body_len: usize,
	body_digest: [u8; 32],
}

// Refuse hostile CBOR length hints before Vec reserves from them. The sealed
// encoded-header byte limit alone does not bound a declared array length.
fn deserialize_members<'de, D: serde::Deserializer<'de>>(
	decoder: D,
) -> std::result::Result<Vec<Member>, D::Error> {
	struct Members;
	impl<'de> serde::de::Visitor<'de> for Members {
		type Value = Vec<Member>;

		fn expecting(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			out.write_str("bounded outgoing membership")
		}

		fn visit_seq<S: serde::de::SeqAccess<'de>>(
			self,
			mut sequence: S,
		) -> std::result::Result<Self::Value, S::Error> {
			if sequence
				.size_hint()
				.is_some_and(|size| size > MEMBER_LIMIT)
			{
				return Err(serde::de::Error::custom("outgoing membership count limit"));
			}
			let mut members = Vec::new();
			while let Some(member) = sequence.next_element()? {
				if members.len() == MEMBER_LIMIT {
					return Err(serde::de::Error::custom("outgoing membership count limit"));
				}
				members.push(member);
			}
			Ok(members)
		}
	}
	decoder.deserialize_seq(Members)
}

impl Record {
	fn chunks(&self) -> usize { self.body_len.div_ceil(CHUNK_SIZE) }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AttemptRef {
	value: Vec<u8>,
	generation: u64,
	chunks: usize,
}

#[derive(Debug)]
pub(in crate::sending) struct PreparedAttempt {
	pub(in crate::sending) recipient: Option<[u8; 32]>,
	pub(in crate::sending) body: Arc<[u8]>,
	pub(in crate::sending) acknowledgement: ActiveAcknowledgement,
	generation: u64,
}

impl PreparedAttempt {
	pub(in crate::sending) fn transaction_id(&self) -> String {
		format!("matrix-{:016x}", self.generation)
	}
}

/// Includes the URL, credentials, sender and extension/namespace policy.
/// Only the digest enters the transaction journal.
pub(in crate::sending) fn appservice_owner(
	registration: &ruma::api::appservice::Registration,
) -> Result<[u8; 32]> {
	Ok(hash(serde_json::to_vec(registration)?))
}

fn encode(record: &Record) -> Result<Vec<u8>> {
	let payload = serialize_to_vec(Cbor(record))?;
	if payload
		.len()
		.saturating_add(MAGIC.len().saturating_add(32))
		> HEADER_LIMIT
	{
		return Err(Error::bad_database("Outgoing transaction membership exceeds byte limit"));
	}
	let mut value = MAGIC.to_vec();
	value.extend_from_slice(&payload);
	let seal = hash(&value);
	value.extend_from_slice(&seal);
	Ok(value)
}

fn decode(value: &[u8], destination: &Destination, counter: u64) -> Result<Record> {
	if value.len() > HEADER_LIMIT
		|| value.len() < MAGIC.len().saturating_add(32)
		|| !value.starts_with(MAGIC)
	{
		return Err(Error::bad_database("Invalid outgoing transaction envelope"));
	}
	let end = value
		.len()
		.checked_sub(32)
		.expect("checked header seal width");
	if value[end..] != hash(&value[..end]) {
		return Err(Error::bad_database("Corrupt outgoing transaction header"));
	}
	let Cbor(record): Cbor<Record> = deserialize_from_slice(&value[MAGIC.len()..end])?;
	let prefix = destination.get_prefix();
	if record.owner.as_ref() != prefix
		|| prefix.len().saturating_add(17) > MAX_KEY_BYTES
		|| record.generation == 0
		|| record.generation > counter
		|| record.body_len > BODY_LIMIT
		|| record.members.len() > MEMBER_LIMIT
	{
		return Err(Error::bad_database("Invalid outgoing transaction ownership or bounds"));
	}
	if record.kind == AttemptKind::Cancelled {
		if record.body_len != 0
			|| !record.members.is_empty()
			|| record.body_digest != hash([])
			|| record.recipient.is_some()
		{
			return Err(Error::bad_database("Invalid outgoing cancellation record"));
		}
	} else if (record.kind == AttemptKind::Appservice) != record.recipient.is_some()
		|| Some(record.kind) != AttemptKind::for_destination(destination)
		|| record.body_len == 0
		|| record.members.is_empty()
	{
		return Err(Error::bad_database("Invalid outgoing transaction kind"));
	}
	let mut keys = HashSet::new();
	for member in &record.members {
		if member.key.len() <= prefix.len()
			|| member.key.len() > MAX_KEY_BYTES
			|| !member.key.starts_with(&prefix)
			|| !keys.insert(member.key.as_ref())
			|| member
				.identity
				.is_some_and(|identity| identity == 0 || identity >= record.generation)
		{
			return Err(Error::bad_database("Invalid outgoing transaction member"));
		}
	}
	Ok(record)
}

fn chunk_key(prefix: &[u8], generation: u64, index: usize) -> Vec<u8> {
	let mut key = prefix.to_vec();
	key.push(0);
	key.extend_from_slice(&generation.to_be_bytes());
	key.extend_from_slice(
		&u64::try_from(index)
			.expect("bounded transaction chunk")
			.to_be_bytes(),
	);
	key
}

fn witness_key(destination: &Destination) -> Vec<u8> {
	let mut key = vec![WITNESS_PREFIX];
	key.extend_from_slice(&destination.get_prefix());
	key
}

fn witness(value: &[u8]) -> Vec<u8> {
	let mut record = b"MOWN\x01".to_vec();
	record.extend_from_slice(&hash(value));
	record
}

impl Data {
	async fn read_attempt_header(
		&self,
		destination: &Destination,
	) -> Result<Option<(Vec<u8>, Record)>> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let ownership = match self.db["global"]
			.get(&witness_key(destination))
			.await
		{
			| Ok(value) => Some(value.as_ref().to_vec()),
			| Err(error) if error.is_not_found() => None,
			| Err(error) => return Err(error),
		};
		match self
			.sendingtransaction_record
			.get(&destination.get_prefix())
			.await
		{
			| Ok(value) => {
				// Journal generations are persisted dispatches, independent of
				// earlier unrelated writes holding the retirement frontier.
				let record =
					decode(&value, destination, services_root.globals.pending_count().end)?;
				if ownership.as_ref() != Some(&witness(&value)) {
					return Err(Error::bad_database(
						"Outgoing transaction ownership witness is missing or changed",
					));
				}
				Ok(Some((value.as_ref().to_vec(), record)))
			},
			| Err(error) if error.is_not_found() => {
				if ownership.is_some() {
					return Err(Error::bad_database(
						"Attempted deliveries lost their outgoing transaction journal",
					));
				}
				if self
					.sendingtransaction_record
					.raw_keys_prefix(&destination.get_prefix())
					.boxed()
					.next()
					.await
					.transpose()?
					.is_some()
				{
					return Err(Error::bad_database(
						"Outgoing transaction body has no ownership header",
					));
				}
				Ok(None)
			},
			| Err(error) => Err(error),
		}
	}

	pub(in crate::sending) async fn load_attempt(
		&self,
		destination: &Destination,
	) -> Result<Option<PreparedAttempt>> {
		if AttemptKind::for_destination(destination).is_none() {
			return Ok(None);
		}
		let _guard = self.active_write.lock().await;
		self.require_active_schema().await?;
		self.resume_cancellation(destination).await?;
		self.load_ready_attempt(destination).await
	}

	pub(in crate::sending) async fn require_current_attempt(
		&self,
		destination: &Destination,
		expected: &PreparedAttempt,
	) -> Result {
		// No cancellation writes while the sender retains registry ownership.
		// The outer load/persist path already resumed interrupted cancellation.
		let _guard = self.active_write.lock().await;
		self.require_active_schema().await?;
		let Some(current) = self.load_ready_attempt(destination).await? else {
			return Err(Error::bad_database("Outgoing transaction was retired before HTTP"));
		};
		if current.generation != expected.generation
			|| current.acknowledgement != expected.acknowledgement
			|| current.body != expected.body
			|| current.recipient != expected.recipient
		{
			return Err(Error::bad_database("Outgoing transaction owner changed before HTTP"));
		}
		Ok(())
	}

	async fn load_ready_attempt(
		&self,
		destination: &Destination,
	) -> Result<Option<PreparedAttempt>> {
		let Some((value, record)) = self.read_attempt_header(destination).await? else {
			return Ok(None);
		};
		if record.kind == AttemptKind::Cancelled {
			return Err(Error::bad_database("Outgoing cancellation has not completed"));
		}
		let mut selected = iter(
			record
				.members
				.iter()
				.map(|member| member.key.as_ref()),
		)
		.get(&self.servercurrentevent_data)
		.boxed();
		let mut rows = Vec::with_capacity(record.members.len());
		let mut size = 0_usize;
		for member in &record.members {
			let current = selected
				.next()
				.await
				.ok_or_else(|| Error::bad_database("Incomplete transaction member lookup"))??;
			self.validate_active_identity(&current)?;
			if hash(&current) != member.digest || active::identity(&current)? != member.identity {
				return Err(Error::bad_database("Outgoing transaction member changed"));
			}
			let (owner, _) = parse_servercurrentevent(&member.key, &current)?;
			if &owner != destination {
				return Err(Error::bad_database(
					"Outgoing transaction member destination mismatch",
				));
			}
			size = size.saturating_add(current.len());
			if size > BODY_LIMIT {
				return Err(Error::bad_database("Outgoing active member bytes exceed limit"));
			}
			rows.push((member.key.to_vec(), current.as_ref().to_vec()));
		}
		if selected.next().await.is_some() {
			return Err(Error::bad_database("Extra transaction member lookup result"));
		}
		drop(selected);
		let body = self
			.read_attempt_body(destination, &record)
			.await?;
		Ok(Some(PreparedAttempt {
			recipient: record.recipient,
			body: body.into(),
			generation: record.generation,
			acknowledgement: ActiveAcknowledgement {
				rows,
				attempt: Some(AttemptRef {
					value,
					generation: record.generation,
					chunks: record.chunks(),
				}),
			},
		}))
	}

	async fn read_attempt_body(
		&self,
		destination: &Destination,
		record: &Record,
	) -> Result<Vec<u8>> {
		let prefix = destination.get_prefix();
		let keys = (0..record.chunks())
			.map(|index| chunk_key(&prefix, record.generation, index))
			.collect::<Vec<_>>();
		let mut chunks = iter(keys.iter().map(Vec::as_slice))
			.get(&self.sendingtransaction_record)
			.boxed();
		let mut body = Vec::with_capacity(record.body_len);
		for index in 0..record.chunks() {
			let chunk = chunks
				.next()
				.await
				.ok_or_else(|| Error::bad_database("Missing transaction chunk lookup"))??;
			let expected = record
				.body_len
				.checked_sub(index.saturating_mul(CHUNK_SIZE))
				.expect("checked chunk offset")
				.min(CHUNK_SIZE);
			if chunk.len() != expected {
				return Err(Error::bad_database("Invalid outgoing transaction chunk length"));
			}
			body.extend_from_slice(&chunk);
		}
		if chunks.next().await.is_some() || hash(&body) != record.body_digest {
			return Err(Error::bad_database("Corrupt outgoing transaction body"));
		}
		drop(chunks);
		validate_body(&body)?;
		Ok(body)
	}

	pub(in crate::sending) async fn persist_attempt(
		&self,
		destination: &Destination,
		selected: ActiveAcknowledgement,
		body: Vec<u8>,
		recipient: Option<[u8; 32]>,
	) -> Result<PreparedAttempt> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let _guard = self.active_write.lock().await;
		self.require_active_schema().await?;
		self.resume_cancellation(destination).await?;
		if let Some(attempt) = self.load_ready_attempt(destination).await? {
			if attempt.recipient != recipient {
				return Err(Error::bad_database(
					"Outgoing transaction registration owner changed",
				));
			}
			return Ok(attempt);
		}
		validate_body(&body)?;
		let kind = AttemptKind::for_destination(destination)
			.ok_or_else(|| Error::bad_database("Push has no transaction journal"))?;
		if (kind == AttemptKind::Appservice) != recipient.is_some() {
			return Err(Error::bad_database("Outgoing transaction lacks registration ownership"));
		}
		let prefix = destination.get_prefix();
		if selected.rows.is_empty()
			|| selected.rows.len() > MEMBER_LIMIT
			|| prefix.len().saturating_add(17) > MAX_KEY_BYTES
		{
			return Err(Error::bad_database("Invalid outgoing transaction membership bounds"));
		}
		let mut unique = HashSet::new();
		let mut members = Vec::with_capacity(selected.rows.len());
		let mut size = 0_usize;
		for (key, value) in &selected.rows {
			let (owner, _) = parse_servercurrentevent(key, value)?;
			if &owner != destination
				|| key.len() > MAX_KEY_BYTES
				|| !unique.insert(key.as_slice())
			{
				return Err(Error::bad_database("Invalid outgoing transaction selected member"));
			}
			self.validate_active_identity(value)?;
			let current = self.servercurrentevent_data.get(key).await?;
			if current.as_ref() != value.as_slice() {
				return Err(Error::bad_database(
					"Selected delivery changed before transaction persistence",
				));
			}
			size = size.saturating_add(value.len());
			if size > BODY_LIMIT {
				return Err(Error::bad_database("Outgoing active member bytes exceed limit"));
			}
			members.push(Member {
				key: key.clone().into(),
				identity: active::identity(value)?,
				digest: hash(value),
			});
		}
		let mut record = Record {
			owner: prefix.clone().into(),
			generation: u64::MAX,
			kind,
			recipient,
			members,
			body_len: body.len(),
			body_digest: hash(&body),
		};
		// Check the largest possible encoded counter before consuming one.
		encode(&record)?;
		record.generation = *services_root.globals.next_count().await?;
		let value = encode(&record)?;
		let mut txn = self.db.txn();
		txn.insert_raw(&self.sendingtransaction_record, &prefix, &value);
		txn.insert_raw(&self.db["global"], witness_key(destination), witness(&value));
		for (index, chunk) in body.chunks(CHUNK_SIZE).enumerate() {
			txn.insert_raw(
				&self.sendingtransaction_record,
				chunk_key(&prefix, record.generation, index),
				chunk,
			);
		}
		txn.execute().await?;
		Ok(PreparedAttempt {
			recipient: record.recipient,
			body: body.into(),
			generation: record.generation,
			acknowledgement: ActiveAcknowledgement {
				rows: selected.rows,
				attempt: Some(AttemptRef {
					value,
					generation: record.generation,
					chunks: record.chunks(),
				}),
			},
		})
	}

	/// Called with active_write held. A newer generation owns all subsequent
	/// work; an old response may not touch it, including its selected rows.
	pub(super) async fn stage_attempt_ack(
		&self,
		destination: &Destination,
		acknowledgement: &ActiveAcknowledgement,
		txn: &mut Txn,
	) -> Result<bool> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let current = self.read_attempt_header(destination).await?;
		let Some(expected) = &acknowledgement.attempt else {
			if current.is_some() {
				return Err(Error::bad_database(
					"Acknowledgement lacks persisted transaction ownership",
				));
			}
			return Ok(true);
		};
		decode(&expected.value, destination, services_root.globals.pending_count().end)?;
		let Some((value, record)) = current else { return Ok(true) };
		if value != expected.value {
			if record.generation > expected.generation {
				return Ok(false);
			}
			return Err(Error::bad_database(
				"Outgoing transaction changed before acknowledgement",
			));
		}
		self.read_attempt_body(destination, &record)
			.await?;
		let prefix = destination.get_prefix();
		txn.del_raw(&self.sendingtransaction_record, &prefix);
		txn.del_raw(&self.db["global"], witness_key(destination));
		for index in 0..expected.chunks {
			txn.del_raw(
				&self.sendingtransaction_record,
				chunk_key(&prefix, expected.generation, index),
			);
		}
		Ok(true)
	}

	/// Retire the immutable body containing this erased member. Other active
	/// rows retain their identities and can compose a new transaction. The
	/// monotonically increasing generation fences an old ACK after
	/// recomposition.
	pub(super) async fn stage_erased_attempt(
		&self,
		txn: &mut Txn,
		destination: &Destination,
		key: &[u8],
	) -> Result {
		if AttemptKind::for_destination(destination).is_none() {
			return Ok(());
		}
		let Some((_, record)) = self.read_attempt_header(destination).await? else {
			return Ok(());
		};
		if !record
			.members
			.iter()
			.any(|member| member.key.as_ref() == key)
		{
			return Ok(());
		}
		self.read_attempt_body(destination, &record)
			.await?;
		let prefix = destination.get_prefix();
		txn.del_raw(&self.sendingtransaction_record, &prefix);
		txn.del_raw(&self.db["global"], witness_key(destination));
		for index in 0..record.chunks() {
			txn.del_raw(
				&self.sendingtransaction_record,
				chunk_key(&prefix, record.generation, index),
			);
		}
		Ok(())
	}

	pub(super) async fn cancel_attempt_and_requests(&self, destination: &Destination) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		self.require_active_schema().await?;
		if AttemptKind::for_destination(destination).is_none() {
			self.delete_queue_pages(destination).await?;
			return self.clear_push_backoff(destination).await;
		}
		let previous = self.read_attempt_header(destination).await?;
		let prefix = destination.get_prefix();
		if prefix.len().saturating_add(17) > MAX_KEY_BYTES {
			return Err(Error::bad_database("Outgoing cancellation owner exceeds key limit"));
		}
		let generation = *services_root.globals.next_count().await?;
		let record = Record {
			owner: prefix.clone().into(),
			generation,
			kind: AttemptKind::Cancelled,
			recipient: None,
			members: Vec::new(),
			body_len: 0,
			body_digest: hash([]),
		};
		let value = encode(&record)?;
		let mut txn = self.db.txn();
		txn.insert_raw(&self.sendingtransaction_record, &prefix, &value);
		txn.insert_raw(&self.db["global"], witness_key(destination), witness(&value));
		if let Some((_, previous)) = previous {
			for index in 0..previous.chunks() {
				txn.del_raw(
					&self.sendingtransaction_record,
					chunk_key(&prefix, previous.generation, index),
				);
			}
		}
		txn.execute().await?;
		self.resume_cancellation(destination).await
	}

	/// Called under active_write before admitting appservice/federation work.
	pub(super) async fn resume_cancellation(&self, destination: &Destination) -> Result {
		if AttemptKind::for_destination(destination).is_none() {
			return Ok(());
		}
		let Some((_, record)) = self.read_attempt_header(destination).await? else {
			return Ok(());
		};
		if record.kind != AttemptKind::Cancelled {
			return Ok(());
		}
		self.require_active_schema().await?;
		self.cancel_federation_sources(destination)
			.await?;
		self.delete_queue_pages(destination).await?;
		let mut txn = self.db.txn();
		txn.del_raw(&self.sendingtransaction_record, destination.get_prefix());
		txn.del_raw(&self.db["global"], witness_key(destination));
		txn.execute().await
	}

	async fn delete_queue_pages(&self, destination: &Destination) -> Result {
		let prefix = destination.get_prefix();
		for map in [&self.servercurrentevent_data, &self.servernameevent_data] {
			loop {
				let keys = map
					.raw_keys_prefix(&prefix)
					.take(CANCEL_PAGE)
					.map_ok(<[u8]>::to_vec)
					.try_collect::<Vec<_>>()
					.await?;
				if keys.is_empty() {
					break;
				}
				let mut txn = self.db.txn();
				for key in keys {
					txn.del_raw(map, key);
				}
				txn.execute().await?;
			}
		}
		Ok(())
	}
}

fn validate_body(body: &[u8]) -> Result {
	if body.is_empty() || body.len() > BODY_LIMIT {
		return Err(Error::bad_database("Outgoing transaction body exceeds byte limit"));
	}
	let raw: &serde_json::value::RawValue = serde_json::from_slice(body)
		.map_err(|_| Error::bad_database("Invalid persisted outgoing transaction JSON"))?;
	if !raw.get().starts_with('{') {
		return Err(Error::bad_database("Outgoing transaction body is not an object"));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use tuwunel_bridge::{Lease, Mutation, REQUEST_ID_LEN, Request, catalog, request};

	use super::{
		AttemptKind, BODY_LIMIT, CHUNK_SIZE, Destination, Member, Record, chunk_key, decode,
		encode, hash, validate_body, witness, witness_key,
	};

	#[test]
	fn maximum_body_with_long_destination_fits_the_actual_bridge_commit() {
		let destination =
			Destination::Appservice("a".repeat(tuwunel_bridge::MAX_KEY_BYTES.saturating_sub(19)));
		let prefix = destination.get_prefix();
		let mut body = br#"{"padding":""#.to_vec();
		body.resize(BODY_LIMIT.saturating_sub(2), b'a');
		body.extend_from_slice(b"\"}");
		validate_body(&body).expect("maximum JSON body");
		let mut key = prefix.clone();
		key.extend_from_slice(&1_u64.to_be_bytes());
		let record = Record {
			owner: prefix.clone().into(),
			generation: 2,
			kind: AttemptKind::Appservice,
			recipient: Some([1; 32]),
			members: vec![Member {
				key: key.into(),
				identity: Some(1),
				digest: hash(b"member"),
			}],
			body_len: body.len(),
			body_digest: hash(&body),
		};
		let header = encode(&record).expect("bounded header");
		decode(&header, &destination, 2).expect("checked header");
		let map = catalog::map_id("sendingtransaction_record")
			.expect("append-only map")
			.0;
		let mut ops = vec![
			Mutation::Put {
				map: catalog::map_id("global").expect("global map").0,
				key: witness_key(&destination).into(),
				val: witness(&header).into(),
			},
			Mutation::Put {
				map,
				key: prefix.clone().into(),
				val: header.into(),
			},
		];
		for (index, bytes) in body.chunks(CHUNK_SIZE).enumerate() {
			ops.push(Mutation::Put {
				map,
				key: chunk_key(&prefix, 2, index).into(),
				val: bytes.to_vec().into(),
			});
		}
		let commit = Request::Commit {
			request_id: vec![1; REQUEST_ID_LEN].into(),
			lease: Lease { holder: "a".repeat(128), epoch: 1 },
			digest: tuwunel_bridge::digest(&ops).to_vec().into(),
			ops,
		};
		let wire =
			request::encode(&commit).expect("maximum body fits actual row/op/request validator");
		assert!(wire.len() < request::MAX_BYTES);
	}

	#[test]
	fn sealed_header_refuses_hostile_member_count_hints_before_allocation() {
		let destination = Destination::Appservice("hint-owner".into());
		let record = Record {
			owner: destination.get_prefix().into(),
			generation: 2,
			kind: AttemptKind::Appservice,
			recipient: Some([1; 32]),
			members: Vec::new(),
			body_len: 2,
			body_digest: hash(b"{}"),
		};
		let header = encode(&record).unwrap();
		let name = b"members";
		let field = header
			.windows(name.len())
			.position(|bytes| bytes == name)
			.expect("CBOR field");
		let offset = field + name.len();
		assert_eq!(header[offset], 0x80, "empty definite CBOR array");
		let mut hostile = header[..offset].to_vec();
		hostile.push(0x9B);
		hostile.extend_from_slice(&u64::MAX.to_be_bytes());
		hostile.extend_from_slice(&header[offset + 1..header.len() - 32]);
		let seal = hash(&hostile);
		hostile.extend_from_slice(&seal);
		decode(&hostile, &destination, 2)
			.expect_err("tiny valid seal cannot advertise unlimited members");
	}

	#[test]
	fn sealed_headers_still_reject_wrong_owner_kind_and_counter() {
		let destination = Destination::Appservice("owner".into());
		let prefix = destination.get_prefix();
		let mut key = prefix.clone();
		key.extend_from_slice(&1_u64.to_be_bytes());
		let mut record = Record {
			owner: prefix.into(),
			generation: 2,
			kind: AttemptKind::Appservice,
			recipient: Some([1; 32]),
			members: vec![Member {
				key: key.into(),
				identity: Some(1),
				digest: hash(b"member"),
			}],
			body_len: 2,
			body_digest: hash(b"{}"),
		};
		decode(&encode(&record).expect("header"), &destination, 0)
			.expect_err("uncommitted generation refuses");
		decode(&encode(&record).expect("header"), &Destination::Appservice("other".into()), 2)
			.expect_err("different owner refuses");
		record.kind = AttemptKind::Federation;
		decode(&encode(&record).expect("sealed wrong kind"), &destination, 2)
			.expect_err("wrong transport kind refuses");
	}
}
