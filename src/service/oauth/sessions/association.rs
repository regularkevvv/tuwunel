//! Pending account associations set by `query oauth associate`.
//!
//! Each records which local account a sign-in whose claims all match
//! reaches. They live in D1 (`oauthidpuserid_pendingclaims`, keyed by
//! provider and user) because the admin command can remove the account's
//! existing sessions, and a claim held only in memory would then be lost with
//! the process.

use std::{
	collections::BTreeMap,
	io::{self, Write},
};

use futures::{Stream, StreamExt, pin_mut};
use ruma::{OwnedUserId, UserId};
use serde_json::Value;
use tuwunel_core::{Error, Result, debug, implement, trace};
use tuwunel_database::{Interfix, Json, deserialize_from_slice, serialize_to_vec};

use super::{Sessions, UserInfo};

pub type Claims = BTreeMap<String, String>;

/// Records the claims a sign-in must present to reach `user_id`, returning
/// the claims it replaces.
#[implement(Sessions)]
pub async fn set_user_association_pending(
	&self,
	idp_id: &str,
	user_id: &UserId,
	claims: Claims,
) -> Result<Option<Claims>> {
	let _admission = self.pending_locks.lock(idp_id).await;
	let inventory = self.pending_associations(idp_id).await?;
	let replaced = inventory.prepare(idp_id, user_id, &claims)?;
	let key = (idp_id, user_id);
	self.db
		.oauthidpuserid_pendingclaims
		.put(key, Json(&claims))
		.await?;
	Ok(replaced.map(|record| record.claims.clone()))
}

/// Install a pending association and replace all committed sessions atomically.
/// Both inventories and every association index are checked before the commit.
#[implement(Sessions)]
pub async fn associate_user(
	&self,
	idp_id: &str,
	user_id: &UserId,
	claims: Claims,
	force: bool,
) -> Result<(Option<Claims>, usize)> {
	// Identity locks precede provider-pending admission, matching SSO callbacks.
	let sessions = self.lock_user_sessions(user_id).await?;
	let committed = sessions.records().len();
	if committed > 0 && !force {
		return Err(tuwunel_core::err!(
			"{user_id} already has {committed} committed OAuth session(s); re-run with --force \
			 to replace them."
		));
	}
	let _admission = self.pending_locks.lock(idp_id).await;
	let inventory = self.pending_associations(idp_id).await?;
	let replaced = inventory
		.prepare(idp_id, user_id, &claims)?
		.map(|record| record.claims.clone());
	let mut txn = self.db.database.txn();
	self.append_deleted_sessions(&sessions, sessions.records(), &mut txn)
		.await?;
	txn.put(&self.db.oauthidpuserid_pendingclaims, (idp_id, user_id), Json(&claims));
	txn.check_bridge_admission()?;
	txn.execute().await?;
	Ok((replaced, committed))
}

/// The account a sign-in with `userinfo` from `idp_id` was associated with,
/// if every recorded claim matches.
#[implement(Sessions)]
pub async fn find_user_association_pending(
	&self,
	idp_id: &str,
	userinfo: &UserInfo,
) -> Result<Option<OwnedUserId>> {
	let _admission = self.pending_locks.lock(idp_id).await;
	let claiming = serde_json::to_value(userinfo)?;
	let claiming = claiming
		.as_object()
		.ok_or_else(|| Error::bad_database("Invalid upstream user information"))?;
	debug!(?idp_id, claims = claiming.len(), "finding pending association");

	// Complete the checked inventory before returning even an early match. An
	// unreadable later row must never turn into permission to register anew.
	let inventory = self.pending_associations(idp_id).await?;
	for record in inventory.records {
		trace!(user_id = ?record.user_id, claims = record.claims.len(), "checking against pending association");
		if !record.claims.is_empty()
			&& record
				.claims
				.iter()
				.all(|(claim, value)| claiming.get(claim).and_then(Value::as_str) == Some(value))
		{
			return Ok(Some(record.user_id));
		}
	}
	Ok(None)
}

/// Maximum complete pending-claim inventory per provider. Admission and login
/// use the same budgets; overflow refuses before writing or matching a user.
const MAX_PENDING_ASSOCIATIONS: usize = 128;
const MAX_PENDING_ASSOCIATION_BYTES: usize = 128 * 1024;

#[derive(Debug)]
struct PendingAssociation {
	user_id: OwnedUserId,
	claims: Claims,
	encoded_bytes: usize,
}

#[derive(Debug, Default)]
struct PendingAssociations {
	records: Vec<PendingAssociation>,
	encoded_bytes: usize,
}

fn pending_limit() -> Error {
	Error::Request(
		ruma::api::error::ErrorKind::LimitExceeded(ruma::api::error::LimitExceededErrorData {
			retry_after: None,
		}),
		"Pending OAuth association inventory limit reached".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}

fn check_pending_budget(rows: usize, bytes: usize) -> Result {
	if rows > MAX_PENDING_ASSOCIATIONS || bytes > MAX_PENDING_ASSOCIATION_BYTES {
		return Err(pending_limit());
	}
	Ok(())
}

// Count without allocating an encoded copy; escaped JSON can be much larger
// than the input strings, and admission must refuse before retaining it.
fn claim_bytes(claims: &Claims) -> Result<usize> {
	struct Count {
		bytes: usize,
		exceeded: bool,
	}
	impl Write for Count {
		fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
			let length = self.bytes.saturating_add(bytes.len());
			if length > MAX_PENDING_ASSOCIATION_BYTES {
				self.exceeded = true;
				return Err(io::Error::from(io::ErrorKind::FileTooLarge));
			}
			self.bytes = length;
			Ok(bytes.len())
		}

		fn flush(&mut self) -> io::Result<()> { Ok(()) }
	}
	let mut count = Count { bytes: 0, exceeded: false };
	match serde_json::to_writer(&mut count, claims) {
		| Ok(()) => Ok(count.bytes),
		| Err(_) if count.exceeded => Err(pending_limit()),
		| Err(error) => Err(error.into()),
	}
}

impl PendingAssociations {
	fn prepare(
		&self,
		idp_id: &str,
		user_id: &UserId,
		claims: &Claims,
	) -> Result<Option<&PendingAssociation>> {
		if claims.is_empty() {
			return Err(Error::bad_database("Pending association claims must not be empty"));
		}
		let replaced = self
			.records
			.iter()
			.find(|record| record.user_id == user_id);
		let encoded_bytes = serialize_to_vec((idp_id, user_id))?
			.len()
			.saturating_add(claim_bytes(claims)?);
		let rows = self
			.records
			.len()
			.saturating_add(usize::from(replaced.is_none()));
		let bytes = self
			.encoded_bytes
			.saturating_sub(replaced.map_or(0, |record| record.encoded_bytes))
			.saturating_add(encoded_bytes);
		check_pending_budget(rows, bytes)?;
		Ok(replaced)
	}

	async fn read<S, K, V>(idp_id: &str, rows: S) -> Result<Self>
	where
		S: Stream<Item = Result<(K, V)>>,
		K: AsRef<[u8]>,
		V: AsRef<[u8]>,
	{
		pin_mut!(rows);
		let mut inventory = Self::default();
		while let Some(row) = rows.next().await {
			let (key, value) = row?;
			let (key, value) = (key.as_ref(), value.as_ref());
			let encoded_bytes = key.len().saturating_add(value.len());
			let bytes = inventory
				.encoded_bytes
				.saturating_add(encoded_bytes);
			check_pending_budget(inventory.records.len().saturating_add(1), bytes)?;
			// Check raw bytes before decoding or retaining keys and claim strings.
			let (provider, user_id): (String, OwnedUserId) = deserialize_from_slice(key)?;
			if provider != idp_id {
				return Err(Error::bad_database("Pending association provider binding differs"));
			}
			let claims = serde_json::from_slice(value)?;
			inventory
				.records
				.push(PendingAssociation { user_id, claims, encoded_bytes });
			inventory.encoded_bytes = bytes;
		}
		Ok(inventory)
	}
}

#[implement(Sessions)]
async fn pending_associations(&self, idp_id: &str) -> Result<PendingAssociations> {
	let prefix = (idp_id, Interfix);
	let rows = self
		.db
		.oauthidpuserid_pendingclaims
		.stream_prefix_capped::<&[u8], &[u8], _>(
			&prefix,
			MAX_PENDING_ASSOCIATIONS.saturating_add(1),
		);
	PendingAssociations::read(idp_id, rows).await
}

#[implement(Sessions)]
pub async fn remove_provider_associations_pending(&self, idp_id: &str) -> Result {
	let _admission = self.pending_locks.lock(idp_id).await;
	self.db
		.oauthidpuserid_pendingclaims
		.del_prefix(&(idp_id, Interfix))
		.await
}

#[implement(Sessions)]
pub async fn remove_user_association_pending(
	&self,
	user_id: &UserId,
	idp_id: Option<&str>,
) -> Result {
	let Some(idp_id) = idp_id else {
		return Ok(());
	};

	let _admission = self.pending_locks.lock(idp_id).await;
	self.db
		.oauthidpuserid_pendingclaims
		.del((idp_id, user_id))
		.await
}

#[implement(Sessions)]
pub async fn is_user_association_pending(&self, user_id: &UserId) -> bool {
	let pending = self.db.oauthidpuserid_pendingclaims.raw_keys();

	pin_mut!(pending);
	while let Some(key) = pending.next().await {
		if key.is_ok_and(|key| key.rsplit(|&b| b == 0xFF).next() == Some(user_id.as_bytes())) {
			return true;
		}
	}

	false
}

#[cfg(test)]
mod tests {
	use futures::stream;
	use ruma::OwnedUserId;
	use tuwunel_core::{Error, Result};
	use tuwunel_database::serialize_to_vec;

	use super::{
		Claims, MAX_PENDING_ASSOCIATION_BYTES, MAX_PENDING_ASSOCIATIONS, PendingAssociations,
		claim_bytes,
	};

	fn row(provider: &str, n: usize) -> Result<(Vec<u8>, Vec<u8>)> {
		let user: OwnedUserId = format!("@pending{n}:example.test").try_into()?;
		Ok((serialize_to_vec((provider, &user))?, br#"{"sub":"matching-subject"}"#.to_vec()))
	}

	#[tokio::test]
	async fn complete_inventory_charges_every_record_and_preserves_full_capacity_replacement()
	-> Result {
		let rows = (0..MAX_PENDING_ASSOCIATIONS).map(|n| row("provider", n));
		let inventory = PendingAssociations::read("provider", stream::iter(rows)).await?;
		assert_eq!(inventory.records.len(), MAX_PENDING_ASSOCIATIONS);
		let claims = Claims::from([("sub".into(), "replacement".into())]);
		assert!(
			inventory
				.prepare("provider", &inventory.records[0].user_id, &claims)?
				.is_some()
		);
		let extra: OwnedUserId = "@new:example.test".try_into()?;
		inventory
			.prepare("provider", &extra, &claims)
			.expect_err("a new row exceeds a full inventory");
		let rows = (0..=MAX_PENDING_ASSOCIATIONS).map(|n| row("provider", n));
		PendingAssociations::read("provider", stream::iter(rows))
			.await
			.expect_err("the overflow probe must refuse");
		Ok(())
	}

	#[tokio::test]
	async fn later_backend_or_decoding_failure_cannot_hide_behind_an_early_matching_claim()
	-> Result {
		let rows = vec![row("provider", 0), Err(Error::bad_database("fixture read refusal"))];
		PendingAssociations::read("provider", stream::iter(rows))
			.await
			.expect_err("a later read failure must refuse the complete lookup");
		let (key, _) = row("provider", 1)?;
		let rows = vec![row("provider", 0), Ok((key, b"invalid-json".to_vec()))];
		PendingAssociations::read("provider", stream::iter(rows))
			.await
			.expect_err("malformed later claims must refuse");
		PendingAssociations::read("provider", stream::iter([row("different", 0)]))
			.await
			.expect_err("foreign provider binding must refuse");
		PendingAssociations::read(
			"provider",
			stream::iter([Ok((b"bad-key".to_vec(), b"{}".to_vec()))]),
		)
		.await
		.expect_err("malformed keys must refuse");
		Ok(())
	}

	#[tokio::test]
	async fn raw_byte_overflow_refuses_before_json_decode_and_replacement_admission() -> Result {
		let (key, _) = row("provider", 0)?;
		let value = vec![0xFE; MAX_PENDING_ASSOCIATION_BYTES];
		let error = PendingAssociations::read("provider", stream::iter([Ok((key, value))]))
			.await
			.expect_err("raw bytes exceed the complete budget");
		assert!(matches!(
			error,
			Error::Request(ruma::api::error::ErrorKind::LimitExceeded(_), _, _)
		));
		let inventory =
			PendingAssociations::read("provider", stream::iter([row("provider", 0)])).await?;
		let claims = Claims::from([("sub".into(), "x".repeat(MAX_PENDING_ASSOCIATION_BYTES))]);
		inventory
			.prepare("provider", &inventory.records[0].user_id, &claims)
			.expect_err("oversized replacement must refuse before mutation");
		inventory
			.prepare("provider", &inventory.records[0].user_id, &Claims::new())
			.expect_err("empty claims must refuse admission");
		Ok(())
	}

	#[tokio::test]
	async fn only_a_complete_empty_inventory_is_a_successful_absence() -> Result {
		let empty = stream::empty::<Result<(Vec<u8>, Vec<u8>)>>();
		assert!(
			PendingAssociations::read("provider", empty)
				.await?
				.records
				.is_empty()
		);
		let unreadable = stream::iter([Err::<(Vec<u8>, Vec<u8>), _>(Error::bad_database(
			"fixture read refusal",
		))]);
		PendingAssociations::read("provider", unreadable)
			.await
			.expect_err("an unreadable inventory cannot mean absence");
		Ok(())
	}

	#[test]
	fn admission_counts_json_escaping_without_building_the_encoded_copy() -> Result {
		let claims = Claims::from([("sub".into(), "normal".into())]);
		assert_eq!(claim_bytes(&claims)?, serde_json::to_vec(&claims)?.len());
		let claims =
			Claims::from([("sub".into(), "\u{0000}".repeat(MAX_PENDING_ASSOCIATION_BYTES / 6))]);
		claim_bytes(&claims).expect_err("JSON escaping exceeds the raw budget");
		Ok(())
	}
}
