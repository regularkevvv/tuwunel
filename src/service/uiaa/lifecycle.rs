//! Durable UIAA age and identity index. The original UiaaInfo row remains
//! unchanged for the preceding Container revision; new readers require both
//! rows. Unaged legacy challenges fail closed and must be started again.
use std::{
	io::{self, Write},
	time::{SystemTime, UNIX_EPOCH},
};

use futures::{StreamExt, TryStreamExt};
use ruma::{DeviceId, OwnedDeviceId, OwnedUserId, UserId, api::client::uiaa::UiaaInfo};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Err, Result, err, implement};
use tuwunel_database::{
	Txn,
	keyval::{KeyBuf, serialize_key},
};

use super::Service;

pub(super) const MAX_SESSIONS: usize = 1024;
pub(super) const MAX_RECORD_BYTES: usize = 64 * 1024;
const LIFETIME_SECONDS: u64 = 15 * 60;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Metadata {
	version: u8,
	pub(super) user: OwnedUserId,
	pub(super) device: OwnedDeviceId,
	created: u64,
	expires: u64,
}

impl Metadata {
	fn new(user: &UserId, device: &DeviceId, now: u64) -> Result<Self> {
		Ok(Self {
			version: 1,
			user: user.to_owned(),
			device: device.to_owned(),
			created: now,
			expires: now
				.checked_add(LIFETIME_SECONDS)
				.ok_or_else(|| err!("UIAA clock overflow"))?,
		})
	}

	pub(super) fn active(&self, now: u64) -> bool {
		self.version == 1
			&& self.created <= now
			&& now < self.expires
			&& self.created.checked_add(LIFETIME_SECONDS) == Some(self.expires)
	}

	fn owner(&self, user: &UserId, device: &DeviceId) -> bool {
		self.user == user && self.device == device
	}
}

pub(super) fn now() -> Result<u64> {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_secs())
		.map_err(|_| err!("UIAA clock is before the Unix epoch"))
}

pub(super) fn valid_session(session: &str) -> bool {
	!session.is_empty()
		&& session.len() <= 64
		&& session
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[implement(Service)]
pub(super) async fn metadata(&self, session: &str) -> Result<Option<Metadata>> {
	if !valid_session(session) {
		return Err!(Request(Forbidden("Invalid UIAA session identifier.")));
	}
	match self.db.uiaasessionid_metadata.get(session).await {
		| Ok(value) if value.len() <= MAX_RECORD_BYTES => serde_json::from_slice(value.as_ref())
			.map(Some)
			.map_err(|_| err!("Invalid UIAA session metadata")),
		| Ok(_) => Err(err!("Oversized UIAA session metadata")),
		| Err(error) if error.is_not_found() => Ok(None),
		| Err(error) => Err(error),
	}
}

/// All callers hold the per-session transition lock. Admission serializes
/// only new rows; its bounded count is derived from durable state on every
/// admission, so process restart cannot reset the limit. Completed/expired
/// sessions are removed by atomic two-map batches, never by independent TTLs.
#[implement(Service)]
pub(super) async fn save_progress(
	&self,
	user: &UserId,
	device: &DeviceId,
	session: &str,
	info: &UiaaInfo,
	new: bool,
) -> Result {
	if !new {
		return self
			.prepare_progress(user, device, session, info)
			.await?
			.execute()
			.await;
	}
	let (key, body) = progress_record(user, device, session, info)?;
	let mut txn = self.db.database.txn();
	let _admission = self.admission.lock().await;
	if self.metadata(session).await?.is_some() {
		return Err!(Request(InvalidParam("UIAA session already exists.")));
	}
	// Drop the bounded scan before committing: remote scans must not be
	// drained by our write barrier while capacity is being measured.
	let count = self
		.db
		.uiaasessionid_metadata
		.raw_keys()
		.map_ok(|_| ())
		.take(MAX_SESSIONS)
		.try_collect::<Vec<()>>()
		.await?
		.len();
	if count >= MAX_SESSIONS {
		return Err(tuwunel_core::Error::Request(
			ruma::api::error::ErrorKind::LimitExceeded(
				ruma::api::error::LimitExceededErrorData {
					retry_after: Some(ruma::api::error::RetryAfter::Delay(
						super::sweep::INTERVAL,
					)),
				},
			),
			"Too many pending UIAA sessions.".into(),
			http::StatusCode::TOO_MANY_REQUESTS,
		));
	}
	let metadata = bounded_json(&Metadata::new(user, device, now()?)?)?;
	txn.insert_raw(&self.db.uiaasessionid_metadata, session, &metadata);
	txn.insert_raw(&self.db.userdevicesessionid_uiaainfo, &key, &body);
	txn.execute().await
}

/// Prepare existing-session progress without committing it. The caller holds
/// the session transition lock through preparation and transaction execution.
/// This permits a registration-token use to share the same atomic batch.
#[implement(Service)]
pub(super) async fn prepare_progress(
	&self,
	user: &UserId,
	device: &DeviceId,
	session: &str,
	info: &UiaaInfo,
) -> Result<Txn> {
	let (key, body) = progress_record(user, device, session, info)?;
	let metadata = self
		.metadata(session)
		.await?
		.filter(|metadata| metadata.owner(user, device))
		.ok_or_else(|| err!(Request(Forbidden("UIAA session does not exist."))))?;
	if !metadata.active(now()?) {
		return Err!(Request(Forbidden("UIAA session has expired.")));
	}
	let mut txn = self.db.database.txn();
	// Do not rewrite or extend the absolute creation/expiry metadata.
	txn.insert_raw(&self.db.userdevicesessionid_uiaainfo, &key, &body);
	Ok(txn)
}

#[implement(Service)]
pub(super) async fn finish_session(
	&self,
	user: &UserId,
	device: &DeviceId,
	session: &str,
	info: &UiaaInfo,
	retain: bool,
) -> Result {
	if retain {
		return self
			.save_progress(user, device, session, info, false)
			.await;
	}
	// Stage validation may await an external system. Recheck the durable
	// deadline immediately before authorizing the protected operation.
	self.get_uiaa_session(user, device, session)
		.await?;
	self.update_uiaa_session(user, device, session, None)
		.await
}

#[implement(Service)]
pub(super) async fn update_uiaa_session(
	&self,
	user: &UserId,
	device: &DeviceId,
	session: &str,
	info: Option<&UiaaInfo>,
) -> Result {
	if let Some(info) = info {
		return self
			.save_progress(user, device, session, info, false)
			.await;
	}
	if self
		.metadata(session)
		.await?
		.is_some_and(|metadata| !metadata.owner(user, device))
	{
		return Err!(Request(Forbidden("UIAA session owner does not match.")));
	}
	let mut txn = self.db.database.txn();
	txn.del(&self.db.userdevicesessionid_uiaainfo, (user, device, session));
	txn.del_raw(&self.db.uiaasessionid_metadata, session);
	txn.execute().await?;
	self.remove_uiaa_request(user, device, session);
	Ok(())
}

#[implement(Service)]
pub(super) async fn get_uiaa_session(
	&self,
	user: &UserId,
	device: &DeviceId,
	session: &str,
) -> Result<UiaaInfo> {
	let metadata = self
		.metadata(session)
		.await?
		.filter(|metadata| metadata.owner(user, device))
		.ok_or_else(|| err!(Request(Forbidden("UIAA session does not exist."))))?;
	self.read_info(session, &metadata).await
}

#[implement(Service)]
pub(super) async fn read_info(&self, session: &str, metadata: &Metadata) -> Result<UiaaInfo> {
	if !metadata.active(now()?) {
		return Err!(Request(Forbidden("UIAA session has expired.")));
	}
	let key = serialize_key((&metadata.user, &metadata.device, session))?;
	// Use the raw-key API: the generic typed-query trace includes its key.
	let body = match self
		.db
		.userdevicesessionid_uiaainfo
		.get(&key)
		.await
	{
		| Ok(body) => body,
		| Err(error) if error.is_not_found() =>
			return Err!(Request(Forbidden("UIAA session does not exist."))),
		| Err(error) => return Err(error),
	};
	if body.len() > MAX_RECORD_BYTES {
		return Err!(Request(Forbidden("Invalid UIAA session data.")));
	}
	let info: UiaaInfo = serde_json::from_slice(body.as_ref())
		.map_err(|_| err!(Request(Forbidden("Invalid UIAA session data."))))?;
	if info.session.as_deref() != Some(session) {
		return Err!(Request(Forbidden("Invalid UIAA session binding.")));
	}
	if !metadata.active(now()?) {
		return Err!(Request(Forbidden("UIAA session has expired.")));
	}
	Ok(info)
}

#[implement(Service)]
pub async fn get_uiaa_session_by_session_id(
	&self,
	session: &str,
) -> Option<(OwnedUserId, OwnedDeviceId, UiaaInfo)> {
	let metadata = self.metadata(session).await.ok()??;
	let info = self.read_info(session, &metadata).await.ok()?;
	Some((metadata.user, metadata.device, info))
}

fn progress_record(
	user: &UserId,
	device: &DeviceId,
	session: &str,
	info: &UiaaInfo,
) -> Result<(KeyBuf, Vec<u8>)> {
	if info.session.as_deref() != Some(session) || !valid_session(session) {
		return Err!(Request(Forbidden("Invalid UIAA session binding.")));
	}
	let body = bounded_json(info)?;
	let key = serialize_key((user, device, session))?;
	if key.len() > tuwunel_bridge::MAX_KEY_BYTES {
		return Err!(Request(TooLarge("UIAA session identity exceeds the storage limit.")));
	}
	Ok((key, body))
}

fn bounded_json(value: &impl Serialize) -> Result<Vec<u8>> {
	let mut output = BoundedJson(Vec::new());
	serde_json::to_writer(&mut output, value)
		.map_err(|_| err!(Request(TooLarge("UIAA session data exceeds the storage limit."))))?;
	Ok(output.0)
}

struct BoundedJson(Vec<u8>);

impl Write for BoundedJson {
	fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
		if self
			.0
			.len()
			.checked_add(bytes.len())
			.is_none_or(|size| size > MAX_RECORD_BYTES)
		{
			return Err(io::Error::other("UIAA record limit"));
		}
		self.0.extend_from_slice(bytes);
		Ok(bytes.len())
	}

	fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

#[cfg(test)]
mod tests {
	use ruma::UserId;

	use super::{LIFETIME_SECONDS, MAX_RECORD_BYTES, Metadata, bounded_json, valid_session};

	#[test]
	fn age_is_absolute_fail_closed_and_checked() {
		let user = UserId::parse("@alice:example.test").expect("user");
		let mut metadata = Metadata::new(&user, "DEVICE".into(), 1000).expect("metadata");
		assert!(!metadata.active(999));
		assert!(metadata.active(1000));
		assert!(
			metadata.active(
				1000_u64
					.checked_add(LIFETIME_SECONDS)
					.expect("time")
					.checked_sub(1)
					.expect("time")
			)
		);
		assert!(
			!metadata.active(
				1000_u64
					.checked_add(LIFETIME_SECONDS)
					.expect("time")
			)
		);
		metadata.expires = metadata.expires.checked_add(1).expect("time");
		assert!(!metadata.active(1000));
		assert!(Metadata::new(&user, "DEVICE".into(), u64::MAX).is_err());
	}

	#[test]
	fn records_and_identifiers_are_bounded() {
		assert!(valid_session("safe-session_123"));
		assert!(!valid_session(""));
		assert!(!valid_session(&"x".repeat(65)));
		assert!(!valid_session("bad/session"));
		bounded_json(&"x".repeat(MAX_RECORD_BYTES)).expect_err("oversized JSON must be refused");
		bounded_json(&"small").expect("small JSON must fit");
	}
}
