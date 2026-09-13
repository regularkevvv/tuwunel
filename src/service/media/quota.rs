//! Byte quotas for stored media, and retention for media cached from remote
//! servers (phase 2 deliverable 6).
//!
//! A local user's original uploads count against `media_user_quota`. Every
//! object stored for a remote server's media counts against
//! `media_remote_server_quota`, thumbnails included, because the remote
//! chooses the bytes of the ones it sends. Thumbnails generated here from a
//! local upload are not charged to the uploader: they derive from media
//! already counted and stay within `media_thumbnail_max_pixels`.
//!
//! Usage is one counter per owner, changed only in the same D1 transaction as
//! the records it counts, under the owner's quota lock. An object's record
//! carries its byte length: it is charged when the record is written, and
//! released from that length when the records are removed. So a kill at any
//! point leaves counter and records agreeing, and a retried delete whose
//! objects are already gone still releases exactly once. Owners charged
//! before counters existed get theirs from a one-time backfill, run as a
//! migration before the server serves (`Service::backfill_usage`). So a
//! missing counter means nothing is charged, and no request ever measures.

use std::{
	collections::HashMap,
	str::from_utf8,
	time::{Duration, SystemTime},
};

use ruma::{Mxc, OwnedMxcUri, OwnedServerName, OwnedUserId, ServerName, UserId};
use tuwunel_core::{Err, Result, implement};
use tuwunel_database::successor;

use super::Dim;

/// Whose byte quota a stored object counts against.
#[derive(Clone, Copy, Debug)]
pub enum Owner<'a> {
	/// A local user's original uploads.
	User(&'a UserId),
	/// Everything stored for media originating on a remote server.
	Server(&'a ServerName),
}

/// How often remote media past `media_remote_retention` is removed.
pub(super) const RETENTION_INTERVAL: Duration = Duration::from_hours(6);

impl Owner<'_> {
	pub(super) fn lock_key(self) -> String {
		match self {
			| Self::User(user) => format!("user:{user}"),
			| Self::Server(server) => format!("server:{server}"),
		}
	}
}

/// The quota an object stored for `mxc` counts against, if any; `original`
/// is whether the object is the media itself rather than a thumbnail.
#[implement(super::Service)]
pub(super) fn quota_owner<'a>(
	&self,
	mxc: &Mxc<'a>,
	uploader: Option<&'a UserId>,
	original: bool,
) -> Option<Owner<'a>> {
	if !self
		.services
		.globals
		.server_is_ours(mxc.server_name)
	{
		return Some(Owner::Server(mxc.server_name));
	}

	uploader.filter(|_| original).map(Owner::User)
}

/// `owner`'s usage total with `len` more bytes, refusing with `M_TOO_LARGE`
/// when that passes the owner's quota; a zero quota counts without limiting.
/// The caller holds the owner's quota lock and writes the total with the
/// object's record.
///
/// A user is charged once per media ID. When `mxc` already has its original
/// recorded, the upload is refused with `M_CANNOT_OVERWRITE_MEDIA` rather
/// than charged again. A retried upload whose first attempt stored the
/// content, or one racing another to the same ID, is caught here, under the
/// lock the first attempt charged under.
#[implement(super::Service)]
pub(super) async fn admit(&self, owner: Owner<'_>, mxc: &Mxc<'_>, len: u64) -> Result<u64> {
	if matches!(owner, Owner::User(_))
		&& self
			.db
			.file_metadata_exists(mxc, &Dim::default())
			.await
	{
		return Err!(Request(CannotOverwriteMedia("Media ID already has content")));
	}

	let next = self.usage_locked(owner).await.saturating_add(len);
	let config = &self.services.server.config;
	let limit = match owner {
		| Owner::User(_) => config.media_user_quota,
		| Owner::Server(_) => config.media_remote_server_quota,
	};

	if limit > 0 && next > limit {
		return Err!(Request(TooLarge("Media storage quota exceeded.")));
	}

	Ok(next)
}

/// `owner`'s usage total with `len` bytes released. The caller holds the
/// owner's quota lock and writes the total as it removes the records.
#[implement(super::Service)]
pub(super) async fn released(&self, owner: Owner<'_>, len: u64) -> u64 {
	self.usage_locked(owner).await.saturating_sub(len)
}

/// Bytes stored against `owner`; zero when it has no counter.
#[implement(super::Service)]
pub async fn quota_usage(&self, owner: Owner<'_>) -> u64 {
	let _lock = self.quota_mutex.lock(&owner.lock_key()).await;

	self.usage_locked(owner).await
}

/// `owner`'s usage counter. None means nothing is charged to `owner`: every
/// charged record is written with its owner's counter, and the backfill
/// wrote one for every owner charged before counters existed.
#[implement(super::Service)]
async fn usage_locked(&self, owner: Owner<'_>) -> u64 {
	self.db.quota_usage(owner).await.unwrap_or(0)
}

/// Media records one batch of `Service::backfill_usage` reads.
const BACKFILL_BATCH: usize = 256;

/// Writes a usage counter for every owner charged for stored media that has
/// none: the lengths of its charged records, summed, or their stored sizes
/// for records older than their length. Returns how many counters it wrote.
///
/// Run once, as a migration, before the server serves. Local originals are
/// found through the uploader index and remote media through the records;
/// both are read in batches of [`BACKFILL_BATCH`] from a cursor, so no read
/// is ever the size of a map. An owner that already has a counter keeps it.
#[implement(super::Service)]
pub async fn backfill_usage(&self) -> Result<usize> {
	let mut users: HashMap<OwnedUserId, u64> = HashMap::new();
	let mut after: Option<Vec<u8>> = None;
	loop {
		let (uploads, next) = self
			.db
			.uploads_after(after.as_deref(), BACKFILL_BATCH)
			.await?;

		for (mxc, user) in uploads {
			if self
				.db
				.quota_usage(Owner::User(&user))
				.await
				.is_some()
			{
				continue;
			}

			let Ok(parts) = mxc.parts() else {
				continue;
			};

			let Some(meta) = self.get_metadata(&parts).await else {
				continue;
			};

			let len = self.object_len(&meta.key).await.unwrap_or(0);
			let total = users.entry(user).or_default();
			*total = total.saturating_add(len);
		}

		match next {
			| Some(next) => after = Some(next),
			| None => break,
		}
	}

	let local = format!("mxc://{}/", self.services.globals.server_name());
	let mut servers: HashMap<OwnedServerName, u64> = HashMap::new();
	let mut from: Option<Vec<u8>> = None;
	loop {
		let keys = self
			.db
			.media_keys_from(from.as_deref(), BACKFILL_BATCH)
			.await?;

		let ended = keys.len() < BACKFILL_BATCH;
		from = keys.last().map(Vec::as_slice).map(successor);
		if from
			.as_deref()
			.is_some_and(|from| from.starts_with(local.as_bytes()))
		{
			from = Some(super::past_prefix(local.as_bytes()));
		}

		for key in keys {
			let Some(server) = record_server(&key) else {
				continue;
			};

			if self.services.globals.server_is_ours(&server)
				|| self
					.db
					.quota_usage(Owner::Server(&server))
					.await
					.is_some()
			{
				continue;
			}

			let len = self.object_len(&key).await.unwrap_or(0);
			let total = servers.entry(server).or_default();
			*total = total.saturating_add(len);
		}

		if ended {
			break;
		}
	}

	let mut written: usize = 0;
	for (user, total) in &users {
		let wrote = self
			.write_usage_if_missing(Owner::User(user), *total)
			.await?;

		written = written.saturating_add(usize::from(wrote));
	}

	for (server, total) in &servers {
		let wrote = self
			.write_usage_if_missing(Owner::Server(server), *total)
			.await?;

		written = written.saturating_add(usize::from(wrote));
	}

	Ok(written)
}

/// Writes `owner`'s counter under its quota lock, unless one exists.
#[implement(super::Service)]
async fn write_usage_if_missing(&self, owner: Owner<'_>, total: u64) -> Result<bool> {
	let _lock = self.quota_mutex.lock(&owner.lock_key()).await;
	if self.db.quota_usage(owner).await.is_some() {
		return Ok(false);
	}

	self.db.put_usage(owner, total).await?;

	Ok(true)
}

/// The length an object's record carries, or its stored size for a record
/// written before records carried one.
#[implement(super::Service)]
pub(super) async fn object_len(&self, key: &[u8]) -> Option<u64> {
	match self.db.file_len(key).await {
		| Some(len) => Some(len),
		| None => self
			.head_meta(key)
			.await
			.map(|object| object.size),
	}
}

/// Removes media cached from remote servers for longer than
/// `media_remote_retention`, returning how many were removed. A zero
/// retention keeps remote media until an admin deletes it.
#[implement(super::Service)]
pub async fn expire_remote_media(&self) -> Result<usize> {
	let retention = self.services.server.config.media_remote_retention;
	if retention == 0 {
		return Ok(0);
	}

	let cutoff = SystemTime::now()
		.checked_sub(Duration::from_secs(retention))
		.unwrap_or(SystemTime::UNIX_EPOCH);

	self.delete_range(cutoff, true, false, false)
		.await
}

/// The server whose media the record `key` stores.
fn record_server(key: &[u8]) -> Option<OwnedServerName> {
	key.split(|&b| b == 0xFF)
		.next()
		.and_then(|mxc| from_utf8(mxc).ok())
		.map(OwnedMxcUri::from)
		.and_then(|mxc| mxc.server_name().ok().map(ToOwned::to_owned))
}
