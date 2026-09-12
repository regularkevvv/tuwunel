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
//! Usage is one counter per owner. Bytes are reserved before an object is
//! written, returned if the write fails, and released when the object is
//! deleted. A counter missing when first needed is measured from the stored
//! objects, so media written before the counters existed are counted too.

use std::{
	str::from_utf8,
	time::{Duration, SystemTime},
};

use futures::StreamExt;
use ruma::{Mxc, OwnedMxcUri, ServerName, UserId};
use tuwunel_core::{
	Err, Result, implement,
	utils::stream::{BroadbandExt, IterStream, ReadyExt},
	warn,
};

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
	fn lock_key(self) -> String {
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

/// Reserves `len` bytes against `owner`, refusing with `M_TOO_LARGE` when
/// that would pass the owner's quota. A zero quota counts without limiting.
#[implement(super::Service)]
pub(super) async fn reserve(&self, owner: Owner<'_>, len: u64) -> Result {
	let _lock = self.quota_mutex.lock(&owner.lock_key()).await;
	let used = self.usage_locked(owner).await;
	let next = used.saturating_add(len);
	let config = &self.services.server.config;
	let limit = match owner {
		| Owner::User(_) => config.media_user_quota,
		| Owner::Server(_) => config.media_remote_server_quota,
	};

	if limit > 0 && next > limit {
		return Err!(Request(TooLarge("Media storage quota exceeded.")));
	}

	self.db.set_quota_usage(owner, next).await
}

/// Returns `len` bytes to `owner` once an object charged to it is deleted or
/// failed to store. An owner without a counter is left to be measured.
#[implement(super::Service)]
pub(super) async fn release(&self, owner: Owner<'_>, len: u64) {
	let _lock = self.quota_mutex.lock(&owner.lock_key()).await;
	let Some(used) = self.db.quota_usage(owner).await else {
		return;
	};

	if let Err(e) = self
		.db
		.set_quota_usage(owner, used.saturating_sub(len))
		.await
	{
		warn!(?owner, "Failed to release media quota: {e}");
	}
}

/// Bytes stored against `owner`, measured from the stored objects when no
/// counter exists yet.
#[implement(super::Service)]
pub async fn quota_usage(&self, owner: Owner<'_>) -> u64 {
	let _lock = self.quota_mutex.lock(&owner.lock_key()).await;

	self.usage_locked(owner).await
}

#[implement(super::Service)]
async fn usage_locked(&self, owner: Owner<'_>) -> u64 {
	match self.db.quota_usage(owner).await {
		| Some(used) => used,
		| None => self.measure(owner).await,
	}
}

/// Sums the stored size of every object `owner` is charged for.
#[implement(super::Service)]
async fn measure(&self, owner: Owner<'_>) -> u64 {
	let keys: Vec<Vec<u8>> = match owner {
		| Owner::User(user) =>
			self.db
				.get_all_user_mxcs(user)
				.await
				.into_iter()
				.stream()
				.broad_filter_map(async |mxc| {
					let parts = mxc.parts().ok()?;

					self.get_metadata(&parts)
						.await
						.map(|meta| meta.key)
				})
				.collect()
				.await,
		| Owner::Server(server) => self
			.db
			.get_all_media_keys()
			.await
			.into_iter()
			.filter(|key| from_server(key, server))
			.collect(),
	};

	keys.into_iter()
		.stream()
		.broad_filter_map(async |key| {
			self.head_meta(&key)
				.await
				.map(|object| object.size)
		})
		.ready_fold(0_u64, u64::saturating_add)
		.await
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

/// Whether the media record `key` belongs to media originating on `server`.
fn from_server(key: &[u8], server: &ServerName) -> bool {
	key.split(|&b| b == 0xFF)
		.next()
		.and_then(|mxc| from_utf8(mxc).ok())
		.map(OwnedMxcUri::from)
		.is_some_and(|mxc| mxc.server_name().is_ok_and(|name| name == server))
}
