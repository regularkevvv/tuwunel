use std::sync::Arc;

use futures::{Stream, StreamExt, pin_mut};
use ruma::{Mxc, OwnedMxcUri, OwnedUserId, UserId, http_headers::ContentDisposition};
use serde::Deserialize;
#[cfg(feature = "url_preview")]
use serde::Serialize;
use tuwunel_core::{
	Err, Result, at, debug, debug_info, err,
	utils::{ReadyExt, str_from_bytes, stream::TryIgnore, string_from_bytes},
};
use tuwunel_database::{Cbor, Database, Deserialized, Ignore, Interfix, Map, Txn, serialize_key};

use super::{Media, preview::CachedPreview, quota::Owner, thumbnail::Dim};

pub(crate) struct Data {
	db: Arc<Database>,
	mediaid_file: Arc<Map>,
	mediaid_lazy: Arc<Map>,
	mediaid_lazycontent: Arc<Map>,
	mediaid_pending: Arc<Map>,
	mediaid_user: Arc<Map>,
	servername_mediabytes: Arc<Map>,
	url_preview: Arc<Map>,
	userid_mediabytes: Arc<Map>,
}

#[derive(Debug)]
pub struct Metadata {
	pub content_disposition: Option<ContentDisposition>,
	pub content_type: Option<String>,
	pub(super) key: Vec<u8>,
}

/// Borrowed staging-cache value: written zero-copy from the measured bytes.
#[cfg(feature = "url_preview")]
#[derive(Serialize)]
struct LazyContentRef<'a> {
	content_type: Option<&'a str>,
	content_disposition: Option<&'a str>,
	#[serde(with = "serde_bytes")]
	content: &'a [u8],
}

/// Owned staging-cache value read back at promotion. `ContentDisposition` is
/// Serialize-only, so the disposition rides as its header string.
#[derive(Deserialize)]
struct LazyContent {
	content_type: Option<String>,
	content_disposition: Option<String>,
	#[serde(with = "serde_bytes")]
	content: Vec<u8>,
}

impl From<LazyContent> for Media {
	fn from(lazy: LazyContent) -> Self {
		let content_disposition = lazy
			.content_disposition
			.and_then(|disposition| disposition.parse().ok());

		Self {
			content: lazy.content,
			content_type: lazy.content_type,
			content_disposition,
		}
	}
}

impl Data {
	pub(super) fn new(db: &Arc<Database>) -> Self {
		Self {
			db: db.clone(),
			mediaid_file: db["mediaid_file"].clone(),
			mediaid_lazy: db["mediaid_lazy"].clone(),
			mediaid_lazycontent: db["mediaid_lazycontent"].clone(),
			mediaid_pending: db["mediaid_pending"].clone(),
			mediaid_user: db["mediaid_user"].clone(),
			servername_mediabytes: db["servername_mediabytes"].clone(),
			url_preview: db["url_preview"].clone(),
			userid_mediabytes: db["userid_mediabytes"].clone(),
		}
	}

	/// Records one stored object. The record carries its byte length, and a
	/// charged object's owner gets its new usage total in the same transaction,
	/// so the counter never disagrees with the records it counts.
	#[expect(clippy::too_many_arguments)]
	pub(super) async fn create_file_metadata(
		&self,
		mxc: &Mxc<'_>,
		user: Option<&UserId>,
		dim: &Dim,
		content_disposition: Option<&ContentDisposition>,
		content_type: Option<&str>,
		len: u64,
		charge: Option<(Owner<'_>, u64)>,
	) -> Result<Vec<u8>> {
		let dim: &[u32] = &[dim.width, dim.height];
		let key = (mxc, dim, content_disposition, content_type);
		let key = serialize_key(key)?;
		let mut txn = self.db.txn();

		txn.insert_raw(&self.mediaid_file, &key, len.to_be_bytes());
		if let Some(user) = user {
			let key = (mxc, user);

			txn.put_raw(&self.mediaid_user, key, user);
		}
		if let Some((owner, total)) = charge {
			self.set_usage(&mut txn, owner, total);
		}

		txn.execute().await?;

		Ok(key.to_vec())
	}

	/// Records a pending upload: its row, keyed by MXC, and its row in the
	/// uploader's index ([`pending_index_key`]), in one transaction.
	pub(super) async fn insert_pending_mxc(
		&self,
		mxc: &Mxc<'_>,
		user: &UserId,
		unused_expires_at: u64,
	) -> Result {
		let value = (unused_expires_at, user);
		debug!(?mxc, ?user, ?unused_expires_at, "Inserting pending");

		let mxc = mxc.to_string();
		let mut txn = self.db.txn();
		txn.raw_put(&self.mediaid_pending, &mxc, value);
		txn.insert_raw(
			&self.mediaid_pending,
			pending_index_key(user, unused_expires_at, &mxc),
			[],
		);

		txn.execute().await
	}

	/// Removes a pending upload's row and its index row, in one transaction.
	pub(super) async fn remove_pending_mxc(
		&self,
		mxc: &Mxc<'_>,
		user: &UserId,
		expires_at: u64,
	) -> Result {
		let mxc = mxc.to_string();
		let mut txn = self.db.txn();
		txn.del_raw(&self.mediaid_pending, &mxc);
		txn.del_raw(&self.mediaid_pending, pending_index_key(user, expires_at, &mxc));

		txn.execute().await
	}

	/// The user's live pending uploads, at most `max` of them, and the
	/// earliest expiry among those counted (`u64::MAX` when none).
	///
	/// Reads the user's rows of the pending-upload index newest expiry first,
	/// and stops at the first expired one or after `max`. So a request reads
	/// at most `max` + 1 index rows, whoever else has uploads pending. A
	/// pending upload recorded before the index existed is not counted; it
	/// expires on its own.
	pub(super) async fn count_pending_mxc_for_user(
		&self,
		user_id: &UserId,
		now: u64,
		max: usize,
	) -> (usize, u64) {
		let prefix = pending_index_prefix(user_id);
		let mut last = prefix.clone();
		last.extend_from_slice(&[0xFF; 9]);

		let keys = self.mediaid_pending.rev_raw_keys_from(&last);
		pin_mut!(keys);

		let (mut count, mut earliest) = (0_usize, u64::MAX);
		while count < max {
			let Some(Ok(key)) = keys.next().await else {
				break;
			};

			let expires_at = pending_index_expiry(key, prefix.len());
			if !key.starts_with(&prefix) || expires_at <= now {
				break;
			}

			count = count.saturating_add(1);
			earliest = earliest.min(expires_at);
		}

		(count, earliest)
	}

	/// Search for a pending MXC URI in the database
	pub(super) async fn search_pending_mxc(&self, mxc: &Mxc<'_>) -> Result<(OwnedUserId, u64)> {
		type Value<'a> = (u64, OwnedUserId);

		self.mediaid_pending
			.get(&mxc.to_string())
			.await
			.deserialized()
			.map(|(expires_at, user_id): Value<'_>| (user_id, expires_at))
			.inspect(|(user_id, expires_at)| debug!(?mxc, ?user_id, ?expires_at, "Found pending"))
			.map_err(|e| err!(Request(NotFound("Pending not found or error: {e}"))))
	}

	/// Map a minted mxc:// URI to the external URL it resolves to on first
	/// download (see `Service::fetch_lazy_media`).
	#[cfg(feature = "url_preview")]
	pub(super) async fn insert_lazy_media(&self, mxc: &str, url: &str) -> Result {
		debug!(?mxc, ?url, "Registering lazy media");

		self.mediaid_lazy
			.insert(mxc, url.as_bytes())
			.await
	}

	#[cfg(feature = "url_preview")]
	pub(super) fn queue_lazy_media(&self, txn: &mut Txn, mxc: &str, url: &str) {
		debug!(?mxc, ?url, "Registering lazy media");

		txn.insert_raw(&self.mediaid_lazy, mxc, url.as_bytes());
	}

	/// Remove a lazy media reference by its mxc:// URI string, unregistering
	/// the mxc.
	pub(super) fn remove_lazy_media(&self, txn: &mut Txn, mxc: &str) {
		txn.del_raw(&self.mediaid_lazy, mxc);
	}

	/// Look up the external URL a lazy media MXC URI refers to.
	pub(super) async fn search_lazy_media(&self, mxc: &Mxc<'_>) -> Result<String> {
		let handle = self.mediaid_lazy.get(&mxc.to_string()).await?;

		string_from_bytes(&handle)
			.map_err(|e| err!(Database(error!(?mxc, "Lazy media URL is invalid: {e}"))))
	}

	/// Stage the measured preview media bytes under its minted mxc so the
	/// first client download can promote without touching the origin.
	#[cfg(feature = "url_preview")]
	pub(super) fn set_lazy_content(
		&self,
		txn: &mut Txn,
		mxc: &str,
		content_type: Option<&str>,
		content_disposition: Option<&str>,
		content: &[u8],
	) {
		let value = LazyContentRef {
			content_type,
			content_disposition,
			content,
		};

		txn.raw_put(&self.mediaid_lazycontent, mxc, Cbor(&value));
	}

	/// Take the staged bytes a preview seeded for a lazy media mxc, if any.
	pub(super) async fn get_lazy_content(&self, mxc: &str) -> Result<Media> {
		self.mediaid_lazycontent
			.get(mxc)
			.await
			.deserialized::<Cbor<LazyContent>>()
			.map(at!(0))
			.map(Into::into)
	}

	pub(super) fn remove_lazy_content(&self, txn: &mut Txn, mxc: &str) {
		txn.del_raw(&self.mediaid_lazycontent, mxc);
	}

	/// Removes one object's record after its object could not be written,
	/// returning its charge in the same transaction.
	pub(super) async fn remove_file_metadata(
		&self,
		mxc: &Mxc<'_>,
		key: &[u8],
		uploader: Option<&UserId>,
		charge: Option<(Owner<'_>, u64)>,
	) -> Result {
		let mut txn = self.db.txn();

		txn.del_raw(&self.mediaid_file, key);
		if let Some(user) = uploader {
			txn.del_raw(&self.mediaid_user, serialize_key((mxc, user))?);
		}
		if let Some((owner, total)) = charge {
			self.set_usage(&mut txn, owner, total);
		}

		txn.execute().await
	}

	/// The byte length an object's record carries; `None` for a record written
	/// before records carried one.
	pub(super) async fn file_len(&self, key: &[u8]) -> Option<u64> {
		let val = self.mediaid_file.get(key).await.ok()?;

		<[u8; 8]>::try_from(&*val)
			.ok()
			.map(u64::from_be_bytes)
	}

	/// Removes every record of `mxc`, and applies `release` (the owner's new
	/// usage total) in the same transaction.
	pub(super) async fn delete_file_mxc(
		&self,
		mxc: &Mxc<'_>,
		release: Option<(Owner<'_>, u64)>,
	) -> Result {
		debug!("MXC URI: {mxc}");

		let prefix = (mxc, Interfix);
		let txn = self
			.mediaid_file
			.keys_prefix_raw(&prefix)
			.ignore_err()
			.ready_fold(self.db.txn(), |mut txn, key| {
				txn.del_raw(&self.mediaid_file, key);

				txn
			})
			.await;

		let txn = self
			.mediaid_user
			.stream_prefix_raw(&prefix)
			.ignore_err()
			.ready_fold(txn, |mut txn, (key, val)| {
				debug_assert!(
					key.starts_with(mxc.to_string().as_bytes()),
					"key should start with the mxc"
				);

				let user = str_from_bytes(val).unwrap_or_default();
				debug_info!("Deleting key {key:?} which was uploaded by user {user}");

				txn.del_raw(&self.mediaid_user, key);

				txn
			})
			.await;

		let mut txn = txn;
		if let Some((owner, total)) = release {
			self.set_usage(&mut txn, owner, total);
		}

		txn.execute().await
	}

	/// Searches for all files with the given MXC
	pub(super) async fn search_mxc_metadata_prefix(&self, mxc: &Mxc<'_>) -> Result<Vec<Vec<u8>>> {
		debug!("MXC URI: {mxc}");

		let prefix = (mxc, Interfix);
		let keys: Vec<Vec<u8>> = self
			.mediaid_file
			.keys_prefix_raw(&prefix)
			.ignore_err()
			.map(<[u8]>::to_vec)
			.collect()
			.await;

		if keys.is_empty() {
			return Err!(Database("Failed to find any keys in database for `{mxc}`",));
		}

		debug!("Got the following keys: {keys:?}");

		Ok(keys)
	}

	pub(super) async fn file_metadata_exists(&self, mxc: &Mxc<'_>, dim: &Dim) -> bool {
		let dim: &[u32] = &[dim.width, dim.height];
		let prefix = (mxc, dim, Interfix);
		let keys = self
			.mediaid_file
			.keys_prefix_raw(&prefix)
			.ignore_err();

		pin_mut!(keys);
		keys.next().await.is_some()
	}

	pub(super) async fn search_file_metadata(
		&self,
		mxc: &Mxc<'_>,
		dim: &Dim,
	) -> Result<Metadata> {
		let dim: &[u32] = &[dim.width, dim.height];
		let prefix = (mxc, dim, Interfix);

		let keys = self
			.mediaid_file
			.keys_prefix_raw(&prefix)
			.ignore_err()
			.map(ToOwned::to_owned);

		pin_mut!(keys);
		let key = keys
			.next()
			.await
			.ok_or_else(|| err!(Request(NotFound("Media not found"))))?;

		let mut parts = key.rsplit(|&b| b == 0xFF);

		let content_type = parts
			.next()
			.map(string_from_bytes)
			.transpose()
			.map_err(|e| err!(Database(error!(?mxc, "Content-type is invalid: {e}"))))?;

		let content_disposition = parts
			.next()
			.map(Some)
			.ok_or_else(|| err!(Database(error!(?mxc, "Media ID in db is invalid."))))?
			.filter(|bytes| !bytes.is_empty())
			.map(string_from_bytes)
			.transpose()
			.map_err(|e| err!(Database(error!(?mxc, "Content-disposition is invalid: {e}"))))?
			.as_deref()
			.map(str::parse)
			.transpose()
			.map_err(|e| err!(Database(error!(?mxc, "Content-disposition is invalid: {e}"))))?;

		Ok(Metadata { content_disposition, content_type, key })
	}

	/// Uploading local user of the media at the given MXC, from the uploader
	/// index.
	pub(super) async fn mxc_user(&self, mxc: &Mxc<'_>) -> Option<OwnedUserId> {
		let prefix = (mxc, Interfix);
		let users = self
			.mediaid_user
			.stream_prefix(&prefix)
			.ignore_err()
			.map(|(_, user): (Ignore, &UserId)| user.to_owned());

		pin_mut!(users);
		users.next().await
	}

	/// Gets all the MXCs associated with a user
	pub(super) async fn get_all_user_mxcs(&self, user_id: &UserId) -> Vec<OwnedMxcUri> {
		self.mediaid_user
			.stream()
			.ignore_err()
			.ready_filter_map(|((key, _), user): ((&str, Ignore), &UserId)| {
				(user == user_id).then(|| key.into())
			})
			.collect()
			.await
	}

	/// Gets all the media keys in our database (this includes all the metadata
	/// associated with it such as width, height, content-type, etc)
	pub(crate) async fn get_all_media_keys(&self) -> Vec<Vec<u8>> {
		self.mediaid_file
			.raw_keys()
			.ignore_err()
			.map(<[u8]>::to_vec)
			.collect()
			.await
	}

	pub(super) async fn set_url_preview(&self, url: &str, cached: &CachedPreview) -> Result {
		self.url_preview.raw_put(url, Cbor(cached)).await
	}

	pub(super) async fn get_url_preview(&self, url: &str) -> Result<CachedPreview> {
		self.url_preview
			.get(url)
			.await
			.deserialized::<Cbor<_>>()
			.map(at!(0))
			.ok()
			.filter(CachedPreview::valid)
			.ok_or(err!(Request(NotFound("Expired from cache"))))
	}

	/// Bytes charged to `owner`, when a counter exists for it.
	pub(super) async fn quota_usage(&self, owner: Owner<'_>) -> Option<u64> {
		match owner {
			| Owner::User(user) => self.userid_mediabytes.get(user).await,
			| Owner::Server(server) => self.servername_mediabytes.get(server).await,
		}
		.deserialized::<u64>()
		.ok()
	}

	/// At most `limit` media record keys from `from`, inclusive, or from the
	/// first. The read is closed before this returns.
	pub(super) async fn media_keys_from(
		&self,
		from: Option<&[u8]>,
		limit: usize,
	) -> Result<Vec<Vec<u8>>> {
		self.mediaid_file
			.raw_keys_capped(from, limit)
			.await
	}

	/// Writes `owner`'s usage total as part of `txn`.
	fn set_usage(&self, txn: &mut Txn, owner: Owner<'_>, total: u64) {
		match owner {
			| Owner::User(user) => txn.raw_put(&self.userid_mediabytes, user, total),
			| Owner::Server(server) => txn.raw_put(&self.servername_mediabytes, server, total),
		}
	}

	/// Streams every (mxc, uploader) pair in the user-media index.
	pub(super) fn all_uploads(
		&self,
	) -> impl Stream<Item = (OwnedMxcUri, OwnedUserId)> + Send + '_ {
		self.mediaid_user
			.keys()
			.ignore_err()
			.map(|(mxc, user): (&str, &UserId)| (mxc.into(), user.to_owned()))
	}
}

/// The start of a user's rows in the pending-upload index.
///
/// Those rows share `mediaid_pending` with the pending rows themselves, which
/// are keyed by MXC: user IDs start with `@` and MXCs with `mxc://`, so the
/// two never meet, and `0xFF`, which no user ID contains, ends the user's
/// part.
fn pending_index_prefix(user: &UserId) -> Vec<u8> {
	let mut key = Vec::with_capacity(user.as_bytes().len().saturating_add(1));
	key.extend_from_slice(user.as_bytes());
	key.push(0xFF);
	key
}

/// One pending upload's index row: [`pending_index_prefix`], then its expiry
/// as eight big-endian bytes, so a user's rows sort by expiry, then its MXC.
fn pending_index_key(user: &UserId, expires_at: u64, mxc: &str) -> Vec<u8> {
	let mut key = pending_index_prefix(user);
	key.extend_from_slice(&expires_at.to_be_bytes());
	key.extend_from_slice(mxc.as_bytes());
	key
}

/// The expiry an index row carries after its `prefix` bytes; zero, so
/// expired, when the row is too short to carry one.
fn pending_index_expiry(key: &[u8], prefix: usize) -> u64 {
	key.get(prefix..prefix.saturating_add(8))
		.and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
		.map_or(0, u64::from_be_bytes)
}

#[cfg(test)]
mod pending_index_tests {
	use ruma::user_id;

	use super::{pending_index_expiry, pending_index_key, pending_index_prefix};

	#[test]
	fn a_users_index_rows_sort_by_expiry_and_meet_no_one_elses() {
		let alice = user_id!("@alice:example.org");
		let longer = user_id!("@alice:example.org2");
		let prefix = pending_index_prefix(alice);

		let soon = pending_index_key(alice, 10, "mxc://example.org/zzz");
		let late = pending_index_key(alice, 20, "mxc://example.org/aaa");
		assert!(soon < late, "a user's rows do not sort by expiry");
		assert!(soon.starts_with(&prefix) && late.starts_with(&prefix));
		assert_eq!(pending_index_expiry(&late, prefix.len()), 20);

		// The reverse seek a count starts from lies past every row of the
		// user, and a user whose id extends this one sorts before it.
		let mut last = prefix.clone();
		last.extend_from_slice(&[0xFF; 9]);
		let max = pending_index_key(alice, u64::MAX, "mxc://example.org/zzz");
		assert!(max < last);
		let other = pending_index_key(longer, u64::MAX, "mxc://example.org/zzz");
		assert!(other < soon && !other.starts_with(&prefix));

		// Index rows and pending rows share the map without meeting.
		assert!(soon.as_slice() < b"mxc://".as_slice());
		assert_eq!(pending_index_expiry(&prefix, prefix.len()), 0);
	}
}

#[cfg(feature = "url_preview")]
#[cfg(test)]
mod tests {
	use minicbor_serde::{from_slice, to_vec};

	use super::{LazyContent, LazyContentRef, Media};

	#[test]
	fn lazy_content_roundtrip() {
		let content: &[u8] = b"\x00\x01\xFF\xFE arbitrary staged bytes";
		let value = LazyContentRef {
			content_type: Some("image/png"),
			content_disposition: Some("inline; filename=\"cat.png\""),
			content,
		};

		let bytes = to_vec(&value).expect("encodes");
		let decoded: LazyContent = from_slice(&bytes).expect("decodes");

		assert_eq!(decoded.content_type.as_deref(), Some("image/png"));
		assert_eq!(decoded.content.as_slice(), content);

		let media = Media::from(decoded);
		assert_eq!(media.content.as_slice(), content);
		assert!(media.content_disposition.is_some(), "disposition re-parses to the ruma type");
	}

	#[test]
	fn lazy_content_bytes_compact() {
		let content = vec![0xAB_u8; 4096];
		let value = LazyContentRef {
			content_type: None,
			content_disposition: None,
			content: content.as_slice(),
		};

		let bytes = to_vec(&value).expect("encodes");

		// serde_bytes must encode a CBOR byte string, not an array-of-uints
		// (~1.9x); only a small fixed header of overhead is permitted
		assert!(bytes.len() <= content.len() + 64);
	}
}
