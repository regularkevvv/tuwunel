use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use rocksdb::Direction;
use tuwunel_bridge::{MAX_KEY_BYTES, MAX_VALUE_BYTES};
use tuwunel_core::{Error, Result, implement};

use super::seek::seek_stream_bounded;
use crate::{backend::remote::scan::Bound, stream};

/// One owned row: key, then value.
pub type Row = (Vec<u8>, Vec<u8>);

/// Reads at most `limit` owned rows after `after`, closing the scan before
/// returning so callers may write. A maximum-width cursor may be read once
/// more and skipped, keeping the seek valid on both native and remote stores.
/// Pass the last key back to resume; a short page means the range ended.
#[implement(super::Map)]
#[tracing::instrument(skip(self, after), fields(%self), level = "trace")]
pub async fn raw_rows_after(
	self: &Arc<Self>,
	after: Option<&[u8]>,
	limit: usize,
) -> Result<Vec<Row>> {
	rows_after(self, None, after, limit).await
}

/// [`Map::raw_rows_after`] within one prefix. No foreign-prefix row is copied.
#[implement(super::Map)]
#[tracing::instrument(skip(self, prefix, after), fields(%self), level = "trace")]
pub async fn raw_rows_prefix_after(
	self: &Arc<Self>,
	prefix: &[u8],
	after: Option<&[u8]>,
	limit: usize,
) -> Result<Vec<Row>> {
	rows_after(self, Some(prefix), after, limit).await
}

/// [`Map::raw_rows_after`] for keys alone; values are not materialized.
#[implement(super::Map)]
pub async fn raw_keys_after(
	self: &Arc<Self>,
	after: Option<&[u8]>,
	limit: usize,
) -> Result<Vec<Vec<u8>>> {
	keys_after(self, None, after, limit).await
}

/// A checked key-only page within a prefix, closed before returning.
#[implement(super::Map)]
pub async fn raw_keys_prefix_after(
	self: &Arc<Self>,
	prefix: &[u8],
	after: Option<&[u8]>,
	limit: usize,
) -> Result<Vec<Vec<u8>>> {
	keys_after(self, Some(prefix), after, limit).await
}

/// A bounded reverse key page within `prefix`, starting at `from` inclusive.
/// No values or foreign-prefix rows are copied; the cursor closes here.
#[implement(super::Map)]
pub async fn raw_keys_prefix_reverse(
	self: &Arc<Self>,
	prefix: &[u8],
	from: &[u8],
	limit: usize,
) -> Result<Vec<Vec<u8>>> {
	check_key(prefix)?;
	check_key(from)?;
	if !from.starts_with(prefix) {
		return Ok(Vec::new());
	}
	let mut scan = seek_stream_bounded::<stream::KeysRev<'_>, _>(
		self,
		Direction::Reverse,
		Some(from),
		Bound { within: Some(prefix), cap: Some(limit) },
	)
	.boxed();
	let mut keys = Vec::new();
	while keys.len() < limit {
		let Some(key) = scan.try_next().await? else {
			break;
		};
		if !key.starts_with(prefix) {
			break;
		}
		keys.push(owned_key(key)?);
	}
	Ok(keys)
}

/// At most `limit` checked keys from `from` inclusive. The scan closes here.
/// Use [`Map::raw_keys_after`] for exclusive cursor continuation, including
/// full-width keys whose bytewise successor exceeds the storage width.
#[implement(super::Map)]
#[tracing::instrument(skip(self, from), fields(%self), level = "trace")]
pub async fn raw_keys_capped(
	self: &Arc<Self>,
	from: Option<&[u8]>,
	limit: usize,
) -> Result<Vec<Vec<u8>>> {
	if let Some(from) = from {
		check_key(from)?;
	}
	let mut scan = seek_stream_bounded::<stream::Keys<'_>, _>(
		self,
		Direction::Forward,
		from,
		Bound::cap(limit),
	)
	.boxed();
	let mut keys = Vec::new();
	while keys.len() < limit {
		let Some(key) = scan.try_next().await? else {
			break;
		};
		keys.push(owned_key(key)?);
	}
	Ok(keys)
}

async fn rows_after(
	map: &Arc<super::Map>,
	prefix: Option<&[u8]>,
	after: Option<&[u8]>,
	limit: usize,
) -> Result<Vec<Row>> {
	if let Some(prefix) = prefix {
		check_key(prefix)?;
	}
	let (from, inclusive) = resume_from(after)?;
	let mut scan = seek_stream_bounded::<stream::Items<'_>, _>(
		map,
		Direction::Forward,
		from.as_deref().or(prefix),
		Bound {
			within: prefix,
			cap: Some(scan_cap(limit, inclusive)),
		},
	)
	.boxed();
	let mut rows = Vec::new();
	while rows.len() < limit {
		let Some((key, value)) = scan.try_next().await? else {
			break;
		};
		if prefix.is_some_and(|prefix| !key.starts_with(prefix)) {
			break;
		}
		if inclusive && after.is_some_and(|after| key <= after) {
			continue;
		}
		rows.push(owned_row(key, value)?);
	}
	Ok(rows)
}

async fn keys_after(
	map: &Arc<super::Map>,
	prefix: Option<&[u8]>,
	after: Option<&[u8]>,
	limit: usize,
) -> Result<Vec<Vec<u8>>> {
	if let Some(prefix) = prefix {
		check_key(prefix)?;
	}
	let (from, inclusive) = resume_from(after)?;
	let mut scan = seek_stream_bounded::<stream::Keys<'_>, _>(
		map,
		Direction::Forward,
		from.as_deref().or(prefix),
		Bound {
			within: prefix,
			cap: Some(scan_cap(limit, inclusive)),
		},
	)
	.boxed();
	let mut keys = Vec::new();
	while keys.len() < limit {
		let Some(key) = scan.try_next().await? else {
			break;
		};
		if prefix.is_some_and(|prefix| !key.starts_with(prefix)) {
			break;
		}
		if inclusive && after.is_some_and(|after| key <= after) {
			continue;
		}
		keys.push(owned_key(key)?);
	}
	Ok(keys)
}

fn check_key(key: &[u8]) -> Result {
	if key.len() > MAX_KEY_BYTES {
		return Err(Error::bad_database("Owned page key exceeds storage width"));
	}
	Ok(())
}
fn owned_key(key: &[u8]) -> Result<Vec<u8>> {
	check_key(key)?;
	Ok(key.to_vec())
}
fn owned_row(key: &[u8], value: &[u8]) -> Result<Row> {
	check_key(key)?;
	if value.len() > MAX_VALUE_BYTES {
		return Err(Error::bad_database("Owned page value exceeds storage width"));
	}
	Ok((key.to_vec(), value.to_vec()))
}
fn resume_from(after: Option<&[u8]>) -> Result<(Option<Vec<u8>>, bool)> {
	let Some(after) = after else {
		return Ok((None, false));
	};
	check_key(after)?;
	let inclusive = after.len() == MAX_KEY_BYTES;
	// Old and new workers bound Scan.from to the stored-key width. Read/skip
	// a full-width cursor rather than emitting an invalid widened seek.
	Ok((Some(if inclusive { after.to_vec() } else { successor(after) }), inclusive))
}
fn scan_cap(limit: usize, inclusive: bool) -> usize {
	limit.saturating_add(usize::from(inclusive && limit != 0))
}
/// The least bytewise key greater than `key`: `key` followed by `0x00`.
#[must_use]
pub fn successor(key: &[u8]) -> Vec<u8> {
	let mut next = Vec::with_capacity(key.len().saturating_add(1));
	next.extend_from_slice(key);
	next.push(0);
	next
}
