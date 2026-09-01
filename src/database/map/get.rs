use std::{fmt::Debug, sync::Arc};

use futures::{
	FutureExt, TryFutureExt,
	future::{Either, ready},
};
use rocksdb::{DBPinnableSlice, ReadOptions};
use tokio::task;
use tuwunel_core::{Err, Result, err, implement, utils::result::MapExpect};

use crate::{
	Handle,
	backend::metrics::STATS,
	map::Inner,
	util::{is_incomplete, map_err, or_else},
};

/// Fetches a raw key asynchronously and returns a value handle.
///
/// On the RocksDB backend, cache results consume cooperative scheduler budget
/// while misses run on the engine's blocking pool, and the returned handle
/// keeps its RocksDB value storage pinned for the handle's lifetime. On the
/// model backend the handle owns its bytes.
#[implement(super::Map)]
#[tracing::instrument(skip(self, key), fields(%self), level = "trace")]
pub fn get<K>(
	self: &Arc<Self>,
	key: &K,
) -> impl Future<Output = Result<Handle<'_>>> + Send + use<'_, K>
where
	K: AsRef<[u8]> + Debug + ?Sized,
{
	use crate::pool::Get;

	if let Inner::Mem(mem) = self.inner() {
		let result = mem.store.get(
			self.id()
				.expect("model-backend maps are catalog maps"),
			key.as_ref(),
		);
		STATS
			.get
			.record(result.as_ref().map_or(0, |val| val.len()));

		return Either::Left(Either::Left(task::consume_budget().map(move |()| {
			result
				.map(Handle::from)
				.ok_or(err!(Request(NotFound("Not found in database"))))
		})));
	}

	let cached = self.get_cached(key);
	if matches!(cached, Err(_) | Ok(Some(_))) {
		STATS.get_cached.record(
			cached
				.as_ref()
				.map_or(0, |c| c.as_ref().map_or(0, |h| h.len())),
		);

		return Either::Left(Either::Right(
			task::consume_budget().map(move |()| cached.map_expect("data found in cache")),
		));
	}

	debug_assert!(matches!(cached, Ok(None)), "expected status Incomplete");
	let cmd = Get {
		map: self.clone(),
		key: [key.as_ref().into()].into(),
		res: None,
	};

	Either::Right(
		self.rocks()
			.engine
			.pool
			.execute_get(cmd)
			.and_then(|mut res| {
				let res = res.remove(0);
				STATS
					.get
					.record(res.as_ref().map_or(0, |h| h.len()));
				ready(res)
			}),
	)
}

/// Fetches a raw key from block cache without storage I/O.
///
/// A cache miss returns `Ok(None)`, while a cached absence or database failure
/// remains an error. RocksDB-path internal helper.
#[implement(super::Map)]
#[tracing::instrument(skip(self, key), name = "cache", level = "trace")]
pub(crate) fn get_cached<K>(&self, key: &K) -> Result<Option<Handle<'_>>>
where
	K: AsRef<[u8]> + Debug + ?Sized,
{
	let res = self.get_blocking_opts(key, &self.rocks().cache_read_options);
	cached_handle_from(res)
}

/// Fetches a raw key synchronously and returns a value handle.
///
/// The call may block on storage and populate RocksDB caches. On the model
/// backend it locks the store briefly and returns owned bytes.
#[implement(super::Map)]
#[tracing::instrument(skip(self, key), name = "blocking", level = "trace")]
pub fn get_blocking<K>(&self, key: &K) -> Result<Handle<'_>>
where
	K: AsRef<[u8]> + ?Sized,
{
	if let Inner::Mem(mem) = self.inner() {
		return mem
			.store
			.get(
				self.id()
					.expect("model-backend maps are catalog maps"),
				key.as_ref(),
			)
			.map(Handle::from)
			.ok_or(err!(Request(NotFound("Not found in database"))));
	}

	let res = self.get_blocking_opts(key, &self.rocks().read_options);
	handle_from(res)
}

/// Performs a pinned point read with explicit RocksDB read options.
///
/// The raw RocksDB result distinguishes absence from storage failure for the
/// caller to classify.
#[implement(super::Map)]
fn get_blocking_opts<K>(
	&self,
	key: &K,
	read_options: &ReadOptions,
) -> Result<Option<DBPinnableSlice<'_>>, rocksdb::Error>
where
	K: AsRef<[u8]> + ?Sized,
{
	let rocks = self.rocks();
	rocks
		.engine
		.db
		.get_pinned_cf_opt(&&*rocks.cf, key, read_options)
}

/// Converts a RocksDB point-read result into a required value handle.
///
/// Missing values become the database not-found error, while RocksDB failures
/// use the shared error mapping.
#[inline]
pub(super) fn handle_from(
	result: Result<Option<DBPinnableSlice<'_>>, rocksdb::Error>,
) -> Result<Handle<'_>> {
	result
		.map_err(map_err)?
		.map(Handle::from)
		.ok_or(err!(Request(NotFound("Not found in database"))))
}

/// Classifies a block-cache point-read result.
///
/// `Ok(None)` represents a cache miss, a cached absence becomes not-found, and
/// other RocksDB failures use the shared error mapping.
#[inline]
pub(super) fn cached_handle_from(
	result: Result<Option<DBPinnableSlice<'_>>, rocksdb::Error>,
) -> Result<Option<Handle<'_>>> {
	match result {
		// cache hit; not found
		| Ok(None) => Err!(Request(NotFound("Not found in database"))),

		// cache hit; value found
		| Ok(Some(result)) => Ok(Some(Handle::from(result))),

		// cache miss; unknown
		| Err(error) if is_incomplete(&error) => Ok(None),

		// some other error occurred
		| Err(error) => or_else(error),
	}
}
