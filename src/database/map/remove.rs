use std::fmt::Debug;

use serde_bytes::ByteBuf;
use tuwunel_core::{Result, implement};

use crate::{
	backend::{Op, metrics::STATS},
	map::Inner,
	util::or_else,
};

/// Deletes a raw key from this map.
///
/// The removal is the degenerate one-operation atomic batch: asynchronous
/// and fallible by contract. On the RocksDB backend it applies
/// synchronously, flushes immediately when the engine is uncorked, and then
/// notifies matching watchers; when the engine is corked, the surrounding
/// cork controls flushing. Removing an absent key succeeds.
#[implement(super::Map)]
#[tracing::instrument(skip(self, key), fields(%self), level = "trace")]
pub async fn remove<K>(&self, key: &K) -> Result
where
	K: AsRef<[u8]> + ?Sized + Debug + Sync,
{
	STATS.write.record(key.as_ref().len());

	match self.inner() {
		| Inner::Rocks(rocks) => {
			rocks
				.engine
				.db
				.delete_cf_opt(&&*rocks.cf, key, &rocks.write_options)
				.or_else(or_else)?;

			if !rocks.engine.corked() {
				rocks.engine.flush()?;
			}
		},
		| Inner::Mem(mem) => {
			mem.store.commit(std::iter::once((
				self.id()
					.expect("model-backend maps are catalog maps"),
				Op::Delete { key: key.as_ref().into() },
			)));
		},
		| Inner::Remote(remote) => {
			remote
				.backend
				.commit(vec![tuwunel_bridge::Mutation::Delete {
					map: self.remote_id(),
					key: ByteBuf::from(key.as_ref().to_vec()),
				}])
				.await?;
		},
	}

	self.notify(key.as_ref());

	Ok(())
}
