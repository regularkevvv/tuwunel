//! Insert a Key+Value into the database.
//!
//! Overloads are provided for the user to choose the most efficient
//! serialization or bypass for pre=serialized (raw) inputs.

use serde_bytes::ByteBuf;
use tuwunel_core::{Result, implement};

use crate::{
	backend::{Op, metrics::STATS},
	map::Inner,
	util::or_else,
};

/// Stores a raw key and raw value in this map.
///
/// The write is the degenerate one-operation atomic batch: it is asynchronous
/// and fallible by contract because a remote backend commits over a network.
/// On the RocksDB backend the write lands synchronously in the memtable,
/// flushes immediately when the engine is uncorked, and then notifies
/// matching watchers; when the engine is corked, the surrounding cork
/// controls flushing. Commit or flush failures are returned, and watchers
/// are notified only after success.
#[implement(super::Map)]
#[tracing::instrument(skip_all, fields(%self), level = "trace")]
pub async fn insert<K, V>(&self, key: &K, val: V) -> Result
where
	K: AsRef<[u8]> + ?Sized + Sync,
	V: AsRef<[u8]> + Send,
{
	STATS.write.record(
		key.as_ref()
			.len()
			.saturating_add(val.as_ref().len()),
	);

	match self.inner() {
		| Inner::Rocks(rocks) => {
			rocks
				.engine
				.db
				.put_cf_opt(&&*rocks.cf, key, val, &rocks.write_options)
				.or_else(or_else)?;

			if !rocks.engine.corked() {
				rocks.engine.flush()?;
			}
		},
		| Inner::Mem(mem) => {
			mem.store.commit(std::iter::once((
				self.id()
					.expect("model-backend maps are catalog maps"),
				Op::Put {
					key: key.as_ref().into(),
					val: val.as_ref().into(),
				},
			)));
		},
		| Inner::Remote(remote) => {
			remote
				.backend
				.commit(vec![tuwunel_bridge::Mutation::Put {
					map: self.remote_id(),
					key: ByteBuf::from(key.as_ref().to_vec()),
					val: ByteBuf::from(val.as_ref().to_vec()),
				}])
				.await?;
		},
	}

	self.notify(key.as_ref());

	Ok(())
}
