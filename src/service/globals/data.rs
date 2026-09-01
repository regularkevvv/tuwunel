use std::{ops::Range, sync::Arc};

use futures::TryFutureExt;
use tokio::sync::watch::Sender;
use tuwunel_core::{
	Result, err, utils,
	utils::two_phase_counter::{
		CommitFn, Counter as TwoPhaseCounter, Permit as TwoPhasePermit, ReleaseFn,
	},
};
use tuwunel_database::{Database, Deserialized, Map};

pub struct Data {
	global: Arc<Map>,
	retires: Sender<u64>,
	counter: Arc<Counter>,
	pub(super) db: Arc<Database>,
}

pub(super) type Permit = TwoPhasePermit;
type Counter = TwoPhaseCounter;

const COUNTER: &[u8] = b"c";

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = args.db.clone();
		let global = args.db["global"].clone();
		let count = Self::stored_count(&args.db["global"]).expect("initialize global counter");
		let retires = Sender::new(count);
		let commit: CommitFn = Box::new(move |count| {
			let db = db.clone();
			let global = global.clone();
			Box::pin(async move { Self::store_count(&db, &global, count).await })
		});
		let release: ReleaseFn = Box::new(move |count| Self::handle_retire(&retires, count));
		Self {
			db: args.db.clone(),
			global: args.db["global"].clone(),
			retires: Sender::new(count),
			counter: Counter::new(count, commit, release),
		}
	}

	#[inline]
	pub(super) async fn wait_pending(&self) -> Result<u64> {
		let count = self.counter.dispatched();
		self.wait_count(&count).await.inspect(|retired| {
			debug_assert!(
				*retired >= count,
				"Expecting retired sequence number >= snapshotted dispatch number"
			);
		})
	}

	#[inline]
	pub(super) async fn wait_count(&self, count: &u64) -> Result<u64> {
		self.retires
			.subscribe()
			.wait_for(|retired| retired.ge(count))
			.map_ok(|retired| *retired)
			.map_err(|e| err!(debug_error!("counter channel error {e:?}")))
			.await
	}

	#[inline]
	pub(super) async fn next_count(&self) -> Result<Permit> { self.counter.next().await }

	#[inline]
	pub(super) fn current_count(&self) -> u64 { self.counter.current() }

	#[inline]
	pub(super) fn pending_count(&self) -> Range<u64> { self.counter.range() }

	#[tracing::instrument(name = "retire", level = "debug", skip(sender))]
	fn handle_retire(sender: &Sender<u64>, count: u64) -> Result {
		let _prev = sender.send_replace(count);

		Ok(())
	}

	#[tracing::instrument(name = "dispatch", level = "debug", skip(db, global))]
	async fn store_count(db: &Arc<Database>, global: &Arc<Map>, count: u64) -> Result {
		let _cork = db.cork();
		global.insert(COUNTER, count.to_be_bytes()).await
	}

	fn stored_count(global: &Arc<Map>) -> Result<u64> {
		global
			.get_blocking(COUNTER)
			.as_deref()
			.map_or(Ok(0_u64), utils::u64_from_bytes)
	}
}

impl Data {
	pub async fn bump_database_version(&self, new_version: u64) -> Result {
		self.global.raw_put(b"version", new_version).await
	}

	pub async fn database_version(&self) -> u64 {
		self.global
			.get(b"version")
			.await
			.deserialized()
			.unwrap_or(0)
	}
}
