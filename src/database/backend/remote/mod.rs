//! `RemoteD1Backend`: the third backend of the storage seam (ADR-0002,
//! ADR-0012).
//!
//! Every read, scan and commit is one call on the private Worker↔Container
//! bridge (`tuwunel_bridge`); no SQL, table name or binding exists inside the
//! Container. The backend owns four pieces of state:
//!
//! - the [`client`] that speaks CBOR over HTTP with a per-request deadline and
//!   the retry policy of ADR-0012 (transport failures re-send the *same* bytes,
//!   so a commit keeps its idempotency key);
//! - the writer [`lease`] (ADR-0003), acquired before this module returns an
//!   open backend, renewed on the Worker's clock, and carried on every commit
//!   and scan page;
//! - a bounded process-local read [`cache`] with positive and negative entries,
//!   coherent because this process is the only writer;
//! - the open-scan registry of [`scan`], which gives paged scans
//!   snapshot-at-creation semantics: [`Backend::commit`] drains every open scan
//!   on the maps it touches *before* it sends the batch.
//!
//! Failure is returned, never panicked. A [`tuwunel_bridge::Error::StaleLease`]
//! anywhere means another process took the lease: the backend goes read-only,
//! commits fail fast, and the server shuts down (ADR-0003). Nothing in this
//! module or its children logs keys, values, or the bearer token.

pub(crate) mod cache;
pub(crate) mod client;
pub(crate) mod lease;
mod outcome;
pub(crate) mod scan;
#[cfg(test)]
pub(crate) mod tests;

use std::{
	collections::BTreeSet,
	sync::{
		Arc, Mutex, PoisonError,
		atomic::{AtomicU64, Ordering::Relaxed},
	},
	time::Duration,
};

use serde_bytes::ByteBuf;
use tokio::task::JoinHandle;
use tuwunel_bridge::{self as bridge, MAX_SCAN_PAGE, REQUEST_ID_LEN, Request, Response};
use tuwunel_core::{Err, Result, Server, debug, err, error, info, warn};

pub use self::lease::LeaseStatus;
use self::{
	client::{Client, database_error},
	lease::Lease,
	scan::{Registry, Scan, State},
};
use crate::backend::{MapId, metrics::STATS};

/// Bytes of one megabyte, the unit `d1_read_cache_mb` is expressed in.
const MEGABYTE: usize = 1024 * 1024;

/// The remote D1 backend: one bridge endpoint, one writer lease, one read
/// cache, one open-scan registry.
pub struct Backend {
	server: Arc<Server>,
	client: Arc<Client>,
	lease: Arc<Lease>,
	cache: cache::Cache,
	scans: Registry,

	/// Commit barrier. A scan page fetch holds it shared; [`Backend::commit`]
	/// holds it exclusively while it drains the open scans and sends the
	/// batch, so no page can be fetched across a commit. Lock order is always
	/// barrier first, then one scan's state.
	barrier: tokio::sync::RwLock<()>,

	/// Rows requested per scan page (`d1_scan_page`).
	scan_page: u32,

	/// Bumped by every applied commit. A read that started before a commit
	/// and returned after it must not fill the cache.
	commits: AtomicU64,

	/// The background renewal task, aborted when the backend closes.
	renewals: Mutex<Option<JoinHandle<()>>>,
}

impl Backend {
	/// Opens the remote backend: handshake, then lease, then renewals.
	///
	/// The handshake refuses to serve on a protocol or schema mismatch
	/// (ADR-0012, "Encoding and versioning"): the Container is one release
	/// behind the Worker at worst, so `protocol` may be this version or the
	/// next one (logged), and `schema_version` must equal
	/// [`tuwunel_bridge::SCHEMA_VERSION`] exactly. Acquisition happens before
	/// this function returns, so a database handle always carries a lease.
	///
	/// No RocksDB engine, worker pool, block cache or filesystem path is
	/// touched on this path.
	pub async fn open(server: &Arc<Server>) -> Result<Arc<Self>> {
		let config = &server.config;
		let client = Arc::new(Client::from_config(config)?);

		hello(&client).await?;

		let ttl = Duration::from_millis(config.d1_lease_ttl_ms.max(1));
		let lease = Lease::acquire(client.clone(), lease::holder_id(), ttl).await?;

		let scan_page = config.d1_scan_page.clamp(1, MAX_SCAN_PAGE);
		let cache_bytes = usize::try_from(config.d1_read_cache_mb)
			.unwrap_or(usize::MAX)
			.saturating_mul(MEGABYTE);

		let backend = Arc::new(Self {
			server: server.clone(),
			client,
			lease: lease.clone(),
			cache: cache::Cache::new(cache_bytes),
			scans: Registry::default(),
			barrier: tokio::sync::RwLock::new(()),
			scan_page,
			commits: AtomicU64::new(0),
			renewals: Mutex::new(None),
		});

		let server = server.clone();
		let renewals = tokio::spawn(lease::renewals(lease, move || {
			error!("the writer lease is lost; shutting down (ADR-0003)");
			server.shutdown().ok();
		}));

		*backend
			.renewals
			.lock()
			.unwrap_or_else(PoisonError::into_inner) = Some(renewals);

		info!(scan_page, "opened the remote D1 backend");

		Ok(backend)
	}

	/// Reads one key, `None` on a miss.
	///
	/// A cache hit costs nothing; a miss is one `Get` of a single key. The
	/// result fills the cache only when no commit landed while it was in
	/// flight.
	pub(crate) async fn get(&self, map: MapId, key: &[u8]) -> Result<Option<Box<[u8]>>> {
		if let Some(hit) = self.cache.get(map, key) {
			STATS
				.remote_cache_hit
				.record(hit.as_ref().map_or(0, |val| val.len()));

			return Ok(hit);
		}

		STATS.remote_cache_miss.record(0);

		let stamp = self.commits.load(Relaxed);
		let request = Request::Get {
			map: map.0,
			keys: vec![ByteBuf::from(key.to_vec())],
		};

		let mut vals = self.got(&request).await?;
		let val = vals.pop().flatten();
		if self.commits.load(Relaxed) == stamp {
			self.cache.insert(map, key, val.as_deref());
		}

		Ok(val)
	}

	/// Reads many keys of one map, preserving request order.
	///
	/// Cached keys never reach the wire; the rest are requested in chunks of
	/// at most [`bridge::MAX_GET_KEYS`] and the encoded request byte budget,
	/// reducing a chunk after a response-byte refusal. Each successful read
	/// advances at least one key. Atomic write batches are never split this
	/// way.
	pub(crate) async fn get_many(
		&self,
		map: MapId,
		keys: &[&[u8]],
	) -> Result<Vec<Option<Box<[u8]>>>> {
		let mut out: Vec<Option<Box<[u8]>>> = vec![None; keys.len()];
		let mut missing: Vec<usize> = Vec::new();

		for (at, key) in keys.iter().enumerate() {
			if let Some(hit) = self.cache.get(map, key) {
				STATS
					.remote_cache_hit
					.record(hit.as_ref().map_or(0, |val| val.len()));

				out[at] = hit;
			} else {
				STATS.remote_cache_miss.record(0);
				missing.push(at);
			}
		}

		let mut remaining = missing.as_slice();
		let mut max_count = bridge::MAX_GET_KEYS;
		while !remaining.is_empty() {
			let mut count = bridge::request::get_key_count(remaining.iter().map(|at| keys[*at]))
				.min(max_count);
			if count == 0 {
				return Err!(Database("bridge get: key exceeds request byte budget"));
			}
			let (vals, stamp) = loop {
				let (chunk, _) = remaining.split_at(count);
				let stamp = self.commits.load(Relaxed);
				let request = Request::Get {
					map: map.0,
					keys: chunk
						.iter()
						.map(|at| ByteBuf::from(keys[*at].to_vec()))
						.collect(),
				};
				match self.client.call(&request, None).await {
					| Ok(Response::Got { vals }) => break (vals, stamp),
					| Ok(_) => return Err!(Database("bridge get: unexpected reply")),
					| Err(error) if error.is_response_too_large() && count > 1 => {
						count /= 2;
						max_count = count;
					},
					| Err(error) => {
						if error.is_stale_lease() {
							self.lose_lease();
						}
						return Err(database_error("get", &error));
					},
				}
			};
			let (chunk, rest) = remaining.split_at(count);
			remaining = rest;
			if vals.len() != chunk.len() {
				return Err!(Database("bridge get: reply length does not match the request"));
			}

			let fill = self.commits.load(Relaxed) == stamp;
			for (at, val) in chunk.iter().zip(vals) {
				let val = val.map(|val| val.into_vec().into_boxed_slice());
				if fill {
					self.cache.insert(map, keys[*at], val.as_deref());
				}

				out[*at] = val;
			}
		}

		Ok(out)
	}

	/// Commits one atomic multi-map batch.
	///
	/// The order is the ADR's: refuse when the lease is not writable, then
	/// take the commit barrier and **drain every open scan on a map this
	/// batch touches**, then send one `Commit`. A transport failure re-sends
	/// the same `request_id`, so a reply lost after the batch applied comes
	/// back as `duplicate` and is reported to the caller as the single
	/// success it is.
	pub(crate) async fn commit(&self, ops: Vec<bridge::Mutation>) -> Result {
		if ops.is_empty() {
			return Ok(());
		}

		let lease = self.writable_lease()?;
		let digest = bridge::digest(&ops);
		let mut request_id = [0_u8; REQUEST_ID_LEN];
		rand::fill(&mut request_id);

		let request = Request::Commit {
			request_id: ByteBuf::from(request_id.to_vec()),
			lease,
			digest: ByteBuf::from(digest.to_vec()),
			ops,
		};

		// A batch over a protocol limit is refused, never split (ADR-0012).
		bridge::check(&request).map_err(|error| err!(Database("bridge commit: {error}")))?;

		let barrier = self.barrier.write().await;
		let Request::Commit { ops, .. } = &request else {
			unreachable!("the request was just built as a commit")
		};

		let maps: BTreeSet<u16> = ops.iter().map(bridge::Mutation::map).collect();
		let admission = self.scans.begin_write(&maps)?;
		self.drain(&maps).await?;
		self.writable_lease()?;

		let mut outcome = outcome::CommitOutcome::dispatched(self);
		let duplicate = match self.client.call(&request, None).await {
			| Ok(Response::Committed { duplicate }) => {
				outcome.acknowledged();
				duplicate
			},
			| Ok(_) => return Err!(Database("bridge commit: unexpected reply")),
			| Err(error) => {
				outcome.refused(&error);
				if error.is_stale_lease() {
					self.lose_lease();
				}
				return Err(database_error("commit", &error));
			},
		};

		self.commits.fetch_add(1, Relaxed);
		for op in ops {
			self.cache.invalidate(MapId(op.map()), op.key());
		}

		drop(admission);
		drop(barrier);

		if duplicate {
			debug!("a retried commit was already durable; reporting the single success");
		}

		Ok(())
	}

	/// Fetches one page into a scan's buffer.
	///
	/// The caller holds the scan's state lock and either the shared barrier
	/// (a stream poll) or the exclusive barrier (a commit drain); this
	/// function never takes the barrier itself, which is what keeps the
	/// drain-inside-commit path deadlock free.
	pub(crate) async fn fetch_page(
		&self,
		scan: &Scan,
		state: &mut State,
		limit: u32,
	) -> Result<usize> {
		if state.exhausted {
			return Ok(0);
		}

		let mut request = Request::Scan {
			map: scan.map.0,
			reverse: scan.reverse,
			from: state
				.from
				.as_ref()
				.map(|from| ByteBuf::from(from.to_vec())),
			inclusive: state.inclusive,
			limit: limit.clamp(1, MAX_SCAN_PAGE),
			lease: Some(self.writable_lease()?),
		};

		// An older Worker may still produce row-count-only pages. Keep the
		// cursor and lease unchanged while reducing that read to fit the wire
		// envelope. At most ten halvings reach one row; no write is split.
		let (items, more) = loop {
			match self.client.call(&request, None).await {
				| Ok(Response::Scanned { items, more }) => break (items, more),
				| Ok(_) => return Err!(Database("bridge scan: unexpected reply")),
				| Err(error) => {
					let Request::Scan { limit, .. } = &mut request else { unreachable!() };
					if error.is_response_too_large() && *limit > 1 {
						*limit /= 2;
						continue;
					}
					if error.is_stale_lease() {
						self.lose_lease();
					}
					return Err(database_error("scan", &error));
				},
			}
		};

		STATS.remote_page.record(items.len());

		if let Some((last, _)) = items.last() {
			state.from = Some(last.as_slice().into());
			state.inclusive = false;
		}

		state.exhausted = !more || items.is_empty();

		let fetched = items.len();
		for (key, val) in items {
			state
				.buffer
				.push_back((key.into_vec().into(), val.into_vec().into()));
		}

		Ok(fetched)
	}

	/// Materializes every open scan on `maps` (ADR-0012, snapshot
	/// semantics).
	///
	/// Called with the exclusive barrier held, so no page fetch is in flight
	/// and no scan's state lock can be held by a stream.
	async fn drain(&self, maps: &BTreeSet<u16>) -> Result {
		for scan in self.scans.touching(maps) {
			let mut state = scan.state.lock().await;
			if state.exhausted {
				continue;
			}

			let mut rows: usize = 0;
			while !state.exhausted {
				rows = rows.saturating_add(
					self.fetch_page(&scan, &mut state, self.scan_page)
						.await?,
				);
			}

			STATS.remote_drain.record(rows);
		}

		Ok(())
	}

	/// The open-scan registry.
	#[inline]
	pub(crate) fn scans(&self) -> &Registry { &self.scans }

	/// The commit barrier.
	#[inline]
	pub(crate) fn barrier(&self) -> &tokio::sync::RwLock<()> { &self.barrier }

	/// Rows requested per scan page.
	#[inline]
	pub(crate) fn scan_page(&self) -> u32 { self.scan_page }

	/// Snapshot of the writer lease for readiness reporting.
	#[inline]
	#[must_use]
	pub fn lease_status(&self) -> LeaseStatus { self.lease.status() }

	/// Whether commits may proceed right now.
	///
	/// False while the lease is uncertain (a renewal failed) or lost, which
	/// is exactly when the facade reports the database read-only.
	#[inline]
	#[must_use]
	pub fn is_writable(&self) -> bool { self.lease.writable() }

	/// Stops renewals and releases the lease so a successor need not wait
	/// out its expiry.
	pub async fn close(&self) {
		self.scans.close();
		self.abort_renewals();
		self.lease.release().await;
	}

	/// One bridge call, with the stale-lease reaction applied.
	async fn call(&self, request: &Request, op: &str) -> Result<Response> {
		match self.client.call(request, None).await {
			| Ok(response) => Ok(response),
			| Err(error) => {
				if error.is_stale_lease() {
					self.lose_lease();
				}

				Err(database_error(op, &error))
			},
		}
	}

	/// One `Get` call, unwrapped to its values.
	async fn got(&self, request: &Request) -> Result<Vec<Option<Box<[u8]>>>> {
		match self.call(request, "get").await? {
			| Response::Got { vals } if matches!(request, Request::Get { keys, .. } if keys.len() == vals.len()) =>
				Ok(vals
					.into_iter()
					.map(|val| val.map(|val| val.into_vec().into_boxed_slice()))
					.collect()),
			| _ => Err!(Database("bridge get: unexpected reply")),
		}
	}

	/// The lease identity to fence a commit with, or the fail-fast error.
	fn writable_lease(&self) -> Result<bridge::Lease> {
		if !self.lease.writable() {
			let status = self.lease.status();
			return Err!(Database(
				"the writer lease is not held (held: {}, uncertain: {}); writes are refused",
				status.held,
				status.uncertain
			));
		}

		self.lease
			.current()
			.ok_or_else(|| err!(Database("the writer lease has been released")))
	}

	/// Reacts to a stale-lease refusal: read-only for good, then shutdown.
	fn lose_lease(&self) {
		self.scans.close();
		if self.lease.mark_lost() {
			error!("another process holds the writer lease; stopping writes and shutting down");
			self.server.shutdown().ok();
		}
	}

	/// Called synchronously by the dispatch guard before releasing the barrier.
	fn stop_indeterminate_commit(&self) {
		self.scans.close();
		self.abort_renewals();
		if self.lease.mark_lost() {
			error!("commit outcome unknown; stopping writes and shutting down");
			self.server.shutdown().ok();
		}
	}

	/// Aborts the renewal task if it is still running.
	///
	/// The guard is released before the task is aborted so nothing holds the
	/// lock across the abort.
	fn abort_renewals(&self) {
		let handle = self
			.renewals
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.take();

		if let Some(handle) = handle {
			handle.abort();
		}
	}
}

impl Drop for Backend {
	/// Best-effort close: renewals always stop, and the lease is released
	/// when a runtime is still available to carry the call. A missed release
	/// only makes a successor wait out the natural expiry.
	fn drop(&mut self) {
		self.abort_renewals();

		let Ok(handle) = tokio::runtime::Handle::try_current() else {
			return;
		};

		let Some(lease) = self.lease.releasable() else {
			return;
		};

		let client = self.client.clone();
		handle.spawn(async move {
			match client
				.call(&Request::LeaseRelease { lease }, None)
				.await
			{
				| Ok(_) => debug!("released the writer lease"),
				| Err(error) => warn!(%error, "lease release failed; it expires on its own"),
			}
		});
	}
}

/// Performs the version handshake and refuses an incompatible Worker.
///
/// N/N-1 interoperability (ADR-0006): the Worker activates before the
/// Container rollout completes, so a Worker one major version ahead is
/// accepted with a warning. Anything else, and any schema-version mismatch,
/// refuses to start.
async fn hello(client: &Client) -> Result {
	let response = client
		.call(&Request::Hello, None)
		.await
		.map_err(|error| database_error("hello", &error))?;

	let Response::Hello { protocol, schema_version, .. } = response else {
		return Err!(Database("bridge hello: unexpected reply"));
	};

	if protocol != bridge::PROTOCOL_VERSION {
		if protocol == bridge::PROTOCOL_VERSION.saturating_add(1) {
			warn!(
				worker = protocol,
				container = bridge::PROTOCOL_VERSION,
				"the Worker speaks the next bridge protocol version; upgrade the Container"
			);
		} else {
			return Err!(Database(
				"bridge protocol mismatch: the Worker speaks v{protocol}, this Container speaks \
				 v{}",
				bridge::PROTOCOL_VERSION
			));
		}
	}

	if schema_version != bridge::SCHEMA_VERSION {
		return Err!(Database(
			"D1 schema mismatch: the database is at version {schema_version}, this Container \
			 needs {}; run the pending migrations",
			bridge::SCHEMA_VERSION
		));
	}

	debug!(protocol, schema_version, "bridge handshake accepted");

	Ok(())
}
