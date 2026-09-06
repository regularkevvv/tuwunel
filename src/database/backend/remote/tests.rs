//! A fake bridge Worker, and the remote-backend contract cases.
//!
//! The fake implements `tuwunel_bridge` over a `BTreeMap<(u16, Vec<u8>),
//! Vec<u8>>`. That key type is the oracle for the D1 schema: `(map_id, key)`
//! with `key` a BLOB compares bytewise, which is exactly SQLite's `memcmp`
//! ordering and exactly RocksDB's default comparator (ADR-0012, "D1 schema").
//! It also keeps the lease row and the `commits` table, including the fence
//! that refuses a stale writer and the request-id/digest rules that turn a
//! timeout-after-commit retry into `duplicate: true` instead of a second
//! application.
//!
//! Faults are injectable so the client's policy can be exercised without a
//! network: drop the reply *after* a commit applied, refuse renewals, refuse
//! everything as `StaleLease`, and answer the handshake with the wrong
//! protocol or schema version.

use std::{
	collections::{BTreeMap, HashMap},
	net::SocketAddr,
	sync::{Arc, Mutex, PoisonError},
	time::{SystemTime, UNIX_EPOCH},
};

use axum::{
	Router,
	body::Bytes,
	extract::State as AxumState,
	http::{HeaderMap, StatusCode},
	response::{IntoResponse, Response as HttpResponse},
	routing::post,
};
use futures::{FutureExt, StreamExt, TryStreamExt};
use tokio::task::JoinHandle;
use tuwunel_bridge::{
	self as bridge, Error as BridgeError, Lease, LeaseState, Mutation, Request, Response,
};
use tuwunel_core::{Result, Server, config::Figment};

use super::Backend;
use crate::{Map, Txn, backend::Sink};

/// The bearer token every fake-bridge test presents.
pub(crate) const TOKEN: &str = "fake-bridge-token";

/// Faults the fake injects, all off by default.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Faults {
	/// Apply this many further commits and then answer `500` instead of the
	/// reply, exactly like a reply lost after the batch became durable.
	pub(crate) swallow_commit_replies: u32,
	/// Answer every renewal with `500`.
	pub(crate) fail_renewals: bool,
	/// Answer every fenced operation with [`BridgeError::StaleLease`].
	pub(crate) stale_lease: bool,
	/// Protocol version reported by `Hello`.
	pub(crate) protocol: Option<u32>,
	/// D1 schema version reported by `Hello`.
	pub(crate) schema_version: Option<u32>,
}

/// The fake's durable state: the kv table, the lease row, the commits table.
#[derive(Default)]
struct Tables {
	kv: BTreeMap<(u16, Vec<u8>), Vec<u8>>,
	lease: Option<LeaseState>,
	commits: HashMap<Vec<u8>, Vec<u8>>,
	/// Commits that actually applied; the duplicate test asserts it stays 1.
	applied: u32,
}

/// Shared handler state.
struct Shared {
	tables: Mutex<Tables>,
	faults: Mutex<Faults>,
}

/// One running fake bridge.
pub(crate) struct Fake {
	shared: Arc<Shared>,
	task: JoinHandle<()>,
	/// Base URL to configure `d1_bridge_url` with.
	pub(crate) url: String,
}

impl Fake {
	/// Starts a fake bridge on an ephemeral loopback port.
	pub(crate) async fn start() -> Result<Self> {
		let shared = Arc::new(Shared {
			tables: Mutex::new(Tables::default()),
			faults: Mutex::new(Faults::default()),
		});

		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
		let addr: SocketAddr = listener.local_addr()?;
		let app = Router::new()
			.route(bridge::PATH_KV, post(kv))
			.with_state(shared.clone());

		let task = tokio::spawn(async move {
			axum::serve(listener, app).await.ok();
		});

		Ok(Self {
			shared,
			task,
			url: format!("http://{addr}"),
		})
	}

	/// Replaces the injected faults.
	pub(crate) fn faults(&self, faults: Faults) {
		*self
			.shared
			.faults
			.lock()
			.unwrap_or_else(PoisonError::into_inner) = faults;
	}

	/// Number of commits the fake actually applied.
	pub(crate) fn applied(&self) -> u32 {
		self.shared
			.tables
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.applied
	}

	/// Every row of one map, in bytewise key order.
	pub(crate) fn rows(&self, map: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
		self.shared
			.tables
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.kv
			.iter()
			.filter(|((id, _), _)| *id == map)
			.map(|((_, key), val)| (key.clone(), val.clone()))
			.collect()
	}
}

impl Drop for Fake {
	fn drop(&mut self) { self.task.abort(); }
}

/// The Worker's clock; the only clock the protocol uses.
fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
}

/// One `POST /_bridge/v1/kv`.
async fn kv(
	AxumState(shared): AxumState<Arc<Shared>>,
	headers: HeaderMap,
	body: Bytes,
) -> HttpResponse {
	let authorized = headers
		.get(axum::http::header::AUTHORIZATION)
		.and_then(|value| value.to_str().ok())
		.is_some_and(|value| value == format!("Bearer {TOKEN}"));

	if !authorized {
		return StatusCode::UNAUTHORIZED.into_response();
	}

	let Ok(request) = bridge::decode::<Request>(&body) else {
		return StatusCode::BAD_REQUEST.into_response();
	};

	match apply(&shared, &request) {
		| Ok(response) => match bridge::encode(&response) {
			| Ok(body) =>
				([(axum::http::header::CONTENT_TYPE, bridge::CONTENT_TYPE)], body).into_response(),
			| Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
		},
		// The reply is lost; whatever the request already changed stays
		// changed. This is the ambiguous outcome the request id exists for.
		| Err(status) => status.into_response(),
	}
}

/// Serves one decoded request, or the HTTP status of a lost reply.
fn apply(shared: &Shared, request: &Request) -> Result<Response, StatusCode> {
	let faults = *shared
		.faults
		.lock()
		.unwrap_or_else(PoisonError::into_inner);

	let mut tables = shared
		.tables
		.lock()
		.unwrap_or_else(PoisonError::into_inner);

	let now = now_ms();

	match request {
		| Request::Hello => Ok(Response::Hello {
			protocol: faults
				.protocol
				.unwrap_or(bridge::PROTOCOL_VERSION),
			schema_version: faults
				.schema_version
				.unwrap_or(bridge::SCHEMA_VERSION),
			lease: tables.lease.clone(),
			now_ms: now,
		}),

		| Request::Get { map, keys } => Ok(Response::Got {
			vals: keys
				.iter()
				.map(|key| {
					tables
						.kv
						.get(&(*map, key.to_vec()))
						.map(|val| serde_bytes::ByteBuf::from(val.clone()))
				})
				.collect(),
		}),

		| Request::Scan {
			map,
			reverse,
			from,
			inclusive,
			limit,
			lease,
		} => Ok(scan(
			&tables,
			&ScanArgs {
				map: *map,
				reverse: *reverse,
				from: from.as_ref().map(|from| from.to_vec()),
				inclusive: *inclusive,
				limit: *limit,
			},
			lease.as_ref(),
			now,
			faults,
		)),

		| Request::Commit { request_id, lease, digest, ops } =>
			commit(shared, &mut tables, request_id, lease, digest, ops, now, faults),

		| Request::LeaseAcquire { holder, ttl_ms } =>
			Ok(acquire(&mut tables, holder, *ttl_ms, now)),

		| Request::LeaseRenew { lease, ttl_ms } => {
			if faults.fail_renewals {
				return Err(StatusCode::INTERNAL_SERVER_ERROR);
			}

			if let Err(error) = fence(&tables, lease, now, faults) {
				return Ok(Response::Error(error));
			}

			let expires_at_ms = now.saturating_add(*ttl_ms);
			if let Some(current) = tables.lease.as_mut() {
				current.expires_at_ms = expires_at_ms;
			}

			Ok(Response::Leased {
				epoch: lease.epoch,
				expires_at_ms,
				now_ms: now,
			})
		},

		| Request::LeaseRelease { lease } => {
			if tables.lease.as_ref().is_some_and(|current| {
				current.holder == lease.holder && current.epoch == lease.epoch
			}) && let Some(current) = tables.lease.as_mut()
			{
				current.expires_at_ms = 0;
			}

			Ok(Response::Released)
		},
	}
}

/// One page request, unpacked from the protocol message.
struct ScanArgs {
	map: u16,
	reverse: bool,
	from: Option<Vec<u8>>,
	inclusive: bool,
	limit: u32,
}

/// One page of an ordered scan.
///
/// Selection and ordering are done over a materialized copy of the map: at
/// test scale that is obviously correct, which is the point of an oracle.
fn scan(
	tables: &Tables,
	args: &ScanArgs,
	lease: Option<&Lease>,
	now: u64,
	faults: Faults,
) -> Response {
	if let Some(lease) = lease
		&& let Err(error) = fence(tables, lease, now, faults)
	{
		return Response::Error(error);
	}

	let mut items: Vec<(Vec<u8>, Vec<u8>)> = tables
		.kv
		.iter()
		.filter(|((id, _), _)| *id == args.map)
		.map(|((_, key), val)| (key.clone(), val.clone()))
		.collect();

	if args.reverse {
		items.reverse();
	}

	if let Some(from) = args.from.as_deref() {
		items.retain(|(key, _)| {
			let ordered = if args.reverse {
				key.as_slice() <= from
			} else {
				key.as_slice() >= from
			};

			ordered && (args.inclusive || key.as_slice() != from)
		});
	}

	let limit = usize::try_from(args.limit).unwrap_or(usize::MAX);
	let more = items.len() > limit;
	items.truncate(limit);

	Response::Scanned {
		items: items
			.into_iter()
			.map(|(key, val)| (serde_bytes::ByteBuf::from(key), serde_bytes::ByteBuf::from(val)))
			.collect(),
		more,
	}
}

/// One atomic batch, with the digest, request-id and fence rules.
#[expect(
	clippy::too_many_arguments,
	reason = "one parameter per protocol field of Commit"
)]
fn commit(
	shared: &Shared,
	tables: &mut Tables,
	request_id: &serde_bytes::ByteBuf,
	lease: &Lease,
	digest: &serde_bytes::ByteBuf,
	ops: &[Mutation],
	now: u64,
	faults: Faults,
) -> Result<Response, StatusCode> {
	if digest.as_slice() != bridge::digest(ops) {
		return Ok(Response::Error(BridgeError::Invalid("digest".into())));
	}

	// The request-id primary key makes a retry collide instead of applying
	// twice (ADR-0012, "Commits").
	if let Some(seen) = tables.commits.get(request_id.as_slice()) {
		return Ok(if seen.as_slice() == digest.as_slice() {
			Response::Committed { duplicate: true }
		} else {
			Response::Error(BridgeError::DigestMismatch)
		});
	}

	if let Err(error) = fence(tables, lease, now, faults) {
		return Ok(Response::Error(error));
	}

	for op in ops {
		match op {
			| Mutation::Put { map, key, val } => {
				tables
					.kv
					.insert((*map, key.to_vec()), val.to_vec());
			},
			| Mutation::Delete { map, key } => {
				tables.kv.remove(&(*map, key.to_vec()));
			},
		}
	}

	tables
		.commits
		.insert(request_id.to_vec(), digest.to_vec());

	tables.applied = tables.applied.saturating_add(1);

	// The batch is durable; losing the reply here is the ambiguous outcome
	// the request id exists for.
	if swallow(shared) {
		return Err(StatusCode::INTERNAL_SERVER_ERROR);
	}

	Ok(Response::Committed { duplicate: false })
}

/// Consumes one swallowed commit reply, reporting whether to drop this one.
fn swallow(shared: &Shared) -> bool {
	let mut faults = shared
		.faults
		.lock()
		.unwrap_or_else(PoisonError::into_inner);

	if faults.swallow_commit_replies == 0 {
		return false;
	}

	faults.swallow_commit_replies = faults.swallow_commit_replies.saturating_sub(1);

	true
}

/// Takes the writer lease when the row is absent, expired, or already ours.
fn acquire(tables: &mut Tables, holder: &str, ttl_ms: u64, now: u64) -> Response {
	let free = tables
		.lease
		.as_ref()
		.is_none_or(|current| current.expires_at_ms <= now || current.holder == holder);

	if !free {
		let current = tables
			.lease
			.clone()
			.expect("a held lease has a row");

		return Response::Error(BridgeError::LeaseHeld { current });
	}

	let epoch = tables
		.lease
		.as_ref()
		.map_or(0, |current| current.epoch)
		.saturating_add(1);

	let expires_at_ms = now.saturating_add(ttl_ms);
	tables.lease = Some(LeaseState {
		holder: holder.to_owned(),
		epoch,
		expires_at_ms,
	});

	Response::Leased { epoch, expires_at_ms, now_ms: now }
}

/// The `commits` trigger: the presented lease must be the current unexpired
/// one.
fn fence(tables: &Tables, lease: &Lease, now: u64, faults: Faults) -> Result<(), BridgeError> {
	let current = tables.lease.clone();
	let held = current.as_ref().is_some_and(|current| {
		current.holder == lease.holder
			&& current.epoch == lease.epoch
			&& current.expires_at_ms > now
	});

	if faults.stale_lease || !held {
		return Err(BridgeError::StaleLease { current });
	}

	Ok(())
}

/// Builds a server whose configuration selects the remote backend at `url`.
///
/// `scan_page` is explicit because the page-continuation cases need a page
/// small enough that continuation is the common path, not the exception.
pub(crate) fn remote_server(url: &str, scan_page: u32, cache_mb: u32) -> Result<Arc<Server>> {
	let raw_config = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_backend", "d1"))
		.merge(("d1_bridge_url", url))
		.merge(("d1_bridge_token", TOKEN))
		.merge(("d1_scan_page", scan_page))
		.merge(("d1_read_cache_mb", cache_mb))
		.merge(("d1_lease_ttl_ms", 15_000))
		.merge(("test", ["fresh", "cleanup"]));

	crate::tests::test_server(&raw_config)
}

/// Starts a fake bridge and opens a remote backend against it.
async fn rig(scan_page: u32, cache_mb: u32) -> Result<(Fake, Arc<Server>, Arc<Backend>)> {
	let fake = Fake::start().await?;
	let server = remote_server(&fake.url, scan_page, cache_mb)?;
	let backend = Backend::open(&server).await?;

	Ok((fake, server, backend))
}

/// The map every case here writes through; `pduid_pdu` is a plain preset.
const MAP: &str = "pduid_pdu";

/// The `u64` prefix the `del_prefix` case deletes under.
const DOOMED: u64 = 7;

/// Its stable id, the one that reaches the wire.
fn map_id() -> u16 {
	crate::backend::ids::map_id(MAP)
		.expect("catalog map")
		.0
}

/// Adversarial key set: separator bytes, 0x00/0xFF runs, shared prefixes.
fn edge_keys() -> Vec<Vec<u8>> {
	vec![
		vec![0x00],
		vec![0x00, 0x00],
		vec![0x01],
		b"p".to_vec(),
		b"p\x00".to_vec(),
		b"p\xFF".to_vec(),
		b"prefix".to_vec(),
		b"prefix\x00".to_vec(),
		b"prefixed".to_vec(),
		b"q".to_vec(),
		vec![0xFE],
		vec![0xFF],
		vec![0xFF, 0x00],
		vec![0xFF, 0xFF],
	]
}

/// Opens a backend that must fail, returning the refusal's message.
///
/// `Backend` is deliberately not `Debug` (it holds the bridge token), so a
/// failed open cannot be unwrapped with `expect_err`.
async fn refused(server: &Arc<Server>, why: &str) -> String {
	match Backend::open(server).await {
		| Ok(_) => panic!("{why}"),
		| Err(error) => error.to_string(),
	}
}

#[tokio::test]
async fn hello_mismatch_refuses_to_open() -> Result {
	let fake = Fake::start().await?;
	let server = remote_server(&fake.url, 256, 1)?;

	fake.faults(Faults {
		schema_version: Some(bridge::SCHEMA_VERSION.saturating_add(1)),
		..Faults::default()
	});
	let error = refused(&server, "a schema mismatch refuses to serve").await;
	assert!(error.contains("schema"), "{error}");

	fake.faults(Faults {
		protocol: Some(bridge::PROTOCOL_VERSION.saturating_add(2)),
		..Faults::default()
	});
	let error = refused(&server, "a protocol mismatch refuses to serve").await;
	assert!(error.contains("protocol"), "{error}");

	// N+1 is the rollout window: the Worker activates first (ADR-0006).
	fake.faults(Faults {
		protocol: Some(bridge::PROTOCOL_VERSION.saturating_add(1)),
		..Faults::default()
	});
	let backend = Backend::open(&server)
		.await
		.expect("the next protocol version is accepted with a warning");
	backend.close().await;

	Ok(())
}

#[test]
fn the_container_environment_names_are_the_ones_the_image_bakes_in() {
	// The container image sets BRIDGE_URL, BRIDGE_TOKEN and
	// TUWUNEL_DATABASE_BACKEND=d1. The first two are read by the bridge
	// client when the matching configuration keys are unset; the third is
	// figment's mapping of the `database_backend` field, exercised by every
	// case in this module through `remote_server`.
	assert_eq!(bridge::ENV_URL, "BRIDGE_URL");
	assert_eq!(bridge::ENV_TOKEN, "BRIDGE_TOKEN");
}

#[tokio::test]
async fn the_d1_backend_opens_no_rocksdb_and_never_creates_database_path() -> Result {
	let fake = Fake::start().await?;

	// The container image bakes in TUWUNEL_DATABASE_PATH, so the path is
	// always configured on d1; it must nonetheless never be created.
	let root = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".into());
	let path = format!("{root}/tuwunel-d1-never-{}", std::process::id());
	std::fs::remove_dir_all(&path).ok();

	let raw_config = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_backend", "d1"))
		.merge(("database_path", &path))
		.merge(("d1_bridge_url", &fake.url))
		.merge(("d1_bridge_token", TOKEN));

	let server = crate::tests::test_server(&raw_config)?;
	let db = crate::Database::open(&server).await?;

	assert_eq!(db.backend(), "d1");
	assert!(
		!std::path::Path::new(&path).exists(),
		"the d1 backend created the RocksDB database directory"
	);

	// The engine is a RocksDB capability; every admin path that needs it
	// reports "unsupported on this backend" instead of panicking.
	let error = db
		.engine()
		.err()
		.expect("the engine is unavailable off RocksDB");
	assert!(
		error
			.to_string()
			.contains("unsupported on this backend"),
		"{error}"
	);

	// The lease is held, the database is writable, and a transaction lands on
	// the remote sink.
	let lease = db
		.lease_status()
		.expect("the remote backend reports its lease");
	assert!(lease.held && !lease.uncertain, "{lease:?}");
	assert!(!db.is_read_only());
	assert!(!db.is_secondary());

	let map = db.get(MAP)?;
	let mut txn = db.txn();
	txn.insert_raw(map, b"through-the-facade", b"value");
	txn.execute().await?;

	assert_eq!(&*map.get(&b"through-the-facade".to_vec()).await?, b"value");
	assert_eq!(fake.rows(map_id()).len(), 1);
	assert!(
		!std::path::Path::new(&path).exists(),
		"a committed transaction created the RocksDB database directory"
	);

	db.close().await;

	Ok(())
}

#[tokio::test]
async fn page_continuation_over_adversarial_keys() -> Result {
	// A page of two rows makes continuation the rule, not the exception.
	let (fake, _server, backend) = rig(2, 1).await?;
	let map = Map::open_remote(&backend, MAP);

	for key in edge_keys() {
		map.insert(&key, &key).await?;
	}

	let mut expect = fake.rows(map_id());
	expect.sort();

	let forward: Vec<(Vec<u8>, Vec<u8>)> = map
		.raw_stream()
		.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
		.try_collect()
		.await?;

	assert_eq!(forward, expect, "paged forward scan is not the whole map in byte order");

	let mut mirror = expect.clone();
	mirror.reverse();
	let reverse: Vec<(Vec<u8>, Vec<u8>)> = map
		.rev_raw_stream()
		.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
		.try_collect()
		.await?;

	assert_eq!(reverse, mirror, "paged reverse scan is not the forward mirror");

	// Seek boundaries survive continuation as well.
	for from in [&b"p"[..], b"prefix\x00", b"\xFF", b"\x00", b"zz-absent"] {
		let seen: Vec<Vec<u8>> = map
			.raw_keys_from(&from)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;

		let want: Vec<Vec<u8>> = expect
			.iter()
			.map(|(key, _)| key.clone())
			.filter(|key| key.as_slice() >= from)
			.collect();

		assert_eq!(seen, want, "keys_from diverges for {from:?}");

		let seen: Vec<Vec<u8>> = map
			.rev_raw_keys_from(&from)
			.map_ok(<[u8]>::to_vec)
			.try_collect()
			.await?;

		let mut want: Vec<Vec<u8>> = expect
			.iter()
			.map(|(key, _)| key.clone())
			.filter(|key| key.as_slice() <= from)
			.collect();
		want.reverse();

		assert_eq!(seen, want, "rev_keys_from diverges for {from:?}");
	}

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn commit_during_an_open_scan_sees_the_pre_commit_snapshot() -> Result {
	// One page holds two rows, so every one of these scans continues across
	// the commits that interleave with it.
	let (fake, _server, backend) = rig(2, 1).await?;
	let map = Map::open_remote(&backend, MAP);

	for key in edge_keys() {
		map.insert(&key, b"x").await?;
	}

	// `DOOMED` is a `u64` prefix, the shape services use; its encoding is
	// eight big-endian bytes, which none of the adversarial keys start with.
	for suffix in [&b"a"[..], b"b", b"c"] {
		let mut key = DOOMED.to_be_bytes().to_vec();
		key.extend_from_slice(suffix);
		map.insert(&key, b"x").await?;
	}

	let before = fake.rows(map_id()).len();

	// `del_prefix` writes while iterating: it must delete exactly the rows
	// its scan saw at creation, and must not deadlock doing so.
	map.del_prefix(&DOOMED).await?;

	let rows = fake.rows(map_id());
	assert_eq!(rows.len(), before.saturating_sub(3), "del_prefix removed the wrong rows");
	assert!(
		!rows
			.iter()
			.any(|(key, _)| key.starts_with(&DOOMED.to_be_bytes())),
		"del_prefix left rows under its prefix"
	);

	// A commit on an unrelated key landing mid-scan is not visible to the
	// scan that was already open (RocksDB iterator semantics).
	let late: Vec<u8> = b"\x00\x00\x00-late".to_vec();
	let mut stream = Box::pin(map.raw_keys());
	let first = stream
		.next()
		.await
		.expect("the scan has rows")?
		.to_vec();

	map.insert(&late, b"late").await?;

	let mut seen = vec![first];
	while let Some(key) = stream.next().await {
		seen.push(key?.to_vec());
	}
	drop(stream);

	assert_eq!(seen.len(), rows.len(), "the open scan lost or gained rows");
	assert!(!seen.contains(&late), "an open scan observed a later write");

	// `for_clear` deletes each row its scan yields, which is every row that
	// existed when the scan began.
	let cleared: Vec<Vec<u8>> = map
		.for_clear()
		.map_ok(<[u8]>::to_vec)
		.try_collect()
		.await?;

	assert_eq!(cleared.len(), seen.len().saturating_add(1), "for_clear missed rows");
	assert!(fake.rows(map_id()).is_empty(), "clear left rows behind");

	// `clear` on an empty map is a no-op that still terminates.
	map.clear().await?;
	assert_eq!(map.count().await, 0);

	// Every scan unregistered itself, so a later commit drains nothing.
	assert_eq!(backend.scans().len(), 0, "a dropped scan stayed registered");

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn read_cache_serves_hits_and_a_commit_invalidates_them() -> Result {
	let (fake, _server, backend) = rig(256, 4).await?;
	let map = Map::open_remote(&backend, MAP);

	map.insert(&b"cached".to_vec(), b"first").await?;
	assert_eq!(&*map.get(&b"cached".to_vec()).await?, b"first");
	assert_eq!(&*map.get(&b"cached".to_vec()).await?, b"first", "second read differs");

	// Overwrite through the facade: the commit must invalidate the entry.
	map.insert(&b"cached".to_vec(), b"second").await?;
	assert_eq!(
		&*map.get(&b"cached".to_vec()).await?,
		b"second",
		"the read cache served a value the commit replaced"
	);

	// A cached absence is invalidated by the write that fills it.
	assert!(
		map.get(&b"absent".to_vec())
			.await
			.unwrap_err()
			.is_not_found()
	);
	map.insert(&b"absent".to_vec(), b"now-there")
		.await?;
	assert_eq!(
		&*map.get(&b"absent".to_vec()).await?,
		b"now-there",
		"the read cache served a negative entry the commit filled"
	);

	// A delete invalidates the positive entry.
	map.remove(&b"cached".to_vec()).await?;
	assert!(
		map.get(&b"cached".to_vec())
			.await
			.unwrap_err()
			.is_not_found(),
		"the read cache served a value the commit deleted"
	);

	assert_eq!(fake.rows(map_id()).len(), 1);

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn get_batch_preserves_order_with_misses() -> Result {
	let (_fake, _server, backend) = rig(256, 4).await?;
	let map = Map::open_remote(&backend, MAP);

	for key in [&b"a"[..], b"c", b"e"] {
		map.insert(&key, key).await?;
	}

	let asked: Vec<Vec<u8>> = [&b"e"[..], b"b", b"a", b"b", b"c", b"zzz", b"a"] // duplicates and misses
		.into_iter()
		.map(<[u8]>::to_vec)
		.collect();

	let got: Vec<Option<Vec<u8>>> = map
		.get_batch(futures::stream::iter(asked.clone()))
		.map(|result| result.ok().map(|handle| handle.to_vec()))
		.collect()
		.await;

	let want: Vec<Option<Vec<u8>>> = asked
		.iter()
		.map(|key| matches!(key.as_slice(), b"a" | b"c" | b"e").then(|| key.clone()))
		.collect();

	assert_eq!(got, want, "batched reads are out of order or lost a miss");

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn a_reply_lost_after_the_commit_reports_one_success() -> Result {
	let (fake, _server, backend) = rig(256, 1).await?;
	let map = Map::open_remote(&backend, MAP);

	fake.faults(Faults {
		swallow_commit_replies: 1,
		..Faults::default()
	});

	// The first attempt applies and its reply is dropped; the retry carries
	// the same request id and comes back as a duplicate.
	map.insert(&b"once".to_vec(), b"value").await?;

	assert_eq!(fake.applied(), 1, "the batch applied more than once");
	assert_eq!(&*map.get(&b"once".to_vec()).await?, b"value");
	assert_eq!(fake.rows(map_id()), vec![(b"once".to_vec(), b"value".to_vec())]);

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn a_stale_lease_makes_the_backend_read_only() -> Result {
	let (fake, _server, backend) = rig(256, 1).await?;
	let map = Map::open_remote(&backend, MAP);

	map.insert(&b"before".to_vec(), b"value").await?;
	assert!(backend.is_writable(), "the lease is held before the fault");

	fake.faults(Faults { stale_lease: true, ..Faults::default() });

	let watcher = map.watch_raw_prefix(b"after");
	futures::pin_mut!(watcher);

	let mut txn = Txn::new_with_sink(Sink::Remote(backend.clone()));
	txn.insert_raw(&map, b"after", b"value");
	let error = txn
		.execute()
		.await
		.expect_err("a stale lease refuses the commit");
	assert!(error.to_string().contains("bridge commit"), "{error}");

	assert!(watcher.now_or_never().is_none(), "watchers fired for a refused commit");
	assert!(
		!backend.is_writable(),
		"the backend still accepts writes after losing the lease"
	);
	assert!(!backend.lease_status().held, "the lease still reports itself held");
	assert_eq!(fake.applied(), 1, "the refused commit applied anyway");

	// Every later write fails fast, without reaching the bridge.
	let error = map
		.insert(&b"later".to_vec(), b"value")
		.await
		.expect_err("writes stay refused");
	assert!(error.to_string().contains("writer lease"), "{error}");
	assert_eq!(fake.applied(), 1);

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn a_failed_renewal_makes_the_lease_uncertain_and_commits_fail_fast() -> Result {
	// A one-second lease renews three times a second, so one failing renewal
	// is observed within a fraction of the test's patience.
	let fake = Fake::start().await?;
	let raw_config = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_backend", "d1"))
		.merge(("d1_bridge_url", &fake.url))
		.merge(("d1_bridge_token", TOKEN))
		.merge(("d1_lease_ttl_ms", 1_000))
		.merge(("d1_request_timeout_ms", 200))
		.merge(("test", ["fresh", "cleanup"]));

	let server = crate::tests::test_server(&raw_config)?;
	let backend = Backend::open(&server).await?;
	let map = Map::open_remote(&backend, MAP);

	map.insert(&b"before".to_vec(), b"value").await?;

	fake.faults(Faults { fail_renewals: true, ..Faults::default() });

	// Wait for the renewal loop to notice, bounded so a hang fails the test.
	let uncertain = tokio::time::timeout(std::time::Duration::from_secs(10), async {
		while backend.is_writable() {
			tokio::time::sleep(std::time::Duration::from_millis(25)).await;
		}
	})
	.await;

	assert!(uncertain.is_ok(), "a failing renewal never marked the lease uncertain");
	assert!(backend.lease_status().uncertain || !backend.lease_status().held);

	let error = map
		.insert(&b"during".to_vec(), b"value")
		.await
		.expect_err("commits fail fast while the lease is uncertain");
	assert!(error.to_string().contains("writer lease"), "{error}");
	assert_eq!(fake.applied(), 1, "a commit escaped an uncertain lease");

	backend.close().await;

	Ok(())
}

#[tokio::test]
async fn a_second_writer_is_refused_the_lease() -> Result {
	let fake = Fake::start().await?;
	let server = remote_server(&fake.url, 256, 1)?;
	let first = Backend::open(&server).await?;

	// The competing acquisition waits out the incumbent and then gives up:
	// four lease lifetimes is longer than this test may run, so the error is
	// the acquisition patience, not a silent second writer.
	let second =
		tokio::time::timeout(std::time::Duration::from_millis(500), Backend::open(&server)).await;

	assert!(second.is_err(), "a second writer took the lease from a live holder");

	first.close().await;

	// Once released, the successor takes it immediately with a higher epoch.
	let epoch = first.lease_status().epoch;
	let third = Backend::open(&server).await?;
	assert!(third.lease_status().epoch > epoch, "the fencing epoch did not increase");
	third.close().await;

	Ok(())
}
