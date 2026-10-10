//! Shared loopback bridge oracle, compiled only by in-tree test crates.
//!
//! Models ordered bytes, atomic batches, request identity and writer fencing.
//! Delays and faults are synthetic; this does not qualify Cloudflare D1.

use std::{
	collections::{BTreeMap, HashMap},
	net::SocketAddr,
	sync::{Arc, Mutex, PoisonError},
	time::{SystemTime, UNIX_EPOCH},
};

use axum::{
	Router,
	body::Bytes,
	extract::{DefaultBodyLimit, State as AxumState},
	http::{
		HeaderMap, StatusCode,
		header::{AUTHORIZATION, CONTENT_TYPE},
	},
	response::{IntoResponse, Response as HttpResponse},
	routing::post,
};
use tokio::task::JoinHandle;
use tuwunel_bridge::{
	self as bridge, Error as BridgeError, Lease, LeaseState, Mutation, Request, Response,
};
use tuwunel_core::Result;

/// The bearer token every fake-bridge test presents.
pub const TOKEN: &str = "fake-bridge-token";

/// Faults the fake injects, all off by default.
#[derive(Clone, Copy, Debug, Default)]
pub struct Faults {
	/// Apply this many further commits and then answer `500` instead of the
	/// reply, exactly like a reply lost after the batch became durable.
	pub swallow_commit_replies: u32,
	/// Answer every renewal with `500`.
	pub fail_renewals: bool,
	/// Answer every fenced operation with [`BridgeError::StaleLease`].
	pub stale_lease: bool,
	/// Protocol version reported by `Hello`.
	pub protocol: Option<u32>,
	/// D1 schema version reported by `Hello`.
	pub schema_version: Option<u32>,
	/// Refuse oversized Get replies in band, like the bounded Worker.
	/// Off exercises compatibility with an older, oversized HTTP reply.
	pub bounded_get: bool,
	/// Explicit rejection before any mutation, distinct from storage ambiguity.
	pub reject_commit: bool,
	/// Refuse only commits touching this catalog map, before any mutation.
	pub reject_map: Option<u16>,
	/// Apply, then fail the batch-response/digest recovery path in band.
	pub ambiguous_commit: bool,
	/// Refuse the batch after the fence, as SQLite refuses a constraint
	/// violation: nothing applies and the storage class decides the outcome.
	pub storage_rejection: bool,
	/// Lose every commit response, including deduplicated retry replies.
	pub lose_all_commit_replies: bool,
	/// Return a valid but wrong reply variant after applying the commit.
	pub wrong_commit_reply: bool,
	/// Fill scan pages by a running byte sum, like the bounded Worker.
	/// Off exercises compatibility with an older, row-count-only Worker.
	pub bounded_scan: bool,
	/// Answer this many further handshakes with `503`, like a bridge host the
	/// Worker has not routed yet.
	pub fail_hellos: u32,
}

/// The fake's durable state: the kv table, the lease row, the commits table.
#[derive(Default)]
struct Tables {
	kv: BTreeMap<(u16, Vec<u8>), Vec<u8>>,
	lease: Option<LeaseState>,
	commits: HashMap<Vec<u8>, Vec<u8>>,
	/// Commits that actually applied; the duplicate test asserts it stays 1.
	applied: u32,
	/// Rows every scan page has returned; the scan-bound cases count reads.
	served: usize,
}

/// Shared handler state.
struct Shared {
	tables: Mutex<Tables>,
	faults: Mutex<Faults>,
	commit_gate: Mutex<Option<Arc<CommitGate>>>,
	release_gate: Mutex<Option<Arc<ReleaseGate>>>,
	accepted: Mutex<Option<JoinHandle<()>>>,
}

impl Shared {
	fn retain_accepted(&self, task: JoinHandle<()>) {
		let mut slot = self.accepted.lock().expect("accepted task");
		assert!(
			slot.as_ref().is_none_or(JoinHandle::is_finished),
			"only one owned delayed commit"
		);
		*slot = Some(task);
	}
}

/// Controllable lease-release boundary.
#[derive(Default)]
pub struct ReleaseGate {
	/// The release request has reached the server.
	pub entered: tokio::sync::Notify,
	/// Permit the server to finish the held request.
	pub release: tokio::sync::Notify,
}

/// Stops an already-dispatched commit after the backend's drain, before apply.
#[derive(Default)]
pub struct CommitGate {
	/// The selected boundary has been reached.
	pub entered: tokio::sync::Notify,
	/// Permit application or acknowledgement to continue.
	pub release: tokio::sync::Notify,
	/// The request was applied and its reply boundary released.
	pub finished: tokio::sync::Notify,
	map: Option<u16>,
	stage: CommitStage,
}

/// Server boundary at which an already-dispatched commit pauses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CommitStage {
	/// The request is received but has not mutated the oracle.
	#[default]
	BeforeApply,
	/// The batch has applied but its acknowledgement has not been returned.
	BeforeReply,
}

impl CommitGate {
	async fn pause_at(&self, stage: CommitStage) {
		if self.stage == stage {
			self.entered.notify_one();
			self.release.notified().await;
		}
	}
}

/// One running fake bridge.
pub struct Fake {
	shared: Arc<Shared>,
	task: JoinHandle<()>,
	/// Base URL to configure `d1_bridge_url` with.
	pub url: String,
}

impl Fake {
	/// Starts a fake bridge on an ephemeral loopback port.
	pub async fn start() -> Result<Self> {
		let shared = Arc::new(Shared {
			tables: Mutex::new(Tables::default()),
			faults: Mutex::new(Faults::default()),
			commit_gate: Mutex::new(None),
			release_gate: Mutex::new(None),
			accepted: Mutex::new(None),
		});

		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
		let addr: SocketAddr = listener.local_addr()?;
		let app = Router::new()
			.route(bridge::PATH_KV, post(kv))
			.layer(DefaultBodyLimit::max(bridge::request::MAX_BYTES))
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
	pub fn faults(&self, faults: Faults) {
		*self
			.shared
			.faults
			.lock()
			.unwrap_or_else(PoisonError::into_inner) = faults;
	}

	/// Hold the next dispatched commit before applying it.
	#[must_use]
	pub fn pause_commit(&self) -> Arc<CommitGate> {
		let gate = Arc::new(CommitGate::default());
		*self
			.shared
			.commit_gate
			.lock()
			.unwrap_or_else(PoisonError::into_inner) = Some(gate.clone());
		gate
	}

	/// Hold the next commit touching `map` at the selected server boundary.
	#[must_use]
	pub fn pause_commit_on(&self, map: u16, stage: CommitStage) -> Arc<CommitGate> {
		let gate = Arc::new(CommitGate {
			map: Some(map),
			stage,
			..CommitGate::default()
		});
		*self
			.shared
			.commit_gate
			.lock()
			.expect("commit gate") = Some(gate.clone());
		gate
	}

	/// Hold the next lease release before applying it.
	#[must_use]
	pub fn pause_release(&self) -> Arc<ReleaseGate> {
		let gate = Arc::new(ReleaseGate::default());
		*self
			.shared
			.release_gate
			.lock()
			.expect("release gate") = Some(gate.clone());
		gate
	}

	/// Number of commits the fake actually applied.
	pub fn applied(&self) -> u32 {
		self.shared
			.tables
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.applied
	}

	/// Rows every scan page so far has returned.
	pub fn served(&self) -> usize {
		self.shared
			.tables
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.served
	}

	/// Writes rows straight into the kv table, as an earlier writer would
	/// have, without a commit per row.
	pub fn fill(&self, map: u16, rows: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>) {
		let mut tables = self
			.shared
			.tables
			.lock()
			.unwrap_or_else(PoisonError::into_inner);

		for (key, val) in rows {
			tables.kv.insert((map, key), val);
		}
	}

	/// Every row of one map, in bytewise key order.
	pub fn rows(&self, map: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
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
	fn drop(&mut self) {
		let accepted = self
			.shared
			.accepted
			.lock()
			.expect("accepted task")
			.take();
		if let Some(task) = accepted {
			task.abort();
		}
		self.task.abort();
	}
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
		.get(AUTHORIZATION)
		.and_then(|value| value.to_str().ok())
		.is_some_and(|value| value == format!("Bearer {TOKEN}"));

	if !authorized {
		return StatusCode::UNAUTHORIZED.into_response();
	}

	let Ok(request) = bridge::decode::<Request>(&body) else {
		return StatusCode::BAD_REQUEST.into_response();
	};
	if matches!(request, Request::Hello) {
		let mut faults = shared
			.faults
			.lock()
			.unwrap_or_else(PoisonError::into_inner);
		if faults.fail_hellos > 0 {
			faults.fail_hellos = faults.fail_hellos.saturating_sub(1);
			return StatusCode::SERVICE_UNAVAILABLE.into_response();
		}
	}
	let gate = if let Request::Commit { ops, .. } = &request {
		let mut slot = shared.commit_gate.lock().expect("commit gate");
		if slot.as_ref().is_some_and(|gate| {
			gate.map
				.is_none_or(|map| ops.iter().any(|op| op.map() == map))
		}) {
			slot.take()
		} else {
			None
		}
	} else {
		None
	};

	if matches!(request, Request::LeaseRelease { .. }) {
		let gate = shared
			.release_gate
			.lock()
			.expect("release gate")
			.take();
		if let Some(gate) = gate {
			gate.entered.notify_one();
			gate.release.notified().await;
		}
	}

	let response = if let Some(gate) = gate {
		gated_apply(&shared, request.clone(), gate).await
	} else {
		apply(&shared, &request)
	};
	if matches!(request, Request::Commit { .. }) {
		let faults = *shared
			.faults
			.lock()
			.unwrap_or_else(PoisonError::into_inner);
		if faults.lose_all_commit_replies {
			return StatusCode::INTERNAL_SERVER_ERROR.into_response();
		}
		if faults.wrong_commit_reply {
			return (
				[(CONTENT_TYPE, bridge::CONTENT_TYPE)],
				bridge::encode(&Response::Released).expect("encode wrong reply"),
			)
				.into_response();
		}
	}
	match response {
		| Ok(response) => match bridge::encode(&response) {
			| Ok(body) => ([(CONTENT_TYPE, bridge::CONTENT_TYPE)], body).into_response(),
			| Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
		},
		// The reply is lost; whatever the request already changed stays
		// changed. This is the ambiguous outcome the request id exists for.
		| Err(status) => status.into_response(),
	}
}

/// A received gated commit can apply after its HTTP caller disconnects. The
/// fixture owns at most one such task and aborts it when the fixture drops.
async fn gated_apply(
	shared: &Arc<Shared>,
	request: Request,
	gate: Arc<CommitGate>,
) -> Result<Response, StatusCode> {
	let (reply, response) = tokio::sync::oneshot::channel();
	let owned = shared.clone();
	let task = tokio::spawn(async move {
		gate.pause_at(CommitStage::BeforeApply).await;
		let result = apply(&owned, &request);
		gate.pause_at(CommitStage::BeforeReply).await;
		gate.finished.notify_one();
		reply.send(result).ok();
	});
	shared.retain_accepted(task);
	response
		.await
		.unwrap_or(Err(StatusCode::INTERNAL_SERVER_ERROR))
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

		| Request::Get { map, keys } => {
			if faults.bounded_get {
				let size = keys.iter().fold(0_usize, |size, key| {
					size.saturating_add(bridge::response::ROW_OVERHEAD)
						.saturating_add(
							tables
								.kv
								.get(&(*map, key.to_vec()))
								.map_or(0, Vec::len),
						)
				});
				if size > bridge::response::DATA_BYTES {
					return Ok(Response::Error(bridge::response::too_large()));
				}
			}
			Ok(Response::Got {
				vals: keys
					.iter()
					.map(|key| {
						tables
							.kv
							.get(&(*map, key.to_vec()))
							.map(|val| serde_bytes::ByteBuf::from(val.clone()))
					})
					.collect(),
			})
		},

		| Request::Scan {
			map,
			reverse,
			from,
			inclusive,
			limit,
			lease,
		} => Ok(scan(
			&mut tables,
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
	tables: &mut Tables,
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
	let mut more = items.len() > limit;
	items.truncate(limit);

	if faults.bounded_scan {
		let mut bytes: usize = 0;
		let fit = items
			.iter()
			.take_while(|(key, val)| {
				bytes = bytes
					.saturating_add(bridge::response::ROW_OVERHEAD)
					.saturating_add(key.len())
					.saturating_add(val.len());

				bytes <= bridge::response::DATA_BYTES
			})
			.count()
			.max(1);

		more |= fit < items.len();
		items.truncate(fit);
	}

	tables.served = tables.served.saturating_add(items.len());

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
	if faults.reject_commit
		|| faults
			.reject_map
			.is_some_and(|map| ops.iter().any(|op| op.map() == map))
	{
		return Ok(Response::Error(BridgeError::Invalid("test refusal".into())));
	}
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

	if faults.storage_rejection {
		return Ok(Response::Error(BridgeError::Storage("constraint".into())));
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
	if faults.ambiguous_commit {
		return Ok(Response::Error(BridgeError::Storage("network".into())));
	}

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
