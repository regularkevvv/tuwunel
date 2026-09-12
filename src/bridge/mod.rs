//! Wire contract of the private Worker↔Container bridge (ADR-0012).
//!
//! The homeserver Container reaches D1 and R2 only through the edge Worker.
//! This crate is the closed protocol both sides speak: named reads, bounded
//! scans, lease operations, typed atomic commits, and media object
//! operations. Nothing here carries SQL, table names, or free-form object
//! keys, and nothing here performs I/O: it is types, limits, the CBOR codec,
//! the commit digest, and the media-key validator, shared by the Worker
//! (compiled to `wasm32`) and by `RemoteD1Backend` in the homeserver.
//!
//! Versioning: [`PROTOCOL_VERSION`] is the major version and appears in the
//! request path ([`PATH_KV`], [`PATH_MEDIA`]). Fields added within a major
//! version are optional or defaulted so a Worker and a Container one release
//! apart interoperate (the Worker activates before the Container rollout
//! completes, ADR-0006).

#![deny(missing_docs)]

pub mod catalog;
pub mod request;
pub mod response;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha2::{Digest, Sha256};

/// Major protocol version; also the `v<N>` path segment.
pub const PROTOCOL_VERSION: u32 = 1;

/// D1 schema version this protocol version expects (`schema_version` table,
/// migration `0002_kv_lease_commits.sql`).
pub const SCHEMA_VERSION: u32 = 2;

/// KV, lease, and hello endpoint: `POST` one CBOR [`Request`], receive one
/// CBOR [`Response`].
pub const PATH_KV: &str = "/_bridge/v1/kv";

/// Media endpoint prefix: `PUT|GET|HEAD|DELETE /_bridge/v1/media/<key>` and
/// `GET /_bridge/v1/media?prefix=&cursor=&limit=`.
pub const PATH_MEDIA: &str = "/_bridge/v1/media";

/// Every request path under this prefix belongs to the bridge and is served
/// only to an authenticated Container on an allowed host (ADR-0012).
pub const PATH_PREFIX: &str = "/_bridge/";

/// Content type of CBOR bodies.
pub const CONTENT_TYPE: &str = "application/cbor";

/// Virtual hostname the Container uses by default; the Worker's outbound
/// interception routes it back into the Worker.
pub const DEFAULT_HOST: &str = "bridge.internal";

/// Environment variable naming the bridge base URL inside the Container.
pub const ENV_URL: &str = "BRIDGE_URL";

/// Environment variable carrying the bearer token inside the Container.
pub const ENV_TOKEN: &str = "BRIDGE_TOKEN";

/// D1: bound parameters per statement
/// (developers.cloudflare.com/d1/platform/limits).
pub const D1_MAX_PARAMS: usize = 100;

/// D1: bytes per row (string/BLOB/row).
pub const D1_MAX_ROW_BYTES: usize = 2_000_000;

/// D1: queries per Worker invocation (Workers Paid); every statement of a
/// batch counts.
pub const D1_MAX_QUERIES: usize = 1_000;

/// Keys per [`Request::Get`]; the Worker splits them into statements of
/// [`GET_KEYS_PER_STATEMENT`].
pub const MAX_GET_KEYS: usize = 900;

/// Keys bound per `IN (...)` statement (one parameter is the map id).
pub const GET_KEYS_PER_STATEMENT: usize = D1_MAX_PARAMS - 1;

/// Rows per scan page.
pub const MAX_SCAN_PAGE: u32 = 1_000;

/// Mutations per commit; one statement each plus the fenced `commits` row,
/// inside the per-invocation query budget.
pub const MAX_COMMIT_OPS: usize = 900;

/// Longest key accepted, in bytes.
pub const MAX_KEY_BYTES: usize = 16 * 1024;

/// Longest value accepted, in bytes: the D1 row limit less the key and the
/// map id, with margin.
pub const MAX_VALUE_BYTES: usize = D1_MAX_ROW_BYTES - MAX_KEY_BYTES - 1_024;

/// Largest media object accepted by one `PUT`.
pub const MAX_MEDIA_BYTES: u64 = 200 * 1024 * 1024;

/// Longest media key, in bytes.
pub const MAX_MEDIA_KEY_BYTES: usize = 512;

/// Objects per media listing page.
pub const MAX_MEDIA_LIST: u32 = 1_000;

/// Retention of `commits` rows; pruned during lease renewal.
pub const COMMIT_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;

/// Length of a commit request id, in bytes.
pub const REQUEST_ID_LEN: usize = 16;

/// Length of a commit digest, in bytes.
pub const DIGEST_LEN: usize = 32;

/// The writer identity a mutation or scan carries (ADR-0003).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Lease {
	/// Opaque process identity chosen by the Container at start.
	pub holder: String,
	/// Fencing epoch granted with the lease; strictly increasing.
	pub epoch: u64,
}

/// The lease row as the Worker sees it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LeaseState {
	/// Current holder.
	pub holder: String,
	/// Current epoch.
	pub epoch: u64,
	/// Expiry on the Worker's clock, milliseconds since the Unix epoch.
	pub expires_at_ms: u64,
}

/// One mutation of a commit, in queue order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Mutation {
	/// Insert or replace one key.
	Put {
		/// Stable map id from [`catalog`].
		map: u16,
		/// Encoded key bytes.
		key: ByteBuf,
		/// Encoded value bytes.
		val: ByteBuf,
	},
	/// Remove one key; removing an absent key succeeds.
	Delete {
		/// Stable map id.
		map: u16,
		/// Encoded key bytes.
		key: ByteBuf,
	},
}

impl Mutation {
	/// Map id addressed by this mutation.
	#[must_use]
	pub fn map(&self) -> u16 {
		match self {
			| Self::Put { map, .. } | Self::Delete { map, .. } => *map,
		}
	}

	/// Key bytes addressed by this mutation.
	#[must_use]
	pub fn key(&self) -> &[u8] {
		match self {
			| Self::Put { key, .. } | Self::Delete { key, .. } => key,
		}
	}

	/// Checks the declared map identity and size limits of this mutation.
	pub fn check(&self) -> Result<(), Error> {
		check_map(self.map())?;
		if self.key().is_empty() || self.key().len() > MAX_KEY_BYTES {
			return Err(Error::TooLarge {
				what: "key".into(),
				limit: u64::try_from(MAX_KEY_BYTES).unwrap_or(u64::MAX),
			});
		}
		if let Self::Put { val, .. } = self
			&& val.len() > MAX_VALUE_BYTES
		{
			return Err(Error::TooLarge {
				what: "value".into(),
				limit: u64::try_from(MAX_VALUE_BYTES).unwrap_or(u64::MAX),
			});
		}
		Ok(())
	}
}

/// One request on [`PATH_KV`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Request {
	/// Version handshake and lease/schema inspection.
	Hello,
	/// Point reads; the reply preserves request order.
	Get {
		/// Stable map id.
		map: u16,
		/// Keys to read; at most [`MAX_GET_KEYS`].
		keys: Vec<ByteBuf>,
	},
	/// One page of an ordered scan.
	Scan {
		/// Stable map id.
		map: u16,
		/// Descending key order when true.
		reverse: bool,
		/// Seek position: `key >= from` (forward) or `key <= from` (reverse);
		/// `None` starts at the corresponding end of the map.
		from: Option<ByteBuf>,
		/// Whether `from` itself may be returned. The first page of a scan is
		/// inclusive (seek semantics); continuation pages pass the last key
		/// seen with `inclusive = false`.
		inclusive: bool,
		/// Rows wanted; clamped to [`MAX_SCAN_PAGE`].
		limit: u32,
		/// Lease to recheck on every page; `None` skips the fence (reads by a
		/// process that never writes).
		lease: Option<Lease>,
	},
	/// One atomic multi-map batch.
	Commit {
		/// Client-chosen idempotency key, [`REQUEST_ID_LEN`] random bytes,
		/// reused across retries of the same logical commit.
		request_id: ByteBuf,
		/// The writer's lease; the D1 fence rejects a stale one.
		lease: Lease,
		/// [`digest`] of `ops`; the Worker recomputes and compares it.
		digest: ByteBuf,
		/// Mutations in queue order; at most [`MAX_COMMIT_OPS`].
		ops: Vec<Mutation>,
	},
	/// Take the writer lease (absent, expired, or already ours).
	LeaseAcquire {
		/// Process identity.
		holder: String,
		/// Requested lifetime.
		ttl_ms: u64,
	},
	/// Extend the exact lease.
	LeaseRenew {
		/// The lease to extend.
		lease: Lease,
		/// New lifetime from now.
		ttl_ms: u64,
	},
	/// Expire the exact lease now.
	LeaseRelease {
		/// The lease to release.
		lease: Lease,
	},
}

/// One reply on [`PATH_KV`]; errors are in band.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Response {
	/// Reply to [`Request::Hello`].
	Hello {
		/// Worker's [`PROTOCOL_VERSION`].
		protocol: u32,
		/// Applied D1 schema version, `0` when no migration has run.
		schema_version: u32,
		/// Current lease row, if any.
		lease: Option<LeaseState>,
		/// Worker clock, milliseconds since the Unix epoch.
		now_ms: u64,
	},
	/// Reply to [`Request::Get`]: one entry per requested key, `None` on a
	/// miss.
	Got {
		/// Values in request order.
		vals: Vec<Option<ByteBuf>>,
	},
	/// Reply to [`Request::Scan`].
	Scanned {
		/// Rows in scan order.
		items: Vec<(ByteBuf, ByteBuf)>,
		/// A row or byte boundary was reached; more rows may follow.
		more: bool,
	},
	/// Reply to [`Request::Commit`]: durably committed.
	Committed {
		/// A previous attempt with this request id had already committed the
		/// identical batch.
		duplicate: bool,
	},
	/// Reply to lease acquisition or renewal.
	Leased {
		/// Granted epoch.
		epoch: u64,
		/// Expiry on the Worker's clock.
		expires_at_ms: u64,
		/// Worker clock at reply time.
		now_ms: u64,
	},
	/// Reply to [`Request::LeaseRelease`].
	Released,
	/// Operation refused or failed.
	Error(Error),
}

/// Bridge-level failure, classified for the client's retry policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Error {
	/// The lease presented is not the current unexpired one. Writes must stop.
	StaleLease {
		/// What D1 currently holds.
		current: Option<LeaseState>,
	},
	/// Acquisition refused: another holder's lease has not expired.
	LeaseHeld {
		/// The competing lease.
		current: LeaseState,
	},
	/// A commit with this request id exists with a different digest.
	DigestMismatch,
	/// A protocol limit was exceeded. Atomic commits are never split;
	/// read callers may reduce a batch after a response-byte refusal.
	TooLarge {
		/// Which limit.
		what: String,
		/// The limit's value.
		limit: u64,
	},
	/// Malformed or unsupported request (also protocol/schema mismatches).
	Invalid(String),
	/// D1/R2 failed; the message is redacted to a class, never raw storage
	/// text with user data.
	Storage(String),
}

impl core::fmt::Display for Error {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		match self {
			| Self::StaleLease { current } => write!(f, "stale lease (current: {current:?})"),
			| Self::LeaseHeld { current } => write!(f, "lease held by {current:?}"),
			| Self::DigestMismatch => f.write_str("request id reused with a different batch"),
			| Self::TooLarge { what, limit } => write!(f, "{what} exceeds the limit of {limit}"),
			| Self::Invalid(why) => write!(f, "invalid request: {why}"),
			| Self::Storage(why) => write!(f, "storage failure: {why}"),
		}
	}
}

impl std::error::Error for Error {}

/// One media listing page (`GET /_bridge/v1/media?prefix=…`).
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MediaList {
	/// Objects under the prefix, in key order.
	pub objects: Vec<MediaObject>,
	/// Opaque continuation cursor; `None` when the listing is complete.
	pub cursor: Option<String>,
}

/// Metadata of one media object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MediaObject {
	/// Object key.
	pub key: String,
	/// Size in bytes.
	pub size: u64,
	/// Last modification, milliseconds since the Unix epoch.
	pub modified_ms: u64,
	/// Content type recorded at upload, if any.
	pub content_type: Option<String>,
	/// Entity tag.
	pub etag: String,
}

/// Encodes one message as CBOR.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
	minicbor_serde::to_vec(value).map_err(|e| Error::Invalid(format!("encode: {e}")))
}

/// Decodes one CBOR message.
pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, Error> {
	minicbor_serde::from_slice(bytes).map_err(|e| Error::Invalid(format!("decode: {e}")))
}

/// Canonical SHA-256 digest of a commit's mutations.
///
/// Per op, in order: one kind byte (`0` put, `1` delete), the map id
/// big-endian, the key length as big-endian `u32` and the key, and for puts
/// the value length as big-endian `u32` and the value. Both sides compute
/// it; the Worker stores it in `commits` so a retried request id can be
/// verified to carry the identical batch.
#[must_use]
pub fn digest(ops: &[Mutation]) -> [u8; DIGEST_LEN] {
	let mut hasher = Sha256::new();
	for op in ops {
		match op {
			| Mutation::Put { map, key, val } => {
				hasher.update([0_u8]);
				hasher.update(map.to_be_bytes());
				hasher.update(len_be(key));
				hasher.update(key);
				hasher.update(len_be(val));
				hasher.update(val);
			},
			| Mutation::Delete { map, key } => {
				hasher.update([1_u8]);
				hasher.update(map.to_be_bytes());
				hasher.update(len_be(key));
				hasher.update(key);
			},
		}
	}
	hasher.finalize().into()
}

fn len_be(bytes: &[u8]) -> [u8; 4] {
	u32::try_from(bytes.len())
		.unwrap_or(u32::MAX)
		.to_be_bytes()
}

/// Validates a media object key.
///
/// Accepts 1–[`MAX_MEDIA_KEY_BYTES`] bytes of `[A-Za-z0-9._-]` with `/` as
/// the separator and no empty, `.` or `..` segments. Keys are opaque names
/// chosen by the homeserver (ADR-0005), never client input, but the Worker
/// still refuses anything else.
#[must_use]
pub fn media_key(key: &str) -> bool {
	if key.is_empty() || key.len() > MAX_MEDIA_KEY_BYTES {
		return false;
	}
	if !key
		.bytes()
		.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'/'))
	{
		return false;
	}
	key.split('/')
		.all(|segment| !matches!(segment, "" | "." | ".."))
}

/// Checks the shape of a request against the protocol limits.
pub fn check(request: &Request) -> Result<(), Error> {
	let too_many = |what: &str, limit: usize| Error::TooLarge {
		what: what.into(),
		limit: u64::try_from(limit).unwrap_or(u64::MAX),
	};
	match request {
		| Request::Get { map, keys } => {
			check_map(*map)?;
			if keys.len() > MAX_GET_KEYS {
				return Err(too_many("keys", MAX_GET_KEYS));
			}
			if keys
				.iter()
				.any(|k| k.is_empty() || k.len() > MAX_KEY_BYTES)
			{
				return Err(too_many("key", MAX_KEY_BYTES));
			}
		},
		| Request::Scan { map, from, limit, .. } => {
			check_map(*map)?;
			if from
				.as_ref()
				.is_some_and(|f| f.len() > MAX_KEY_BYTES)
			{
				return Err(too_many("key", MAX_KEY_BYTES));
			}
			if *limit == 0 {
				return Err(Error::Invalid("scan limit is zero".into()));
			}
		},
		| Request::Commit { request_id, digest: d, ops, .. } => {
			if request_id.len() != REQUEST_ID_LEN {
				return Err(Error::Invalid("request id length".into()));
			}
			if d.len() != DIGEST_LEN {
				return Err(Error::Invalid("digest length".into()));
			}
			if ops.is_empty() {
				return Err(Error::Invalid("empty commit".into()));
			}
			if ops.len() > MAX_COMMIT_OPS {
				return Err(too_many("ops", MAX_COMMIT_OPS));
			}
			for op in ops {
				op.check()?;
			}
			if d.as_slice() != digest(ops) {
				return Err(Error::Invalid("digest does not match ops".into()));
			}
		},
		| Request::LeaseAcquire { holder, ttl_ms }
		| Request::LeaseRenew { lease: Lease { holder, .. }, ttl_ms } => {
			if holder.is_empty() || holder.len() > 128 {
				return Err(Error::Invalid("holder length".into()));
			}
			if *ttl_ms == 0 || *ttl_ms > 5 * 60 * 1000 {
				return Err(Error::Invalid("ttl out of range".into()));
			}
		},
		| Request::Hello | Request::LeaseRelease { .. } => {},
	}
	request::check_size(request)
}

fn check_map(map: u16) -> Result<(), Error> {
	if catalog::contains(map) {
		Ok(())
	} else {
		Err(Error::Invalid("unknown map id".into()))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn bytes(b: &[u8]) -> ByteBuf { ByteBuf::from(b.to_vec()) }

	#[test]
	fn aggregate_request_bytes_are_bounded() {
		let request = Request::Get {
			map: 0,
			keys: vec![bytes(&vec![1; MAX_KEY_BYTES]); 300],
		};
		assert!(
			matches!(check(&request), Err(Error::TooLarge { .. })),
			"aggregate keys were accepted"
		);
		let ops = vec![
			Mutation::Put {
				map: 0,
				key: bytes(b"k"),
				val: bytes(&vec![1; MAX_VALUE_BYTES])
			};
			3
		];
		let request = Request::Commit {
			request_id: bytes(&[1; REQUEST_ID_LEN]),
			lease: Lease {
				holder: "request-budget".into(),
				epoch: 1,
			},
			digest: bytes(&digest(&ops)),
			ops,
		};
		assert!(
			matches!(check(&request), Err(Error::TooLarge { .. })),
			"aggregate values were accepted"
		);
	}

	#[test]
	fn map_catalog_is_unique_and_exactly_closed() {
		let mut ids = std::collections::BTreeSet::new();
		let mut names = std::collections::BTreeSet::new();
		for (name, id) in catalog::MAP_IDS {
			assert!(ids.insert(id.0), "duplicate map identifier");
			assert!(names.insert(*name), "duplicate map name");
			assert_eq!(catalog::map_id(name), Some(*id));
			check(&Request::Get { map: id.0, keys: vec![bytes(b"k")] }).expect("known map read");
			check(&Request::Scan {
				map: id.0,
				reverse: false,
				from: None,
				inclusive: true,
				limit: 1,
				lease: None,
			})
			.expect("known map scan");
			Mutation::Put {
				map: id.0,
				key: bytes(b"k"),
				val: bytes(b"v"),
			}
			.check()
			.expect("known map put");
			Mutation::Delete { map: id.0, key: bytes(b"k") }
				.check()
				.expect("known map delete");
		}
		for id in 0..=u16::MAX {
			assert_eq!(catalog::contains(id), ids.contains(&id));
		}
	}

	#[test]
	fn unknown_maps_are_rejected_on_every_kv_path() {
		// the first identifier past the append-only catalog, whatever its length
		let next = catalog::MAP_IDS
			.iter()
			.map(|(_, id)| id.0)
			.max()
			.expect("catalog is not empty")
			.saturating_add(1);
		for map in [next, 900, 901, u16::MAX] {
			let lease = Lease { holder: "map-check".into(), epoch: 1 };
			let mut requests =
				vec![Request::Get { map, keys: vec![bytes(b"k")] }, Request::Scan {
					map,
					reverse: false,
					from: None,
					inclusive: true,
					limit: 1,
					lease: None,
				}];
			for op in
				[Mutation::Put { map, key: bytes(b"k"), val: bytes(b"v") }, Mutation::Delete {
					map,
					key: bytes(b"k"),
				}] {
				let ops = vec![op];
				requests.push(Request::Commit {
					request_id: bytes(&[1; REQUEST_ID_LEN]),
					lease: lease.clone(),
					digest: bytes(&digest(&ops)),
					ops,
				});
			}
			for request in requests {
				assert!(
					matches!(check(&request), Err(Error::Invalid(_))),
					"unknown map {map} was accepted"
				);
			}
		}
	}

	#[test]
	fn roundtrip_every_request() {
		let ops = vec![
			Mutation::Put {
				map: 41,
				key: bytes(b"k\x00\xff"),
				val: bytes(b""),
			},
			Mutation::Delete { map: 0, key: bytes(&[0xFF; 64]) },
		];
		let d = digest(&ops);
		let reqs = vec![
			Request::Hello,
			Request::Get {
				map: 17,
				keys: vec![bytes(b"a"), bytes(b"\x00")],
			},
			Request::Scan {
				map: 1,
				reverse: true,
				from: Some(bytes(b"\xff")),
				inclusive: false,
				limit: 10,
				lease: Some(Lease { holder: "h".into(), epoch: 3 }),
			},
			Request::Commit {
				request_id: bytes(&[7; REQUEST_ID_LEN]),
				lease: Lease { holder: "h".into(), epoch: 3 },
				digest: bytes(&d),
				ops,
			},
			Request::LeaseAcquire { holder: "h".into(), ttl_ms: 15_000 },
			Request::LeaseRenew {
				lease: Lease { holder: "h".into(), epoch: 3 },
				ttl_ms: 15_000,
			},
			Request::LeaseRelease {
				lease: Lease { holder: "h".into(), epoch: 3 },
			},
		];
		for req in reqs {
			check(&req).expect("well formed");
			let wire = encode(&req).expect("encode");
			let back: Request = decode(&wire).expect("decode");
			assert_eq!(back, req);
			assert_eq!(request::encode(&req).expect("bounded encode"), wire);
			assert_eq!(request::decode(&wire).expect("bounded decode"), req);
		}
	}

	#[test]
	fn roundtrip_every_response() {
		let state = LeaseState {
			holder: "h".into(),
			epoch: 1,
			expires_at_ms: 5,
		};
		let resps = vec![
			Response::Hello {
				protocol: 1,
				schema_version: 2,
				lease: Some(state.clone()),
				now_ms: 1,
			},
			Response::Got { vals: vec![None, Some(bytes(b"v"))] },
			Response::Scanned {
				items: vec![(bytes(b"k"), bytes(b"v"))],
				more: true,
			},
			Response::Committed { duplicate: false },
			Response::Leased { epoch: 2, expires_at_ms: 9, now_ms: 3 },
			Response::Released,
			Response::Error(Error::StaleLease { current: None }),
			Response::Error(Error::LeaseHeld { current: state }),
			Response::Error(Error::DigestMismatch),
			Response::Error(Error::TooLarge { what: "ops".into(), limit: 900 }),
			Response::Error(Error::Invalid("x".into())),
			Response::Error(Error::Storage("y".into())),
		];
		for resp in resps {
			let wire = encode(&resp).expect("encode");
			let back: Response = decode(&wire).expect("decode");
			assert_eq!(back, resp);
		}
	}

	#[test]
	fn bytes_encode_as_cbor_byte_strings() {
		// A 3-byte key must not become a CBOR array of three integers.
		let req = Request::Get { map: 0, keys: vec![bytes(b"abc")] };
		let wire = encode(&req).expect("encode");
		assert!(
			wire.windows(4)
				.any(|w| w == [0x43, b'a', b'b', b'c']),
			"{wire:02x?}"
		);
	}

	#[test]
	fn digest_is_canonical_and_order_sensitive() {
		let a = Mutation::Put {
			map: 1,
			key: bytes(b"k"),
			val: bytes(b"v"),
		};
		let b = Mutation::Delete { map: 1, key: bytes(b"k") };
		assert_ne!(digest(&[a.clone(), b.clone()]), digest(&[b.clone(), a.clone()]));
		assert_eq!(digest(std::slice::from_ref(&a)), digest(std::slice::from_ref(&a)));
		// Length framing: ("k","v") and ("kv","") must differ.
		let c = Mutation::Put {
			map: 1,
			key: bytes(b"kv"),
			val: bytes(b""),
		};
		assert_ne!(digest(&[a]), digest(&[c]));
		assert_ne!(digest(&[]), digest(&[b]));
	}

	#[test]
	fn digest_test_vector_is_frozen() {
		// Any change here is a protocol change: bump PROTOCOL_VERSION.
		let ops = [Mutation::Put {
			map: 0x0102,
			key: bytes(b"k"),
			val: bytes(b"v"),
		}];
		let mut h = Sha256::new();
		h.update([0, 1, 2, 0, 0, 0, 1, b'k', 0, 0, 0, 1, b'v']);
		let expect: [u8; 32] = h.finalize().into();
		assert_eq!(digest(&ops), expect);
	}

	#[test]
	fn limits_are_enforced_never_split() {
		let lease = Lease { holder: "h".into(), epoch: 1 };
		let ops: Vec<_> = (0..=MAX_COMMIT_OPS)
			.map(|i| Mutation::Delete { map: 0, key: bytes(&i.to_be_bytes()) })
			.collect();
		let d = digest(&ops);
		let req = Request::Commit {
			request_id: bytes(&[0; REQUEST_ID_LEN]),
			lease: lease.clone(),
			digest: bytes(&d),
			ops,
		};
		assert!(matches!(check(&req), Err(Error::TooLarge { .. })));

		let big = Mutation::Put {
			map: 0,
			key: bytes(b"k"),
			val: bytes(&vec![0; MAX_VALUE_BYTES + 1]),
		};
		assert!(matches!(big.check(), Err(Error::TooLarge { .. })));

		let keys = vec![bytes(b"k"); MAX_GET_KEYS + 1];
		assert!(matches!(check(&Request::Get { map: 0, keys }), Err(Error::TooLarge { .. })));

		let wrong = Request::Commit {
			request_id: bytes(&[0; REQUEST_ID_LEN]),
			lease,
			digest: bytes(&[0; DIGEST_LEN]),
			ops: vec![Mutation::Delete { map: 0, key: bytes(b"k") }],
		};
		assert!(matches!(check(&wrong), Err(Error::Invalid(_))));
	}

	#[test]
	fn media_keys() {
		assert!(media_key("ab/cd-ef_01.bin"));
		assert!(media_key("x"));
		assert!(!media_key(""));
		assert!(!media_key("/abs"));
		assert!(!media_key("a//b"));
		assert!(!media_key("a/../b"));
		assert!(!media_key("a/./b"));
		assert!(!media_key("a b"));
		assert!(!media_key("a?b"));
		assert!(!media_key("ünï"));
		assert!(!media_key(&"a".repeat(MAX_MEDIA_KEY_BYTES + 1)));
	}
}
