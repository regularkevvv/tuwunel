//! Unit tests of the storage providers.
//!
//! `chunked` is exercised directly. The R2 bridge provider is exercised
//! against a fake bridge media server (ADR-0012) bound to an ephemeral
//! loopback port: it holds objects in memory, honors `Range`, pages listings
//! with a deliberately tiny page so the client must follow a cursor, answers
//! `404` for a missing object and `401` without the bearer token, and records
//! every request it saw. The client's wire behavior is therefore tested with
//! no Worker, no bucket, and no network beyond loopback.

use std::{
	collections::BTreeMap,
	iter::repeat_n,
	net::SocketAddr,
	ops::Range,
	sync::{Arc, Mutex},
	time::Duration,
};

use axum::{
	Router,
	body::Body,
	extract::State,
	http::{HeaderMap, HeaderValue, Method, Response as HttpResponse, StatusCode, Uri, header},
	response::Response,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::{
	Attribute, AttributeValue, Attributes, Error, GetOptions, MultipartUpload, ObjectMeta,
	ObjectStore, ObjectStoreExt, PutOptions, PutPayload, path::Path,
};
use tokio::{net::TcpListener, spawn};
use tuwunel_bridge::{
	CONTENT_TYPE as CBOR, MAX_MEDIA_BYTES, MediaList, MediaObject, PATH_MEDIA, encode, media_key,
};
use url::{Url, form_urlencoded};

use super::{chunked, r2::R2Bridge};

/// Bearer token the fake bridge accepts.
const TOKEN: &str = "bridge-test-token";

/// Objects per listing page. Two forces the client to follow cursors for
/// every fixture in this file.
const PAGE: usize = 2;

/// Timestamp every stored object reports, milliseconds since the Unix epoch.
const MODIFIED_MS: u64 = 1_700_000_000_000;

/// One mebibyte, the block the oversize fixture repeats.
const MIB: usize = 1024 * 1024;

#[test]
fn chunked_splits_into_part_sized_chunks() {
	let payload = PutPayload::from(vec![0_u8; 25]);
	let chunks: Vec<PutPayload> = chunked(payload, 10).collect();

	assert_eq!(chunks.len(), 3);
	assert_eq!(chunks[0].content_length(), 10);
	assert_eq!(chunks[1].content_length(), 10);
	assert_eq!(chunks[2].content_length(), 5);
}

#[test]
fn chunked_aligned_size_yields_no_remainder() {
	let payload = PutPayload::from(vec![0_u8; 30]);
	let chunks: Vec<PutPayload> = chunked(payload, 10).collect();

	assert_eq!(chunks.len(), 3);
	assert!(chunks.iter().all(|c| c.content_length() == 10));
}

#[test]
fn chunked_smaller_than_part_size_yields_one() {
	let payload = PutPayload::from(vec![0_u8; 5]);
	let chunks: Vec<PutPayload> = chunked(payload, 10).collect();

	assert_eq!(chunks.len(), 1);
	assert_eq!(chunks[0].content_length(), 5);
}

#[test]
fn chunked_empty_payload_yields_nothing() {
	let payload = PutPayload::from(Vec::<u8>::new());

	assert!(chunked(payload, 10).next().is_none());
}

#[test]
fn chunked_usize_max_yields_one_part() {
	let payload = PutPayload::from(vec![0_u8; 1024]);
	let chunks: Vec<PutPayload> = chunked(payload, usize::MAX).collect();

	assert_eq!(chunks.len(), 1);
	assert_eq!(chunks[0].content_length(), 1024);
}

/// One object the fake bridge holds.
#[derive(Clone)]
struct Object {
	bytes: Vec<u8>,
	content_type: Option<String>,
	etag: String,
}

/// One request the fake bridge saw.
#[derive(Clone)]
struct Seen {
	method: Method,
	path: String,
	authorization: Option<String>,
}

/// The fake bridge's state.
#[derive(Default)]
struct Bridge {
	objects: Mutex<BTreeMap<String, Object>>,
	seen: Mutex<Vec<Seen>>,
}

impl Bridge {
	/// Every request seen, in order.
	fn requests(&self) -> Vec<Seen> { self.seen.lock().expect("request log").clone() }

	/// How many listing requests (as opposed to object requests) were made.
	fn listings(&self) -> usize {
		self.requests()
			.iter()
			.filter(|seen| seen.path == PATH_MEDIA)
			.count()
	}
}

/// Starts a fake bridge on an ephemeral loopback port and returns its state
/// and base URL.
async fn bridge() -> (Arc<Bridge>, Url) {
	let state = Arc::new(Bridge::default());
	let app = Router::new()
		.fallback(handle)
		.with_state(state.clone());

	let listener = TcpListener::bind("127.0.0.1:0")
		.await
		.expect("bound a loopback listener");

	let addr: SocketAddr = listener.local_addr().expect("listener address");

	spawn(async move {
		axum::serve(listener, app)
			.await
			.expect("fake bridge served");
	});

	(state, Url::parse(&format!("http://{addr}")).expect("bridge URL"))
}

/// A client of the bridge at `url` presenting `token`.
fn store(url: &Url, token: &str) -> R2Bridge {
	R2Bridge::new(url, token, Duration::from_secs(5), "tuwunel-test").expect("client built")
}

/// Stores one object through the client, so fixtures take the same path as
/// the code under test.
async fn seed(store: &R2Bridge, key: &str, bytes: &'static [u8], content_type: &'static str) {
	let mut attributes = Attributes::new();
	attributes.insert(Attribute::ContentType, content_type.into());

	let opts = PutOptions { attributes, ..Default::default() };

	store
		.put_opts(&Path::from(key), PutPayload::from_static(bytes), opts)
		.await
		.expect("fixture stored");
}

/// Reply with no body.
fn empty(status: StatusCode) -> Response {
	HttpResponse::builder()
		.status(status)
		.body(Body::empty())
		.expect("empty response built")
}

/// The whole fake bridge: authentication, then the listing endpoint or one
/// object operation.
async fn handle(
	State(state): State<Arc<Bridge>>,
	method: Method,
	uri: Uri,
	headers: HeaderMap,
	body: Bytes,
) -> Response {
	let path = uri.path().to_owned();
	let authorization = headers
		.get(header::AUTHORIZATION)
		.and_then(|value| value.to_str().ok())
		.map(ToOwned::to_owned);

	state
		.seen
		.lock()
		.expect("request log")
		.push(Seen {
			method: method.clone(),
			path: path.clone(),
			authorization: authorization.clone(),
		});

	if authorization.as_deref() != Some(format!("Bearer {TOKEN}").as_str()) {
		return empty(StatusCode::UNAUTHORIZED);
	}

	if path == PATH_MEDIA {
		return if method == Method::GET {
			list(&state, uri.query())
		} else {
			empty(StatusCode::METHOD_NOT_ALLOWED)
		};
	}

	let Some(key) = path.strip_prefix(format!("{PATH_MEDIA}/").as_str()) else {
		return empty(StatusCode::NOT_FOUND);
	};

	if !media_key(key) {
		return empty(StatusCode::BAD_REQUEST);
	}

	if method == Method::PUT {
		put(&state, key, &headers, &body)
	} else if method == Method::GET {
		object(&state, key, headers.get(header::RANGE))
	} else if method == Method::HEAD {
		object(&state, key, None)
	} else if method == Method::DELETE {
		delete(&state, key)
	} else {
		empty(StatusCode::METHOD_NOT_ALLOWED)
	}
}

/// Stores one object, refusing a body over the protocol limit as the Worker
/// does.
fn put(state: &Bridge, key: &str, headers: &HeaderMap, body: &Bytes) -> Response {
	let len = u64::try_from(body.len()).unwrap_or(u64::MAX);
	if len > MAX_MEDIA_BYTES {
		return empty(StatusCode::PAYLOAD_TOO_LARGE);
	}

	let content_type = headers
		.get(header::CONTENT_TYPE)
		.and_then(|value| value.to_str().ok())
		.map(ToOwned::to_owned);

	let etag = format!("\"{key}-{len}\"");
	let object = Object {
		bytes: body.to_vec(),
		content_type,
		etag: etag.clone(),
	};

	state
		.objects
		.lock()
		.expect("object map")
		.insert(key.to_owned(), object);

	HttpResponse::builder()
		.status(StatusCode::OK)
		.header(header::ETAG, etag)
		.body(Body::empty())
		.expect("put response built")
}

/// Serves one object whole or ranged. A `HEAD` arrives here with no range and
/// its body is discarded by the HTTP layer, leaving the metadata headers.
fn object(state: &Bridge, key: &str, range: Option<&HeaderValue>) -> Response {
	let objects = state.objects.lock().expect("object map");
	let Some(object) = objects.get(key) else {
		return empty(StatusCode::NOT_FOUND);
	};

	let size = u64::try_from(object.bytes.len()).unwrap_or(u64::MAX);
	let mut builder = HttpResponse::builder()
		.header(header::LAST_MODIFIED, http_date(MODIFIED_MS))
		.header(header::ETAG, object.etag.clone());

	if let Some(content_type) = object.content_type.as_deref() {
		builder = builder.header(header::CONTENT_TYPE, content_type);
	}

	let Some(range) = range
		.and_then(|value| value.to_str().ok())
		.and_then(|value| parse_range(value, size))
	else {
		return builder
			.status(StatusCode::OK)
			.body(Body::from(object.bytes.clone()))
			.expect("object response built");
	};

	let first = usize::try_from(range.start).expect("range start fits in memory");
	let end = usize::try_from(range.end).expect("range end fits in memory");
	let last = range.end.saturating_sub(1);
	let part = object
		.bytes
		.get(first..end)
		.expect("range lies within the object")
		.to_vec();

	builder
		.status(StatusCode::PARTIAL_CONTENT)
		.header(header::CONTENT_RANGE, format!("bytes {}-{last}/{size}", range.start))
		.body(Body::from(part))
		.expect("ranged response built")
}

/// Removes one object; removing an absent object is a `404`.
fn delete(state: &Bridge, key: &str) -> Response {
	let removed = state
		.objects
		.lock()
		.expect("object map")
		.remove(key)
		.is_some();

	if removed {
		empty(StatusCode::NO_CONTENT)
	} else {
		empty(StatusCode::NOT_FOUND)
	}
}

/// One CBOR listing page of at most [`PAGE`] objects in key order.
fn list(state: &Bridge, query: Option<&str>) -> Response {
	let mut prefix = None;
	let mut cursor = None;
	for (name, value) in form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
		match name.as_ref() {
			| "prefix" => prefix = Some(value.into_owned()),
			| "cursor" => cursor = Some(value.into_owned()),
			| _ => {},
		}
	}

	let objects = state.objects.lock().expect("object map");
	let keys: Vec<String> = objects
		.keys()
		.filter(|key| {
			prefix
				.as_deref()
				.is_none_or(|prefix| key.starts_with(prefix))
		})
		.filter(|key| {
			cursor
				.as_deref()
				.is_none_or(|cursor| key.as_str() > cursor)
		})
		.cloned()
		.collect();

	let page: Vec<MediaObject> = keys
		.iter()
		.take(PAGE)
		.filter_map(|key| {
			objects.get(key).map(|object| MediaObject {
				key: key.clone(),
				size: u64::try_from(object.bytes.len()).unwrap_or(u64::MAX),
				modified_ms: MODIFIED_MS,
				content_type: object.content_type.clone(),
				etag: object.etag.clone(),
			})
		})
		.collect();

	let cursor = keys
		.len()
		.gt(&page.len())
		.then(|| page.last().map(|object| object.key.clone()))
		.flatten();

	let body = encode(&MediaList { objects: page, cursor }).expect("listing encoded");

	HttpResponse::builder()
		.status(StatusCode::OK)
		.header(header::CONTENT_TYPE, CBOR)
		.body(Body::from(body))
		.expect("listing response built")
}

/// Formats a millisecond timestamp as an IMF-fixdate (RFC 9110 §5.6.7).
fn http_date(ms: u64) -> String {
	let ms = i64::try_from(ms).unwrap_or(i64::MAX);

	DateTime::<Utc>::from_timestamp_millis(ms)
		.expect("timestamp within range")
		.format("%a, %d %b %Y %H:%M:%S GMT")
		.to_string()
}

/// Parses the `Range` forms [`object_store::GetRange`] emits.
fn parse_range(value: &str, size: u64) -> Option<Range<u64>> {
	let spec = value.trim().strip_prefix("bytes=")?;
	let (first, last) = spec.split_once('-')?;

	match (first.trim(), last.trim()) {
		| ("", suffix) => {
			let suffix: u64 = suffix.parse().ok()?;

			Some(size.saturating_sub(suffix)..size)
		},
		| (first, "") => {
			let first: u64 = first.parse().ok()?;

			(first < size).then_some(first..size)
		},
		| (first, last) => {
			let first: u64 = first.parse().ok()?;
			let last: u64 = last.parse().ok()?;
			let end = last.checked_add(1)?.min(size);

			(first < end).then_some(first..end)
		},
	}
}

#[tokio::test]
async fn r2_put_and_get_round_trip_bytes_and_content_type() {
	let (_bridge, url) = bridge().await;
	let store = store(&url, TOKEN);
	let path = Path::from("object");

	seed(&store, "object", b"payload", "image/png").await;

	let got = store.get(&path).await.expect("object fetched");

	assert_eq!(got.meta.size, 7, "the metadata reports the stored length");
	assert_eq!(got.range, 0..7, "an unranged get serves the whole object");
	assert_eq!(got.meta.location, path, "the metadata names the requested object");
	assert!(got.meta.e_tag.is_some(), "the reply's etag reaches the caller");
	assert_eq!(
		got.attributes.get(&Attribute::ContentType),
		Some(&AttributeValue::from("image/png")),
		"the content type recorded at upload comes back"
	);

	let bytes = got.bytes().await.expect("body read");

	assert_eq!(bytes.as_ref(), b"payload", "the body round trips unchanged");
}

#[tokio::test]
async fn r2_get_with_a_range_serves_exactly_that_range() {
	let (_bridge, url) = bridge().await;
	let store = store(&url, TOKEN);
	let path = Path::from("object");

	seed(&store, "object", b"abcdefghij", "application/octet-stream").await;

	let options = GetOptions::new().with_range(Some(2_u64..5));
	let got = store
		.get_opts(&path, options)
		.await
		.expect("range fetched");

	assert_eq!(got.range, 2..5, "the served range is the requested range");
	assert_eq!(got.meta.size, 10, "content-range carries the whole object's size");

	let bytes = got.bytes().await.expect("body read");

	assert_eq!(bytes.as_ref(), b"cde", "only the requested bytes arrive");
}

#[tokio::test]
async fn r2_head_reports_metadata_without_a_body() {
	let (bridge, url) = bridge().await;
	let store = store(&url, TOKEN);

	seed(&store, "object", b"abcdefghij", "text/plain").await;

	let meta = store
		.head(&Path::from("object"))
		.await
		.expect("metadata fetched");

	assert_eq!(meta.size, 10, "head reports the object's length");
	assert_eq!(meta.location, Path::from("object"), "head names the requested object");
	assert!(
		bridge
			.requests()
			.iter()
			.any(|seen| seen.method == Method::HEAD),
		"head is issued as an HTTP HEAD, not a discarded GET"
	);
}

#[tokio::test]
async fn r2_delete_removes_the_object_and_a_missing_object_is_not_found() {
	let (_bridge, url) = bridge().await;
	let store = store(&url, TOKEN);
	let path = Path::from("object");

	seed(&store, "object", b"payload", "text/plain").await;

	store.delete(&path).await.expect("object deleted");

	let error = store
		.get(&path)
		.await
		.expect_err("the deleted object is gone");

	assert!(
		matches!(error, Error::NotFound { .. }),
		"a 404 from the bridge is NotFound: {error}"
	);
}

#[tokio::test]
async fn r2_list_follows_the_cursor_across_pages() {
	let (bridge, url) = bridge().await;
	let store = store(&url, TOKEN);

	for key in ["a", "b", "c", "d", "e"] {
		seed(&store, key, b"x", "text/plain").await;
	}

	let listed: Vec<ObjectMeta> = store
		.list(None)
		.try_collect()
		.await
		.expect("listing collected");

	let keys: Vec<String> = listed
		.iter()
		.map(|meta| meta.location.as_ref().to_owned())
		.collect();

	assert_eq!(keys, ["a", "b", "c", "d", "e"], "every page is yielded, in key order");
	assert_eq!(
		bridge.listings(),
		3,
		"five objects at two per page take three cursor-paged requests"
	);
	assert!(
		listed.iter().all(|meta| meta.size == 1),
		"listing metadata carries each object's size"
	);
}

#[tokio::test]
async fn r2_list_with_delimiter_separates_objects_from_common_prefixes() {
	let (_bridge, url) = bridge().await;
	let store = store(&url, TOKEN);

	for key in ["a/b", "a/c/d", "a/c/e", "z"] {
		seed(&store, key, b"x", "text/plain").await;
	}

	let result = store
		.list_with_delimiter(Some(&Path::from("a")))
		.await
		.expect("delimited listing");

	let objects: Vec<String> = result
		.objects
		.iter()
		.map(|meta| meta.location.as_ref().to_owned())
		.collect();

	let prefixes: Vec<String> = result
		.common_prefixes
		.iter()
		.map(|prefix| prefix.as_ref().to_owned())
		.collect();

	assert_eq!(objects, ["a/b"], "only direct children are objects");
	assert_eq!(prefixes, ["a/c"], "deeper keys collapse into one common prefix");
}

#[tokio::test]
async fn r2_copy_duplicates_the_object_and_the_conditional_form_refuses_an_existing_key() {
	let (_bridge, url) = bridge().await;
	let store = store(&url, TOKEN);
	let src = Path::from("src");
	let dst = Path::from("dst");
	let fresh = Path::from("fresh");

	seed(&store, "src", b"payload", "image/png").await;

	store
		.copy(&src, &dst)
		.await
		.expect("object copied");

	let got = store.get(&dst).await.expect("copy fetched");

	assert_eq!(
		got.attributes.get(&Attribute::ContentType),
		Some(&AttributeValue::from("image/png")),
		"the copy keeps the recorded content type"
	);

	let bytes = got.bytes().await.expect("body read");

	assert_eq!(bytes.as_ref(), b"payload", "the copy has the source's bytes");

	let error = store
		.copy_if_not_exists(&src, &dst)
		.await
		.expect_err("the destination exists");

	assert!(
		matches!(error, Error::AlreadyExists { .. }),
		"a conditional copy onto an existing key is AlreadyExists: {error}"
	);

	store
		.copy_if_not_exists(&src, &fresh)
		.await
		.expect("conditional copy to an unused key");

	assert!(store.head(&src).await.is_ok(), "a copy leaves the source in place");
}

#[tokio::test]
async fn r2_rename_moves_the_object() {
	let (_bridge, url) = bridge().await;
	let store = store(&url, TOKEN);
	let src = Path::from("src");
	let dst = Path::from("dst");

	seed(&store, "src", b"payload", "text/plain").await;

	store
		.rename(&src, &dst)
		.await
		.expect("object renamed");

	let error = store
		.head(&src)
		.await
		.expect_err("the source is gone");

	assert!(matches!(error, Error::NotFound { .. }), "a rename deletes the source: {error}");

	let bytes = store
		.get(&dst)
		.await
		.expect("renamed object fetched")
		.bytes()
		.await
		.expect("body read");

	assert_eq!(bytes.as_ref(), b"payload", "the renamed object keeps its bytes");
}

#[tokio::test]
async fn r2_rename_if_not_exists_refuses_an_existing_destination() {
	let (_bridge, url) = bridge().await;
	let store = store(&url, TOKEN);
	let src = Path::from("src");
	let dst = Path::from("dst");

	seed(&store, "src", b"payload", "text/plain").await;
	seed(&store, "dst", b"other", "text/plain").await;

	let error = store
		.rename_if_not_exists(&src, &dst)
		.await
		.expect_err("the destination exists");

	assert!(
		matches!(error, Error::AlreadyExists { .. }),
		"a conditional rename onto an existing key is AlreadyExists: {error}"
	);
	assert!(store.head(&src).await.is_ok(), "a refused rename leaves the source in place");
}

#[tokio::test]
async fn r2_refuses_an_invalid_key_before_any_request() {
	let (bridge, url) = bridge().await;
	let store = store(&url, TOKEN);
	let bad = Path::from("has space");

	let error = store
		.put(&bad, PutPayload::from_static(b"x"))
		.await
		.expect_err("an invalid key is refused");

	assert!(
		matches!(error, Error::InvalidPath { .. }),
		"a key the Worker would reject is InvalidPath: {error}"
	);

	let error = store
		.head(&bad)
		.await
		.expect_err("an invalid key is refused");

	assert!(matches!(error, Error::InvalidPath { .. }), "head validates too: {error}");

	let listed: Result<Vec<ObjectMeta>, Error> = store.list(Some(&bad)).try_collect().await;
	let error = listed.expect_err("an invalid listing prefix is refused");

	assert!(
		matches!(error, Error::InvalidPath { .. }),
		"the listing prefix is validated too: {error}"
	);
	assert!(bridge.requests().is_empty(), "an invalid key never reaches the bridge");
}

#[tokio::test]
async fn r2_refuses_an_oversize_upload_locally() {
	let (bridge, url) = bridge().await;
	let store = store(&url, TOKEN);

	// One shared mebibyte repeated past the limit: the payload's declared
	// length exceeds MAX_MEDIA_BYTES while only a mebibyte is allocated.
	let block = Bytes::from(vec![0_u8; MIB]);
	let parts = usize::try_from(MAX_MEDIA_BYTES)
		.expect("the media limit fits in memory arithmetic")
		.checked_div(MIB)
		.expect("a mebibyte is not zero")
		.checked_add(1)
		.expect("one block past the limit");

	let payload: PutPayload = repeat_n(block, parts).collect();

	assert!(
		u64::try_from(payload.content_length()).unwrap_or(u64::MAX) > MAX_MEDIA_BYTES,
		"the fixture is over the protocol limit"
	);

	let error = store
		.put(&Path::from("big"), payload)
		.await
		.expect_err("an oversize upload is refused");

	assert!(
		matches!(error, Error::Precondition { .. }),
		"an oversize upload is a precondition failure: {error}"
	);
	assert!(
		bridge.requests().is_empty(),
		"not one byte of an oversize upload leaves the client"
	);
}

#[tokio::test]
async fn r2_carries_the_bearer_token_on_every_request() {
	let (bridge, url) = bridge().await;
	let store = store(&url, TOKEN);

	seed(&store, "object", b"payload", "text/plain").await;
	store
		.head(&Path::from("object"))
		.await
		.expect("metadata fetched");

	let listed: Vec<ObjectMeta> = store
		.list(None)
		.try_collect()
		.await
		.expect("listing collected");

	assert_eq!(listed.len(), 1, "the listing sees the stored object");

	let requests = bridge.requests();

	assert!(!requests.is_empty(), "the bridge saw requests");
	assert!(
		requests
			.iter()
			.all(|seen| seen.authorization.as_deref() == Some("Bearer bridge-test-token")),
		"every bridge request carries the bearer token"
	);
}

#[tokio::test]
async fn r2_reports_a_rejected_token_as_unauthenticated() {
	let (_bridge, url) = bridge().await;
	let store = store(&url, "not-the-token");

	let error = store
		.put(&Path::from("object"), PutPayload::from_static(b"x"))
		.await
		.expect_err("the bridge rejects the token");

	assert!(
		matches!(error, Error::Unauthenticated { .. }),
		"a 401 from the bridge is Unauthenticated: {error}"
	);
}

#[tokio::test]
async fn r2_multipart_upload_buffers_and_issues_one_put() {
	let (bridge, url) = bridge().await;
	let store = store(&url, TOKEN);
	let path = Path::from("object");

	let mut upload = store
		.put_multipart(&path)
		.await
		.expect("multipart upload started");

	upload
		.put_part(PutPayload::from_static(b"abc"))
		.await
		.expect("first part buffered");

	upload
		.put_part(PutPayload::from_static(b"def"))
		.await
		.expect("second part buffered");

	upload.complete().await.expect("upload completed");

	let puts = bridge
		.requests()
		.iter()
		.filter(|seen| seen.method == Method::PUT)
		.count();

	assert_eq!(puts, 1, "the buffered parts are sent as one request");

	let bytes = store
		.get(&path)
		.await
		.expect("object fetched")
		.bytes()
		.await
		.expect("body read");

	assert_eq!(bytes.as_ref(), b"abcdef", "the parts are concatenated in order");
}

#[tokio::test]
async fn r2_aborting_a_multipart_upload_stores_nothing() {
	let (bridge, url) = bridge().await;
	let store = store(&url, TOKEN);

	let mut upload = store
		.put_multipart(&Path::from("object"))
		.await
		.expect("multipart upload started");

	upload
		.put_part(PutPayload::from_static(b"abc"))
		.await
		.expect("part buffered");

	upload.abort().await.expect("upload aborted");

	assert!(
		bridge
			.requests()
			.iter()
			.all(|seen| seen.method != Method::PUT),
		"an aborted upload never sends a request"
	);
}
