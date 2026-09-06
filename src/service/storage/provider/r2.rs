//! Private R2 media through the bridge (ADR-0005, ADR-0012).
//!
//! [`R2Bridge`] is an [`ObjectStore`] over the media endpoints of the edge
//! Worker: `PUT|GET|HEAD|DELETE {BRIDGE_URL}/_bridge/v1/media/<key>` and
//! `GET {BRIDGE_URL}/_bridge/v1/media?prefix=&cursor=&limit=`. The Worker
//! holds the bucket binding and refuses every key that fails
//! [`tuwunel_bridge::media_key`]; this side validates the same way before a
//! request leaves, holds only a bearer token, and never learns a bucket name.
//!
//! There are no presigned URLs on this path: media is served through the
//! authenticated Matrix media routes only (ADR-0005), so the provider's
//! signer is `None`. The bridge has no multipart protocol either, so a
//! "multipart" upload is buffered in memory and sent as one `PUT` on
//! completion; the homeserver's `max_request_size` bounds what it ever hands
//! a provider, far below [`MAX_MEDIA_BYTES`], and an upload past that limit
//! is refused here before a byte is sent.
//!
//! Wire conventions this client relies on beyond ADR-0012's text: a `GET` or
//! `HEAD` reply carries `content-length`, `last-modified` (an HTTP-date), the
//! `etag`, and the `content-type` recorded at upload; a ranged `GET` answers
//! `206` with `content-range`; a listing answers CBOR [`MediaList`]. Every
//! failure maps to the [`object_store::Error`] class a caller acts on: `404`
//! is [`Error::NotFound`] (an unauthenticated request gets the same `404`, by
//! design), `413` and a locally refused oversize body are
//! [`Error::Precondition`], and anything else is [`Error::Generic`] carrying
//! the status and a bounded excerpt of the reply body. Keys appear in errors
//! and in `trace!`/`debug!` output only; the token appears nowhere.

use std::{
	collections::BTreeSet,
	env,
	fmt::{self, Debug, Display, Formatter},
	fs,
	ops::Range,
	sync::Arc,
	time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{
	StreamExt, TryStreamExt,
	future::ready,
	stream::{self, BoxStream},
};
use object_store::{
	Attribute, Attributes, CopyMode, CopyOptions, Error, Extensions, GetOptions, GetRange,
	GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
	PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
	path::{self, Path},
};
use reqwest::{
	Client, Method, RequestBuilder, Response, StatusCode,
	header::{
		AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, HeaderMap, HeaderName,
		HeaderValue, LAST_MODIFIED, RANGE,
	},
	redirect::Policy,
};
use tuwunel_bridge::{
	DEFAULT_HOST, ENV_TOKEN, ENV_URL, MAX_MEDIA_BYTES, MAX_MEDIA_LIST, MediaList, MediaObject,
	PATH_MEDIA, decode, media_key,
};
use tuwunel_core::{
	Err, Result,
	config::{StorageProvider, StorageProviderR2},
	debug_info, err, error, trace,
	version::user_agent,
};
use url::Url;

use super::Provider;

/// Store name carried by [`Error::Generic`].
const STORE: &str = "R2";

/// Content type sent for an upload that declares none.
const OCTET_STREAM: &str = "application/octet-stream";

/// Longest reply-body excerpt kept in an error.
const EXCERPT_LEN: usize = 256;

/// The HTTP-date layout (IMF-fixdate, RFC 9110 §5.6.7), for the replies
/// whose `last-modified` chrono's RFC 2822 parser does not accept.
const IMF_FIXDATE: &str = "%a, %d %b %Y %H:%M:%S GMT";

type StoreResult<T> = std::result::Result<T, Error>;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tracing::instrument(name = "new", level = "info", skip_all, err)]
pub(in super::super) fn new(
	args: &crate::Args<'_>,
	name: &str,
	config: &StorageProviderR2,
) -> Result<Option<(String, Arc<Provider>)>> {
	let url = bridge_url(config)?;
	let token = bridge_token(config)?;
	let timeout = Duration::from_secs(config.request_timeout);

	trace!(?name, ?config, "Initializing R2 bridge media client...");

	let client = R2Bridge::new(&url, &token, timeout, user_agent())
		.inspect_err(|e| error!("Failed to configure R2 bridge media client: {e}"))?;

	debug_info!(
		name = %name,
		url = %url,
		"Started R2 bridge media client.",
	);

	let provider = Provider {
		name: name.to_owned(),
		base_path: config.base_path.clone().map(Into::into),
		config: StorageProvider::r2(Box::new(config.clone())),
		startup_check: config.startup_check,
		services: args.services.clone(),
		provider: Box::new(client),
		// Authenticated media only (ADR-0005): the bridge presigns nothing.
		signer: None,
	};

	Ok(Some((name.to_owned(), Arc::new(provider))))
}

/// The bridge base URL: the configured `url`, else `BRIDGE_URL`, else the
/// virtual host the Worker intercepts (ADR-0012).
fn bridge_url(config: &StorageProviderR2) -> Result<Url> {
	let configured = config
		.url
		.as_deref()
		.map(str::trim)
		.filter(|url| !url.is_empty());

	let from_env = env::var(ENV_URL).ok();
	let from_env = from_env
		.as_deref()
		.map(str::trim)
		.filter(|url| !url.is_empty());

	let default = format!("http://{DEFAULT_HOST}");
	let (source, url) = match (configured, from_env) {
		| (Some(url), _) => ("storage_provider.r2.url", url),
		| (None, Some(url)) => (ENV_URL, url),
		| (None, None) => ("storage_provider.r2.url", default.as_str()),
	};

	let url = Url::parse(url)
		.map_err(|e| err!(Config("storage_provider.r2.url", "{source} is not a URL: {e}")))?;

	if !matches!(url.scheme(), "http" | "https") {
		return Err!(Config("storage_provider.r2.url", "The bridge URL must use http or https"));
	}

	Ok(url)
}

/// The bearer token: the configured `token`, else the contents of
/// `token_file`, else `BRIDGE_TOKEN`; trimmed, and never empty.
fn bridge_token(config: &StorageProviderR2) -> Result<String> {
	let token = if let Some(token) = &config.token {
		token.clone()
	} else if let Some(path) = &config.token_file {
		fs::read_to_string(path).map_err(|e| {
			err!(Config(
				"storage_provider.r2.token_file",
				"Failed to read the bridge token file: {e}"
			))
		})?
	} else {
		env::var(ENV_TOKEN).map_err(|_| {
			err!(Config(
				"storage_provider.r2.token",
				"No bridge token: set `token`, `token_file`, or {ENV_TOKEN} (ADR-0012)"
			))
		})?
	};

	let token = token.trim();
	if token.is_empty() {
		return Err!(Config("storage_provider.r2.token", "The bridge token is empty"));
	}

	Ok(token.to_owned())
}

/// Media object store over the bridge's media endpoints.
///
/// Cheap to clone: every clone shares one connection pool and one token.
#[derive(Clone)]
pub struct R2Bridge {
	shared: Arc<Shared>,
}

struct Shared {
	client: Client,

	/// `{BRIDGE_URL}/_bridge/v1/media`, without a trailing slash.
	media: Url,

	/// `Bearer <token>`, marked sensitive so no formatter prints it.
	authorization: HeaderValue,

	/// Deadline of a whole metadata request or upload, and the read
	/// inactivity deadline of a download.
	timeout: Duration,
}

impl R2Bridge {
	/// Connects to the bridge at `base_url` with `token`.
	///
	/// `base_url` is the bridge base (scheme, host, and any path prefix);
	/// [`PATH_MEDIA`] is appended. The client follows no redirect and uses
	/// no proxy: the bridge is a virtual host the Worker intercepts on the
	/// Container's own host (ADR-0012), never a route through a third party.
	pub fn new(base_url: &Url, token: &str, timeout: Duration, user_agent: &str) -> Result<Self> {
		let media = format!("{}{PATH_MEDIA}", base_url.as_str().trim_end_matches('/'));
		let media = Url::parse(&media)
			.map_err(|e| err!(Config("storage_provider.r2.url", "Bridge media URL: {e}")))?;

		let mut authorization =
			HeaderValue::from_str(&format!("Bearer {token}")).map_err(|e| {
				err!(Config(
					"storage_provider.r2.token",
					"The bridge token is not a valid header value: {e}"
				))
			})?;
		authorization.set_sensitive(true);

		let client = Client::builder()
			.user_agent(user_agent)
			.connect_timeout(timeout)
			.read_timeout(timeout)
			.redirect(Policy::none())
			.no_proxy()
			.build()?;

		let shared = Shared { client, media, authorization, timeout };

		Ok(Self { shared: Arc::new(shared) })
	}
}

impl Display for R2Bridge {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "R2Bridge({})", self.shared.media)
	}
}

impl Debug for R2Bridge {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("R2Bridge")
			.field("media", &self.shared.media.as_str())
			.field("timeout", &self.shared.timeout)
			.finish_non_exhaustive()
	}
}

#[async_trait]
impl ObjectStore for R2Bridge {
	async fn put_opts(
		&self,
		location: &Path,
		payload: PutPayload,
		opts: PutOptions,
	) -> StoreResult<PutResult> {
		let key = key(location)?;

		// `Create` is checked with a HEAD first; a writer creating the key
		// between that check and the PUT would be overwritten. The Container is
		// the single writer of its bucket (ADR-0003), so the race has no second
		// party.
		match &opts.mode {
			| PutMode::Create if self.shared.exists(key).await? => {
				return Err(already_exists(key));
			},
			| PutMode::Update(_) => {
				return Err(not_supported(
					"conditional update: the bridge has no object versions",
				));
			},
			| PutMode::Overwrite | PutMode::Create => {},
		}

		let len = u64::try_from(payload.content_length()).unwrap_or(u64::MAX);
		if len > MAX_MEDIA_BYTES {
			return Err(too_large(key, len));
		}

		let content_type = opts
			.attributes
			.get(&Attribute::ContentType)
			.map_or(OCTET_STREAM, AsRef::as_ref);

		trace!(key, len, content_type, "Uploading object through the bridge");

		let request = self
			.shared
			.control(Method::PUT, self.shared.object_url(key)?)
			.header(CONTENT_TYPE, content_type)
			.header(CONTENT_LENGTH, len)
			.body(Bytes::from(payload));

		let response = self.shared.send(request, key).await?;

		Ok(PutResult {
			e_tag: header(response.headers(), &ETAG).map(ToOwned::to_owned),
			version: None,
			extensions: Extensions::default(),
		})
	}

	async fn put_multipart_opts(
		&self,
		location: &Path,
		opts: PutMultipartOptions,
	) -> StoreResult<Box<dyn MultipartUpload>> {
		key(location)?;

		Ok(Box::new(BufferedUpload {
			store: self.clone(),
			location: location.clone(),
			attributes: opts.attributes,
			parts: Vec::new(),
			len: 0,
		}))
	}

	async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
		let key = key(location)?;

		if options.version.is_some() {
			return Err(not_supported("object versions"));
		}

		let url = self.shared.object_url(key)?;
		let request = if options.head {
			self.shared.control(Method::HEAD, url)
		} else {
			self.shared.request(Method::GET, url)
		};

		let request = match &options.range {
			| Some(range) => {
				range.is_valid().map_err(generic)?;
				request.header(RANGE, range.to_string())
			},
			| None => request,
		};

		trace!(key, head = options.head, range = ?options.range, "Fetching object through the bridge");

		let response = self.shared.send(request, key).await?;
		let (meta, range) = object_meta(location, &response, options.range.as_ref())?;
		options.check_preconditions(&meta)?;

		let attributes = attributes(response.headers());
		let payload = if options.head {
			stream::empty().boxed()
		} else {
			body_stream(response)
		};

		Ok(GetResult {
			payload: GetResultPayload::Stream(payload),
			meta,
			range,
			attributes,
			extensions: Extensions::default(),
		})
	}

	fn delete_stream(
		&self,
		locations: BoxStream<'static, StoreResult<Path>>,
	) -> BoxStream<'static, StoreResult<Path>> {
		let this = self.clone();

		locations
			.and_then(move |location| {
				let this = this.clone();
				async move {
					this.shared.delete(key(&location)?).await?;

					Ok(location)
				}
			})
			.boxed()
	}

	fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
		let prefix = match prefix.map(list_prefix).transpose() {
			| Ok(prefix) => prefix.flatten(),
			| Err(e) => return stream::once(ready(Err(e))).boxed(),
		};

		let this = self.clone();

		// `Some(None)` is the first page, `Some(Some(cursor))` a continuation,
		// `None` the end.
		stream::try_unfold(Some(None), move |cursor: Option<Option<String>>| {
			let this = this.clone();
			let prefix = prefix.clone();
			async move {
				let Some(cursor) = cursor else {
					return StoreResult::Ok(None);
				};

				let page = this
					.shared
					.list_page(prefix.as_deref(), cursor.as_deref())
					.await?;

				let next = page.cursor.map(Some);
				let objects: Vec<StoreResult<ObjectMeta>> = page
					.objects
					.into_iter()
					.map(listed_meta)
					.collect();

				Ok(Some((stream::iter(objects), next)))
			}
		})
		.try_flatten()
		.boxed()
	}

	async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
		let base = prefix.cloned().unwrap_or_default();
		let listed: Vec<ObjectMeta> = self.list(prefix).try_collect().await?;

		let mut objects = Vec::new();
		let mut common_prefixes = BTreeSet::new();
		for meta in listed {
			let child = {
				let Some(mut rest) = meta.location.prefix_match(&base) else {
					continue;
				};

				let Some(first) = rest.next() else {
					continue;
				};

				rest.next()
					.is_some()
					.then(|| base.clone().join(first))
			};

			match child {
				| Some(prefix) => {
					common_prefixes.insert(prefix);
				},
				| None => objects.push(meta),
			}
		}

		Ok(ListResult {
			common_prefixes: common_prefixes.into_iter().collect(),
			objects,
			extensions: Extensions::default(),
		})
	}

	/// Copies by downloading and re-uploading through the bridge, keeping
	/// the recorded content type.
	///
	/// `CopyMode::Create` checks the destination with a `HEAD` first; a
	/// writer creating it between that check and the `PUT` would be
	/// overwritten. The Container is the single writer of its bucket
	/// (ADR-0003), so the race has no second party.
	async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
		let from_key = key(from)?;
		let to_key = key(to)?;

		if matches!(options.mode, CopyMode::Create) && self.shared.exists(to_key).await? {
			return Err(already_exists(to_key));
		}

		trace!(from = from_key, to = to_key, "Copying object through the bridge");

		let source = self.get_opts(from, GetOptions::default()).await?;
		let content_type = source
			.attributes
			.get(&Attribute::ContentType)
			.cloned();

		let bytes = source.bytes().await?;

		let mut attributes = Attributes::new();
		if let Some(content_type) = content_type {
			attributes.insert(Attribute::ContentType, content_type);
		}

		let opts = PutOptions { attributes, ..Default::default() };

		self.put_opts(to, PutPayload::from(bytes), opts)
			.await
			.map(|_| ())
	}
}

impl Shared {
	fn object_url(&self, key: &str) -> StoreResult<Url> {
		Url::parse(&format!("{}/{key}", self.media))
			.map_err(|e| generic(format!("object URL for {key:?}: {e}")))
	}

	fn list_url(&self, prefix: Option<&str>, cursor: Option<&str>) -> Url {
		let mut url = self.media.clone();

		append_list_query(&mut url, prefix, cursor);

		url
	}

	fn request(&self, method: Method, url: Url) -> RequestBuilder {
		self.client
			.request(method, url)
			.header(AUTHORIZATION, self.authorization.clone())
	}

	/// A request whose whole exchange, body included, must finish within
	/// the timeout: everything but a body download.
	fn control(&self, method: Method, url: Url) -> RequestBuilder {
		self.request(method, url).timeout(self.timeout)
	}

	/// Sends, turning any non-success status into the error class the
	/// status names.
	async fn send(&self, request: RequestBuilder, key: &str) -> StoreResult<Response> {
		let response = request
			.send()
			.await
			.map_err(|e| transport(key, e))?;

		let status = response.status();
		if status.is_success() {
			return Ok(response);
		}

		let excerpt = excerpt(response).await;

		Err(status_error(status, key, excerpt))
	}

	async fn exists(&self, key: &str) -> StoreResult<bool> {
		let request = self.control(Method::HEAD, self.object_url(key)?);

		match self.send(request, key).await {
			| Ok(_) => Ok(true),
			| Err(Error::NotFound { .. }) => Ok(false),
			| Err(e) => Err(e),
		}
	}

	/// Deletes one object; deleting an absent object is a `404` from the
	/// bridge and [`Error::NotFound`] here.
	async fn delete(&self, key: &str) -> StoreResult<()> {
		trace!(key, "Deleting object through the bridge");

		let request = self.control(Method::DELETE, self.object_url(key)?);

		self.send(request, key).await.map(|_| ())
	}

	/// One listing page. A `404` here cannot name a missing object, so it
	/// is reported as the authentication or routing failure ADR-0012 makes
	/// it, which is what the startup check needs to say.
	async fn list_page(
		&self,
		prefix: Option<&str>,
		cursor: Option<&str>,
	) -> StoreResult<MediaList> {
		let request = self.control(Method::GET, self.list_url(prefix, cursor));
		let response = request
			.send()
			.await
			.map_err(|e| transport("", e))?;

		let status = response.status();
		if status == StatusCode::NOT_FOUND {
			return Err(Error::Unauthenticated {
				path: String::new(),
				source: format!(
					"the bridge answered 404 to a media listing: wrong {ENV_URL} or bridge host \
					 allowlist, wrong {ENV_TOKEN}, or a Worker without the media routes \
					 (ADR-0012)"
				)
				.into(),
			});
		}

		if !status.is_success() {
			let excerpt = excerpt(response).await;

			return Err(status_error(status, "", excerpt));
		}

		let body = response
			.bytes()
			.await
			.map_err(|e| transport("", e))?;

		decode::<MediaList>(&body).map_err(|e| generic(format!("media listing reply: {e}")))
	}
}

/// A multipart upload buffered in memory and sent as one `PUT`.
///
/// The bridge has no multipart protocol (ADR-0012), and the homeserver's
/// `max_request_size` bounds what any provider receives per object, so the
/// buffer is bounded by configuration and by [`MAX_MEDIA_BYTES`] here.
struct BufferedUpload {
	store: R2Bridge,
	location: Path,
	attributes: Attributes,
	parts: Vec<Bytes>,
	len: u64,
}

impl Debug for BufferedUpload {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("BufferedUpload")
			.field("location", &self.location)
			.field("parts", &self.parts.len())
			.field("len", &self.len)
			.finish_non_exhaustive()
	}
}

#[async_trait]
impl MultipartUpload for BufferedUpload {
	fn put_part(&mut self, data: PutPayload) -> UploadPart {
		let bytes = Bytes::from(data);
		let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);

		self.len = self.len.saturating_add(len);
		if self.len > MAX_MEDIA_BYTES {
			self.parts.clear();

			return Box::pin(ready(Err(too_large(self.location.as_ref(), self.len))));
		}

		self.parts.push(bytes);

		Box::pin(ready(Ok(())))
	}

	async fn complete(&mut self) -> StoreResult<PutResult> {
		let payload: PutPayload = self.parts.drain(..).collect();
		let opts = PutOptions {
			attributes: self.attributes.clone(),
			..Default::default()
		};

		self.len = 0;
		self.store
			.put_opts(&self.location, payload, opts)
			.await
	}

	async fn abort(&mut self) -> StoreResult<()> {
		self.parts.clear();
		self.len = 0;

		Ok(())
	}
}

/// The reply body as a stream, read chunk by chunk.
fn body_stream(response: Response) -> BoxStream<'static, StoreResult<Bytes>> {
	stream::try_unfold(response, |mut response| async move {
		response
			.chunk()
			.await
			.map(|chunk| chunk.map(|bytes| (bytes, response)))
			.map_err(generic)
	})
	.boxed()
}

/// The object key of `location`, validated exactly as the Worker validates
/// it.
fn key(location: &Path) -> StoreResult<&str> {
	let key = location.as_ref();

	media_key(key)
		.then_some(key)
		.ok_or_else(|| invalid_path(key))
}

/// A listing prefix: `None` for the root, else the validated prefix with the
/// trailing delimiter [`ObjectStore::list`] promises (prefixes match whole
/// path segments).
fn list_prefix(prefix: &Path) -> StoreResult<Option<String>> {
	let prefix = prefix.as_ref();
	if prefix.is_empty() {
		return Ok(None);
	}

	media_key(prefix)
		.then(|| Some(format!("{prefix}/")))
		.ok_or_else(|| invalid_path(prefix))
}

/// Metadata and served range of a `GET`/`HEAD` reply.
fn object_meta(
	location: &Path,
	response: &Response,
	requested: Option<&GetRange>,
) -> StoreResult<(ObjectMeta, Range<u64>)> {
	let key = location.as_ref();
	let headers = response.headers();

	let last_modified = header(headers, &LAST_MODIFIED)
		.ok_or_else(|| missing_header(key, "last-modified"))
		.and_then(|value| {
			parse_http_date(value).ok_or_else(|| {
				generic(format!("unparseable last-modified {value:?} for {key:?}"))
			})
		})?;

	let e_tag = header(headers, &ETAG).map(ToOwned::to_owned);

	let (range, size) = if response.status() == StatusCode::PARTIAL_CONTENT {
		let value = header(headers, &CONTENT_RANGE)
			.ok_or_else(|| missing_header(key, "content-range"))?;

		parse_content_range(value)
			.ok_or_else(|| generic(format!("unparseable content-range {value:?} for {key:?}")))?
	} else {
		if requested.is_some() {
			return Err(generic(format!("the bridge ignored the requested range for {key:?}")));
		}

		let size = header(headers, &CONTENT_LENGTH)
			.and_then(|value| value.parse().ok())
			.ok_or_else(|| missing_header(key, "content-length"))?;

		(0..size, size)
	};

	let meta = ObjectMeta {
		location: location.clone(),
		last_modified,
		size,
		e_tag,
		version: None,
	};

	Ok((meta, range))
}

/// Metadata of one listed object.
fn listed_meta(object: MediaObject) -> StoreResult<ObjectMeta> {
	let location = Path::parse(&object.key).map_err(|source| Error::InvalidPath { source })?;

	let last_modified = i64::try_from(object.modified_ms)
		.ok()
		.and_then(DateTime::<Utc>::from_timestamp_millis)
		.ok_or_else(|| {
			generic(format!(
				"modified_ms {} of {:?} is out of range",
				object.modified_ms, object.key
			))
		})?;

	Ok(ObjectMeta {
		location,
		last_modified,
		size: object.size,
		e_tag: Some(object.etag),
		version: None,
	})
}

/// The attributes a reply's headers carry: the recorded content type.
fn attributes(headers: &HeaderMap) -> Attributes {
	let mut attributes = Attributes::new();
	if let Some(content_type) = header(headers, &CONTENT_TYPE) {
		attributes.insert(Attribute::ContentType, content_type.to_owned().into());
	}

	attributes
}

/// Appends the listing parameters to a media listing URL.
///
/// `limit` is always sent: the bridge clamps it to [`MAX_MEDIA_LIST`] and the
/// client follows the returned cursor until the listing is complete.
fn append_list_query(url: &mut Url, prefix: Option<&str>, cursor: Option<&str>) {
	let mut query = url.query_pairs_mut();

	if let Some(prefix) = prefix {
		query.append_pair("prefix", prefix);
	}

	if let Some(cursor) = cursor {
		query.append_pair("cursor", cursor);
	}

	query.append_pair("limit", &MAX_MEDIA_LIST.to_string());
}

fn header<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Option<&'a str> {
	headers
		.get(name)
		.and_then(|value| value.to_str().ok())
}

/// Parses an HTTP-date, accepting the RFC 2822 forms too.
pub(super) fn parse_http_date(value: &str) -> Option<DateTime<Utc>> {
	let value = value.trim();

	DateTime::parse_from_rfc2822(value)
		.map(|date| date.with_timezone(&Utc))
		.ok()
		.or_else(|| {
			NaiveDateTime::parse_from_str(value, IMF_FIXDATE)
				.ok()
				.map(|naive| naive.and_utc())
		})
}

/// Parses `bytes <first>-<last>/<size>` into the served range and the
/// object size.
pub(super) fn parse_content_range(value: &str) -> Option<(Range<u64>, u64)> {
	let (first, rest) = value
		.trim()
		.strip_prefix("bytes ")?
		.split_once('-')?;

	let (last, size) = rest.split_once('/')?;
	let first: u64 = first.trim().parse().ok()?;
	let last: u64 = last.trim().parse().ok()?;
	let size: u64 = size.trim().parse().ok()?;
	let end = last.checked_add(1)?;

	(first < end && end <= size).then_some((first..end, size))
}

/// Up to [`EXCERPT_LEN`] bytes of a failed reply's body, for the error.
async fn excerpt(mut response: Response) -> String {
	let mut buf = Vec::new();
	while let Some(chunk) = response.chunk().await.ok().flatten() {
		buf.extend_from_slice(&chunk);
		if buf.len() >= EXCERPT_LEN {
			break;
		}
	}

	buf.truncate(EXCERPT_LEN);

	String::from_utf8_lossy(&buf).into_owned()
}

/// A non-success reply, as the source of the error it maps to.
#[derive(Debug)]
struct StatusError {
	status: StatusCode,
	excerpt: String,
}

impl Display for StatusError {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "the bridge replied {}", self.status)?;
		if !self.excerpt.is_empty() {
			write!(f, ": {}", self.excerpt)?;
		}

		Ok(())
	}
}

impl std::error::Error for StatusError {}

/// The error class of a non-success status: `404` is a missing object,
/// `413` (and the related `411`, `409`, `412`) a refused write, `401`/`403`
/// an authentication or permission failure, and anything else generic.
fn status_error(status: StatusCode, key: &str, excerpt: String) -> Error {
	let path = key.to_owned();
	let source: BoxError = Box::new(StatusError { status, excerpt });

	match status.as_u16() {
		| 404 => Error::NotFound { path, source },
		| 401 => Error::Unauthenticated { path, source },
		| 403 => Error::PermissionDenied { path, source },
		| 409 | 411 | 412 | 413 => Error::Precondition { path, source },
		| _ => Error::Generic { store: STORE, source },
	}
}

fn transport(key: &str, error: reqwest::Error) -> Error {
	if error.is_timeout() {
		return generic(format!("request for {key:?} timed out: {error}"));
	}

	generic(error)
}

fn generic<E: Into<BoxError>>(source: E) -> Error {
	Error::Generic { store: STORE, source: source.into() }
}

fn not_supported(what: &str) -> Error {
	Error::NotSupported {
		source: format!("{what} is not supported by the bridge").into(),
	}
}

fn too_large(key: &str, len: u64) -> Error {
	Error::Precondition {
		path: key.to_owned(),
		source: format!("{len} bytes exceeds the bridge media limit of {MAX_MEDIA_BYTES} bytes")
			.into(),
	}
}

fn already_exists(key: &str) -> Error {
	Error::AlreadyExists {
		path: key.to_owned(),
		source: "the destination already exists".into(),
	}
}

fn missing_header(key: &str, name: &str) -> Error {
	generic(format!("the bridge reply for {key:?} lacks the {name} header"))
}

fn invalid_path(key: &str) -> Error {
	Error::InvalidPath {
		source: path::Error::InvalidPath { path: key.into() },
	}
}
