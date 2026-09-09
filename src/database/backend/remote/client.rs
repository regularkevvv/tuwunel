//! HTTP client of the bridge KV endpoint: encoding, deadlines, retries.
//!
//! One logical call is `POST {base}/_bridge/v1/kv` with one CBOR
//! [`Request`] and one CBOR [`Response`] back (ADR-0012, "Encoding and
//! versioning"). Any non-200 status, connection failure, timeout, or
//! undecodable body is a *transport* failure carrying no protocol meaning;
//! an in-band [`Response::Error`] is a *bridge* failure the caller
//! classifies. Transport failures retry with exponential backoff, bounded by
//! [`MAX_ATTEMPTS`] and an optional deadline; the request is re-sent
//! unchanged, which for commits means the same `request_id`.
//!
//! Nothing here logs keys, values, or the bearer token.

use std::{
	fmt,
	time::{Duration, Instant},
};

use futures::future::BoxFuture;
use reqwest::{StatusCode, header::CONTENT_TYPE};
use tokio::time::sleep;
use tuwunel_bridge::{
	self as bridge, DEFAULT_HOST, ENV_TOKEN, ENV_URL, PATH_KV, Request, Response,
};
use tuwunel_core::{Config, Error, Result, debug, err};

/// Attempts per logical call, including the first.
pub(crate) const MAX_ATTEMPTS: u32 = 5;

/// Backoff before the second attempt; doubles per attempt.
pub(crate) const MIN_BACKOFF: Duration = Duration::from_millis(50);

/// Backoff ceiling.
pub(crate) const MAX_BACKOFF: Duration = Duration::from_secs(2);

/// The bridge endpoint, credentials, and per-request deadline.
pub(crate) struct Client {
	http: reqwest::Client,
	endpoint: String,
	token: String,
	timeout: Duration,
}

/// Why one bridge call failed.
#[derive(Debug)]
pub(crate) enum CallError {
	/// No protocol reply arrived: `class` names the failure without any
	/// payload and `retryable` is the retry policy's verdict.
	Transport {
		/// Failure class: `connect`, `timeout`, `request`, `status`, `body`,
		/// `encode`, or `decode`.
		class: &'static str,
		/// Human-readable detail without user data.
		detail: String,
		/// Whether re-sending the same request may succeed.
		retryable: bool,
	},
	/// The Worker answered with an in-band protocol error.
	Bridge(bridge::Error),
}

impl fmt::Display for CallError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			| Self::Transport { class, detail, .. } => write!(f, "transport {class}: {detail}"),
			| Self::Bridge(error) => write!(f, "bridge: {error}"),
		}
	}
}

impl CallError {
	/// Whether this is the in-band stale-lease refusal.
	#[inline]
	pub(crate) fn is_stale_lease(&self) -> bool {
		matches!(self, Self::Bridge(bridge::Error::StaleLease { .. }))
	}

	/// Whether this is a transport failure the retry policy may re-send.
	#[inline]
	pub(crate) fn is_retryable(&self) -> bool {
		matches!(self, Self::Transport { retryable: true, .. })
	}
}

impl Client {
	/// Builds the client from the configuration and the Container's
	/// environment.
	///
	/// The base URL comes from `d1_bridge_url`, then `BRIDGE_URL`, then the
	/// virtual host `http://bridge.internal`; the token from
	/// `d1_bridge_token`, then `BRIDGE_TOKEN`. A missing token is a
	/// configuration error: the bridge serves nothing without it.
	pub(crate) fn from_config(config: &Config) -> Result<Self> {
		let base = config
			.d1_bridge_url
			.clone()
			.filter(|url| !url.is_empty())
			.or_else(|| std::env::var(ENV_URL).ok())
			.filter(|url| !url.is_empty())
			.unwrap_or_else(|| format!("http://{DEFAULT_HOST}"));

		let token = config
			.d1_bridge_token
			.clone()
			.filter(|token| !token.is_empty())
			.or_else(|| std::env::var(ENV_TOKEN).ok())
			.filter(|token| !token.is_empty())
			.ok_or_else(|| {
				err!(Config(
					"d1_bridge_token",
					"The d1 backend needs the bridge token: set d1_bridge_token or the \
					 {ENV_TOKEN} environment variable."
				))
			})?;

		let endpoint = format!("{}{PATH_KV}", base.trim_end_matches('/'));
		let timeout = Duration::from_millis(config.d1_request_timeout_ms.max(1));
		let http = reqwest::Client::builder()
			.timeout(timeout)
			.connect_timeout(timeout)
			.no_proxy()
			.user_agent(format!("tuwunel-bridge/{}", bridge::PROTOCOL_VERSION))
			.build()
			.map_err(|error| err!(Database("bridge client: {error}")))?;

		Ok(Self { http, endpoint, token, timeout })
	}

	/// Sends one request once and classifies the outcome.
	pub(crate) async fn call_once(&self, request: &Request) -> Result<Response, CallError> {
		// Refuse the complete request before allocating/sending its wire body.
		// Oversized atomic commits are never split or retried as transport errors.
		let body = bridge::request::encode(request).map_err(CallError::Bridge)?;

		let reply = self
			.http
			.post(&self.endpoint)
			.header(CONTENT_TYPE, bridge::CONTENT_TYPE)
			.bearer_auth(&self.token)
			.timeout(self.timeout)
			.body(body)
			.send()
			.await
			.map_err(classify)?;

		let status = reply.status();
		if status != StatusCode::OK {
			return Err(CallError::Transport {
				class: "status",
				detail: status.to_string(),
				retryable: status.is_server_error()
					|| matches!(
						status,
						StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
					),
			});
		}

		let bytes = reply.bytes().await.map_err(classify)?;
		let response: Response =
			bridge::decode(&bytes).map_err(|error| CallError::Transport {
				class: "decode",
				detail: error.to_string(),
				retryable: false,
			})?;

		match response {
			| Response::Error(error) => Err(CallError::Bridge(error)),
			| response => Ok(response),
		}
	}

	/// Sends one request, retrying transport failures with backoff.
	///
	/// At most [`MAX_ATTEMPTS`] attempts are made; with a `deadline`, no
	/// attempt starts once its backoff would cross it. The request is
	/// re-sent byte-for-byte, so a commit keeps its idempotency key.
	///
	/// The future is boxed deliberately. It holds an HTTP request/response
	/// future and a retry loop, and it is awaited from every facade read and
	/// write; inlined, that state machine is carried by each caller's future
	/// in turn, and the long startup migration path exceeded the workspace's
	/// `stack-size-threshold` (clippy.toml) purely from the sum of them. One
	/// heap allocation per bridge round trip is not measurable next to the
	/// round trip itself, and it keeps every caller's future small
	/// (`future-size-threshold`, same file).
	pub(crate) fn call<'a>(
		&'a self,
		request: &'a Request,
		deadline: Option<Instant>,
	) -> BoxFuture<'a, Result<Response, CallError>> {
		Box::pin(self.call_inner(request, deadline))
	}

	async fn call_inner(
		&self,
		request: &Request,
		deadline: Option<Instant>,
	) -> Result<Response, CallError> {
		let mut backoff = MIN_BACKOFF;
		let mut attempt: u32 = 1;
		loop {
			let error = match self.call_once(request).await {
				| Ok(response) => return Ok(response),
				| Err(error) => error,
			};

			let may_retry = error.is_retryable()
				&& attempt < MAX_ATTEMPTS
				&& deadline.is_none_or(|deadline| {
					Instant::now()
						.checked_add(backoff)
						.is_some_and(|next| next < deadline)
				});

			if !may_retry {
				return Err(error);
			}

			debug!(attempt, %error, "bridge call failed; retrying");
			sleep(backoff).await;
			backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
			attempt = attempt.saturating_add(1);
		}
	}
}

/// Classifies a reqwest failure for the retry policy.
fn classify(error: reqwest::Error) -> CallError {
	let (class, retryable) = if error.is_timeout() {
		("timeout", true)
	} else if error.is_connect() {
		("connect", true)
	} else if error.is_body() || error.is_decode() {
		("body", true)
	} else if error.is_builder() {
		("request", false)
	} else {
		("request", true)
	};

	CallError::Transport {
		class,
		// reqwest's message names the failure and the URL, never headers.
		detail: error.without_url().to_string(),
		retryable,
	}
}

/// Lifts a bridge failure into the facade's database error.
///
/// `op` names the operation; the message carries the failure class only.
pub(crate) fn database_error(op: &str, error: &CallError) -> Error {
	err!(Database("bridge {op} failed: {error}"))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn oversized_request_is_nonretryable_before_transport() {
		let client = Client {
			http: reqwest::Client::new(),
			endpoint: "invalid://must-not-reach-transport".into(),
			token: "test-only".into(),
			timeout: Duration::from_millis(1),
		};
		let request = Request::Get {
			map: 0,
			keys: vec![serde_bytes::ByteBuf::from(vec![0; bridge::MAX_KEY_BYTES]); 300],
		};
		let error = client
			.call_once(&request)
			.await
			.expect_err("oversize refusal");
		assert!(!error.is_retryable());
		assert!(
			matches!(error, CallError::Bridge(bridge::Error::TooLarge { what, .. }) if what == "request bytes")
		);
	}
}
