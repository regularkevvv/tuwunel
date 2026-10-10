use std::time::Instant;

use axum::{Json, extract::State, response::IntoResponse};
use http::{HeaderMap, StatusCode};
use ruma::api::{client::tuwunel::get_remote_version, federation::discovery::get_server_version};
use subtle::ConstantTimeEq;
use tuwunel_core::Result;

use crate::Ruma;

/// # `GET /_tuwunel/server_version`
///
/// Tuwunel-specific API to get the server version, results akin to
/// `/_matrix/federation/v1/version`
pub(crate) async fn tuwunel_server_version() -> Result<impl IntoResponse> {
	Ok(Json(serde_json::json!({
		"name": tuwunel_core::version::name(),
		"version": tuwunel_core::version::version(),
	})))
}

/// # `GET /_tuwunel/local_user_count`
///
/// Tuwunel-specific API to return the amount of users registered on this
/// homeserver. Endpoint is disabled if federation is disabled for privacy. This
/// only includes active users (not deactivated, no guests, etc)
pub(crate) async fn tuwunel_local_user_count(
	State(services): State<crate::State>,
) -> Result<impl IntoResponse> {
	let user_count = services.users.bounded_local_user_count().await?;

	Ok(Json(serde_json::json!({
		"count": user_count
	})))
}

/// # `GET /_tuwunel/remote_version/{server_name}`
///
/// Tuwunel-specific API to probe a remote server's
/// `/_matrix/federation/v1/version` endpoint, returning that response body
/// along with the round-trip time of the probe.
pub(crate) async fn tuwunel_remote_version(
	State(services): State<crate::State>,
	body: Ruma<get_remote_version::unstable::Request>,
) -> Result<get_remote_version::unstable::Response> {
	let timer = Instant::now();

	let server = if services.globals.server_is_ours(&body.server_name) {
		Some(get_server_version::v1::Server {
			name: Some(tuwunel_core::version::name().into()),
			version: Some(tuwunel_core::version::version().into()),
			compiler: tuwunel_core::info::rustc::version().map(Into::into),
			..Default::default()
		})
	} else {
		services
			.federation
			.execute(&body.server_name, get_server_version::v1::Request {})
			.await?
			.server
	};

	let elapsed = timer.elapsed();

	Ok(get_remote_version::unstable::Response {
		data: serde_json::value::to_raw_value(&server)?,
		rtt_ms: elapsed,
	})
}

/// # `GET /_tuwunel/readiness`
///
/// Reports the storage backend and, on the remote D1 backend, the writer
/// lease (ADR-0003, ADR-0012). `200` means this process may write: it holds
/// the lease, has confirmed a renewal, and the countdown has not run out.
/// `503` means it may not, whether because the lease is uncertain, lost, or
/// never held, or because a RocksDB database was opened read-only.
///
/// The body is `{"backend": "d1"|"rocksdb", "lease": {"held", "epoch",
/// "expires_in_ms", "uncertain"} | null}`; the lease is `null` off the remote
/// backend, which has none.
///
/// Unauthenticated on purpose: the platform's health probe runs before any
/// credential exists, and the response carries no user data.
pub(crate) async fn tuwunel_readiness(State(services): State<crate::State>) -> impl IntoResponse {
	let db = &services.db;
	let lease = db.lease_status().map(|lease| {
		serde_json::json!({
			"held": lease.held,
			"epoch": lease.epoch,
			"expires_in_ms": lease.expires_in_ms,
			"uncertain": lease.uncertain,
		})
	});

	let writable = !db.is_read_only();
	let status = if writable {
		StatusCode::OK
	} else {
		StatusCode::SERVICE_UNAVAILABLE
	};

	(
		status,
		Json(serde_json::json!({
			"backend": db.backend(),
			"lease": lease,
		})),
	)
}

/// # `GET /_tuwunel/operation_metrics`
///
/// Bounded aggregate snapshot for the operator's measurement harness. The
/// Worker restricts this route to the admin hostname; the Container also
/// requires the existing runtime bridge credential. No Matrix token grants
/// access. Readiness stays unauthenticated and never contains these counters.
pub(crate) async fn tuwunel_operation_metrics(
	State(services): State<crate::State>,
	headers: HeaderMap,
) -> impl IntoResponse {
	let expected = services
		.server
		.config
		.d1_bridge_token
		.as_deref()
		.filter(|token| !token.is_empty())
		.map(str::to_owned)
		.or_else(|| std::env::var("BRIDGE_TOKEN").ok());
	if services.db.backend() != "d1" || !metrics_request_authorized(&headers, expected.as_deref())
	{
		return (
			StatusCode::NOT_FOUND,
			[(http::header::CACHE_CONTROL, "no-store")],
			Json(serde_json::json!({"errcode": "M_NOT_FOUND", "error": "Not found"})),
		);
	}
	let counters = services.db.operation_metrics();
	let status = if counters.get("unavailable").is_some() {
		StatusCode::SERVICE_UNAVAILABLE
	} else {
		StatusCode::OK
	};
	(
		status,
		[(http::header::CACHE_CONTROL, "no-store")],
		Json(serde_json::json!({
			"schema": 1,
			"backend": "d1",
			"writer_epoch": services.db.lease_status().map(|lease| lease.epoch),
			"consistency": "completed metric records",
			"counters": counters,
		})),
	)
}

fn metrics_request_authorized(headers: &HeaderMap, expected: Option<&str>) -> bool {
	if headers
		.get_all(http::header::AUTHORIZATION)
		.iter()
		.count()
		!= 1
	{
		return false;
	}
	let presented = headers
		.get(http::header::AUTHORIZATION)
		.and_then(|value| value.to_str().ok());
	let Some(expected) = expected.filter(|token| !token.is_empty()) else {
		return false;
	};
	let Some((scheme, token)) = presented.and_then(|header| header.split_once(' ')) else {
		return false;
	};
	scheme.eq_ignore_ascii_case("bearer")
		&& bool::from(token.trim().as_bytes().ct_eq(expected.as_bytes()))
}

#[cfg(test)]
mod metrics_tests {
	use http::{HeaderMap, HeaderValue, header::AUTHORIZATION};

	use super::metrics_request_authorized;

	fn metrics_authorized(presented: Option<&str>, expected: Option<&str>) -> bool {
		let mut headers = HeaderMap::new();
		if let Some(value) = presented {
			headers.insert(AUTHORIZATION, HeaderValue::from_str(value).expect("test header"));
		}
		metrics_request_authorized(&headers, expected)
	}

	#[test]
	fn metrics_require_the_runtime_credential() {
		for header in [
			None,
			Some(""),
			Some("Basic operator"),
			Some("Bearer matrix-token"),
			Some("Bearer bridge-token-prefix"),
			Some("Bearer bridge-toke"),
		] {
			assert!(!metrics_authorized(header, Some("bridge-token")));
		}
		assert!(!metrics_authorized(Some("Bearer bridge-token"), None));
		assert!(!metrics_authorized(Some("Bearer "), Some("")));
		assert!(metrics_authorized(Some("Bearer bridge-token"), Some("bridge-token")));
		assert!(metrics_authorized(Some("bearer bridge-token"), Some("bridge-token")));
	}

	#[test]
	fn metrics_refuse_duplicate_and_invalid_credentials() {
		let mut headers = HeaderMap::new();
		headers.append(AUTHORIZATION, HeaderValue::from_static("Bearer bridge-token"));
		headers.append(AUTHORIZATION, HeaderValue::from_static("Bearer bridge-token"));
		assert!(!metrics_request_authorized(&headers, Some("bridge-token")));
		headers.clear();
		headers.insert(
			AUTHORIZATION,
			HeaderValue::from_bytes(b"Bearer \xff").expect("opaque header"),
		);
		assert!(!metrics_request_authorized(&headers, Some("bridge-token")));
	}
}
