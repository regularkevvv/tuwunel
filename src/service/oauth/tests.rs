//! Tests for the pieces of the OAuth service that decide, rather than fetch.
//!
//! `classify_upstream` is the whole of the refresh-time revocation policy: it
//! is what turns a provider's answer into "end this Matrix session" or "come
//! back later". It is exercised here against errors produced by real HTTP
//! exchanges with an in-process provider, so the mapping from a status line to
//! a decision is tested through the same `reqwest` error path production uses,
//! not against hand-built error values.

use std::{net::SocketAddr, time::Duration};

use axum::{Router, http::StatusCode, routing::post};
use tokio::{net::TcpListener, spawn};
use tuwunel_core::{Error, Result};
use url::Url;

use super::{Recheck, Session, TokenResponse, classify_upstream};

/// Start a provider whose token endpoint always answers `status`, and return
/// its token URL.
async fn provider(status: StatusCode) -> Url {
	let app = Router::new()
		.route("/token", post(move || async move { (status, r#"{"error":"invalid_grant"}"#) }));

	let listener = TcpListener::bind("127.0.0.1:0")
		.await
		.expect("bound a loopback listener");

	let addr: SocketAddr = listener.local_addr().expect("listener address");

	spawn(async move {
		axum::serve(listener, app)
			.await
			.expect("mock provider served");
	});

	Url::parse(&format!("http://{addr}/token")).expect("token URL")
}

/// Post to `url` the way the service does, and surface the error it would.
async fn post_token(url: Url) -> Result<()> {
	let response = reqwest::Client::builder()
		.timeout(Duration::from_secs(10))
		.build()
		.expect("client built")
		.post(url)
		.body("grant_type=refresh_token")
		.send()
		.await?
		.error_for_status()?;

	drop(response);

	Ok(())
}

async fn outcome(status: StatusCode) -> Recheck {
	let url = provider(status).await;
	let error = post_token(url)
		.await
		.expect_err("the mock provider refuses every exchange");

	classify_upstream(&error)
}

#[tokio::test]
async fn a_refused_grant_denies_the_session() {
	// The provider says the grant is gone or the policy no longer admits the
	// user. This is the only class of answer allowed to end a Matrix session.
	for status in [StatusCode::BAD_REQUEST, StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
		assert!(
			matches!(outcome(status).await, Recheck::Denied(..)),
			"{status} should deny the session"
		);
	}
}

#[tokio::test]
async fn a_failing_provider_is_retryable_and_revokes_nothing() {
	// A provider outage must not log the whole server out, so every server-side
	// failure — and the two client-side statuses that mean "later" — is
	// retryable.
	for status in [
		StatusCode::REQUEST_TIMEOUT,
		StatusCode::TOO_MANY_REQUESTS,
		StatusCode::INTERNAL_SERVER_ERROR,
		StatusCode::BAD_GATEWAY,
		StatusCode::SERVICE_UNAVAILABLE,
		StatusCode::GATEWAY_TIMEOUT,
	] {
		assert!(
			matches!(outcome(status).await, Recheck::Unavailable(..)),
			"{status} should be retryable"
		);
	}
}

#[tokio::test]
async fn an_unreachable_provider_is_retryable() {
	// Bind and drop, so the port is one nothing listens on.
	let listener = TcpListener::bind("127.0.0.1:0")
		.await
		.expect("bound a loopback listener");

	let addr: SocketAddr = listener.local_addr().expect("listener address");
	drop(listener);

	let url = Url::parse(&format!("http://{addr}/token")).expect("token URL");
	let error = post_token(url)
		.await
		.expect_err("nothing is listening on that port");

	assert!(
		matches!(classify_upstream(&error), Recheck::Unavailable(..)),
		"a connection failure must not revoke anything"
	);
}

#[test]
fn a_local_failure_is_retryable() {
	// Our own parse or configuration failures are not the provider refusing.
	let error = Error::Json(
		serde_json::from_str::<serde_json::Value>("not json").expect_err("invalid JSON"),
	);

	assert!(matches!(classify_upstream(&error), Recheck::Unavailable(..)));
}

#[test]
fn a_provider_error_body_denies_the_session() {
	// A provider that answers HTTP 200 with an OAuth error body (RFC 6749 §5.2)
	// reaches us as the forbidden request error `request()` raises for it.
	let error = Error::Request(
		ruma::api::error::ErrorKind::forbidden(),
		"Error from provider: invalid_grant: (no description)".into(),
		StatusCode::FORBIDDEN,
	);

	assert!(matches!(classify_upstream(&error), Recheck::Denied(..)));
}

#[test]
fn a_refresh_response_without_a_refresh_token_keeps_the_stored_one() {
	// RFC 6749 §5.1: a refresh response may omit `refresh_token` (the presented
	// one stays valid) and `scope` (identical to the request). Overwriting them
	// would discard the grant on the very first re-check.
	let session = Session {
		access_token: Some("old-access".to_owned()),
		refresh_token: Some("stored-refresh".to_owned()),
		scope: Some("openid email".to_owned()),
		..Default::default()
	};

	let response: TokenResponse = serde_json::from_value(serde_json::json!({
		"token_type": "bearer",
		"access_token": "new-access",
		"expires_in": 900,
	}))
	.expect("token response deserializes");

	let refreshed = session
		.apply_token_response(response)
		.expect("token response applies");

	assert_eq!(refreshed.access_token.as_deref(), Some("new-access"));
	assert_eq!(refreshed.refresh_token.as_deref(), Some("stored-refresh"));
	assert_eq!(refreshed.scope.as_deref(), Some("openid email"));
	assert!(refreshed.expires_at.is_some(), "the new expiry is recorded");
}

#[test]
fn a_rotated_refresh_token_replaces_the_stored_one() {
	let session = Session {
		refresh_token: Some("stored-refresh".to_owned()),
		..Default::default()
	};

	let response: TokenResponse = serde_json::from_value(serde_json::json!({
		"access_token": "new-access",
		"refresh_token": "rotated-refresh",
	}))
	.expect("token response deserializes");

	let refreshed = session
		.apply_token_response(response)
		.expect("token response applies");

	assert_eq!(refreshed.refresh_token.as_deref(), Some("rotated-refresh"));
}
