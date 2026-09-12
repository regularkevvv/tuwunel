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

fn authorization(required: bool) -> Result<(super::Provider, Session)> {
	let provider = serde_json::from_value(serde_json::json!({
		"brand": "oidc", "client_id": "fixture", "require_upstream_refresh": required,
	}))
	.expect("valid provider fixture");
	Ok((provider, Session::default()))
}

async fn rechecks(
	items: Vec<Result<(super::Provider, Session)>>,
	answers: Vec<Recheck>,
) -> Option<Recheck> {
	let mut answers = answers.into_iter();
	let outcome = super::recheck_authorizations(futures::stream::iter(items), |_, _| {
		std::future::ready(
			answers
				.next()
				.expect("only applicable grants may be rechecked"),
		)
	})
	.await;
	assert!(answers.next().is_none(), "all supplied applicable grants must be checked");
	outcome
}

#[test]
fn only_an_absent_association_index_becomes_an_empty_list() {
	use tuwunel_database::{Handle, serialize_to_vec};

	use super::sessions::association_ids;
	let missing = Error::BadRequest(ruma::api::error::ErrorKind::NotFound, "missing index");
	assert!(association_ids(Err(missing)).unwrap().is_empty());
	let unreadable = Error::Database("READ-CANARY-SECRET".into());
	association_ids(Err(unreadable)).expect_err("unreadable index must fail");
	association_ids(Ok(Handle::from(vec![0xFE]))).expect_err("corrupt index must fail");
	let ids = vec!["one".to_owned(), "two".to_owned()];
	let bytes = serialize_to_vec(&ids).unwrap();
	assert_eq!(association_ids(Ok(Handle::from(bytes))).unwrap(), ids);
}

#[tokio::test]
async fn absent_and_readable_optional_authorizations_remain_exempt() {
	assert!(rechecks(vec![], vec![]).await.is_none());
	assert!(
		rechecks(vec![authorization(false)], vec![])
			.await
			.is_none()
	);
}

#[tokio::test]
async fn dangling_session_provider_and_storage_errors_refuse_refresh_without_echoing_details() {
	for error in [
		Error::BadRequest(ruma::api::error::ErrorKind::NotFound, "SESSION-CANARY-SECRET"),
		Error::BadRequest(ruma::api::error::ErrorKind::NotFound, "PROVIDER-CANARY-SECRET"),
		Error::Database("READ-CANARY-SECRET".into()),
	] {
		let outcome = rechecks(vec![Err(error)], vec![]).await;
		assert!(matches!(outcome, Some(Recheck::Unavailable(_))));
		assert!(!format!("{outcome:?}").contains("CANARY"));
	}
}

#[tokio::test]
async fn lookup_failure_cannot_be_overridden_by_success_or_optional_provider() {
	for error_first in [true, false] {
		let error = Err(Error::Database("READ-CANARY-SECRET".into()));
		let mut items = vec![authorization(true), authorization(false)];
		if error_first {
			items.insert(0, error);
		} else {
			items.push(error);
		}
		let outcome = rechecks(items, vec![Recheck::Allowed]).await;
		assert!(matches!(outcome, Some(Recheck::Unavailable(_))));
		assert!(!format!("{outcome:?}").contains("CANARY"));
	}
}

#[tokio::test]
async fn explicit_denial_wins_over_lookup_failure_and_provider_outage() {
	let outcome = rechecks(
		vec![
			Err(Error::Database("READ-CANARY-SECRET".into())),
			authorization(true),
			authorization(true),
		],
		vec![
			Recheck::Unavailable("provider outage".into()),
			Recheck::Denied("revoked".into()),
		],
	)
	.await;
	assert!(matches!(outcome, Some(Recheck::Denied(reason)) if reason == "revoked"));
}

#[tokio::test]
async fn every_applicable_provider_must_allow_a_refresh() {
	let outcome =
		rechecks(vec![authorization(true), authorization(false), authorization(true)], vec![
			Recheck::Allowed,
			Recheck::Allowed,
		])
		.await;
	assert!(matches!(outcome, Some(Recheck::Allowed)));
	let outcome = rechecks(vec![authorization(true), authorization(true)], vec![
		Recheck::Unavailable("provider outage".into()),
		Recheck::Allowed,
	])
	.await;
	assert!(matches!(outcome, Some(Recheck::Unavailable(_))));
}

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

fn grant_key(byte: u8) -> String {
	use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

	URL_SAFE_NO_PAD.encode([byte; super::seal::KEY_LEN])
}

fn material(refresh: &str) -> super::seal::Material {
	super::seal::Material {
		access_token: Some("ACCESS-CANARY".to_owned()),
		refresh_token: Some(refresh.to_owned()),
		id_token: None,
	}
}

#[test]
fn a_sealed_grant_hides_its_tokens_and_opens_only_on_its_own_record() {
	let keys = super::seal::Keys::new(Some(&grant_key(1)), &[]).expect("valid key");
	let sealed = keys
		.seal("record-a", &material("REFRESH-CANARY"))
		.expect("sealing succeeds")
		.expect("a current key seals");

	let stored = serde_json::to_string(&sealed).expect("sealed form serializes");
	assert!(!stored.contains("CANARY"), "no token may appear in the stored form");

	let opened = keys
		.open("record-a", &sealed)
		.expect("its own record opens it");
	assert_eq!(opened.refresh_token.as_deref(), Some("REFRESH-CANARY"));
	assert_eq!(opened.access_token.as_deref(), Some("ACCESS-CANARY"));

	assert!(
		keys.open("record-b", &sealed).is_err(),
		"material moved onto another record must not open"
	);

	let mut tampered = sealed;
	let first = if tampered.data.starts_with('A') { "B" } else { "A" };
	tampered.data.replace_range(0..1, first);
	assert!(keys.open("record-a", &tampered).is_err(), "tampered material must not open");
}

#[test]
fn rotation_opens_with_previous_keys_and_seals_with_the_current_one() {
	let old = super::seal::Keys::new(Some(&grant_key(1)), &[]).expect("valid key");
	let sealed = old
		.seal("record", &material("R1"))
		.expect("sealing succeeds")
		.expect("a current key seals");

	let rotated =
		super::seal::Keys::new(Some(&grant_key(2)), &[grant_key(1)]).expect("valid keys");
	let opened = rotated
		.open("record", &sealed)
		.expect("a previous key still opens");
	assert_eq!(opened.refresh_token.as_deref(), Some("R1"));

	let resealed = rotated
		.seal("record", &material("R1"))
		.expect("sealing succeeds")
		.expect("a current key seals");
	assert_eq!(Some(resealed.kid.as_str()), rotated.current_kid());
	assert_ne!(resealed.kid, sealed.kid);

	let forgotten = super::seal::Keys::new(Some(&grant_key(2)), &[]).expect("valid key");
	assert!(forgotten.open("record", &sealed).is_err(), "a removed key must not open");
}

#[test]
fn malformed_grant_keys_are_refused_without_echoing_them() {
	let short: String = grant_key(1).chars().take(40).collect();
	for key in ["KEY-CANARY", short.as_str(), "=====", ""] {
		let error = super::seal::Keys::new(Some(key), &[]).expect_err("malformed key is refused");
		assert!(!error.to_string().contains("KEY-CANARY"));
	}

	let unkeyed = super::seal::Keys::new(None, &[]).expect("no key is valid");
	assert!(
		unkeyed
			.seal("record", &material("R1"))
			.expect("sealing without a key succeeds")
			.is_none(),
		"without a key nothing is sealed"
	);
}

#[tokio::test]
async fn a_missing_grant_never_outranks_an_answer_and_never_masks_a_refusal() {
	let outcome = rechecks(vec![authorization(true), authorization(true)], vec![
		Recheck::NoGrant("cleared".into()),
		Recheck::Allowed,
	])
	.await;
	assert!(matches!(outcome, Some(Recheck::Allowed)));

	let outcome =
		rechecks(vec![authorization(true)], vec![Recheck::NoGrant("cleared".into())]).await;
	assert!(matches!(outcome, Some(Recheck::NoGrant(_))));

	let outcome = rechecks(vec![authorization(true), authorization(true)], vec![
		Recheck::NoGrant("cleared".into()),
		Recheck::Denied("revoked".into()),
	])
	.await;
	assert!(matches!(outcome, Some(Recheck::Denied(_))));

	let outcome = rechecks(vec![authorization(true), authorization(true)], vec![
		Recheck::Unavailable("outage".into()),
		Recheck::NoGrant("cleared".into()),
	])
	.await;
	assert!(matches!(outcome, Some(Recheck::Unavailable(_))));
}

#[test]
fn clearing_a_grant_keeps_its_identity_and_drops_every_credential() {
	let session = Session {
		sess_id: Some("record".to_owned()),
		idp_id: Some("provider".to_owned()),
		user_id: Some(ruma::user_id!("@alice:example.test").to_owned()),
		user_info: Some(super::UserInfo {
			sub: "subject".to_owned(),
			..Default::default()
		}),
		refresh_token: Some("stored-refresh".to_owned()),
		code_verifier: Some("verifier".to_owned()),
		device_id: Some("DEVICE".into()),
		login_token_hash: Some("hash".to_owned()),
		..Default::default()
	};
	assert!(session.has_grant());

	let cleared = session.cleared();
	assert!(!cleared.has_grant());
	assert!(cleared.code_verifier.is_none() && cleared.login_token_hash.is_none());
	assert!(cleared.device_id.is_none() && cleared.sealed.is_none());
	assert_eq!(cleared.sess_id.as_deref(), Some("record"));
	assert_eq!(
		cleared
			.user_id
			.as_deref()
			.map(ruma::UserId::as_str),
		Some("@alice:example.test")
	);
	assert_eq!(cleared.user_info.map(|info| info.sub).as_deref(), Some("subject"));
}
