#![cfg(test)]

//! SSO identity mapping through the real HTTP handlers (Phase 2 gate A2).
//!
//! An upstream identity is its provider plus its `sub`; the account it signs
//! in to is decided once and then found by that identity alone. So:
//!
//! - a returning identity keeps its account when its username and email change;
//! - a second identity claiming a name already taken gets a distinct,
//!   deterministic fallback id and never the other account;
//! - no identity can claim the server account's localpart;
//! - the first account created this way is not made an administrator, with
//!   `grant_admin_to_first_user = false` as the product configures it
//!   (homeserver/tuwunel.toml).
//!
//! The provider is a local fixture whose userinfo reply the test sets before
//! each sign-in.

use std::{
	env::temp_dir,
	fs::remove_dir_all,
	net::TcpListener,
	process::id as process_id,
	sync::{Arc, Mutex},
	time::Duration,
};

use futures::future::join;
use reqwest::{Client, Response, Url, redirect::Policy};
use serde_json::{Value, json};
use tokio::{
	io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
	net::TcpListener as AsyncListener,
	time::{sleep, timeout},
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Err, Result, err, ruma::OwnedUserId};
use tuwunel_service::Services;

/// 32 bytes of 0x07, URL-safe base64 without padding.
const GRANT_KEY: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc";

const REDIRECT: &str = "im.fluffychat.auth:/login";

/// The userinfo the provider fixture answers with, and the grants it issued.
struct Fixture {
	userinfo: Value,
	issued: u32,
}

type Shared = Arc<Mutex<Fixture>>;

#[test]
fn sso_identities_map_by_subject_and_never_take_another_account() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let provider = TcpListener::bind(("127.0.0.1", 0))?;
	let issuer = format!("http://{}", provider.local_addr()?);
	provider.set_nonblocking(true)?;
	let db_path = temp_dir().join(format!("tuwunel-test-sso-identity-{}", process_id()));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path=\"{}\"", db_path.display()),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"log=\"warn\"".to_owned(),
		"ip_range_denylist=[]".to_owned(),
		"access_token_ttl=900".to_owned(),
		"refresh_token_required=true".to_owned(),
		// As the product configures it (homeserver/tuwunel.toml).
		"grant_admin_to_first_user=false".to_owned(),
		format!("oauth_grant_key=\"{GRANT_KEY}\""),
		"sso_allowed_redirect_hosts=[\"im.fluffychat.auth\"]".to_owned(),
		"identity_provider.test.brand=\"test\"".to_owned(),
		"identity_provider.test.client_id=\"test-client\"".to_owned(),
		"identity_provider.test.client_secret=\"fixture-secret\"".to_owned(),
		"identity_provider.test.discovery=true".to_owned(),
		format!("identity_provider.test.issuer_url=\"{issuer}\""),
		format!("identity_provider.test.authorization_url=\"{issuer}/authorize\""),
		format!("identity_provider.test.token_url=\"{issuer}/token\""),
		format!("identity_provider.test.userinfo_url=\"{issuer}/userinfo\""),
		format!(
			"identity_provider.test.callback_url=\"http://127.0.0.1:{port}/_matrix/client/unstable/login/sso/callback/test-client\""
		),
	]);

	let fixture: Shared = Arc::new(Mutex::new(Fixture { userinfo: Value::Null, issued: 0 }));

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result: Result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let serving =
			tokio::spawn(serve_provider(AsyncListener::from_std(provider)?, fixture.clone()));
		let client = Client::builder()
			.redirect(Policy::none())
			.timeout(Duration::from_secs(20))
			.build()?;
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);
		let exercise = async {
			let outcome =
				timeout(Duration::from_mins(2), exercise(&services, &client, &base, &fixture))
					.await
					.map_err(|error| err!("identity mapping test timed out: {error}"))
					.and_then(|result| result);
			serving.abort();
			let shutdown = server.server.shutdown();
			outcome.and(shutdown)
		};
		let (run, outcome) = join(async_run(&server), exercise).await;
		drop(services);
		let stop = async_stop(&server).await;
		outcome.and(run).and(stop)
	});
	drop(server);
	drop(runtime);
	remove_dir_all(&db_path).ok();
	result
}

async fn exercise(services: &Services, client: &Client, base: &str, fixture: &Shared) -> Result {
	while client
		.get(format!("{base}/_matrix/client/versions"))
		.send()
		.await
		.is_err()
	{
		sleep(Duration::from_millis(50)).await;
	}

	// The first identity takes the name it asks for, and is no administrator.
	let first =
		sign_in_as(client, base, fixture, "subject-a", "alice", "alice@example.test").await?;
	assert_eq!(first.localpart(), "alice");
	assert!(
		!services.admin.user_is_admin(&first).await,
		"the first SSO account must not be made an administrator"
	);

	// The same identity with a new username and email keeps its account.
	let returning =
		sign_in_as(client, base, fixture, "subject-a", "alice-renamed", "renamed@example.test")
			.await?;
	assert_eq!(returning, first, "a returning identity moved to another account");

	// Another identity asking for the taken name never gets that account: it
	// gets a fallback id derived from its own identity.
	let second =
		sign_in_as(client, base, fixture, "subject-b", "alice", "alice@example.test").await?;
	assert_ne!(second, first, "a second identity took the first identity's account");
	assert!(
		(15..=23).contains(&second.localpart().len()),
		"the fallback id is the identity's truncated unique id: {}",
		second.localpart()
	);
	// ...and it finds that same account again when it returns.
	let second_again =
		sign_in_as(client, base, fixture, "subject-b", "alice", "alice@example.test").await?;
	assert_eq!(second_again, second);

	// No identity can claim the server account's localpart.
	let server_user = &services.globals.server_user;
	let third = sign_in_as(
		client,
		base,
		fixture,
		"subject-c",
		server_user.localpart(),
		&format!("{}@example.test", server_user.localpart()),
	)
	.await?;
	assert_ne!(&third, server_user, "an SSO identity signed in as the server account");

	Ok(())
}

/// Sets the provider's identity, then signs in through the SSO redirect and
/// redeems the login token. Returns the account the homeserver chose.
async fn sign_in_as(
	client: &Client,
	base: &str,
	fixture: &Shared,
	sub: &str,
	username: &str,
	email: &str,
) -> Result<OwnedUserId> {
	fixture.lock()?.userinfo = json!({
		"sub": sub,
		"preferred_username": username,
		"email": email,
	});
	let start = client
		.get(format!("{base}/_matrix/client/v3/login/sso/redirect/test-client"))
		.query(&[("redirectUrl", REDIRECT)])
		.send()
		.await?;
	assert_eq!(start.status().as_u16(), 302, "SSO start failed");
	let cookie = start
		.headers()
		.get("set-cookie")
		.ok_or_else(|| err!("SSO start omitted cookie"))?
		.to_str()
		.map_err(|error| err!("invalid cookie header: {error}"))?
		.split(';')
		.next()
		.ok_or_else(|| err!("SSO start returned empty cookie"))?
		.to_owned();
	let state = query_value(&location(&start)?, "state")?;
	let callback = client
		.get(format!("{base}/_matrix/client/unstable/login/sso/callback/test-client"))
		.query(&[("state", state.as_str()), ("code", "fixture-code")])
		.header("cookie", cookie)
		.send()
		.await?;
	assert_eq!(callback.status().as_u16(), 302, "SSO callback failed");
	let token = query_value(&location(&callback)?, "loginToken")?;
	let login = client
		.post(format!("{base}/_matrix/client/v3/login"))
		.json(&json!({"type": "m.login.token", "token": token, "refresh_token": true}))
		.send()
		.await?;
	assert_eq!(login.status().as_u16(), 200, "login token redemption failed");
	let body: Value = login.json().await?;
	let user_id = body
		.get("user_id")
		.and_then(Value::as_str)
		.ok_or_else(|| err!("login reply omitted user_id"))?;
	OwnedUserId::try_from(user_id).map_err(|error| err!("invalid user id: {error}"))
}

fn location(response: &Response) -> Result<Url> {
	let value = response
		.headers()
		.get("location")
		.ok_or_else(|| err!("redirect omitted Location"))?
		.to_str()
		.map_err(|error| err!("invalid Location header: {error}"))?;
	Url::parse(value).map_err(Into::into)
}

fn query_value(url: &Url, name: &str) -> Result<String> {
	url.query_pairs()
		.find(|(key, _)| key == name)
		.map(|(_, value)| value.into_owned())
		.ok_or_else(|| err!("redirect omitted {name}"))
}

/// A local OpenID provider: discovery, a code exchange that issues a fresh
/// grant each time, and a userinfo reply taken from the fixture.
async fn serve_provider(listener: AsyncListener, fixture: Shared) -> Result {
	let issuer = format!("http://{}", listener.local_addr()?);
	loop {
		let (stream, _) = listener.accept().await?;
		let mut stream = BufReader::new(stream);
		let mut request = String::new();
		stream.read_line(&mut request).await?;
		let mut length = 0;
		loop {
			let mut line = String::new();
			stream.read_line(&mut line).await?;
			if line == "\r\n" || line.is_empty() {
				break;
			}
			if let Some(value) = line
				.to_lowercase()
				.strip_prefix("content-length:")
			{
				length = value
					.trim()
					.parse::<usize>()
					.map_err(|error| err!("bad length: {error}"))?;
			}
		}
		let mut body = vec![0; length];
		stream.read_exact(&mut body).await?;

		let (status, reply) = if request.starts_with("GET /.well-known/openid-configuration ") {
			(
				200,
				json!({
					"issuer": issuer,
					"authorization_endpoint": format!("{issuer}/authorize"),
					"token_endpoint": format!("{issuer}/token"),
					"userinfo_endpoint": format!("{issuer}/userinfo"),
				}),
			)
		} else if request.starts_with("POST /token ") {
			let mut fixture = fixture.lock()?;
			fixture.issued = fixture.issued.saturating_add(1);
			let n = fixture.issued;
			(
				200,
				json!({
					"access_token": format!("access-{n}"),
					"refresh_token": format!("refresh-{n}"),
					"token_type": "Bearer",
					"expires_in": 300,
				}),
			)
		} else if request.starts_with("GET /userinfo ") {
			(200, fixture.lock()?.userinfo.clone())
		} else {
			return Err!("unexpected OAuth fixture request");
		};

		let body = reply.to_string();
		let reason = if status == 200 { "OK" } else { "Service Unavailable" };
		let response = format!(
			"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: \
			 {}\r\nConnection: close\r\n\r\n{body}",
			body.len()
		);
		stream
			.get_mut()
			.write_all(response.as_bytes())
			.await?;
	}
}
