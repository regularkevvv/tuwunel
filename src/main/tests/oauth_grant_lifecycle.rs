#![cfg(test)]

//! Upstream grant lifecycle through the real HTTP handlers (ADR-0004).
//!
//! Two SSO sign-ins of one identity each obtain their own upstream grant,
//! stored sealed and bound to the device the sign-in created. A session that
//! holds no grant of its own is refused at refresh. Refreshing a device
//! re-checks exactly its own grant, also when both devices refresh at once;
//! logging a device out revokes and clears exactly its own grant; a provider
//! outage revokes nothing; a provider refusal ends every session of the
//! account and keeps the identity association; an operator's `query oauth
//! revoke-sessions` rejects every access token of the account at once, with
//! no refresh waited for, and revokes and drops its grants (gate A5); logging
//! out everywhere clears every grant; a login token cannot re-link an
//! identity; and deactivation clears every grant. The provider is a local
//! fixture that records every refresh and revocation it serves.
//!
//! Every log line the run produces at debug and above is captured, span
//! context included, by a subscriber installed in place of the server's own
//! (gate A7). The audit events for sign-in, refusal, refresh denial, operator
//! revocation and logout must be among them. No captured line may carry the
//! run's Matrix access, refresh or login tokens, its upstream tokens or the
//! grant key; no line at info or above — the level the image runs at — may
//! carry the authorization code, the client secret or the email address
//! either.

use std::{
	env::temp_dir,
	fs::remove_dir_all,
	io,
	net::TcpListener,
	process::id as process_id,
	sync::{Arc, Mutex},
	time::Duration,
};

use futures::{StreamExt, TryStreamExt, future::join};
use reqwest::{Client, Response, Url, redirect::Policy};
use serde_json::{Value, json};
use tokio::{
	io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
	net::TcpListener as AsyncListener,
	time::{sleep, timeout},
};
use tracing_subscriber::{Layer, filter::LevelFilter, fmt, layer::SubscriberExt};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_admin::{fini, init};
use tuwunel_core::{Err, Result, err, ruma::OwnedUserId};
use tuwunel_service::Services;

/// 32 bytes of 0x07, URL-safe base64 without padding.
const GRANT_KEY: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc";

const CLIENT_SECRET: &str = "fixture-secret";

const CODE: &str = "fixture-code";

const EMAIL: &str = "grantalice@example.test";

const REDIRECT: &str = "im.fluffychat.auth:/login";

/// The audit events of sign-in (`grant_bound`), refresh denial
/// (`grant_missing`, `upstream_denied`), refusal and operator revocation
/// (`sessions_revoked`, `grants_cleared`) and logout.
const AUDITS: [&str; 7] = [
	"grant_bound",
	"grant_missing",
	"upstream_denied",
	"sessions_revoked",
	"grants_cleared",
	"logout",
	"logout_all",
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Answer {
	Allow,
	Refuse,
	Outage,
}

/// What the provider fixture was asked, how it answers refreshes, and every
/// secret the run handed out.
struct Fixture {
	answer: Answer,
	issued: u32,
	refreshed: Vec<String>,
	revoked: Vec<String>,
	secrets: Vec<(&'static str, String)>,
}

type Shared = Arc<Mutex<Fixture>>;

/// Everything the run logged.
#[derive(Clone, Default)]
struct Lines(Arc<Mutex<Vec<u8>>>);

impl Lines {
	fn text(&self) -> Result<String> { Ok(String::from_utf8_lossy(&self.0.lock()?).into_owned()) }
}

impl io::Write for Lines {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		self.0
			.lock()
			.map_err(|_| io::Error::other("the log capture is poisoned"))?
			.extend_from_slice(buf);

		Ok(buf.len())
	}

	fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

#[test]
fn upstream_grants_are_sealed_bound_rechecked_and_revoked_per_device() -> Result {
	let lines = Lines::default();
	let writer = lines.clone();
	tracing::subscriber::set_global_default(
		tracing_subscriber::registry().with(
			fmt::layer()
				.with_ansi(false)
				.with_writer(move || writer.clone())
				.with_filter(LevelFilter::DEBUG),
		),
	)
	.map_err(|error| err!("the log capture could not be installed: {error}"))?;

	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let provider = TcpListener::bind(("127.0.0.1", 0))?;
	let issuer = format!("http://{}", provider.local_addr()?);
	provider.set_nonblocking(true)?;
	let db_path = temp_dir().join(format!("tuwunel-test-oauth-grants-{}", process_id()));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path=\"{}\"", db_path.display()),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		// The capture above is the global subscriber; the server keeps its own
		// out of the way.
		"log_global_default=false".to_owned(),
		"ip_range_denylist=[]".to_owned(),
		"access_token_ttl=900".to_owned(),
		"refresh_token_required=true".to_owned(),
		"grant_admin_to_first_user=false".to_owned(),
		format!("oauth_grant_key=\"{GRANT_KEY}\""),
		"sso_allowed_redirect_hosts=[\"im.fluffychat.auth\"]".to_owned(),
		"identity_provider.test.brand=\"test\"".to_owned(),
		"identity_provider.test.client_id=\"test-client\"".to_owned(),
		format!("identity_provider.test.client_secret=\"{CLIENT_SECRET}\""),
		"identity_provider.test.discovery=true".to_owned(),
		"identity_provider.test.require_upstream_refresh=true".to_owned(),
		format!("identity_provider.test.issuer_url=\"{issuer}\""),
		format!("identity_provider.test.authorization_url=\"{issuer}/authorize\""),
		format!("identity_provider.test.token_url=\"{issuer}/token\""),
		format!("identity_provider.test.userinfo_url=\"{issuer}/userinfo\""),
		format!("identity_provider.test.revocation_url=\"{issuer}/revoke\""),
		format!(
			"identity_provider.test.callback_url=\"http://127.0.0.1:{port}/_matrix/client/unstable/login/sso/callback/test-client\""
		),
	]);

	let fixture: Shared = Arc::new(Mutex::new(Fixture {
		answer: Answer::Allow,
		issued: 0,
		refreshed: Vec::new(),
		revoked: Vec::new(),
		secrets: vec![
			("authorization code", CODE.to_owned()),
			("client secret", CLIENT_SECRET.to_owned()),
			("grant key", GRANT_KEY.to_owned()),
			("email address", EMAIL.to_owned()),
		],
	}));

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
			let outcome = timeout(
				Duration::from_mins(3),
				exercise(&services, &client, &base, &fixture, &lines),
			)
			.await
			.map_err(|error| err!("grant lifecycle test timed out: {error}"))
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

async fn exercise(
	services: &Services,
	client: &Client,
	base: &str,
	fixture: &Shared,
	lines: &Lines,
) -> Result {
	while client
		.get(format!("{base}/_matrix/client/versions"))
		.send()
		.await
		.is_err()
	{
		sleep(Duration::from_millis(20)).await;
	}

	let (user_id, a, b) = two_devices(services, client, base, fixture).await?;

	// A session holding no upstream grant of its own is refused at refresh, and
	// only it is revoked.
	let grantless = grantless_device(services, client, base, fixture, &user_id).await?;
	let (status, denied) =
		refresh(client, base, fixture, &text(&grantless, "refresh_token")?).await?;
	assert_eq!(status, 401, "a session without a grant must be refused: {denied}");
	assert_eq!(denied["errcode"], "M_UNKNOWN_TOKEN");
	assert_eq!(devices(services, &user_id).await, 2, "only the grantless device may go");

	// Refreshing a device re-checks exactly its own grant.
	let (status, a1) = refresh(client, base, fixture, &text(&a, "refresh_token")?).await?;
	assert_eq!(status, 200, "refresh of device A failed: {a1}");
	assert_eq!(last(fixture, |state| &state.refreshed).as_deref(), Some("refresh-2"));

	// Both devices refreshing at once both succeed: their upstream exchanges run
	// one after the other inside the identity's critical section.
	let (first, second) = join(
		refresh(client, base, fixture, &text(&a1, "refresh_token")?),
		refresh(client, base, fixture, &text(&b, "refresh_token")?),
	)
	.await;
	let ((status_a, a2), (status_b, b1)) = (first?, second?);
	assert_eq!((status_a, status_b), (200, 200), "concurrent refreshes failed: {a2} {b1}");
	let refreshed = fixture.lock()?.refreshed.clone();
	assert!(
		refreshed
			.iter()
			.any(|token| token == "refresh-2r")
	);
	assert!(refreshed.iter().any(|token| token == "refresh-3"));

	// Logging device A out revokes and clears exactly A's grant.
	let logout = client
		.post(format!("{base}/_matrix/client/v3/logout"))
		.bearer_auth(text(&a2, "access_token")?)
		.json(&json!({}))
		.send()
		.await?;
	assert_eq!(logout.status().as_u16(), 200, "logout failed");
	assert_eq!(last(fixture, |state| &state.revoked).as_deref(), Some("refresh-2rr"));

	let (status, b2) = refresh(client, base, fixture, &text(&b1, "refresh_token")?).await?;
	assert_eq!(status, 200, "the other device must keep its grant: {b2}");
	assert_eq!(devices(services, &user_id).await, 1);

	// A provider outage refuses the refresh and revokes nothing.
	answer(fixture, Answer::Outage)?;
	let b2_refresh = text(&b2, "refresh_token")?;
	let (status, _) = refresh(client, base, fixture, &b2_refresh).await?;
	assert_eq!(status, 502, "an outage must be retryable");
	assert_eq!(devices(services, &user_id).await, 1, "an outage must revoke nothing");

	// A refusal ends every session of the account and drops every grant, but
	// keeps the identity association.
	answer(fixture, Answer::Refuse)?;
	let (status, refused) = refresh(client, base, fixture, &b2_refresh).await?;
	assert_eq!(status, 401, "a refusal must end the session: {refused}");
	assert_eq!(refused["errcode"], "M_UNKNOWN_TOKEN");
	// A hard logout: `soft_logout` is false, which the error body omits.
	assert!(
		refused
			.get("soft_logout")
			.is_none_or(|soft| soft.as_bool() == Some(false)),
		"a refusal must be a hard logout: {refused}"
	);
	assert_eq!(devices(services, &user_id).await, 0, "every device must be revoked");
	let sessions: Vec<_> = services
		.oauth
		.sessions
		.get_by_user(&user_id)
		.try_collect()
		.await?;
	assert!(!sessions.is_empty(), "the identity association must survive");
	assert!(
		sessions
			.iter()
			.all(|session| !session.has_grant()),
		"grants must be dropped"
	);
	assert!(revoked(fixture, "refresh-3rr")?, "the refused grant must be revoked upstream");

	// The next sign-in reaches the same account.
	answer(fixture, Answer::Allow)?;
	let (status, c) = sign_in(client, base, fixture, true).await?;
	assert_eq!(status, 200, "sign-in after refusal failed: {c}");
	assert_eq!(text(&c, "user_id")?, user_id.as_str());

	operator_revocation(services, client, base, fixture, &user_id, &c).await?;

	// Logging out everywhere revokes and clears every grant.
	let (status, everywhere) = sign_in(client, base, fixture, true).await?;
	assert_eq!(status, 200, "sign-in after operator revocation failed: {everywhere}");
	let everywhere_grant = issued_refresh(fixture)?;
	let logout_all = client
		.post(format!("{base}/_matrix/client/v3/logout/all"))
		.bearer_auth(text(&everywhere, "access_token")?)
		.json(&json!({}))
		.send()
		.await?;
	assert_eq!(logout_all.status().as_u16(), 200, "logout everywhere failed");
	assert_eq!(devices(services, &user_id).await, 0, "logout everywhere left a device");
	assert!(
		revoked(fixture, &everywhere_grant)?,
		"logout everywhere must revoke the grant upstream"
	);

	// A login token in a sign-in link cannot re-link an identity.
	let relink = client
		.get(format!("{base}/_matrix/client/v3/login/sso/redirect/test-client"))
		.query(&[("redirectUrl", REDIRECT), ("loginToken", "an-attacker-token")])
		.send()
		.await?;
	assert_eq!(relink.status().as_u16(), 403, "login-token linking must be refused");

	// Deactivation revokes and drops every grant, keeping the association.
	let (status, last_login) = sign_in(client, base, fixture, true).await?;
	assert_eq!(status, 200, "sign-in before deactivation failed: {last_login}");
	let last_grant = issued_refresh(fixture)?;
	services
		.users
		.deactivate_account(&user_id)
		.await?;
	let sessions: Vec<_> = services
		.oauth
		.sessions
		.get_by_user(&user_id)
		.try_collect()
		.await?;
	assert!(
		!sessions.is_empty()
			&& sessions
				.iter()
				.all(|session| !session.has_grant())
	);
	assert_eq!(last(fixture, |state| &state.revoked), Some(last_grant));

	audited(fixture, lines)
}

/// Operator revocation is immediate (gate A5). With two devices signed in and
/// both access tokens live, `query oauth revoke-sessions` rejects both at once
/// — access tokens live 15 minutes here and no refresh is waited for — revokes
/// both upstream grants at the provider and drops them, and keeps the account
/// and its identity association.
async fn operator_revocation(
	services: &Services,
	client: &Client,
	base: &str,
	fixture: &Shared,
	user_id: &OwnedUserId,
	c: &Value,
) -> Result {
	let c_grant = issued_refresh(fixture)?;
	let (status, d) = sign_in(client, base, fixture, true).await?;
	assert_eq!(status, 200, "second sign-in after refusal failed: {d}");
	let d_grant = issued_refresh(fixture)?;
	assert_eq!(devices(services, user_id).await, 2);

	let tokens = [text(c, "access_token")?, text(&d, "access_token")?];
	for token in &tokens {
		let (status, body) = whoami(client, base, token).await?;
		assert_eq!(status, 200, "a session must be live before the revocation: {body}");
	}

	init(&services.admin);
	let revocation = admin(services, format!("query oauth revoke-sessions {user_id}")).await;
	fini(&services.admin);
	revocation?;

	for token in &tokens {
		let (status, body) = whoami(client, base, token).await?;
		assert_eq!(status, 401, "an access token outlived the operator revocation: {body}");
		assert_eq!(body["errcode"], "M_UNKNOWN_TOKEN");
	}
	assert_eq!(devices(services, user_id).await, 0, "every device must be removed");

	let sessions: Vec<_> = services
		.oauth
		.sessions
		.get_by_user(user_id)
		.try_collect()
		.await?;
	assert!(!sessions.is_empty(), "the identity association must survive");
	assert!(
		sessions
			.iter()
			.all(|session| !session.has_grant()),
		"operator revocation must drop every grant"
	);
	assert!(
		revoked(fixture, &c_grant)? && revoked(fixture, &d_grant)?,
		"operator revocation must revoke both grants upstream"
	);

	Ok(())
}

/// Every audit event was logged, and no log line carries a secret of the run.
///
/// Tokens — Matrix access, refresh and login tokens, upstream tokens and the
/// grant key — reach no captured line at all. The authorization code, the
/// client secret and the email address are held to info, the level the image
/// runs at (`TUWUNEL_LOG=info` in the template's homeserver/Dockerfile): at
/// debug the fork prints them, the code in the router's response line for the
/// callback URI and the others in the `IdentityProvider` and `UserInfo` debug
/// output of span fields and return values. A leak is reported with every
/// secret in the line replaced by its kind.
fn audited(fixture: &Shared, lines: &Lines) -> Result {
	const DEBUG_ONLY: [&str; 3] = ["authorization code", "client secret", "email address"];

	let captured = lines.text()?;
	for audit in AUDITS {
		let field = format!("audit=\"{audit}\"");
		assert!(
			captured.lines().any(|line| line.contains(&field)),
			"no {audit} audit event was logged"
		);
	}

	let secrets = fixture.lock()?.secrets.clone();
	assert!(secrets.len() > 20, "the run must hand out the secrets it checks for");
	let leaks: Vec<String> = captured
		.lines()
		.filter(|line| {
			let debug = line.split_whitespace().nth(1) == Some("DEBUG");
			secrets.iter().any(|(kind, secret)| {
				line.contains(secret.as_str()) && !(debug && DEBUG_ONLY.contains(kind))
			})
		})
		.map(|line| redact(line, &secrets))
		.collect();
	assert!(
		leaks.is_empty(),
		"{} log line(s) carry a secret of the run:\n{}",
		leaks.len(),
		leaks
			.iter()
			.take(10)
			.map(|line| line.chars().take(600).collect::<String>())
			.collect::<Vec<_>>()
			.join("\n")
	);

	Ok(())
}

fn redact(line: &str, secrets: &[(&'static str, String)]) -> String {
	secrets
		.iter()
		.fold(line.to_owned(), |line, (kind, secret)| {
			line.replace(secret.as_str(), &format!("<{kind}>"))
		})
}

/// Two SSO sign-ins of one identity, after a refused one that asked for no
/// refresh token. Checks the account is not an administrator, the access
/// token lifetime, that each grant is bound to its own device, and that no
/// upstream token is stored in plaintext. Returns the account and both logins.
async fn two_devices(
	services: &Services,
	client: &Client,
	base: &str,
	fixture: &Shared,
) -> Result<(OwnedUserId, Value, Value)> {
	// A client that does not ask for refresh tokens is refused rather than
	// handed an access token that never expires.
	let (status, _) = sign_in(client, base, fixture, false).await?;
	assert_eq!(status, 400, "a login without refresh tokens must be refused");

	let (status, a) = sign_in(client, base, fixture, true).await?;
	assert_eq!(status, 200, "first device sign-in failed: {a}");
	let (status, b) = sign_in(client, base, fixture, true).await?;
	assert_eq!(status, 200, "second device sign-in failed: {b}");

	let user_id: OwnedUserId = text(&a, "user_id")?.try_into()?;
	assert_eq!(text(&b, "user_id")?, user_id.as_str(), "one identity must map to one account");
	assert!(
		!services.admin.user_is_admin(&user_id).await,
		"an SSO identity must not become the first administrator"
	);
	assert!(
		a["expires_in_ms"]
			.as_u64()
			.is_some_and(|ms| ms <= 900_000),
		"access tokens must live at most 15 minutes"
	);

	let (device_a, device_b) = (text(&a, "device_id")?, text(&b, "device_id")?);
	assert_ne!(device_a, device_b);

	let sessions: Vec<_> = services
		.oauth
		.sessions
		.get_by_user(&user_id)
		.try_collect()
		.await?;
	let bound: Vec<String> = sessions
		.iter()
		.filter_map(|session| {
			session
				.device_id
				.as_ref()
				.map(ToString::to_string)
		})
		.collect();
	assert!(bound.contains(&device_a) && bound.contains(&device_b), "grants were not bound");
	for session in &sessions {
		let sess_id = session
			.sess_id
			.as_deref()
			.ok_or_else(|| err!("grant record without an id"))?;
		let raw = services.db["oauthid_session"]
			.get(sess_id)
			.await?;
		assert!(
			!contains(&raw, b"refresh-") && !contains(&raw, b"access-"),
			"an upstream token was stored unsealed"
		);
	}

	Ok((user_id, a, b))
}

/// Signs a device in with a login token that no sign-in's upstream grant was
/// issued with, as a token minted outside an SSO sign-in is.
async fn grantless_device(
	services: &Services,
	client: &Client,
	base: &str,
	fixture: &Shared,
	user_id: &OwnedUserId,
) -> Result<Value> {
	let token = "grantless-login-token";
	fixture
		.lock()?
		.secrets
		.push(("login token", token.to_owned()));
	let _expires_in: u64 = services
		.users
		.create_login_token(user_id, token)
		.await;
	let login = client
		.post(format!("{base}/_matrix/client/v3/login"))
		.json(&json!({"type": "m.login.token", "token": token, "refresh_token": true}))
		.send()
		.await?;
	assert_eq!(login.status().as_u16(), 200, "a login token without a grant must sign in");
	let body: Value = login.json().await?;
	keep(fixture, &body)?;

	Ok(body)
}

async fn sign_in(
	client: &Client,
	base: &str,
	fixture: &Shared,
	refresh_token: bool,
) -> Result<(u16, Value)> {
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
		.query(&[("state", state.as_str()), ("code", CODE)])
		.header("cookie", cookie)
		.send()
		.await?;
	assert_eq!(callback.status().as_u16(), 302, "SSO callback failed");
	let token = query_value(&location(&callback)?, "loginToken")?;
	fixture
		.lock()?
		.secrets
		.push(("login token", token.clone()));
	let login = client
		.post(format!("{base}/_matrix/client/v3/login"))
		.json(&json!({"type": "m.login.token", "token": token, "refresh_token": refresh_token}))
		.send()
		.await?;
	let status = login.status().as_u16();
	let body = login.json().await.unwrap_or(Value::Null);
	keep(fixture, &body)?;

	Ok((status, body))
}

async fn refresh(
	client: &Client,
	base: &str,
	fixture: &Shared,
	token: &str,
) -> Result<(u16, Value)> {
	let response = client
		.post(format!("{base}/_matrix/client/v3/refresh"))
		.json(&json!({"refresh_token": token}))
		.send()
		.await?;
	let status = response.status().as_u16();
	let body = response.json().await.unwrap_or(Value::Null);
	keep(fixture, &body)?;

	Ok((status, body))
}

async fn whoami(client: &Client, base: &str, token: &str) -> Result<(u16, Value)> {
	let response = client
		.get(format!("{base}/_matrix/client/v3/account/whoami"))
		.bearer_auth(token)
		.send()
		.await?;
	let status = response.status().as_u16();

	Ok((status, response.json().await.unwrap_or(Value::Null)))
}

/// Runs an admin command in place, as the admin room would.
async fn admin(services: &Services, command: String) -> Result<String> {
	match services
		.admin
		.command_in_place(command, None)
		.await
	{
		| Ok(output) => Ok(output
			.map(|output| output.as_str().to_owned())
			.unwrap_or_default()),
		| Err(output) => Err!("admin command refused: {}", output.as_str()),
	}
}

/// Records the Matrix tokens a login or refresh answered with.
fn keep(fixture: &Shared, reply: &Value) -> Result {
	let mut fixture = fixture.lock()?;
	for (kind, field) in [("access token", "access_token"), ("refresh token", "refresh_token")] {
		if let Some(token) = reply.get(field).and_then(Value::as_str) {
			fixture.secrets.push((kind, token.to_owned()));
		}
	}

	Ok(())
}

async fn devices(services: &Services, user_id: &OwnedUserId) -> usize {
	services
		.users
		.all_device_ids(user_id)
		.count()
		.await
}

fn answer(fixture: &Shared, answer: Answer) -> Result {
	fixture.lock()?.answer = answer;
	Ok(())
}

/// The upstream refresh token of the grant issued last.
fn issued_refresh(fixture: &Shared) -> Result<String> {
	Ok(format!("refresh-{}", fixture.lock()?.issued))
}

fn revoked(fixture: &Shared, token: &str) -> Result<bool> {
	Ok(fixture
		.lock()?
		.revoked
		.iter()
		.any(|revoked| revoked == token))
}

fn last(fixture: &Shared, list: impl Fn(&Fixture) -> &Vec<String>) -> Option<String> {
	fixture
		.lock()
		.ok()
		.and_then(|fixture| list(&fixture).last().cloned())
}

fn text(value: &Value, field: &str) -> Result<String> {
	value
		.get(field)
		.and_then(Value::as_str)
		.map(ToOwned::to_owned)
		.ok_or_else(|| err!("response omitted {field}: {value}"))
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
	haystack
		.windows(needle.len())
		.any(|window| window == needle)
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

/// A local OAuth provider: every code exchange issues a distinct grant, and
/// every refresh and revocation is recorded. Refreshes are answered according
/// to the fixture's current `answer`. Every token it hands out is recorded as a
/// secret of the run.
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
		let form: Vec<(String, String)> = serde_urlencoded::from_bytes(&body).unwrap_or_default();
		let field = |name: &str| {
			form.iter()
				.find(|(key, _)| key == name)
				.map(|(_, value)| value.clone())
		};

		let (status, reply) = if request.starts_with("GET /.well-known/openid-configuration ") {
			(
				200,
				json!({
					"issuer": issuer,
					"authorization_endpoint": format!("{issuer}/authorize"),
					"token_endpoint": format!("{issuer}/token"),
					"userinfo_endpoint": format!("{issuer}/userinfo"),
					"revocation_endpoint": format!("{issuer}/revoke"),
				}),
			)
		} else if request.starts_with("POST /token ") {
			let mut fixture = fixture.lock()?;
			let (status, reply) = match (field("grant_type").as_deref(), field("refresh_token")) {
				| (Some("authorization_code"), _) => {
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
				},
				| (Some("refresh_token"), Some(presented)) => {
					fixture.refreshed.push(presented.clone());
					match fixture.answer {
						| Answer::Allow => (
							200,
							json!({
								"access_token": format!("access-{presented}"),
								"refresh_token": format!("{presented}r"),
								"token_type": "Bearer",
								"expires_in": 300,
							}),
						),
						| Answer::Refuse => (400, json!({"error": "invalid_grant"})),
						| Answer::Outage => (503, json!({})),
					}
				},
				| _ => return Err!("unexpected token request"),
			};
			for (kind, name) in [
				("upstream access token", "access_token"),
				("upstream refresh token", "refresh_token"),
			] {
				if let Some(token) = reply.get(name).and_then(Value::as_str) {
					fixture.secrets.push((kind, token.to_owned()));
				}
			}
			(status, reply)
		} else if request.starts_with("GET /userinfo ") {
			(
				200,
				json!({
					"sub": "fixture-subject",
					"preferred_username": "grantalice",
					"email": EMAIL,
				}),
			)
		} else if request.starts_with("POST /revoke ") {
			if let Some(token) = field("token") {
				fixture.lock()?.revoked.push(token);
			}
			(200, Value::Null)
		} else {
			return Err!("unexpected OAuth fixture request");
		};

		let body = if reply.is_null() {
			String::new()
		} else {
			reply.to_string()
		};
		let reason = match status {
			| 200 => "OK",
			| 400 => "Bad Request",
			| _ => "Service Unavailable",
		};
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
