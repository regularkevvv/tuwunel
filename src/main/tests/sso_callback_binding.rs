#![cfg(test)]

//! The SSO callback binds its authorization state to the browser that started
//! the sign-in, and the code it redeems to that sign-in's PKCE verifier (Phase
//! 2 gate A1), through the real HTTP handlers.
//!
//! - A `state` naming no grant session is refused (403).
//! - A `state` from one sign-in presented with another sign-in's grant cookie
//!   is refused (401), and so is a valid `state` without any grant cookie
//!   (401), with the provider's `check_cookie` left on as it is by default.
//!   Neither refusal reaches the provider, and neither spends the sign-in: it
//!   still completes with its own cookie.
//! - A completed sign-in's `state`, replayed with its own cookie and code, is
//!   refused without reaching the provider: the grant session is single-use. No
//!   refusal carries a login token.
//! - The provider fixture enforces PKCE (RFC 7636 section 4.6): every code it
//!   issues is bound to the `code_challenge` of the authorization request that
//!   obtained it, and its token endpoint refuses unless
//!   `BASE64URL(SHA256(code_verifier))` equals that challenge. The homeserver
//!   asks for S256 with the challenge of the verifier it stored for that
//!   sign-in's grant session, sends that stored verifier to the token endpoint,
//!   and does not keep it once the exchange succeeded. A code obtained for one
//!   sign-in and redeemed through another sign-in's callback is refused by the
//!   provider, and no login token is issued.

use std::{
	collections::BTreeMap,
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
use tuwunel_core::{
	Err, Result, err,
	ruma::serde::{Base64, base64::UrlSafe},
	utils::hash::sha256,
};
use tuwunel_service::Services;

/// 32 bytes of 0x07, URL-safe base64 without padding.
const GRANT_KEY: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc";

const REDIRECT: &str = "im.fluffychat.auth:/login";

const CALLBACK: &str = "/_matrix/client/unstable/login/sso/callback/test-client";

/// What an issued code is bound to.
struct Grant {
	challenge: String,
	redirect_uri: String,
}

/// The codes the provider fixture issued and how it answered their exchanges.
#[derive(Default)]
struct Fixture {
	issued: u32,
	codes: BTreeMap<String, Grant>,
	exchanged: u32,
	refused: Vec<&'static str>,
	/// The `code_verifier` of every exchange, in the order they arrived.
	verifiers: Vec<Option<String>>,
}

type Shared = Arc<Mutex<Fixture>>;

/// One started sign-in: its `state`, its grant cookie, and the provider
/// authorization request the homeserver redirected the browser to.
struct Started {
	state: String,
	cookie: String,
	authorize: Url,
}

#[test]
fn sso_callbacks_bind_state_cookie_and_pkce_to_one_sign_in() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let provider = TcpListener::bind(("127.0.0.1", 0))?;
	let issuer = format!("http://{}", provider.local_addr()?);
	provider.set_nonblocking(true)?;
	let db_path = temp_dir().join(format!("tuwunel-test-sso-callback-{}", process_id()));
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
		format!("identity_provider.test.callback_url=\"http://127.0.0.1:{port}{CALLBACK}\""),
	]);

	let fixture: Shared = Arc::default();

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
					.map_err(|error| err!("callback binding test timed out: {error}"))
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

	// The homeserver asks for S256 PKCE with the challenge of the verifier it
	// stored for this sign-in's grant session, and a sign-in completes against a
	// provider that enforces it: the verifier it sends is that stored verifier.
	let first = start(client, base).await?;
	assert_eq!(query_value(&first.authorize, "code_challenge_method")?, "S256");
	let challenge = query_value(&first.authorize, "code_challenge")?;
	assert_eq!(challenge.len(), 43, "an S256 challenge is 43 base64url characters");
	let stored = services
		.oauth
		.sessions
		.get(&first.state)
		.await?
		.code_verifier
		.ok_or_else(|| err!("the grant session stored no PKCE verifier"))?;
	assert_eq!(
		s256(&stored),
		challenge,
		"the challenge must be S256 of the verifier stored for this sign-in"
	);
	let code = authorize(client, &first).await?;
	let completed = callback(client, base, &first.state, &code, Some(&first.cookie)).await?;
	let token = login_token(&completed)?;
	let login = client
		.post(format!("{base}/_matrix/client/v3/login"))
		.json(&json!({"type": "m.login.token", "token": token, "refresh_token": true}))
		.send()
		.await?;
	assert_eq!(login.status().as_u16(), 200, "login token redemption failed");
	assert_eq!(exchanges(fixture)?, (1, Vec::new()), "the PKCE exchange must succeed");
	assert_eq!(
		fixture.lock()?.verifiers,
		[Some(stored)],
		"the token request must carry the verifier stored for this sign-in"
	);
	assert!(
		!services
			.oauth
			.sessions
			.get(&first.state)
			.await
			.is_ok_and(|session| session.code_verifier.is_some()),
		"the PKCE verifier must not be kept after its exchange"
	);

	// A state naming no grant session is refused.
	let a = start(client, base).await?;
	let b = start(client, base).await?;
	let code_a = authorize(client, &a).await?;
	let unknown =
		callback(client, base, "no-such-grant-session", &code_a, Some(&a.cookie)).await?;
	refused(unknown, 403, "M_FORBIDDEN", "a state naming no session").await?;

	// A's state with B's grant cookie is refused.
	let crossed = callback(client, base, &a.state, &code_a, Some(&b.cookie)).await?;
	refused(crossed, 401, "M_UNAUTHORIZED", "A's state with B's grant cookie").await?;

	// A's valid state without any grant cookie is refused.
	let cookieless = callback(client, base, &a.state, &code_a, None).await?;
	refused(cookieless, 401, "M_UNAUTHORIZED", "A's state without a grant cookie").await?;

	// None of the refusals reached the provider or spent A's sign-in.
	assert_eq!(exchanges(fixture)?, (1, Vec::new()), "a refused callback reached the provider");
	let completed = callback(client, base, &a.state, &code_a, Some(&a.cookie)).await?;
	login_token(&completed)?;
	assert_eq!(exchanges(fixture)?, (2, Vec::new()), "A's own callback must complete");

	// A's completed state, replayed with its own cookie and code, is refused
	// without reaching the provider: the grant session is single-use.
	let replayed = callback(client, base, &a.state, &code_a, Some(&a.cookie)).await?;
	refused(replayed, 401, "M_UNAUTHORIZED", "A's state replayed after completing").await?;
	assert_eq!(exchanges(fixture)?, (2, Vec::new()), "a replayed callback reached the provider");

	// A code obtained for C's sign-in, redeemed through D's callback with D's
	// state and cookie, reaches the provider with D's verifier: the provider
	// refuses it, and no login token is issued.
	let c = start(client, base).await?;
	let d = start(client, base).await?;
	let code_c = authorize(client, &c).await?;
	let substituted = callback(client, base, &d.state, &code_c, Some(&d.cookie)).await?;
	let status = substituted.status().as_u16();
	let issued = substituted
		.headers()
		.get("location")
		.and_then(|location| location.to_str().ok())
		.is_some_and(|location| location.contains("loginToken"));
	assert!(!issued, "a login token was issued for another sign-in's code");
	assert!(status >= 400, "a substituted code must be refused, got {status}");
	assert_eq!(
		exchanges(fixture)?,
		(2, vec!["pkce"]),
		"the provider must refuse the substituted code on its PKCE binding"
	);

	Ok(())
}

/// Starts a sign-in at the homeserver.
async fn start(client: &Client, base: &str) -> Result<Started> {
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
	let authorize = location(&start)?;
	let state = query_value(&authorize, "state")?;

	Ok(Started { state, cookie, authorize })
}

/// Follows a started sign-in to the provider's authorization endpoint, which
/// answers with a redirect to the callback carrying a code and the `state`.
async fn authorize(client: &Client, started: &Started) -> Result<String> {
	let response = client
		.get(started.authorize.clone())
		.send()
		.await?;
	assert_eq!(response.status().as_u16(), 302, "the provider refused the authorization");
	let back = location(&response)?;
	assert_eq!(query_value(&back, "state")?, started.state, "the provider must echo the state");

	query_value(&back, "code")
}

async fn callback(
	client: &Client,
	base: &str,
	state: &str,
	code: &str,
	cookie: Option<&str>,
) -> Result<Response> {
	let request = client
		.get(format!("{base}{CALLBACK}"))
		.query(&[("state", state), ("code", code)]);
	let request = match cookie {
		| Some(cookie) => request.header("cookie", cookie),
		| None => request,
	};

	Ok(request.send().await?)
}

/// The login token a completed callback redirected the client with.
fn login_token(response: &Response) -> Result<String> {
	let status = response.status().as_u16();
	if status != 302 {
		return Err!("SSO callback failed with {status}");
	}

	query_value(&location(response)?, "loginToken")
}

/// A callback refused with `status` and `errcode`, redirecting nowhere and so
/// issuing no login token.
async fn refused(response: Response, status: u16, errcode: &str, case: &str) -> Result {
	assert_eq!(response.status().as_u16(), status, "{case} must be refused");
	assert!(
		response.headers().get("location").is_none(),
		"{case} must not redirect with a login token"
	);
	let body: Value = response.json().await?;
	assert_eq!(body["errcode"], errcode, "{case}: {body}");

	Ok(())
}

/// The provider's successful exchanges and the reasons it refused the others.
fn exchanges(fixture: &Shared) -> Result<(u32, Vec<&'static str>)> {
	let fixture = fixture.lock()?;

	Ok((fixture.exchanged, fixture.refused.clone()))
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

/// `BASE64URL(SHA256(verifier))`, the S256 code challenge (RFC 7636 4.2).
fn s256(verifier: &str) -> String {
	Base64::<UrlSafe, _>::new(sha256::hash(verifier.as_bytes())).encode()
}

/// A local OpenID provider that enforces PKCE: its authorization endpoint
/// binds each code it issues to the request's S256 challenge and redirect
/// URI, and its token endpoint redeems a code once, only with a verifier
/// matching that challenge and with the same redirect URI.
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
		let mut parts = request.split_whitespace();
		let method = parts.next().unwrap_or_default().to_owned();
		let target = Url::parse(&format!("{issuer}{}", parts.next().unwrap_or_default()))?;
		let query = |name: &str| query_value(&target, name).ok();

		let (status, redirect, reply) = match (method.as_str(), target.path()) {
			| ("GET", "/.well-known/openid-configuration") => (
				200,
				None,
				json!({
					"issuer": issuer,
					"authorization_endpoint": format!("{issuer}/authorize"),
					"token_endpoint": format!("{issuer}/token"),
					"userinfo_endpoint": format!("{issuer}/userinfo"),
				}),
			),
			| ("GET", "/authorize") => {
				let (Some(challenge), Some(redirect_uri), Some(state)) =
					(query("code_challenge"), query("redirect_uri"), query("state"))
				else {
					return Err!("authorization request without PKCE, redirect or state");
				};
				if query("response_type").as_deref() != Some("code")
					|| query("code_challenge_method").as_deref() != Some("S256")
				{
					return Err!("authorization request is not code + S256 PKCE");
				}
				let mut fixture = fixture.lock()?;
				fixture.issued = fixture.issued.saturating_add(1);
				let code = format!("pkce-bound-code-{}", fixture.issued);
				let mut back = Url::parse(&redirect_uri)?;
				back.query_pairs_mut()
					.append_pair("code", &code)
					.append_pair("state", &state);
				fixture
					.codes
					.insert(code, Grant { challenge, redirect_uri });
				(302, Some(back.to_string()), Value::Null)
			},
			| ("POST", "/token") => {
				let mut fixture = fixture.lock()?;
				let grant = field("code").and_then(|code| fixture.codes.remove(&code));
				fixture.verifiers.push(field("code_verifier"));
				let refusal = match (grant, field("code_verifier")) {
					| (None, _) => Some("unknown_code"),
					| (Some(_), None) => Some("pkce"),
					| (Some(grant), Some(verifier)) if s256(&verifier) != grant.challenge =>
						Some("pkce"),
					| (Some(grant), Some(_))
						if field("redirect_uri").as_deref() != Some(&grant.redirect_uri) =>
						Some("redirect_uri"),
					| (Some(_), Some(_)) => None,
				};
				if let Some(refusal) = refusal {
					fixture.refused.push(refusal);
					(400, None, json!({"error": "invalid_grant"}))
				} else {
					fixture.exchanged = fixture.exchanged.saturating_add(1);
					let n = fixture.exchanged;
					(
						200,
						None,
						json!({
							"access_token": format!("access-{n}"),
							"refresh_token": format!("refresh-{n}"),
							"token_type": "Bearer",
							"expires_in": 300,
						}),
					)
				}
			},
			| ("GET", "/userinfo") =>
				(200, None, json!({"sub": "binding-subject", "preferred_username": "binder"})),
			| _ => return Err!("unexpected OAuth fixture request"),
		};

		let body = if reply.is_null() {
			String::new()
		} else {
			reply.to_string()
		};
		let (reason, location) = match (status, redirect) {
			| (302, Some(location)) => ("Found", format!("Location: {location}\r\n")),
			| (200, _) => ("OK", String::new()),
			| _ => ("Bad Request", String::new()),
		};
		let response = format!(
			"HTTP/1.1 {status} {reason}\r\n{location}Content-Type: \
			 application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
			body.len()
		);
		stream
			.get_mut()
			.write_all(response.as_bytes())
			.await?;
	}
}
