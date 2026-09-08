#![cfg(test)]

use std::{fs::remove_dir_all, net::TcpListener, process::id as process_id, time::Duration};

use futures::future::join;
use reqwest::{Client, Response, Url, redirect::Policy};
use serde_json::{Value, json};
use tokio::{
	io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
	net::TcpListener as AsyncListener,
	time::{sleep, timeout},
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Err, Result, err, ruma::UserId};
use tuwunel_service::{Services, users::Register};

/// Exercise the actual HTTP handlers, not just create_login_token itself:
/// an unawaited database write used to produce a valid-looking redirect whose
/// token was immediately rejected by /login. Both redirect producers must
/// persist before responding, and the resulting token must remain single-use.
#[test]
fn login_redirects_persist_tokens_before_responding() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let provider = TcpListener::bind(("127.0.0.1", 0))?;
	let provider_port = provider.local_addr()?.port();
	provider.set_nonblocking(true)?;
	let db_path = format!("/tmp/tuwunel-test-login-redirect-{}", process_id());
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path=\"{db_path}\""),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"log=\"warn\"".to_owned(),
		format!("well_known.client=\"http://127.0.0.1:{port}\""),
		"oidc_native_auth=true".to_owned(),
		"ip_range_denylist=[]".to_owned(),
		"identity_provider.test.brand=\"test\"".to_owned(),
		"identity_provider.test.client_id=\"test-client\"".to_owned(),
		"identity_provider.test.client_secret=\"fixture-secret\"".to_owned(),
		"identity_provider.test.discovery=true".to_owned(),
		format!("identity_provider.test.issuer_url=\"http://127.0.0.1:{provider_port}\""),
		format!("identity_provider.test.authorization_url=\"http://127.0.0.1:{provider_port}/authorize\""),
		format!("identity_provider.test.token_url=\"http://127.0.0.1:{provider_port}/token\""),
		format!("identity_provider.test.userinfo_url=\"http://127.0.0.1:{provider_port}/userinfo\""),
		format!("identity_provider.test.callback_url=\"http://127.0.0.1:{port}/_matrix/client/unstable/login/sso/callback/test-client\""),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result: Result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let provider = tokio::spawn(serve_provider(AsyncListener::from_std(provider)?));
		let client = Client::builder()
			.redirect(Policy::none())
			.timeout(Duration::from_secs(10))
			.build()?;
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);
		let exercise = async {
			let outcome = timeout(Duration::from_mins(1), exercise(&services, &client, &base))
				.await
				.map_err(|error| err!("login redirect test timed out: {error}"))
				.and_then(|result| result);
			provider.abort();
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

async fn exercise(services: &Services, client: &Client, base: &str) -> Result {
	while client
		.get(format!("{base}/_matrix/client/versions"))
		.send()
		.await
		.is_err()
	{
		sleep(Duration::from_millis(20)).await;
	}

	let start = client
		.get(format!("{base}/_matrix/client/v3/login/sso/redirect/test-client"))
		.query(&[("redirectUrl", "im.fluffychat.auth:/login")])
		.send()
		.await?;
	assert_eq!(start.status().as_u16(), 302);
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
	let authorization = location(&start)?;
	let state = query_value(&authorization, "state")?;
	let callback = client
		.get(format!("{base}/_matrix/client/unstable/login/sso/callback/test-client"))
		.query(&[("state", state.as_str()), ("code", "fixture-code")])
		.header("cookie", cookie)
		.send()
		.await?;
	assert_eq!(callback.status().as_u16(), 302, "SSO callback failed");
	let redirect = location(&callback)?;
	assert_eq!(redirect.scheme(), "im.fluffychat.auth");
	assert_eq!(redirect.path(), "/login");
	redeem_once(client, base, &query_value(&redirect, "loginToken")?).await?;

	let user_id = UserId::parse_with_server_name("nativealice", services.globals.server_name())?;
	services
		.users
		.full_register(Register {
			user_id: Some(&user_id),
			password: Some("fixture-password"),
			..Default::default()
		})
		.await?;
	let native = client
		.post(format!("{base}/_tuwunel/oidc/native"))
		.form(&[
			("oidc_req_id", "fixture-request"),
			("username", "nativealice"),
			("password", "fixture-password"),
		])
		.send()
		.await?;
	assert_eq!(native.status().as_u16(), 303, "native submit failed");
	let redirect = location(&native)?;
	redeem_once(client, base, &query_value(&redirect, "loginToken")?).await
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

async fn redeem_once(client: &Client, base: &str, token: &str) -> Result {
	let body = json!({"type": "m.login.token", "token": token, "refresh_token": true});
	let login = client
		.post(format!("{base}/_matrix/client/v3/login"))
		.json(&body)
		.send()
		.await?;
	let status = login.status();
	let data: Value = login.json().await?;
	assert_eq!(status.as_u16(), 200, "redirect token was not persisted: {data}");
	let access_token = data
		.get("access_token")
		.and_then(Value::as_str)
		.ok_or_else(|| err!("login omitted access_token"))?;
	let whoami = client
		.get(format!("{base}/_matrix/client/v3/account/whoami"))
		.bearer_auth(access_token)
		.send()
		.await?;
	assert_eq!(whoami.status().as_u16(), 200, "new session could not authenticate");
	let identity: Value = whoami.json().await?;
	assert_eq!(identity.get("user_id"), data.get("user_id"));
	let replay = client
		.post(format!("{base}/_matrix/client/v3/login"))
		.json(&body)
		.send()
		.await?;
	assert_eq!(replay.status().as_u16(), 403, "login token was reusable");
	Ok(())
}

/// A local, plain OAuth fixture. It deliberately issues no id_token; the
/// existing signature/nonce tests cover OIDC verification independently.
/// Cookie validation remains enabled through the real SSO callback above.
async fn serve_provider(listener: AsyncListener) -> Result {
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
		let body = if request.starts_with("GET /.well-known/openid-configuration ") {
			json!({"issuer": issuer})
		} else if request.starts_with("POST /token ") {
			json!({"access_token": "fixture-access", "token_type": "Bearer", "expires_in": 3600})
		} else if request.starts_with("GET /userinfo ") {
			json!({"sub": "fixture-subject", "preferred_username": "ssoalice"})
		} else {
			return Err!("unexpected OAuth fixture request");
		};
		let body = body.to_string();
		let response = format!(
			"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
			 {}\r\nConnection: close\r\n\r\n{body}",
			body.len()
		);
		stream
			.get_mut()
			.write_all(response.as_bytes())
			.await?;
	}
}
