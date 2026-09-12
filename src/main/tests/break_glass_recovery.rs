#![cfg(test)]

//! Break-glass recovery (ADR-0004, phase 2 A6). With ordinary password login
//! off, an emergency password, set only by a signed break-glass release,
//! opens password login to the server account alone: any other account gets
//! the disabled-method answer whether or not its password is right. The next
//! start without it signs the server account out and clears its password.
//! Two processes over one database play the break-glass release and the
//! release that withdraws it, each serving real HTTP.

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, var},
	fs::{DirBuilder, read_to_string, remove_dir_all, write},
	net::TcpListener,
	path::{Path, PathBuf},
	process::{Command, id as process_id},
	time::Duration,
};

use futures::future::join;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Err, Result, err, ruma::UserId, utils::random_string};
use tuwunel_service::{Services, users::Register};

const PHASE_ENV: &str = "TUWUNEL_BREAK_GLASS_PHASE";
const DIRECTORY_ENV: &str = "TUWUNEL_BREAK_GLASS_DIRECTORY";
const PASSWORD_ENV: &str = "TUWUNEL_BREAK_GLASS_PASSWORD";
const ALICE_PASSWORD: &str = "alice-correct-horse";

struct OwnedDirectory(PathBuf);

impl Drop for OwnedDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn break_glass_opens_only_the_server_account_and_closes_again() -> Result {
	if let Ok(phase) = var(PHASE_ENV) {
		let directory = PathBuf::from(var(DIRECTORY_ENV).expect("child directory"));
		return run_server(&directory, &phase);
	}
	let root = PathBuf::from(var("TMPDIR").unwrap_or_else(|_| "/tmp".into()));
	let directory = root.join(format!("break-glass-recovery-{}", process_id()));
	let mut builder = DirBuilder::new();
	builder.recursive(false);
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&directory)?; // Never adopt a pre-existing directory.
	let directory = OwnedDirectory(directory);
	let password = random_string(32);
	for phase in ["open", "sealed"] {
		let status = Command::new(current_exe()?)
			.env(PHASE_ENV, phase)
			.env(DIRECTORY_ENV, &directory.0)
			.env(PASSWORD_ENV, &password)
			.status()?;
		assert!(status.success(), "break-glass child {phase} failed");
	}
	Ok(())
}

fn run_server(directory: &Path, phase: &str) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let password = var(PASSWORD_ENV).expect("child password");
	let modes: &[&str] = if phase == "open" { &["fresh"] } else { &[] };
	let mut args = Args::default_test(modes);
	args.option.extend([
		format!("database_path={:?}", directory.join("database")),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"log=\"warn\"".to_owned(),
		"login_with_password=false".to_owned(),
		"grant_admin_to_first_user=false".to_owned(),
	]);
	if phase == "open" {
		args.option
			.push(format!("emergency_password=\"{password}\""));
	}

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result: Result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let client = Client::builder()
			.timeout(Duration::from_secs(20))
			.build()?;
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);
		let exercise = async {
			let steps = async {
				ready(&client, &base).await?;
				match phase {
					| "open" => open(&services, &client, &base, &password, directory).await,
					| "sealed" => sealed(&services, &client, &base, &password, directory).await,
					| _ => panic!("unexpected child phase"),
				}
			};
			let outcome = timeout(Duration::from_mins(1), steps)
				.await
				.map_err(|error| err!("break-glass phase {phase} timed out: {error}"))
				.and_then(|result| result);
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
	result
}

/// The break-glass release: the server account alone may sign in with a
/// password, and only with the emergency one.
async fn open(
	services: &Services,
	client: &Client,
	base: &str,
	password: &str,
	directory: &Path,
) -> Result {
	let alice = UserId::parse_with_server_name("alice", services.globals.server_name())?;
	services
		.users
		.full_register(Register {
			user_id: Some(&alice),
			password: Some(ALICE_PASSWORD),
			grant_first_user_admin: false,
			..Default::default()
		})
		.await?;

	if !password_listed(client, base).await? {
		return Err!("a break-glass release does not list password login");
	}

	let (status, body) = login(client, base, "conduit", password).await?;
	let Some(token) = body["access_token"]
		.as_str()
		.filter(|_| status == StatusCode::OK)
	else {
		return Err!(
			"the server account could not sign in with the emergency password: {status}"
		);
	};

	let (status, body) = login(client, base, "conduit", "wrong-password").await?;
	if status != StatusCode::FORBIDDEN || body["errcode"] != "M_FORBIDDEN" {
		return Err!("a wrong emergency password was not refused: {status}");
	}

	// Another account: the same refusal whether or not its password is right.
	let right = login(client, base, "alice", ALICE_PASSWORD).await?;
	let wrong = login(client, base, "alice", "wrong-password").await?;
	if right.1["errcode"] != "M_UNKNOWN" || right.1.get("access_token").is_some() {
		return Err!("an ordinary account signed in with a password: {}", right.0);
	}
	if right != wrong {
		return Err!("the refusal differs between a right and a wrong password");
	}

	write(directory.join("token"), token)?;

	Ok(())
}

/// The release that withdraws it: no password login at all, and the
/// break-glass session and password are gone.
async fn sealed(
	services: &Services,
	client: &Client,
	base: &str,
	password: &str,
	directory: &Path,
) -> Result {
	let token = read_to_string(directory.join("token"))?;

	if password_listed(client, base).await? {
		return Err!("password login is still listed after the break-glass release");
	}

	let (status, body) = login(client, base, "conduit", password).await?;
	if body["errcode"] != "M_UNKNOWN" || body.get("access_token").is_some() {
		return Err!("the emergency password still signs in: {status}");
	}

	// The start seals the account in the background; give it a moment.
	let mut revoked = false;
	for _ in 0..100 {
		let response = client
			.get(format!("{base}/_matrix/client/v3/account/whoami"))
			.bearer_auth(&token)
			.send()
			.await?;
		if response.status() == StatusCode::UNAUTHORIZED {
			revoked = true;
			break;
		}
		sleep(Duration::from_millis(100)).await;
	}
	if !revoked {
		return Err!("the break-glass session survived the release that withdrew it");
	}

	if services
		.users
		.password_hash(&services.globals.server_user)
		.await
		.is_ok_and(|hash| !hash.is_empty())
	{
		return Err!("the server account kept its password");
	}

	Ok(())
}

async fn ready(client: &Client, base: &str) -> Result {
	for _ in 0..200 {
		if client
			.get(format!("{base}/_matrix/client/versions"))
			.send()
			.await
			.is_ok_and(|response| response.status().is_success())
		{
			return Ok(());
		}
		sleep(Duration::from_millis(100)).await;
	}

	Err!("the server never answered")
}

async fn login(
	client: &Client,
	base: &str,
	user: &str,
	password: &str,
) -> Result<(StatusCode, Value)> {
	let response = client
		.post(format!("{base}/_matrix/client/v3/login"))
		.json(&json!({
			"type": "m.login.password",
			"identifier": {"type": "m.id.user", "user": user},
			"password": password,
		}))
		.send()
		.await?;
	let status = response.status();

	Ok((status, response.json().await.unwrap_or(Value::Null)))
}

async fn password_listed(client: &Client, base: &str) -> Result<bool> {
	let flows: Value = client
		.get(format!("{base}/_matrix/client/v3/login"))
		.send()
		.await?
		.json()
		.await?;

	Ok(flows["flows"].as_array().is_some_and(|flows| {
		flows
			.iter()
			.any(|flow| flow["type"] == "m.login.password")
	}))
}
