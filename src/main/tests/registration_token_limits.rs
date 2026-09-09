#![cfg(test)]

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, var},
	fs::{DirBuilder, remove_dir_all},
	path::{Path, PathBuf},
	process::{Command, id as process_id},
	sync::Arc,
};

use futures::{StreamExt, future::join_all};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Result, config::registration_tokens::MAX_TOKEN_BYTES, http};
use tuwunel_database::Json;
use tuwunel_service::{
	Services,
	registration_tokens::{DatabaseTokenInfo, MAX_DATABASE_TOKENS, TokenExpires},
};

const PHASE_ENV: &str = "TUWUNEL_REGISTRATION_LIMITS_PHASE";
const DATABASE_ENV: &str = "TUWUNEL_REGISTRATION_LIMITS_DATABASE";
const MAP: &str = "registrationtoken_info";

struct OwnedDirectory(PathBuf);

impl Drop for OwnedDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn registration_token_limits_survive_restart() -> Result {
	if let Ok(phase) = var(PHASE_ENV) {
		return run_server(&PathBuf::from(var(DATABASE_ENV).expect("child database")), &phase);
	}
	let root = PathBuf::from(var("TMPDIR").unwrap_or_else(|_| "/tmp".into()));
	let path = root.join(format!("registration-limits-{}", process_id()));
	let mut builder = DirBuilder::new();
	builder.recursive(false);
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&path)?;
	let owned = OwnedDirectory(path);
	for phase in ["seed", "resume"] {
		let status = Command::new(current_exe()?)
			.env(PHASE_ENV, phase)
			.env(DATABASE_ENV, owned.0.join("database"))
			.status()?;
		assert!(status.success(), "registration limit child {phase} failed");
	}
	Ok(())
}

fn run_server(path: &Path, phase: &str) -> Result {
	let modes: &[&str] = if phase == "seed" { &["fresh"] } else { &[] };
	let mut args = Args::default_test(modes);
	args.maintenance = true;
	args.option
		.push(format!("database_path={path:?}"));
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let outcome = match phase {
			| "seed" => seed(&services).await,
			| "resume" => resume(&services).await,
			| _ => panic!("invalid fixture phase"),
		};
		let shutdown = server.server.shutdown();
		drop(services);
		let run = async_run(&server).await;
		let stop = async_stop(&server).await;
		outcome.and(shutdown).and(run).and(stop)
	});
	drop(runtime);
	result
}

fn expiry() -> TokenExpires { TokenExpires { max_uses: Some(1), max_age: None } }

async fn create(services: &Services, token: &str) -> Result {
	services
		.registration_tokens
		.create_token(Some(token), None, expiry())
		.await
		.map(|_| ())
}

async fn count(services: &Services) -> Result<usize> {
	Ok(services
		.registration_tokens
		.iterate_tokens()
		.await?
		.count()
		.await)
}

async fn seed(services: &Services) -> Result {
	assert_eq!(MAX_DATABASE_TOKENS, 1024);
	assert_eq!(MAX_TOKEN_BYTES, 256);
	for length in [0, MAX_TOKEN_BYTES.saturating_add(1), usize::MAX] {
		let error = services
			.registration_tokens
			.create_token(None, Some(length), expiry())
			.await
			.expect_err("reject before allocating a generated token");
		assert_eq!(error.status_code(), http::StatusCode::BAD_REQUEST);
	}
	for token in
		[String::new(), "has space".into(), "x".repeat(MAX_TOKEN_BYTES.saturating_add(1))]
	{
		assert_eq!(
			create(services, &token)
				.await
				.expect_err("invalid token")
				.status_code(),
			http::StatusCode::BAD_REQUEST
		);
	}
	for index in 0..MAX_DATABASE_TOKENS {
		create(services, &format!("capacity-{index:04}")).await?;
	}
	assert_eq!(count(services).await?, MAX_DATABASE_TOKENS);
	Ok(())
}

async fn resume(services: &Services) -> Result {
	assert_eq!(
		create(services, "overflow-after-restart")
			.await
			.expect_err("restart cannot reset admission")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	assert_eq!(count(services).await?, MAX_DATABASE_TOKENS);
	services
		.registration_tokens
		.try_consume("capacity-0000")
		.await?;
	create(services, "reclaimed-capacity").await?;
	services
		.registration_tokens
		.revoke_token("capacity-0001")
		.await?;
	last_slot_is_exclusive(services).await?;
	legacy_inventory_is_preserved(services).await?;
	legacy_records_are_bounded(services).await
}

async fn last_slot_is_exclusive(services: &Services) -> Result {
	let barrier = Arc::new(tokio::sync::Barrier::new(32));
	let tasks = (0..32).map(|index| {
		let tokens = Arc::clone(&services.registration_tokens);
		let barrier = Arc::clone(&barrier);
		tokio::spawn(async move {
			barrier.wait().await;
			tokens
				.create_token(Some(&format!("last-slot-{index}")), None, expiry())
				.await
		})
	});
	let mut created = 0_usize;
	for result in join_all(tasks).await {
		match result.expect("creator must not panic") {
			| Ok(_) => created = created.saturating_add(1),
			| Err(error) => assert_eq!(error.status_code(), http::StatusCode::TOO_MANY_REQUESTS),
		}
	}
	assert_eq!(created, 1, "one free durable slot admits one creator");
	assert_eq!(count(services).await?, MAX_DATABASE_TOKENS);
	Ok(())
}

async fn legacy_inventory_is_preserved(services: &Services) -> Result {
	let info = DatabaseTokenInfo { uses: 0, expires: expiry() };
	services.db[MAP]
		.raw_put("legacy-extra", Json(&info))
		.await?;
	assert_eq!(
		count(services)
			.await
			.expect_err("oversize inventory cannot be truncated")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	assert_eq!(
		create(services, "over-legacy-limit")
			.await
			.expect_err("legacy rows count toward admission")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	services
		.registration_tokens
		.get_token_info("legacy-extra")
		.await?;
	services
		.registration_tokens
		.get_token_info("capacity-0002")
		.await?;
	services
		.registration_tokens
		.revoke_token("legacy-extra")
		.await?;
	assert_eq!(count(services).await?, MAX_DATABASE_TOKENS);
	Ok(())
}

async fn legacy_records_are_bounded(services: &Services) -> Result {
	let info = DatabaseTokenInfo { uses: 0, expires: expiry() };
	services
		.registration_tokens
		.revoke_token("capacity-0002")
		.await?;
	let key = "L".repeat(MAX_TOKEN_BYTES.saturating_add(1));
	services.db[MAP]
		.raw_put(&key, Json(&info))
		.await?;
	assert!(
		count(services)
			.await
			.expect_err("oversized legacy key")
			.status_code()
			.is_server_error()
	);
	services
		.registration_tokens
		.get_token_info(&key)
		.await?;
	services
		.registration_tokens
		.revoke_token(&key)
		.await?;
	create(services, "after-legacy-key").await?;
	let mut body = serde_json::to_vec(&info)?;
	body.resize(1025, b' ');
	services.db[MAP]
		.insert("capacity-0003", &body)
		.await?;
	assert!(
		services
			.registration_tokens
			.get_token_info("capacity-0003")
			.await
			.expect_err("metadata byte limit")
			.status_code()
			.is_server_error()
	);
	assert!(
		count(services)
			.await
			.expect_err("oversized metadata cannot be omitted")
			.status_code()
			.is_server_error()
	);
	assert_eq!(
		services.db[MAP].get("capacity-0003").await?.len(),
		1025,
		"failed listing must preserve the legacy row"
	);
	services
		.registration_tokens
		.revoke_token("capacity-0003")
		.await?;
	create(services, "after-legacy-metadata").await?;
	assert_eq!(count(services).await?, MAX_DATABASE_TOKENS);
	Ok(())
}
