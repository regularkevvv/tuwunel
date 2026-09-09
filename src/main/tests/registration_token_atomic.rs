#![cfg(test)]

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, var},
	fs::{DirBuilder, remove_dir_all},
	path::{Path, PathBuf},
	process::{Command, id as process_id},
	time::UNIX_EPOCH,
};

use serde_json::{json, value::to_raw_value};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, http,
	ruma::{
		CanonicalJsonValue, DeviceId, UserId,
		api::client::uiaa::{AuthData, AuthFlow, AuthType, Dummy, RegistrationToken, UiaaInfo},
	},
};
use tuwunel_database::Json;
use tuwunel_service::{
	Services,
	registration_tokens::{DatabaseTokenInfo, TokenExpires, TokenInfo},
};

const PHASE_ENV: &str = "TUWUNEL_REGISTRATION_ATOMIC_PHASE";
const DATABASE_ENV: &str = "TUWUNEL_REGISTRATION_ATOMIC_DATABASE";
const INFO_MAP: &str = "userdevicesessionid_uiaainfo";
const MAX_UIAA_BYTES: usize = 64 * 1024;

struct OwnedDirectory(PathBuf);

impl Drop for OwnedDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn registration_token_and_stage_commit_together() -> Result {
	if let Ok(phase) = var(PHASE_ENV) {
		return run_server(&PathBuf::from(var(DATABASE_ENV).expect("child database")), &phase);
	}
	let root = PathBuf::from(var("TMPDIR").unwrap_or_else(|_| "/tmp".into()));
	let path = root.join(format!("registration-atomic-{}", process_id()));
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
		assert!(status.success(), "registration atomic child {phase} failed");
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
		let user = UserId::parse_with_server_name("alice", services.globals.server_name())?;
		let outcome = match phase {
			| "seed" => seed(&services, &user).await,
			| "resume" => resume(&services, &user).await,
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

fn info(session: &str) -> UiaaInfo {
	UiaaInfo {
		flows: vec![AuthFlow::new(vec![AuthType::RegistrationToken, AuthType::Dummy])],
		session: Some(session.into()),
		..Default::default()
	}
}

fn auth(token: &str, session: &str) -> AuthData {
	let mut token = RegistrationToken::new(token.into());
	token.session = Some(session.into());
	AuthData::RegistrationToken(token)
}

async fn create_session(services: &Services, user: &UserId, info: &UiaaInfo) -> Result {
	services
		.uiaa
		.create(user, "DEVICE".into(), info, &CanonicalJsonValue::Object(Default::default()))
		.await
}

async fn create_token(services: &Services, token: &str, uses: u64) -> Result {
	services
		.registration_tokens
		.create_token(Some(token), None, TokenExpires { max_uses: Some(uses), max_age: None })
		.await
		.map(|_| ())
}

async fn uses(services: &Services, token: &str) -> Result<u64> {
	let TokenInfo::Database(info) = services
		.registration_tokens
		.get_token_info(token)
		.await?
	else {
		panic!("fixture must be a database token");
	};
	Ok(info.uses)
}

async fn seed(services: &Services, user: &UserId) -> Result {
	create_token(services, "rejected-progress", 2).await?;
	let mut oversized = info("size-rejection");
	oversized.params = Some(to_raw_value(&json!({"padding": ""}))?);
	let padding = MAX_UIAA_BYTES
		.checked_sub(serde_json::to_vec(&oversized)?.len())
		.expect("small fixture overhead");
	oversized.params = Some(to_raw_value(&json!({"padding": "x".repeat(padding)}))?);
	assert_eq!(serde_json::to_vec(&oversized)?.len(), MAX_UIAA_BYTES);
	create_session(services, user, &oversized).await?;
	let error = services
		.uiaa
		.try_auth(user, "DEVICE".into(), &auth("rejected-progress", "size-rejection"), &oversized)
		.await
		.expect_err("adding the token stage exceeds the durable record limit");
	assert_eq!(error.status_code(), http::StatusCode::PAYLOAD_TOO_LARGE);
	assert_eq!(
		uses(services, "rejected-progress").await?,
		0,
		"rejected UIAA progress must not spend a registration token"
	);
	let (_, _, unchanged) = services
		.uiaa
		.get_uiaa_session_by_session_id("size-rejection")
		.await
		.expect("failed progress preserves the live session");
	assert!(unchanged.completed.is_empty());

	// Repair only this owned fixture, then retry the same durable session.
	let repaired = info("size-rejection");
	let device: &DeviceId = "DEVICE".into();
	services.db[INFO_MAP]
		.put((user, device, "size-rejection"), Json(&repaired))
		.await?;
	let (worked, progress) = services
		.uiaa
		.try_auth(user, device, &auth("rejected-progress", "size-rejection"), &repaired)
		.await?;
	assert!(!worked);
	assert_eq!(progress.completed, vec![AuthType::RegistrationToken]);
	assert_eq!(uses(services, "rejected-progress").await?, 1);

	create_token(services, "restart-one-use", 1).await?;
	let challenge = info("restart-stage");
	create_session(services, user, &challenge).await?;
	let (worked, progress) = services
		.uiaa
		.try_auth(user, device, &auth("restart-one-use", "restart-stage"), &challenge)
		.await?;
	assert!(!worked);
	assert_eq!(progress.completed, vec![AuthType::RegistrationToken]);
	assert!(
		services
			.registration_tokens
			.get_token_info("restart-one-use")
			.await
			.expect_err("one-use token is exhausted")
			.is_not_found()
	);
	token_failures_preserve_progress(services, user).await?;
	Ok(())
}

async fn token_failures_preserve_progress(services: &Services, user: &UserId) -> Result {
	let expired = DatabaseTokenInfo {
		uses: 0,
		expires: TokenExpires {
			max_uses: None,
			max_age: Some(UNIX_EPOCH),
		},
	};
	let overflow = DatabaseTokenInfo {
		uses: u64::MAX,
		expires: TokenExpires { max_uses: None, max_age: None },
	};
	for (token, body, invalid) in [
		("expired-stage", serde_json::to_vec(&expired)?, true),
		("overflow-stage", serde_json::to_vec(&overflow)?, false),
		("corrupt-stage", b"not-json".to_vec(), false),
	] {
		services.db["registrationtoken_info"]
			.insert(token, &body)
			.await?;
		let challenge = info(token);
		create_session(services, user, &challenge).await?;
		let result = services
			.uiaa
			.try_auth(user, "DEVICE".into(), &auth(token, token), &challenge)
			.await;
		if invalid {
			let (worked, progress) = result?;
			assert!(!worked);
			assert!(progress.completed.is_empty());
			assert!(progress.auth_error.is_some());
		} else {
			assert!(
				result
					.expect_err("invalid storage cannot authorize a stage")
					.status_code()
					.is_server_error()
			);
		}
		assert_eq!(
			services.db["registrationtoken_info"]
				.get(token)
				.await?
				.as_ref(),
			body.as_slice(),
			"failed stage must preserve its token record"
		);
		let (_, _, unchanged) = services
			.uiaa
			.get_uiaa_session_by_session_id(token)
			.await
			.expect("failed stage preserves its UIAA session");
		assert!(unchanged.completed.is_empty());
		// Retire only these inspected negative fixtures before the restart.
		services
			.registration_tokens
			.revoke_token(token)
			.await?;
		services
			.uiaa
			.delete_session(user, "DEVICE".into(), token)
			.await?;
	}
	Ok(())
}

async fn resume(services: &Services, user: &UserId) -> Result {
	for (token, session) in
		[("rejected-progress", "size-rejection"), ("restart-one-use", "restart-stage")]
	{
		for _ in 0..3 {
			let (worked, progress) = services
				.uiaa
				.try_auth(user, "DEVICE".into(), &auth(token, session), &info(session))
				.await?;
			assert!(!worked);
			assert_eq!(progress.completed, vec![AuthType::RegistrationToken]);
		}
	}
	assert_eq!(uses(services, "rejected-progress").await?, 1);
	let mut dummy = Dummy::new();
	dummy.session = Some("restart-stage".into());
	assert!(
		services
			.uiaa
			.try_auth(user, "DEVICE".into(), &AuthData::Dummy(dummy), &info("restart-stage"))
			.await?
			.0
	);
	let error = services
		.uiaa
		.try_auth(
			user,
			"DEVICE".into(),
			&auth("restart-one-use", "restart-stage"),
			&info("restart-stage"),
		)
		.await
		.expect_err("completed UIAA authorization is single-use");
	assert_eq!(error.status_code(), http::StatusCode::FORBIDDEN);
	Ok(())
}
