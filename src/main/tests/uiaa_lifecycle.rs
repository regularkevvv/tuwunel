#![cfg(test)]

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, var},
	fs::{DirBuilder, remove_dir_all},
	path::{Path, PathBuf},
	process::{Command, id as process_id},
	time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, http,
	ruma::{
		CanonicalJsonValue, DeviceId, UserId,
		api::client::uiaa::{AuthData, AuthFlow, AuthType, FallbackAcknowledgement, UiaaInfo},
	},
};
use tuwunel_database::{Deserialized, Json};
use tuwunel_service::Services;

const PHASE_ENV: &str = "TUWUNEL_UIAA_LIFECYCLE_PHASE";
const DATABASE_ENV: &str = "TUWUNEL_UIAA_LIFECYCLE_DATABASE";
const INDEX: &str = "uiaasessionid_metadata";
const INFO: &str = "userdevicesessionid_uiaainfo";

struct OwnedDirectory(PathBuf);

impl Drop for OwnedDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn uiaa_lifecycle_survives_restart() -> Result {
	if let Ok(phase) = var(PHASE_ENV) {
		let path = PathBuf::from(var(DATABASE_ENV).expect("child database path"));
		return run_server(&path, &phase);
	}
	let root = PathBuf::from(var("TMPDIR").unwrap_or_else(|_| "/tmp".into()));
	let directory = root.join(format!("uiaa-lifecycle-{}", process_id()));
	let mut builder = DirBuilder::new();
	builder.recursive(false);
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&directory)?; // Never adopt a pre-existing directory.
	let directory = OwnedDirectory(directory);
	for phase in ["seed", "resume"] {
		let status = Command::new(current_exe()?)
			.env(PHASE_ENV, phase)
			.env(DATABASE_ENV, directory.0.join("database"))
			.status()?;
		assert!(status.success(), "UIAA lifecycle child {phase} failed");
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
			| _ => panic!("unexpected child phase"),
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
		flows: vec![AuthFlow::new(vec![AuthType::Sso])],
		session: Some(session.into()),
		..Default::default()
	}
}

async fn create(services: &Services, user: &UserId, session: &str) -> Result {
	services
		.uiaa
		.create(
			user,
			"DEVICE".into(),
			&info(session),
			&CanonicalJsonValue::Object(Default::default()),
		)
		.await
}

async fn metadata(services: &Services, session: &str) -> Result<Value> {
	services.db[INDEX]
		.get(session)
		.await
		.deserialized()
}

async fn age(services: &Services, session: &str, seconds: u64) -> Result<Value> {
	let mut metadata = metadata(services, session).await?;
	let now = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("test clock follows the Unix epoch")
		.as_secs();
	let created = now.checked_sub(seconds).expect("age fits");
	metadata["created"] = created.into();
	metadata["expires"] = created
		.checked_add(900)
		.expect("expiry fits")
		.into();
	services.db[INDEX]
		.raw_put(session, Json(&metadata))
		.await?;
	Ok(metadata)
}

async fn seed(services: &Services, user: &UserId) -> Result {
	create(services, user, "restart-proof").await?;
	let before = age(services, "restart-proof", 60).await?;
	services
		.uiaa
		.complete_sso(user, "restart-proof")
		.await?;
	assert_eq!(metadata(services, "restart-proof").await?, before, "SSO must not refresh age");
	for index in 0..1023 {
		create(services, user, &format!("capacity-{index:04}")).await?;
	}
	let device: &DeviceId = "DEVICE".into();
	services.db[INFO]
		.put((user, device, "legacy-unaged"), Json(&info("legacy-unaged")))
		.await?;
	Ok(())
}

async fn resume(services: &Services, user: &UserId) -> Result {
	assert!(
		services
			.uiaa
			.get_uiaa_request(user, Some("DEVICE".into()), "restart-proof")
			.is_none()
	);
	let error = create(services, user, "overflow-after-restart")
		.await
		.expect_err("durable cap survives restart");
	assert_eq!(error.status_code(), http::StatusCode::TOO_MANY_REQUESTS);
	assert!(
		services
			.uiaa
			.get_uiaa_request(user, Some("DEVICE".into()), "overflow-after-restart")
			.is_none()
	);
	capacity_precedes_token_consumption(services, user).await?;
	assert!(
		services
			.uiaa
			.get_uiaa_session_by_session_id("restart-proof")
			.await
			.is_some()
	);
	assert!(
		services
			.uiaa
			.get_uiaa_session_by_session_id("legacy-unaged")
			.await
			.is_none()
	);
	let before = metadata(services, "restart-proof").await?;
	services
		.uiaa
		.complete_sso(user, "restart-proof")
		.await?;
	assert_eq!(
		metadata(services, "restart-proof").await?,
		before,
		"restart and callback preserve deadline"
	);
	let auth =
		AuthData::FallbackAcknowledgement(FallbackAcknowledgement::new("restart-proof".into()));
	assert!(
		services
			.uiaa
			.try_auth(user, "DEVICE".into(), &auth, &info("restart-proof"))
			.await?
			.0
	);
	assert_absent(services, INDEX, b"restart-proof").await;
	create(services, user, "expired-proof").await?;
	age(services, "expired-proof", 901).await?;
	assert!(
		services
			.uiaa
			.get_uiaa_session_by_session_id("expired-proof")
			.await
			.is_none()
	);
	services
		.uiaa
		.complete_sso(user, "expired-proof")
		.await
		.expect_err("expired callback must fail");
	let auth =
		AuthData::FallbackAcknowledgement(FallbackAcknowledgement::new("expired-proof".into()));
	services
		.uiaa
		.try_auth(user, "DEVICE".into(), &auth, &info("expired-proof"))
		.await
		.expect_err("expired proof must fail");
	collect(services).await?;
	assert_absent(services, INDEX, b"expired-proof").await;
	let key = tuwunel_database::keyval::serialize_key((
		user,
		<&DeviceId>::from("DEVICE"),
		"legacy-unaged",
	))?;
	assert_absent(services, INFO, &key).await;
	create(services, user, "reclaimed-capacity").await?;
	orphan_and_corruption(services, user).await
}

async fn collect(services: &Services) -> Result {
	// 40 bounded passes cover both maps twice, including cursor wraparound.
	for _ in 0..40 {
		services.uiaa.sweep_sessions().await?;
	}
	Ok(())
}

async fn orphan_and_corruption(services: &Services, user: &UserId) -> Result {
	let device: &DeviceId = "DEVICE".into();
	services.db[INFO]
		.del((user, device, "capacity-0000"))
		.await?;
	collect(services).await?;
	assert_absent(services, INDEX, b"capacity-0000").await;
	create(services, user, "after-orphan").await?;
	services.db[INDEX]
		.insert("capacity-0001", b"invalid-json")
		.await?;
	assert!(
		services
			.uiaa
			.get_uiaa_session_by_session_id("capacity-0001")
			.await
			.is_none()
	);
	collect(services).await?;
	assert_absent(services, INDEX, b"capacity-0001").await;
	create(services, user, "after-corruption").await?;
	let error = create(services, user, "still-bounded")
		.await
		.expect_err("reclamation must not reset the cap");
	assert_eq!(error.status_code(), http::StatusCode::TOO_MANY_REQUESTS);
	Ok(())
}

async fn assert_absent(services: &Services, map: &str, key: &[u8]) {
	let error = services.db[map]
		.get(key)
		.await
		.expect_err("test record must be absent");
	assert!(error.is_not_found(), "absence must not be an unrelated storage failure");
}

async fn capacity_precedes_token_consumption(services: &Services, user: &UserId) -> Result {
	use tuwunel_core::ruma::api::client::uiaa::RegistrationToken;
	use tuwunel_service::registration_tokens::{TokenExpires, TokenInfo};
	let (token, _) = services
		.registration_tokens
		.create_token(Some("capacity-proof"), None, TokenExpires {
			max_uses: Some(2),
			max_age: None,
		})
		.await?;
	let auth = AuthData::RegistrationToken(RegistrationToken::new(token.clone()));
	let challenge = UiaaInfo {
		flows: vec![AuthFlow::new(vec![AuthType::RegistrationToken, AuthType::Sso])],
		..Default::default()
	};
	let error = services
		.uiaa
		.try_auth(user, "DEVICE".into(), &auth, &challenge)
		.await
		.expect_err("capacity refusal precedes proof consumption");
	assert_eq!(error.status_code(), http::StatusCode::TOO_MANY_REQUESTS);
	let TokenInfo::Database(stored) = services
		.registration_tokens
		.get_token_info(&token)
		.await?
	else {
		panic!("fixture uses a database registration token");
	};
	assert_eq!(stored.uses, 0);
	services
		.registration_tokens
		.revoke_token(&token)
		.await
}
