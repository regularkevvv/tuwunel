#![cfg(test)]

#[path = "uiaa_transitions/registration_tokens.rs"]
mod token_races;

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::var,
	fmt::Debug,
	fs::{DirBuilder, remove_dir_all},
	iter::repeat_with,
	path::PathBuf,
	process::id as process_id,
	sync::Arc,
};

use futures::{future::join_all, join};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result,
	ruma::{
		CanonicalJsonValue, UserId,
		api::client::uiaa::{
			AuthData, AuthFlow, AuthType, FallbackAcknowledgement, MatrixUserIdentifier,
			Password, RegistrationToken, UiaaInfo, UserIdentifier,
		},
	},
};
use tuwunel_database::Json;
use tuwunel_service::{
	Services,
	registration_tokens::{TokenExpires, TokenInfo},
};

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn uiaa_transitions_are_owner_bound_and_single_consumer() -> Result {
	let root = var("TMPDIR").unwrap_or_else(|_| "/tmp".into());
	let path = PathBuf::from(root).join(format!("uiaa-transitions-{}", process_id()));
	let mut builder = DirBuilder::new();
	builder.recursive(false);
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&path)?; // Never adopt an existing path as test-owned state.
	let db = DatabasePath(path);
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.push(format!("database_path={:?}", db.0.join("database")));

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let outcome = exercise(&services).await;
		let shutdown = server.server.shutdown();
		drop(services);
		let run = async_run(&server).await;
		let stop = async_stop(&server).await;
		outcome.and(shutdown).and(run).and(stop)
	});
	drop(runtime);
	result
}

async fn exercise(services: &Services) -> Result {
	let user = UserId::parse_with_server_name("alice", services.globals.server_name())?;
	let other = UserId::parse_with_server_name("bob", services.globals.server_name())?;
	services
		.users
		.create(&user, Some("correct-local-password"), None)
		.await?;

	let body = CanonicalJsonValue::Object(Default::default());
	let mut info = UiaaInfo {
		flows: vec![AuthFlow::new(vec![AuthType::Sso])],
		session: Some("concurrent-consumption".into()),
		..Default::default()
	};
	services
		.uiaa
		.create(&user, "DEVICE".into(), &info, &body)
		.await?;
	assert!(
		services
			.uiaa
			.complete_sso(&other, "concurrent-consumption")
			.await
			.is_err()
	);
	services
		.uiaa
		.complete_sso(&user, "concurrent-consumption")
		.await?;
	let auth = AuthData::FallbackAcknowledgement(FallbackAcknowledgement::new(
		"concurrent-consumption".into(),
	));
	assert_forbidden(
		services
			.uiaa
			.try_auth(&user, "WRONGDEVICE".into(), &auth, &info)
			.await,
	);
	assert_forbidden(
		services
			.uiaa
			.try_auth(&other, "DEVICE".into(), &auth, &info)
			.await,
	);

	// Release distinct runtime tasks together, rather than polling every
	// request serially in one task against a warm database cache.
	let barrier = Arc::new(tokio::sync::Barrier::new(32));
	let results = join_all(
		repeat_with(|| {
			let barrier = Arc::clone(&barrier);
			let uiaa = Arc::clone(&services.uiaa);
			let user = user.clone();
			let auth = auth.clone();
			let info = info.clone();
			tokio::spawn(async move {
				barrier.wait().await;
				uiaa.try_auth(&user, "DEVICE".into(), &auth, &info)
					.await
			})
		})
		.take(32),
	)
	.await
	.into_iter()
	.map(|task| task.expect("UIAA contender task must complete"))
	.collect::<Vec<_>>();
	assert_eq!(
		results
			.iter()
			.filter(|result| matches!(result, Ok((true, _))))
			.count(),
		1
	);
	assert_eq!(
		results
			.iter()
			.filter(|result| result.is_err())
			.count(),
		31
	);
	for result in results.into_iter().filter(Result::is_err) {
		assert_forbidden(result);
	}
	assert!(
		services
			.uiaa
			.get_uiaa_request(&user, Some("DEVICE".into()), "concurrent-consumption")
			.is_none()
	);
	assert!(
		services
			.uiaa
			.complete_sso(&user, "concurrent-consumption")
			.await
			.is_err()
	);

	// Exercise callback/consumption overlap repeatedly against real DB calls.
	// Whichever owns the transition first, consumption must be final.
	for iteration in 0..16 {
		let session = format!("callback-race-{iteration}");
		info.session = Some(session.clone());
		services
			.uiaa
			.create(&user, "DEVICE".into(), &info, &body)
			.await?;
		services
			.uiaa
			.complete_sso(&user, &session)
			.await?;
		let auth =
			AuthData::FallbackAcknowledgement(FallbackAcknowledgement::new(session.clone()));
		let (consumed, callback) = join!(
			services
				.uiaa
				.try_auth(&user, "DEVICE".into(), &auth, &info),
			services.uiaa.complete_sso(&user, &session),
		);
		assert!(consumed?.0);
		// A callback before consumption is harmless; one afterwards is refused.
		drop(callback);
		assert!(
			services
				.uiaa
				.get_uiaa_session_by_session_id(&session)
				.await
				.is_none()
		);
		assert_forbidden(
			services
				.uiaa
				.try_auth(&user, "DEVICE".into(), &auth, &info)
				.await,
		);
	}

	password_owner(services, &user, &body).await?;
	password_retry(services, &user).await?;
	completed_stage_retry(services, &user, &body).await?;
	shared_registration_token(services, &user, &body).await?;
	token_races::exercise(services, &user).await?;
	corrupt_binding_is_refused(services, &user).await
}

async fn shared_registration_token(
	services: &Services,
	user: &UserId,
	body: &CanonicalJsonValue,
) -> Result {
	for round in 0..4 {
		let (token, _) = services
			.registration_tokens
			.create_token(Some(&format!("one-use-{round}")), None, TokenExpires {
				max_uses: Some(1),
				max_age: None,
			})
			.await?;
		let mut challenges = Vec::new();
		for index in 0..32 {
			let info = UiaaInfo {
				flows: vec![AuthFlow::new(vec![AuthType::RegistrationToken, AuthType::Sso])],
				session: Some(format!("shared-token-{round}-{index}")),
				..Default::default()
			};
			services
				.uiaa
				.create(user, "DEVICE".into(), &info, body)
				.await?;
			challenges.push(info);
		}
		let barrier = Arc::new(tokio::sync::Barrier::new(challenges.len()));
		let tasks = challenges.iter().map(|challenge| {
			let barrier = Arc::clone(&barrier);
			let uiaa = Arc::clone(&services.uiaa);
			let user = user.to_owned();
			let challenge = challenge.clone();
			let mut auth = RegistrationToken::new(token.clone());
			auth.session = challenge.session.clone();
			tokio::spawn(async move {
				barrier.wait().await;
				uiaa.try_auth(
					user.as_ref(),
					"DEVICE".into(),
					&AuthData::RegistrationToken(auth),
					&challenge,
				)
				.await
			})
		});
		let mut accepted = 0_usize;
		for result in join_all(tasks).await {
			let (finished, progress) = result.expect("UIA contender must not panic")?;
			assert!(!finished, "SSO remains required");
			if progress
				.completed
				.contains(&AuthType::RegistrationToken)
			{
				accepted = accepted.saturating_add(1);
			}
		}
		assert_eq!(accepted, 1, "one token use must authorize exactly one distinct UIA session");
		for challenge in challenges {
			services
				.uiaa
				.delete_session(
					user,
					"DEVICE".into(),
					challenge.session.as_deref().expect("session"),
				)
				.await?;
		}
	}
	Ok(())
}

async fn password_owner(services: &Services, user: &UserId, body: &CanonicalJsonValue) -> Result {
	let info = UiaaInfo {
		flows: vec![AuthFlow::new(vec![AuthType::Password])],
		session: Some("not-sso".into()),
		..Default::default()
	};
	services
		.uiaa
		.create(user, "DEVICE".into(), &info, body)
		.await?;
	assert!(
		services
			.uiaa
			.complete_sso(user, "not-sso")
			.await
			.is_err()
	);
	services
		.uiaa
		.delete_session(user, "DEVICE".into(), "not-sso")
		.await?;

	// A matching localpart on a different server is a different principal,
	// even if a password hash for that foreign principal exists locally.
	let foreign = UserId::parse("@alice:foreign.invalid")?;
	services
		.users
		.create(&foreign, Some("foreign-password"), None)
		.await?;
	let auth = AuthData::Password(Password::new(
		UserIdentifier::Matrix(MatrixUserIdentifier::new(foreign.to_string())),
		"foreign-password".into(),
	));
	assert_forbidden(
		services
			.uiaa
			.try_auth(user, "DEVICE".into(), &auth, &info)
			.await,
	);
	let auth = AuthData::Password(Password::new(
		UserIdentifier::Matrix(MatrixUserIdentifier::new(user.to_string())),
		"correct-local-password".into(),
	));
	assert!(
		services
			.uiaa
			.try_auth(user, "DEVICE".into(), &auth, &info)
			.await?
			.0
	);

	Ok(())
}

async fn corrupt_binding_is_refused(services: &Services, user: &UserId) -> Result {
	let auth =
		AuthData::FallbackAcknowledgement(FallbackAcknowledgement::new("stored-binding".into()));
	for session in [None, Some("different-session".into())] {
		let mut info = UiaaInfo {
			flows: vec![AuthFlow::new(vec![AuthType::Sso])],
			completed: vec![AuthType::Sso],
			session: Some("stored-binding".into()),
			..Default::default()
		};
		let device: &tuwunel_core::ruma::DeviceId = "DEVICE".into();
		services
			.uiaa
			.create(user, device, &info, &CanonicalJsonValue::Object(Default::default()))
			.await?;
		info.session = session;
		services.db["userdevicesessionid_uiaainfo"]
			.put((user, device, "stored-binding"), Json(&info))
			.await?;
		assert_forbidden(
			services
				.uiaa
				.try_auth(user, device, &auth, &info)
				.await,
		);
		assert!(
			services
				.uiaa
				.complete_sso(user, "stored-binding")
				.await
				.is_err()
		);
		services
			.uiaa
			.delete_session(user, device, "stored-binding")
			.await?;
	}
	Ok(())
}

fn assert_forbidden<T: Debug>(result: Result<T>) {
	let error = result.expect_err("UIAA must refuse this authentication");
	assert_eq!(error.kind(), tuwunel_core::ruma::api::error::ErrorKind::forbidden());
}

async fn password_retry(services: &Services, user: &UserId) -> Result {
	let info = UiaaInfo {
		flows: vec![AuthFlow::new(vec![AuthType::Password])],
		..Default::default()
	};
	let wrong = AuthData::Password(Password::new(
		UserIdentifier::Matrix(MatrixUserIdentifier::new(user.to_string())),
		"wrong-password".into(),
	));
	let (worked, challenge) = services
		.uiaa
		.try_auth(user, "DEVICE".into(), &wrong, &info)
		.await?;
	assert!(!worked);
	let mut correct = Password::new(
		UserIdentifier::Matrix(MatrixUserIdentifier::new(user.to_string())),
		"correct-local-password".into(),
	);
	correct.session = challenge.session.clone();
	let auth = AuthData::Password(correct);
	assert!(
		services
			.uiaa
			.try_auth(user, "DEVICE".into(), &auth, &info)
			.await?
			.0,
		"a failed first attempt must return a retryable durable session"
	);
	Ok(())
}

async fn completed_stage_retry(
	services: &Services,
	user: &UserId,
	body: &CanonicalJsonValue,
) -> Result {
	let (token, _) = services
		.registration_tokens
		.create_token(Some("stage-retry-token"), None, TokenExpires {
			max_uses: Some(3),
			max_age: None,
		})
		.await?;
	let info = UiaaInfo {
		flows: vec![AuthFlow::new(vec![AuthType::RegistrationToken, AuthType::Sso])],
		session: Some("stage-retry".into()),
		..Default::default()
	};
	services
		.uiaa
		.create(user, "DEVICE".into(), &info, body)
		.await?;
	let mut registration = RegistrationToken::new(token.clone());
	registration.session = info.session.clone();
	let auth = AuthData::RegistrationToken(registration);
	for _ in 0..3 {
		let (completed, info) = services
			.uiaa
			.try_auth(user, "DEVICE".into(), &auth, &info)
			.await?;
		assert!(!completed);
		assert_eq!(info.completed, vec![AuthType::RegistrationToken]);
		assert!(info.auth_error.is_none());
	}
	let TokenInfo::Database(stored) = services
		.registration_tokens
		.get_token_info(&token)
		.await?
	else {
		panic!("fixture must use a database registration token");
	};
	assert_eq!(stored.uses, 1);
	services
		.uiaa
		.complete_sso(user, "stage-retry")
		.await?;
	assert!(
		services
			.uiaa
			.try_auth(user, "DEVICE".into(), &auth, &info)
			.await?
			.0
	);
	services
		.registration_tokens
		.revoke_token(&token)
		.await?;
	Ok(())
}
