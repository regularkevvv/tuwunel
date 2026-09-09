use std::{iter::repeat_with, sync::Arc};

use futures::future::join_all;
use tuwunel_core::{
	Result,
	ruma::{
		CanonicalJsonValue, UserId,
		api::client::uiaa::{AuthData, AuthFlow, AuthType, RegistrationToken, UiaaInfo},
	},
};
use tuwunel_service::{
	Services,
	registration_tokens::{TokenExpires, TokenInfo},
};

pub(super) async fn exercise(services: &Services, user: &UserId) -> Result {
	creation_is_exclusive(services).await?;
	updates_preserve_consumption(services).await?;
	cleanup_respects_renewal(services).await?;
	corruption_is_not_absence(services, user).await
}

fn expiry() -> TokenExpires { TokenExpires { max_uses: Some(1000), max_age: None } }

async fn creation_is_exclusive(services: &Services) -> Result {
	let barrier = Arc::new(tokio::sync::Barrier::new(32));
	let tasks = repeat_with(|| {
		let barrier = Arc::clone(&barrier);
		let tokens = Arc::clone(&services.registration_tokens);
		tokio::spawn(async move {
			barrier.wait().await;
			tokens
				.create_token(Some("exclusive-create"), None, expiry())
				.await
		})
	})
	.take(32);
	let mut created = 0_usize;
	for result in join_all(tasks).await {
		match result.expect("creator must not panic") {
			| Ok(_) => created = created.saturating_add(1),
			| Err(error) =>
				assert_eq!(error.kind(), tuwunel_core::ruma::api::error::ErrorKind::InvalidParam),
		}
	}
	assert_eq!(created, 1, "duplicate creators must not overwrite the same token");
	services
		.registration_tokens
		.revoke_token("exclusive-create")
		.await
}

async fn updates_preserve_consumption(services: &Services) -> Result {
	services
		.registration_tokens
		.create_token(Some("update-race"), None, expiry())
		.await?;
	let barrier = Arc::new(tokio::sync::Barrier::new(64));
	let tasks = (0..64).map(|index| {
		let barrier = Arc::clone(&barrier);
		let tokens = Arc::clone(&services.registration_tokens);
		tokio::spawn(async move {
			barrier.wait().await;
			if index % 2 == 0 {
				tokens.try_consume("update-race").await
			} else {
				tokens
					.update_token("update-race", expiry())
					.await
					.map(|_| ())
			}
		})
	});
	for result in join_all(tasks).await {
		result.expect("token operation must not panic")?;
	}
	let TokenInfo::Database(info) = services
		.registration_tokens
		.get_token_info("update-race")
		.await?
	else {
		panic!("fixture must use database token");
	};
	assert_eq!(info.uses, 32, "expiry edits cannot lose successful consumptions");
	revocation_wins(services).await
}

async fn revocation_wins(services: &Services) -> Result {
	let barrier = Arc::new(tokio::sync::Barrier::new(33));
	let tasks = (0..33).map(|index| {
		let barrier = Arc::clone(&barrier);
		let tokens = Arc::clone(&services.registration_tokens);
		tokio::spawn(async move {
			barrier.wait().await;
			if index == 0 {
				tokens.revoke_token("update-race").await
			} else {
				match tokens.update_token("update-race", expiry()).await {
					| Ok(_) => Ok(()),
					| Err(error) if error.is_not_found() => Ok(()),
					| Err(error) => Err(error),
				}
			}
		})
	});
	for result in join_all(tasks).await {
		result.expect("revocation contender must not panic")?;
	}
	let error = services
		.registration_tokens
		.get_token_info("update-race")
		.await
		.expect_err("revoked token must not be resurrected");
	assert!(error.is_not_found());
	Ok(())
}

async fn cleanup_respects_renewal(services: &Services) -> Result {
	for round in 0..16 {
		let token = format!("cleanup-race-{round}");
		services
			.registration_tokens
			.create_token(Some(&token), None, TokenExpires {
				max_age: Some(std::time::UNIX_EPOCH),
				..expiry()
			})
			.await?;
		let barrier = Arc::new(tokio::sync::Barrier::new(2));
		let cleaner = {
			let barrier = Arc::clone(&barrier);
			let tokens = Arc::clone(&services.registration_tokens);
			tokio::spawn(async move {
				barrier.wait().await;
				tokens.iterate_tokens().await.map(|_| ())
			})
		};
		let renewal = {
			let tokens = Arc::clone(&services.registration_tokens);
			let token = token.clone();
			tokio::spawn(async move {
				barrier.wait().await;
				tokens.update_token(&token, expiry()).await
			})
		};
		cleaner.await.expect("cleanup must not panic")?;
		match renewal.await.expect("renewal must not panic") {
			| Ok(_) => {
				services
					.registration_tokens
					.get_token_info(&token)
					.await?;
				services
					.registration_tokens
					.is_token_valid(&token)
					.await?;
				services
					.registration_tokens
					.revoke_token(&token)
					.await?;
			},
			| Err(error) if error.is_not_found() => {},
			| Err(error) => return Err(error),
		}
	}
	Ok(())
}

async fn corruption_is_not_absence(services: &Services, user: &UserId) -> Result {
	services
		.registration_tokens
		.create_token(Some("corrupt-token"), None, expiry())
		.await?;
	services.db["registrationtoken_info"]
		.insert("corrupt-token", b"secret-corrupt-body")
		.await?;
	let duplicate = services
		.registration_tokens
		.create_token(Some("corrupt-token"), None, expiry())
		.await
		.expect_err("corruption must not permit replacement");
	assert_eq!(duplicate.kind(), tuwunel_core::ruma::api::error::ErrorKind::InvalidParam);
	let error = services
		.registration_tokens
		.try_consume("corrupt-token")
		.await
		.expect_err("corrupt metadata is not valid proof");
	assert!(error.status_code().is_server_error());
	assert!(!error.to_string().contains("secret-corrupt-body"));
	assert!(
		services
			.registration_tokens
			.is_enabled()
			.await
			.expect_err("corruption must not disable token enforcement")
			.status_code()
			.is_server_error()
	);
	match services
		.registration_tokens
		.iterate_tokens()
		.await
	{
		| Err(error) => assert!(error.status_code().is_server_error()),
		| Ok(_) => panic!("corrupt metadata must not be silently omitted from listing"),
	}
	let challenge = UiaaInfo {
		flows: vec![AuthFlow::new(vec![AuthType::RegistrationToken])],
		session: Some("corrupt-token-challenge".into()),
		..Default::default()
	};
	services
		.uiaa
		.create(
			user,
			"DEVICE".into(),
			&challenge,
			&CanonicalJsonValue::Object(Default::default()),
		)
		.await?;
	let mut token = RegistrationToken::new("corrupt-token".into());
	token.session = challenge.session.clone();
	let error = services
		.uiaa
		.try_auth(user, "DEVICE".into(), &AuthData::RegistrationToken(token), &challenge)
		.await
		.expect_err("storage failure must not become invalid credentials");
	assert!(error.status_code().is_server_error());
	services
		.uiaa
		.delete_session(user, "DEVICE".into(), "corrupt-token-challenge")
		.await?;
	services
		.registration_tokens
		.revoke_token("corrupt-token")
		.await
}
