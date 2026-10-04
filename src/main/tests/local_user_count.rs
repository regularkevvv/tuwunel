#![cfg(test)]

use std::{env::var, fs::remove_dir_all, path::PathBuf, process::id as process_id};

use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_admin::{fini, init};
use tuwunel_core::{
	Error, Result, http,
	ruma::{UserId, api::error::ErrorKind},
};
use tuwunel_service::{Services, users::MAX_LOCAL_USER_COUNT_ROWS};

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn local_user_count_is_complete_or_refused_including_disabled_rows() -> Result {
	let root = var("TMPDIR").unwrap_or_else(|_| "/tmp".into());
	let db_path = DatabasePath(
		PathBuf::from(root).join(format!("tuwunel-local-user-count-{}", process_id())),
	);
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.push(format!("database_path={:?}", db_path.0));
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		init(&services.admin);
		let outcome = exercise(&services).await;
		fini(&services.admin);
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
	let map = &services.db["userid_password"];
	// Only this fresh disposable database is modified; no existing user data.
	map.clear().await?;
	assert_counts(services, 0, 0).await?;
	assert!(
		services
			.users
			.bounded_local_users()
			.await?
			.is_empty()
	);
	map.insert("@active:localhost", "hash").await?;
	map.insert("@sso:localhost", "*").await?;
	map.insert("@disabled:localhost", "").await?;
	assert_counts(services, 2, 3).await?;
	assert_eq!(
		services
			.users
			.bounded_local_users()
			.await?
			.iter()
			.map(|user| user.as_str())
			.collect::<Vec<_>>(),
		vec!["@active:localhost", "@sso:localhost"]
	);
	map.insert(&[0xFF], "hash").await?;
	services
		.users
		.bounded_local_user_count()
		.await
		.expect_err("malformed row cannot disappear from a count");
	services
		.users
		.bounded_user_count()
		.await
		.expect_err("malformed row cannot disappear from a registered-user count");
	services
		.users
		.bounded_local_users()
		.await
		.expect_err("malformed row cannot disappear from an inventory");
	map.remove(&[0xFF]).await?;
	for index in 3..MAX_LOCAL_USER_COUNT_ROWS {
		map.insert(&format!("@disabled-{index:04}:localhost"), "")
			.await?;
	}
	assert_counts(services, 2, MAX_LOCAL_USER_COUNT_ROWS).await?;
	assert_eq!(services.users.bounded_local_users().await?.len(), 2);
	map.insert("@overflow-disabled:localhost", "")
		.await?;
	let error = services
		.users
		.bounded_local_user_count()
		.await
		.expect_err("disabled rows still consume the read budget");
	assert_eq!(error.status_code(), http::StatusCode::TOO_MANY_REQUESTS);
	assert!(matches!(error, Error::Request(ErrorKind::LimitExceeded(_), _, _)));
	assert_eq!(
		services
			.users
			.bounded_user_count()
			.await
			.expect_err("all-user count cannot become a successful prefix")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	let Err(output) = services
		.admin
		.command_in_place("query users count-users".into(), None)
		.await
	else {
		panic!("registered-user count must propagate overflow");
	};
	assert!(
		output
			.as_str()
			.contains("User count inventory limit reached")
	);
	assert_eq!(
		services
			.users
			.bounded_local_users()
			.await
			.expect_err("disabled overflow refuses the complete inventory")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	let Err(output) = services
		.admin
		.command_in_place(
			"users force-join-all-local-users !unused:localhost --yes-i-want-to-do-this".into(),
			None,
		)
		.await
	else {
		panic!("bulk join must refuse an oversized inventory");
	};
	assert!(
		output
			.as_str()
			.contains("User inventory limit reached"),
		"inventory refusal must precede room resolution and membership changes"
	);
	map.remove("@overflow-disabled:localhost").await?;
	assert_counts(services, 2, MAX_LOCAL_USER_COUNT_ROWS).await?;
	assert_eq!(services.users.bounded_local_users().await?.len(), 2);
	map.clear().await?;
	for index in 0..600 {
		let user = UserId::parse(format!("@long-{index:03}{}:localhost", "x".repeat(210)))
			.expect("valid long user id");
		map.insert(&user, "*").await?;
	}
	assert_counts(services, 600, 600).await?;
	assert_eq!(
		services
			.users
			.bounded_local_users()
			.await
			.expect_err("user-ID byte budget refuses a row-bounded oversized inventory")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	Ok(())
}

async fn assert_counts(services: &Services, active: usize, total: usize) -> Result {
	assert_eq!(services.users.bounded_local_user_count().await?, active);
	assert_eq!(services.users.bounded_user_count().await?, total);
	Ok(())
}
