#![cfg(test)]

use std::{env::var, fs::remove_dir_all, path::PathBuf, process::id as process_id};

use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Error, Result, http, ruma::api::error::ErrorKind};
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
	let map = &services.db["userid_password"];
	// Only this fresh disposable database is modified; no existing user data.
	map.clear().await?;
	assert_eq!(services.users.bounded_local_user_count().await?, 0);
	map.insert("@active:localhost", "hash").await?;
	map.insert("@sso:localhost", "*").await?;
	map.insert("@disabled:localhost", "").await?;
	assert_eq!(services.users.bounded_local_user_count().await?, 2);
	map.insert(&[0xFF], "hash").await?;
	services
		.users
		.bounded_local_user_count()
		.await
		.expect_err("malformed row cannot disappear from a count");
	map.remove(&[0xFF]).await?;
	for index in 3..MAX_LOCAL_USER_COUNT_ROWS {
		map.insert(&format!("@disabled-{index:04}:localhost"), "")
			.await?;
	}
	assert_eq!(services.users.bounded_local_user_count().await?, 2);
	map.insert("@overflow-disabled:localhost", "")
		.await?;
	let error = services
		.users
		.bounded_local_user_count()
		.await
		.expect_err("disabled rows still consume the read budget");
	assert_eq!(error.status_code(), http::StatusCode::TOO_MANY_REQUESTS);
	assert!(matches!(error, Error::Request(ErrorKind::LimitExceeded(_), _, _)));
	map.remove("@overflow-disabled:localhost").await?;
	assert_eq!(services.users.bounded_local_user_count().await?, 2);
	Ok(())
}
