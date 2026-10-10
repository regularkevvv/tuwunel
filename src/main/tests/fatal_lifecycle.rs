//! A run error must finish teardown before returning to a live caller runtime.

#![cfg(test)]

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	fs::{DirBuilder, remove_dir_all},
	path::PathBuf,
	sync::Arc,
	time::Duration,
};

use tokio::time::timeout;
use tuwunel::{Args, Runtime, Server, async_exec};
use tuwunel_core::{Result, utils::rand};
use tuwunel_database::Database;
use tuwunel_service::Services;

struct OwnedDirectory(PathBuf);

impl Drop for OwnedDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn startup_command_error_releases_database_before_runtime_exit() -> Result {
	let root = std::env::temp_dir().join(format!("matrix-fatal-run-{}", rand::string(20)));
	let mut builder = DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&root)?;
	let directory = OwnedDirectory(root);
	let mut args = Args::default_test(&["fatal_lifecycle"]);
	args.option.extend([
		format!("database_path={:?}", directory.0.join("database")),
		"database_backend=\"rocksdb\"".into(),
		"database_migrations=false".into(),
		"create_admin_room=false".into(),
		"listening=false".into(),
		"admin_execute_errors_ignore=false".into(),
		"log=\"error\"".into(),
	]);
	args.execute
		.push("fixture_command_that_does_not_exist".into());
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		// Seed a native fixture without starting workers or changing the core
		// stopping flag. Schema 24 is the source-pinned reader in this test.
		let prepared = Services::build(server.server.clone()).await?;
		prepared
			.globals
			.db
			.bump_database_version(24)
			.await?;
		prepared.db["global"]
			.insert(b"fatal-run-preservation", b"durable")
			.await?;
		let prepared_root = Arc::downgrade(&prepared);
		let prepared_database = Arc::downgrade(&prepared.db);
		drop(prepared);
		assert_eq!(prepared_root.strong_count(), 0);
		assert_eq!(prepared_database.strong_count(), 0);

		let error = timeout(Duration::from_secs(15), async_exec(&server))
			.await
			.expect("run error returns without external shutdown")
			.expect_err("invalid configured startup command must fail");
		assert!(
			error
				.to_string()
				.contains("Execute command #0 failed"),
			"preserve the run error: {error}"
		);
		let stopping = server.server.is_stopping();
		let retained = server.services.lock().await.take();
		let clean = retained.is_none();
		// A failing control still cleans its owned fixture before reporting
		// failure, so it does not leave background tasks or a database behind.
		if let Some(retained) = retained {
			retained.stop().await;
		}
		assert!(clean, "main run error left Services installed instead of finishing teardown");
		assert!(stopping, "run errors request orderly shutdown");
		assert_eq!(Arc::strong_count(&server), 1, "signal handler has released the main server");
		tuwunel::async_stop(&server).await?;
		let reopened = Database::open(&server.server).await?;
		assert_eq!(
			reopened["global"]
				.get(b"fatal-run-preservation")
				.await?
				.as_ref(),
			b"durable"
		);
		let reopened_database = Arc::downgrade(&reopened);
		reopened.close().await;
		drop(reopened);
		assert_eq!(reopened_database.strong_count(), 0);
		Ok(())
	});
	drop(server);
	drop(runtime);
	result
}
