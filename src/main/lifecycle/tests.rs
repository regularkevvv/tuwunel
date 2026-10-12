#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{fs, sync::Arc};

use clap::Parser;
use futures::poll;
use tuwunel_core::{Result, utils::rand};
use tuwunel_database::Database;
use tuwunel_service::Services;

use super::install;
use crate::{Args, Runtime, Server, async_stop};

struct Directory(std::path::PathBuf);
impl Drop for Directory {
	fn drop(&mut self) { fs::remove_dir_all(&self.0).ok(); }
}

#[test]
fn cancelled_started_graph_handoff_is_joined() -> Result {
	let directory =
		Directory(std::env::temp_dir().join(format!("tuwunel-handoff-{}", rand::string(20))));
	let mut builder = fs::DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&directory.0)?;
	let mut args = Args::parse_from(["tuwunel"]);
	args.test.push("handoff".into());
	args.option.extend([
		"server_name=\"localhost\"".into(),
		format!("database_path={:?}", directory.0.join("database")),
		"database_backend=\"rocksdb\"".into(),
		"database_migrations=false".into(),
		"create_admin_room=false".into(),
		"listening=false".into(),
		"log=\"error\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	runtime.block_on(async {
		let services = Services::build(server.server.clone()).await?;
		services
			.globals
			.db
			.bump_database_version(22)
			.await?;
		services.db["global"]
			.insert(b"handoff-preservation", b"durable")
			.await?;
		services.start().await?;
		let root = Arc::downgrade(&services);
		let database = Arc::downgrade(&services.db);
		let slot = server.services.lock().await;
		let mut handoff = Box::pin(install(&server, services));
		assert!(poll!(handoff.as_mut()).is_pending(), "handoff waits for occupied slot mutex");
		drop(handoff);
		drop(slot);
		async_stop(&server).await?;
		assert_eq!(root.strong_count(), 0, "cancelled handoff stops and drops workers");
		assert_eq!(database.strong_count(), 0, "cancelled handoff releases native database");
		let reopened = Database::open(&server.server).await?;
		assert_eq!(
			reopened["global"]
				.get(b"handoff-preservation")
				.await?
				.as_ref(),
			b"durable"
		);
		reopened.close().await;
		Ok(())
	})
}
