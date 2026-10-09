//! Cancellation must retain teardown ownership until an explicit stop joins it.

#![cfg(test)]

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, var},
	fs::{DirBuilder, remove_dir_all},
	net::{Ipv4Addr, TcpListener},
	path::PathBuf,
	process::Command,
	sync::Arc,
	time::Duration,
};

use futures::poll;
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_exec, async_run, async_start, async_stop};
use tuwunel_core::{Result, utils::rand};
use tuwunel_database::Database;
use tuwunel_service::Services;

const CASE: &str = "TUWUNEL_CANCELLATION_CASE";

fn isolated(name: &str) -> Result<bool> {
	if let Ok(selected) = var(CASE) {
		return Ok(selected != name);
	}
	let output = Command::new(current_exe()?)
		.env(CASE, name)
		.output()?;
	assert!(
		output.status.success(),
		"{name} child failed:\n{}\n{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
	Ok(true)
}

struct Fixture {
	server: Arc<Server>,
	runtime: Runtime,
	directory: PathBuf,
	port: u16,
}

impl Fixture {
	fn new(listening: bool) -> Result<Self> {
		let directory = std::env::temp_dir().join(format!("tuwunel-cancel-{}", rand::string(20)));
		let mut builder = DirBuilder::new();
		#[cfg(unix)]
		builder.mode(0o700);
		builder.create(&directory)?;
		let socket = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
		let port = socket.local_addr()?.port();
		drop(socket);
		let mut args = Args::default_test(&["cancellation_lifecycle"]);
		args.option.extend([
			format!("database_path={:?}", directory.join("database")),
			"database_backend=\"rocksdb\"".into(),
			"database_migrations=false".into(),
			"create_admin_room=false".into(),
			format!("listening={listening}"),
			"address=[\"127.0.0.1\"]".into(),
			format!("port=[{port}]"),
			"client_shutdown_timeout=1".into(),
			"log=\"error\"".into(),
		]);
		let runtime = Runtime::new(Some(&args))?;
		let server = Server::new(Some(&args), Some(&runtime))?;
		runtime.block_on(async {
			let prepared = Services::build(server.server.clone()).await?;
			prepared
				.globals
				.db
				.bump_database_version(23)
				.await?;
			prepared.db["global"]
				.insert(b"cancel-preservation", b"durable")
				.await?;
			Ok::<_, tuwunel_core::Error>(())
		})?;
		Ok(Self { server, runtime, directory, port })
	}

	async fn ready(port: u16) {
		let client = reqwest::Client::new();
		let url = format!("http://127.0.0.1:{port}/_matrix/client/versions");
		timeout(Duration::from_secs(15), async {
			loop {
				if client
					.get(&url)
					.send()
					.await
					.is_ok_and(|response| response.status().is_success())
				{
					return;
				}
				sleep(Duration::from_millis(10)).await;
			}
		})
		.await
		.expect("owned listener becomes ready");
	}

	async fn preserved(server: &Arc<Server>) -> Result {
		let reopened = Database::open(&server.server).await?;
		assert_eq!(
			reopened["global"]
				.get(b"cancel-preservation")
				.await?
				.as_ref(),
			b"durable"
		);
		reopened.close().await;
		Ok(())
	}
}

impl Drop for Fixture {
	fn drop(&mut self) { remove_dir_all(&self.directory).ok(); }
}

#[test]
fn run_releases_services_mutex_while_waiting() -> Result {
	if isolated("run_releases_services_mutex_while_waiting")? {
		return Ok(());
	}
	let fixture = Fixture::new(false)?;
	fixture.runtime.block_on(async {
		let services = async_start(&fixture.server).await?;
		let root = Arc::downgrade(&services);
		drop(services);
		let mut run = Box::pin(async_run(&fixture.server));
		assert!(poll!(run.as_mut()).is_pending());
		let available = fixture.server.services.try_lock().is_ok();
		fixture.server.server.shutdown().ok();
		run.await?;
		async_stop(&fixture.server).await?;
		assert_eq!(root.strong_count(), 0);
		Fixture::preserved(&fixture.server).await?;
		assert!(available, "run must release the services mutex before waiting for shutdown");
		Ok(())
	})
}

#[test]
fn cancelled_run_is_joined_before_stop_returns() -> Result {
	if isolated("cancelled_run_is_joined_before_stop_returns")? {
		return Ok(());
	}
	let fixture = Fixture::new(true)?;
	fixture.runtime.block_on(async {
		let services = async_start(&fixture.server).await?;
		let root = Arc::downgrade(&services);
		let database = Arc::downgrade(&services.db);
		drop(services);
		let owner = fixture.server.clone();
		let run = tokio::spawn(async move { async_run(&owner).await });
		Fixture::ready(fixture.port).await;
		run.abort();
		assert!(
			run.await
				.expect_err("run cancelled")
				.is_cancelled()
		);
		timeout(Duration::from_secs(15), async_stop(&fixture.server))
			.await
			.expect("stop joins cancelled run")?;
		assert_eq!(root.strong_count(), 0, "listener released service ownership");
		assert_eq!(database.strong_count(), 0, "listener released database ownership");
		assert!(
			TcpListener::bind((Ipv4Addr::LOCALHOST, fixture.port)).is_ok(),
			"listener socket released"
		);
		Fixture::preserved(&fixture.server).await
	})
}

#[test]
fn cancelled_exec_joins_signals_and_releases_database() -> Result {
	if isolated("cancelled_exec_joins_signals_and_releases_database")? {
		return Ok(());
	}
	let fixture = Fixture::new(true)?;
	fixture.runtime.block_on(async {
		let owner = fixture.server.clone();
		let execution = tokio::spawn(async move { async_exec(&owner).await });
		Fixture::ready(fixture.port).await;
		execution.abort();
		assert!(
			execution
				.await
				.expect_err("execution cancelled")
				.is_cancelled()
		);
		timeout(Duration::from_secs(15), async_stop(&fixture.server))
			.await
			.expect("stop joins cancelled process")?;
		assert_eq!(
			Arc::strong_count(&fixture.server),
			1,
			"signal and process cleanup owners joined"
		);
		assert!(fixture.server.services.lock().await.is_none());
		assert!(
			TcpListener::bind((Ipv4Addr::LOCALHOST, fixture.port)).is_ok(),
			"listener socket released"
		);
		Fixture::preserved(&fixture.server).await
	})
}
