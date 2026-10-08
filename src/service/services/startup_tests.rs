//! Startup must not leave a manager or workers owning a failed service graph.

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	fs::{self, DirBuilder},
	path::Path,
	process::Command,
	sync::{Arc, Weak, mpsc},
	time::{Duration, Instant},
};

use tokio::{runtime::Handle, sync::oneshot, task::yield_now, time::timeout};
use tracing::subscriber::NoSubscriber;
use tuwunel_core::{
	Result, Server,
	config::{Config, Figment, Sources},
	log::{LogLevelReloadHandles, Logging, capture::State},
	metrics::Metrics,
	utils::{rand, sys},
};

use super::Services;

pub(crate) fn isolated(name: &str, exercise: impl AsyncFnOnce(&Path) -> Result) -> Result {
	const CHILD: &str = "TUWUNEL_STARTUP_LIFECYCLE_CHILD";
	const DIRECTORY: &str = "TUWUNEL_STARTUP_LIFECYCLE_DIRECTORY";
	if std::env::var(CHILD).as_deref() == Ok(name) {
		let root = std::env::var_os(DIRECTORY).expect("owned parent fixture directory");
		return tokio::runtime::Builder::new_multi_thread()
			.worker_threads(2)
			.enable_all()
			.build()?
			.block_on(exercise(Path::new(&root)));
	}

	let root = std::env::temp_dir().join(format!("matrix-startup-{}", rand::string(20)));
	let mut directory = DirBuilder::new();
	#[cfg(unix)]
	directory.mode(0o700);
	directory.create(&root)?;
	let mut child = Command::new(std::env::current_exe()?)
		.args([name, "--exact", "--nocapture", "--test-threads=1"])
		.env(CHILD, name)
		.env(DIRECTORY, &root)
		.spawn()?;
	let started = Instant::now();
	let status = loop {
		if let Some(status) = child.try_wait()? {
			break status;
		}
		if started.elapsed() >= Duration::from_secs(90) {
			child.kill().ok();
			child.wait().ok();
			fs::remove_dir_all(&root).ok();
			panic!("startup fixture exceeded its deadline");
		}
		std::thread::sleep(Duration::from_millis(20));
	};
	fs::remove_dir_all(&root)?;
	assert!(status.success(), "isolated startup case {name}");
	Ok(())
}

pub(crate) async fn services(root: &Path) -> Result<Arc<Services>> {
	sys::maximize_fd_limit()?;
	let raw = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_backend", "rocksdb"))
		.merge(("database_path", root.join("database")))
		.merge(("database_migrations", false))
		.merge(("create_admin_room", false));
	let server = server(Config::new(&raw)?);
	let services = Services::build(server).await?;
	services
		.globals
		.db
		.bump_database_version(crate::migrations::DATABASE_VERSION)
		.await?;
	Ok(services)
}

pub(crate) fn server(config: Config) -> Arc<Server> {
	let runtime = Handle::current();
	Arc::new(Server::new(
		config,
		Sources::default(),
		Some(&runtime),
		Logging {
			subscriber: Arc::new(NoSubscriber::new()),
			reload: LogLevelReloadHandles::default(),
			capture: Arc::new(State::new()),
		},
		Metrics::new(Some(&runtime)),
	))
}

pub(crate) async fn released(root: &Weak<Services>, database: &Weak<tuwunel_database::Database>) {
	let result = timeout(Duration::from_secs(5), async {
		while root.strong_count() != 0 || database.strong_count() != 0 {
			yield_now().await;
		}
	})
	.await;
	assert!(
		result.is_ok(),
		"startup left {} root and {} database references",
		root.strong_count(),
		database.strong_count()
	);
}

pub(crate) async fn installed_manager(graph: &Services) -> Arc<crate::manager::Manager> {
	graph
		.manager
		.lock()
		.await
		.as_ref()
		.expect("started graph has a manager")
		.clone()
}

#[test]
fn failed_startup_releases_installed_manager_and_database() -> Result {
	isolated(
		"services::startup_tests::failed_startup_releases_installed_manager_and_database",
		async |directory| {
			let services = services(directory).await?;
			services.db["global"]
				.insert(b"startup-preservation", b"durable")
				.await?;
			let root = Arc::downgrade(&services);
			let database = Arc::downgrade(&services.db);
			services.server.shutdown()?;
			let error = services
				.start()
				.await
				.expect_err("stopped server refuses worker startup");
			assert!(
				error
					.to_string()
					.contains("worker not starting during server shutdown"),
				"failure must reach installed manager: {error}"
			);
			drop(services);
			released(&root, &database).await;
			let reopened = self::services(directory).await?;
			assert_eq!(
				reopened.db["global"]
					.get(b"startup-preservation")
					.await?
					.as_ref(),
				b"durable"
			);
			reopened.stop().await;
			Ok(())
		},
	)
}

#[test]
fn cancelled_startup_releases_started_workers_and_database() -> Result {
	isolated(
		"services::startup_tests::cancelled_startup_releases_started_workers_and_database",
		async |directory| {
			let services = services(directory).await?;
			services.db["global"]
				.insert(b"startup-preservation", b"durable")
				.await?;
			let root = Arc::downgrade(&services);
			let database = Arc::downgrade(&services.db);
			let server = services.server.clone();
			let mut startup = Box::pin(services.start());
			timeout(Duration::from_secs(5), async {
				loop {
					assert!(
						futures::poll!(&mut startup).is_pending(),
						"cancel before startup can return success"
					);
					let installed = services
						.manager
						.try_lock()
						.map_or(true, |slot| slot.is_some());
					if installed {
						break;
					}
					yield_now().await;
				}
			})
			.await
			.expect("manager must install before cancellation");
			drop(startup);
			drop(services);
			released(&root, &database).await;
			assert!(server.is_stopping(), "cancelled startup requests worker shutdown");
			let reopened = self::services(directory).await?;
			assert_eq!(
				reopened.db["global"]
					.get(b"startup-preservation")
					.await?
					.as_ref(),
				b"durable"
			);
			reopened.stop().await;
			Ok(())
		},
	)
}

struct BlockTaskDrop {
	entered: Option<oneshot::Sender<()>>,
	release: mpsc::Receiver<()>,
	_owner: Arc<Services>,
}

impl Drop for BlockTaskDrop {
	fn drop(&mut self) {
		if let Some(entered) = self.entered.take() {
			entered.send(()).ok();
		}
		self.release
			.recv_timeout(Duration::from_secs(10))
			.expect("fixture releases the interrupted task destructor");
	}
}

#[test]
fn cancelled_stop_finishes_joining_admin_tasks_and_releases_database() -> Result {
	isolated(
		"services::startup_tests::cancelled_stop_finishes_joining_admin_tasks_and_releases_database",
		async |directory| {
			let services = services(directory).await?;
			services.db["global"]
				.insert(b"startup-preservation", b"durable")
				.await?;
			drop(services.start().await?);
			let root = Arc::downgrade(&services);
			let database = Arc::downgrade(&services.db);
			let (started, running) = oneshot::channel();
			let (entered, mut dropping) = oneshot::channel();
			let (release, blocked) = mpsc::channel();
			let barrier = BlockTaskDrop {
				entered: Some(entered),
				release: blocked,
				_owner: services.clone(),
			};
			services.tasks.spawn(
				"purge_history",
				"!cancel-stop:localhost".into(),
				serde_json::json!({}),
				async move {
					let _barrier = barrier;
					started.send(()).expect("fixture waits for task startup");
					std::future::pending().await
				},
			).await?;
			timeout(Duration::from_secs(5), running)
				.await
				.expect("admin task starts within the fixture deadline")
				.expect("admin task reports startup");
			let mut stop = Box::pin(services.stop());
			timeout(Duration::from_secs(5), async {
				loop {
					assert!(futures::poll!(&mut stop).is_pending(), "task join must block stop");
					if dropping.try_recv().is_ok() {
						break;
					}
					yield_now().await;
				}
			}).await.expect("shutdown must begin joining the admin task");
			drop(stop);
			drop(services);
			assert!(root.strong_count() > 0, "cleanup retains ownership until joins finish");
			release.send(()).expect("interrupted task is still being joined");
			released(&root, &database).await;
			let reopened = self::services(directory).await?;
			assert_eq!(reopened.db["global"].get(b"startup-preservation").await?.as_ref(), b"durable");
			reopened.stop().await;
			Ok(())
		},
	)
}

#[test]
fn shutdown_refuses_new_admin_work_without_journal_mutation() -> Result {
	isolated(
		"services::startup_tests::shutdown_refuses_new_admin_work_without_journal_mutation",
		async |directory| {
			let services = services(directory).await?;
			services.server.shutdown()?;
			let error = services
				.tasks
				.spawn(
					"purge_history",
					"!late-admission:localhost".into(),
					serde_json::json!({}),
					async { panic!("refused task must never execute") },
				)
				.await
				.expect_err("shutdown refuses admission");
			assert!(
				error
					.to_string()
					.contains("unavailable during shutdown")
			);
			assert!(
				services.tasks.list().await?.is_empty(),
				"refused admission does not write a journal record"
			);
			services.stop().await;
			Ok(())
		},
	)
}
