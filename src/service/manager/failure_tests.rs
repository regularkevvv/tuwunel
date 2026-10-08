//! A manager failure must stop and join every surviving worker.

use std::{future::pending, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::{task::yield_now, time::timeout};
use tuwunel_core::{Result, Server, err};

use super::{AtomicUsize, Manager, Ordering, WorkerResult};
use crate::{
	service::{Args, Service},
	services::startup_tests::{installed_manager, isolated, released, services},
};

struct FixtureWorker {
	server: Arc<Server>,
	started: Arc<AtomicUsize>,
	fail: bool,
	wait_for: usize,
}

#[async_trait]
impl Service for FixtureWorker {
	fn build(args: &Args<'_>) -> Result<Arc<impl Service>> {
		Ok(Arc::new(Self {
			server: args.server.clone(),
			started: Arc::new(AtomicUsize::new(0)),
			fail: false,
			wait_for: 1,
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		self.started.fetch_add(1, Ordering::AcqRel);
		if self.fail {
			while self.started.load(Ordering::Acquire) < self.wait_for {
				yield_now().await;
			}
			return Err(err!("fixture fatal worker failure"));
		}
		self.server.until_shutdown().await;
		Ok(())
	}

	fn name(&self) -> &'static str { "fixture_worker" }
}

#[test]
fn fatal_worker_drains_survivors_and_releases_database() -> Result {
	isolated(
		"manager::failure_tests::fatal_worker_drains_survivors_and_releases_database",
		async |directory| {
			let graph = services(directory).await?;
			graph.db["global"]
				.insert(b"fatal-preservation", b"durable")
				.await?;
			let root = Arc::downgrade(&graph);
			let database = Arc::downgrade(&graph.db);
			let server = graph.server.clone();
			let started = Arc::new(AtomicUsize::new(0));
			let manager = Manager::new(&graph);
			{
				let mut workers = manager.workers.lock().await;
				for fail in [false, false, true] {
					let worker: Arc<dyn Service> = Arc::new(FixtureWorker {
						server: server.clone(),
						started: started.clone(),
						fail,
						wait_for: 3,
					});
					manager.start_worker(&mut workers, &worker)?;
				}
			}
			let error = timeout(Duration::from_secs(5), manager.worker())
				.await
				.expect("fatal worker terminates its manager")
				.expect_err("fatal service error propagates");
			assert!(!error.is_panic(), "retain the original ordinary worker error");
			assert!(
				error
					.to_string()
					.contains("fixture fatal worker failure")
			);
			let active = manager.active.load(Ordering::Acquire);
			let remaining = manager.workers.lock().await.len();
			drop(manager);
			drop(graph);
			released(&root, &database).await;
			assert!(server.is_stopping(), "fatal worker must request shutdown");
			assert_eq!(active, 0, "all workers finish before manager returns");
			assert_eq!(remaining, 0, "every worker completion is joined");
			let reopened = services(directory).await?;
			assert_eq!(
				reopened.db["global"]
					.get(b"fatal-preservation")
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
fn unexpected_worker_abort_is_reported_and_survivors_are_joined() -> Result {
	isolated(
		"manager::failure_tests::unexpected_worker_abort_is_reported_and_survivors_are_joined",
		async |directory| {
			let graph = services(directory).await?;
			let root = Arc::downgrade(&graph);
			let database = Arc::downgrade(&graph.db);
			let server = graph.server.clone();
			let started = Arc::new(AtomicUsize::new(0));
			let manager = Manager::new(&graph);
			let abort = {
				let mut workers = manager.workers.lock().await;
				let worker: Arc<dyn Service> = Arc::new(FixtureWorker {
					server: server.clone(),
					started: started.clone(),
					fail: false,
					wait_for: 1,
				});
				manager.start_worker(&mut workers, &worker)?;
				workers.spawn(pending::<WorkerResult>())
			};
			timeout(Duration::from_secs(5), async {
				while started.load(Ordering::Acquire) == 0 {
					yield_now().await;
				}
			})
			.await
			.expect("surviving worker starts before abort");
			abort.abort();
			let error = timeout(Duration::from_secs(5), manager.worker())
				.await
				.expect("unexpected abort terminates its manager")
				.expect_err("unexpected task cancellation is reported");
			assert!(error.to_string().contains("cancelled"), "retain the task failure: {error}");
			let active = manager.active.load(Ordering::Acquire);
			let remaining = manager.workers.lock().await.len();
			drop(manager);
			drop(graph);
			released(&root, &database).await;
			assert!(server.is_stopping());
			assert_eq!(active, 0);
			assert_eq!(remaining, 0);
			Ok(())
		},
	)
}

#[test]
fn installed_manager_fatal_error_drains_real_workers_before_final_teardown() -> Result {
	isolated(
		"manager::failure_tests::installed_manager_fatal_error_drains_real_workers_before_final_teardown",
		async |directory| {
			let graph = services(directory).await?;
			graph.db["global"].insert(b"installed-fatal-preservation", b"durable").await?;
			drop(graph.start().await?);
			let manager = installed_manager(&graph).await;
			let root = Arc::downgrade(&graph);
			let database = Arc::downgrade(&graph.db);
			let server = graph.server.clone();
			let inject = async {
				let mut workers = manager.workers.lock().await;
				let worker: Arc<dyn Service> = Arc::new(FixtureWorker {
					server: server.clone(),
					started: Arc::new(AtomicUsize::new(0)),
					fail: true,
					wait_for: 1,
				});
				manager.start_worker(&mut workers, &worker)
			};
			// The manager borrows its JoinSet while awaiting a completion. An
			// ordinary admin-worker completion lets the queued fixture acquire
			// that same lock and inject the failure into the installed manager.
			let wake = async {
				while !graph.admin.worker_ready() {
					yield_now().await;
				}
				Service::interrupt(graph.admin.as_ref()).await;
			};
			let (injected, ()) = timeout(Duration::from_secs(5), async {
				tokio::join!(inject, wake)
			}).await.expect("inject into the running native graph");
			injected?;
			let error = timeout(Duration::from_secs(5), graph.poll())
				.await.expect("installed manager finishes after fatal failure")
				.expect_err("original fatal worker failure reaches the caller");
			assert!(error.to_string().contains("fixture fatal worker failure"));
			assert!(server.is_stopping(), "manager requests shutdown without caller teardown");
			assert_eq!(manager.active.load(Ordering::Acquire), 0);
			assert!(manager.workers.lock().await.is_empty());
			drop(manager);
			graph.stop().await;
			drop(graph);
			released(&root, &database).await;
			let reopened = services(directory).await?;
			assert_eq!(reopened.db["global"].get(b"installed-fatal-preservation").await?.as_ref(), b"durable");
			reopened.stop().await;
			Ok(())
		},
	)
}

#[test]
fn admin_worker_started_after_shutdown_exits_and_releases_database() -> Result {
	isolated(
		"manager::failure_tests::admin_worker_started_after_shutdown_exits_and_releases_database",
		async |directory| {
			let graph = services(directory).await?;
			let root = Arc::downgrade(&graph);
			let database = Arc::downgrade(&graph.db);
			graph.server.shutdown()?;
			timeout(Duration::from_secs(5), Service::worker(graph.admin.clone()))
				.await
				.expect("late admin worker sees shutdown without another signal")?;
			drop(graph);
			released(&root, &database).await;
			let reopened = services(directory).await?;
			reopened.stop().await;
			Ok(())
		},
	)
}
