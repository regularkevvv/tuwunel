use std::{
	panic::AssertUnwindSafe,
	sync::{Arc, Weak, atomic::Ordering},
	time::Duration,
};

use futures::{FutureExt, TryFutureExt};
#[cfg(all(feature = "systemd", target_os = "linux"))]
use sd_notify::{NotifyState, notify, notify_and_unset_env, watchdog_enabled};
use tokio::{sync::oneshot, task::AbortHandle};
use tuwunel_core::{Error, Result, Server, debug, debug_error, debug_info, err, error, info};
use tuwunel_service::Services;

use crate::{handle::ServerHandle, serve};

mod ownership;

use ownership::{Resources, join_listener};

struct CancelRun {
	server: Arc<Server>,
	task: Option<AbortHandle>,
}

impl Drop for CancelRun {
	fn drop(&mut self) {
		if let Some(task) = self.task.take() {
			self.server.shutdown().ok();
			task.abort();
		}
	}
}

/// Main loop base. The registry retains the driver until its resources are
/// joined.
#[tracing::instrument(skip_all)]
pub(crate) async fn run(services: Arc<Services>) -> Result {
	let server = services.server.clone();
	let (sender, receiver) = oneshot::channel();
	let task = server
		.cleanup
		.spawn(server.runtime(), async move {
			let result = AssertUnwindSafe(run_inner(services))
				.catch_unwind()
				.map_err(Error::from_panic)
				.unwrap_or_else(Err)
				.await;
			sender.send(result).ok();
		});
	let mut cancel = CancelRun { server: server.clone(), task: Some(task) };
	let result = receiver
		.await
		.map_err(|_| err!("Router task closed without a result"))?;
	cancel.task.take();
	let joined = server.cleanup.join().await;
	result.and(joined)
}

#[tracing::instrument(skip_all)]
async fn run_inner(services: Arc<Services>) -> Result {
	let server = &services.server;
	let mut resources = Resources::new(server.clone());
	resources.services = Some(services.clone());
	debug!("Start");
	// Install ownership before any startup await or command-root mutation.
	resources.admin = true;
	tuwunel_admin::init(&services.admin);
	services.admin.startup_execute().await?;

	resources
		.auxiliary
		.spawn_on(signal(server.clone(), resources.handle.clone()), server.runtime());
	#[cfg(all(feature = "systemd", target_os = "linux"))]
	resources
		.auxiliary
		.spawn_on(start_systemd_watchdog(), server.runtime());
	if services.config.listening {
		resources.listener = Some(
			server
				.runtime()
				.spawn(serve::serve(services.clone(), resources.handle.clone())),
		);
	}

	debug!("Running");
	let res = {
		let listener = async {
			if resources.listener.is_some() {
				join_listener(&mut resources.listener).await
			} else {
				server.until_shutdown().await;
				Ok(())
			}
		};
		tokio::pin!(listener);
		tokio::select! {
			res = &mut listener => res,
			res = services.poll() => {
				server.shutdown().ok();
				handle_services_finish(server, res, Some(listener.await))
			},
		}
	};
	resources.finish_auxiliary().await;
	resources.disarm();
	debug_info!("Finish");
	res
}

/// Async initializations
#[tracing::instrument(skip_all)]
pub(crate) async fn start(server: Arc<Server>) -> Result<Arc<Services>> {
	debug!("Starting...");

	let mut resources = Resources::new(server.clone());
	#[cfg(all(feature = "systemd", target_os = "linux"))]
	resources
		.auxiliary
		.spawn_on(extend_systemd_startup(), server.runtime());

	let services = Services::build(server).await?;
	resources.services = Some(services.clone());
	let services = services.start().await?;
	resources.finish_auxiliary().await;

	// The status is set here so it reads as a baseline rather than staying blank
	// until the first reload replaces it.
	#[cfg(all(feature = "systemd", target_os = "linux"))]
	notify(&[NotifyState::Ready, NotifyState::Status("Running")])
		.expect("failed to notify systemd of ready state");

	resources.disarm();
	debug!("Started");
	Ok(services)
}

/// Async destructions
#[tracing::instrument(skip_all)]
pub(crate) fn stop(services: Arc<Services>) -> impl Future<Output = Result> + Send {
	let mut resources = Resources::new(services.server.clone());
	resources.services = Some(services.clone());
	async move {
		debug!("Shutting down...");

		#[cfg(all(feature = "systemd", target_os = "linux"))]
		notify_systemd_shutdown(&services.server);

		services.server.shutdown().ok();
		let cleanup = services.server.cleanup.join().await;

		// Wait for all completions before dropping or we'll lose them to the module
		// unload and explode.
		services.stop().await;

		// Dangling references below can keep Database alive past process exit, so
		// flush the backend operation metrics explicitly rather than from drop.
		services.db.dump_operation_metrics();
		resources.disarm();

		// Check that Services and Database will drop as expected, The complex of Arc's
		// used for various components can easily lead to references being held
		// somewhere improperly; this can hang shutdowns.
		debug!("Cleaning up...");
		let db = Arc::downgrade(&services.db);
		if let Err(services) = Arc::try_unwrap(services) {
			debug_error!(
				"{} dangling references to Services after shutdown",
				Arc::strong_count(&services)
			);
		}

		if Weak::strong_count(&db) > 0 {
			debug_error!(
				"{} dangling references to Database after shutdown",
				Weak::strong_count(&db)
			);
		}

		info!("Shutdown complete.");
		cleanup
	}
}

#[cfg(all(feature = "systemd", target_os = "linux"))]
fn notify_systemd_shutdown(server: &Server) {
	// An in-place exec restart keeps this PID; report a reload, not an exit, so
	// the unit stays active and NOTIFY_SOCKET survives for the next image. The
	// watchdog stays armed while reloading, so reset it to give teardown and
	// exec the full interval.
	if server.is_restarting() {
		let monotonic = NotifyState::monotonic_usec_now().expect("failed to get monotonic time");

		notify(&[NotifyState::Reloading, monotonic, NotifyState::Watchdog])
			.expect("failed to notify systemd of reloading state");

		return;
	}

	// SAFETY: clears NOTIFY_SOCKET from the process environment. Safe because no
	// other thread reads or writes that variable; this matches the previous
	// `notify(unset_env=true, ...)` semantics from sd-notify 0.4.
	unsafe { notify_and_unset_env(&[NotifyState::Stopping]) }
		.expect("failed to notify systemd of stopping state");
}

#[tracing::instrument(skip_all)]
async fn signal(server: Arc<Server>, handle: ServerHandle) {
	server.until_shutdown().await;
	handle_shutdown(&server, &handle);
}

fn handle_shutdown(server: &Arc<Server>, handle: &ServerHandle) {
	let timeout = server.config.client_shutdown_timeout;
	let timeout = Duration::from_secs(timeout);
	debug!(
		?timeout,
		handle_active = ?server.metrics.requests_handle_active.load(Ordering::Relaxed),
		"Notifying for graceful shutdown"
	);

	handle.graceful_shutdown(Some(timeout));
}

fn handle_services_finish(
	server: &Arc<Server>,
	result: Result,
	listener: Option<Result>,
) -> Result {
	debug!("Service manager finished: {result:?}");

	if server.is_running()
		&& let Err(e) = server.shutdown()
	{
		error!("Failed to send shutdown signal: {e}");
	}

	if let Some(Err(e)) = &listener {
		error!("Client listener task finished with error: {e}");
	}

	result.and(listener.unwrap_or(Ok(())))
}

#[cfg(all(feature = "systemd", target_os = "linux"))]
#[expect(clippy::infinite_loop)]
async fn start_systemd_watchdog() {
	use tokio::time::MissedTickBehavior;

	let Some(watchdog) = watchdog_enabled() else {
		return;
	};

	let watchdog_usec = u64::try_from(watchdog.as_micros()).unwrap_or(u64::MAX);
	let interval_usec = (watchdog_usec / 2).max(1);
	let interval = Duration::from_micros(interval_usec);

	let mut ticker = tokio::time::interval(interval);
	ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
	loop {
		ticker.tick().await;

		if let Err(e) = notify(&[NotifyState::Watchdog]) {
			error!(%e, "failed to notify systemd watchdog state");
		}
	}
}

#[cfg(all(feature = "systemd", target_os = "linux"))]
#[expect(clippy::infinite_loop)]
async fn extend_systemd_startup() {
	use std::env;

	use tokio::time::MissedTickBehavior;

	const INTERVAL: Duration = Duration::from_secs(15);

	// Keep systemd's start timeout extended while a slow boot such as a database
	// migration runs, so a healthy service is not killed before it signals ready.
	if env::var_os("NOTIFY_SOCKET").is_none() {
		return;
	}

	let extend_usec = u32::try_from(INTERVAL.as_micros())
		.unwrap_or(u32::MAX)
		.saturating_mul(2);

	let mut ticker = tokio::time::interval(INTERVAL);
	ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
	loop {
		ticker.tick().await;

		if let Err(e) = notify(&[NotifyState::ExtendTimeoutUsec(extend_usec)]) {
			error!(%e, "failed to extend systemd startup timeout");
		}
	}
}
