pub mod args;
pub mod config;
pub mod health;
pub mod logging;
pub mod mods;
pub mod restart;
pub mod runtime;
pub mod sentry;
pub mod server;
pub mod signals;

#[cfg(any(not(tuwunel_mods), not(feature = "tuwunel_mods")))]
mod lifecycle;

use std::sync::Arc;

use log as _;
use tuwunel_core::{Result, debug_info, error, mod_ctor, mod_dtor, rustc_flags_capture};
use tuwunel_service::Services;

pub use self::{args::Args, runtime::Runtime, server::Server};

mod_ctor! {}
mod_dtor! {}
rustc_flags_capture! {}

pub fn exec(server: &Arc<Server>, runtime: Runtime) -> Result {
	run(server, &runtime)?;
	drop(runtime);

	Ok(())
}

pub fn run(server: &Arc<Server>, runtime: &Runtime) -> Result {
	runtime.block_on(async_exec(server))
}

/// Operate the server normally in release-mode static builds. This will start,
/// run and stop the server within the asynchronous runtime.
#[cfg(any(not(tuwunel_mods), not(feature = "tuwunel_mods")))]
#[tracing::instrument(
    name = "main",
    parent = None,
    skip_all
)]
pub async fn async_exec(server: &Arc<Server>) -> Result {
	let signals = server
		.server
		.runtime()
		.spawn(signals::enable(server.clone()));

	let mut cleanup = lifecycle::Process::new(server.clone(), signals);
	let execution = async {
		async_start(server).await?;
		async_run(server).await
	}
	.await;
	let stopped = cleanup.finish().await;

	debug_info!("Exit runtime");
	execution.and(stopped)
}

#[cfg(any(not(tuwunel_mods), not(feature = "tuwunel_mods")))]
pub async fn async_start(server: &Arc<Server>) -> Result<Arc<Services>> {
	extern crate tuwunel_router as router;

	match router::start(&server.server).await {
		| Ok(services) => lifecycle::install(server, services).await,
		| Err(error) => {
			error!("Critical error starting server: {error}");
			Err(error)
		},
	}
}

/// Operate the server normally in release-mode static builds. This will start,
/// run and stop the server within the asynchronous runtime.
#[cfg(any(not(tuwunel_mods), not(feature = "tuwunel_mods")))]
pub async fn async_run(server: &Arc<Server>) -> Result {
	extern crate tuwunel_router as router;

	let run = {
		let slot = server.services.lock().await;
		router::run(slot.as_ref().expect("services initialized"))
	};
	if let Err(error) = run.await {
		error!("Critical error running server: {error}");
		return Err(error);
	}

	Ok(())
}

#[cfg(any(not(tuwunel_mods), not(feature = "tuwunel_mods")))]
pub async fn async_stop(server: &Arc<Server>) -> Result {
	server.server.shutdown().ok();
	let cleanup = server.cleanup.join().await;
	let stopped = async_stop_inner(server).await;
	cleanup.and(stopped)
}

#[cfg(any(not(tuwunel_mods), not(feature = "tuwunel_mods")))]
async fn async_stop_inner(server: &Arc<Server>) -> Result {
	extern crate tuwunel_router as router;

	server.server.shutdown().ok();
	let cleanup = server.server.cleanup.join().await;
	let Some(services) = server.services.lock().await.take() else {
		return cleanup;
	};
	let stopped = router::stop(services).await;
	if let Err(error) = &stopped {
		error!("Critical error stopping server: {error}");
	}
	// A caught stop panic may enqueue fallback cleanup after the first join.
	let remaining = server.server.cleanup.join().await;
	cleanup.and(stopped).and(remaining)
}

/// Operate the server in developer-mode dynamic builds. This will start, run,
/// and hot-reload portions of the server as-needed before returning for an
/// actual shutdown. This is not available in release-mode or static builds.
#[cfg(all(tuwunel_mods, feature = "tuwunel_mods"))]
pub async fn async_exec(server: &Arc<Server>) -> Result {
	let mut starts = true;
	let mut reloads = true;
	while reloads {
		if let Err(error) = mods::open(server).await {
			error!("Loading router: {error}");
			return Err(error);
		}

		let result = mods::run(server, starts).await;
		if let Ok(result) = result {
			(starts, reloads) = result;
		}

		let force = !reloads || result.is_err();
		if let Err(error) = mods::close(server, force).await {
			error!("Unloading router: {error}");
			return Err(error);
		}

		if let Err(error) = result {
			error!("{error}");
			return Err(error);
		}
	}

	debug_info!("Exit runtime");
	Ok(())
}
