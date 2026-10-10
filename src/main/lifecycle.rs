//! Retained ownership for cancelled main-process teardown and startup handoff.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use tokio::task::JoinHandle;
use tuwunel_core::{Result, err, error};
use tuwunel_service::Services;

use crate::{Server, async_stop_inner};

pub(super) struct Process {
	server: Arc<Server>,
	signals: Option<JoinHandle<()>>,
	armed: bool,
}

impl Process {
	pub(super) fn new(server: Arc<Server>, signals: JoinHandle<()>) -> Self {
		Self {
			server,
			signals: Some(signals),
			armed: true,
		}
	}

	pub(super) async fn finish(&mut self) -> Result {
		self.server.server.shutdown().ok();
		let stopped = async_stop_inner(&self.server).await;
		let signals = join_signals(&mut self.signals).await;
		self.armed = false;
		stopped.and(signals)
	}
}

impl Drop for Process {
	fn drop(&mut self) {
		if !self.armed {
			return;
		}
		self.server.server.shutdown().ok();
		let server = self.server.clone();
		let mut signals = self.signals.take();
		self.server
			.cleanup
			.spawn(self.server.server.runtime(), async move {
				if let Err(error) = async_stop_inner(&server).await {
					error!(%error, "Cancelled process cleanup failed");
				}
				if let Err(error) = join_signals(&mut signals).await {
					error!(%error, "Cancelled process signal join failed");
				}
			});
	}
}

async fn join_signals(signals: &mut Option<JoinHandle<()>>) -> Result {
	let Some(task) = signals.as_mut() else {
		return Ok(());
	};
	task.abort();
	let result = task.await;
	signals.take();
	match result {
		| Ok(()) => Ok(()),
		| Err(error) if error.is_cancelled() => Ok(()),
		| Err(error) => Err(error.into()),
	}
}

struct Handoff(Option<Arc<Services>>);

impl Drop for Handoff {
	fn drop(&mut self) {
		if let Some(services) = self.0.take() {
			let server = services.server.clone();
			server.shutdown().ok();
			server
				.cleanup
				.spawn(server.runtime(), async move {
					services.stop().await;
				});
		}
	}
}

pub(super) async fn install(
	server: &Arc<Server>,
	services: Arc<Services>,
) -> Result<Arc<Services>> {
	let mut owner = Handoff(Some(services));
	let mut slot = server.services.lock().await;
	if server.server.is_stopping() || slot.is_some() {
		return Err(err!("Cannot install services after shutdown or over an existing graph"));
	}
	let services = owner.0.take().expect("handoff owns services");
	*slot = Some(services.clone());
	Ok(services)
}
