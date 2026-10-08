//! Router-owned resources must be joined even when their caller disappears.

use std::{sync::Arc, time::Duration};

use tokio::task::{JoinHandle, JoinSet};
use tuwunel_core::{Result, Server, error};
use tuwunel_service::Services;

use crate::handle::ServerHandle;

pub(super) struct Resources {
	pub(super) server: Arc<Server>,
	pub(super) services: Option<Arc<Services>>,
	pub(super) handle: ServerHandle,
	pub(super) listener: Option<JoinHandle<Result>>,
	pub(super) auxiliary: JoinSet<()>,
	pub(super) admin: bool,
	armed: bool,
}

impl Resources {
	pub(super) fn new(server: Arc<Server>) -> Self {
		Self {
			server,
			services: None,
			handle: ServerHandle::new(),
			listener: None,
			auxiliary: JoinSet::new(),
			admin: false,
			armed: true,
		}
	}

	pub(super) async fn finish_auxiliary(&mut self) {
		self.auxiliary.abort_all();
		while let Some(result) = self.auxiliary.join_next().await {
			if let Err(error) = result
				&& !error.is_cancelled()
			{
				error!(%error, "Router auxiliary task failed");
			}
		}
	}

	pub(super) fn disarm(&mut self) {
		self.uninstall_admin();
		self.services.take();
		self.armed = false;
	}

	fn uninstall_admin(&mut self) {
		if self.admin {
			if let Some(services) = &self.services {
				tuwunel_admin::fini(&services.admin);
			}
			self.admin = false;
		}
	}
}

impl Drop for Resources {
	fn drop(&mut self) {
		if !self.armed {
			return;
		}
		self.server.shutdown().ok();
		self.handle
			.graceful_shutdown(Some(Duration::from_secs(
				self.server.config.client_shutdown_timeout,
			)));
		self.uninstall_admin();
		let listener = self.listener.take();
		let mut auxiliary = std::mem::take(&mut self.auxiliary);
		let services = self.services.take();
		self.server
			.cleanup
			.spawn(self.server.runtime(), async move {
				if let Some(listener) = listener {
					match listener.await {
						| Ok(Ok(())) => (),
						| Ok(Err(error)) =>
							error!(%error, "Listener failed during cancellation cleanup"),
						| Err(error) =>
							error!(%error, "Listener join failed during cancellation cleanup"),
					}
				}
				auxiliary.abort_all();
				while let Some(result) = auxiliary.join_next().await {
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						error!(%error, "Router auxiliary cleanup failed");
					}
				}
				if let Some(services) = services {
					services.stop().await;
				}
			});
	}
}

pub(super) async fn join_listener(listener: &mut Option<JoinHandle<Result>>) -> Result {
	let result = listener
		.as_mut()
		.expect("listener installed")
		.await;
	listener.take();
	result
		.map_err(tuwunel_core::Error::from)
		.unwrap_or_else(Err)
}
