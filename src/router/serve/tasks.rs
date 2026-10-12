use std::{sync::Arc, time::Duration};

use tokio::task::JoinSet;
use tuwunel_core::{Result, Server, error};

use crate::handle::ServerHandle;

pub(super) struct Listeners {
	server: Arc<Server>,
	pub(super) tasks: JoinSet<std::io::Result<()>>,
}

impl Listeners {
	pub(super) fn new(server: Arc<Server>) -> Self { Self { server, tasks: JoinSet::new() } }

	pub(super) async fn finish(&mut self, handle: &ServerHandle) -> Result {
		let mut first = Ok(());
		while let Some(joined) = self.tasks.join_next().await {
			let result = joined
				.map_err(tuwunel_core::Error::from)
				.and_then(|result| result.map_err(Into::into));
			if let Err(error) = result {
				self.server.shutdown().ok();
				handle.graceful_shutdown(Some(Duration::from_secs(
					self.server.config.client_shutdown_timeout,
				)));
				if first.is_ok() {
					first = Err(error);
				}
			}
		}
		first
	}
}

impl Drop for Listeners {
	fn drop(&mut self) {
		if self.tasks.is_empty() {
			return;
		}
		self.server.shutdown().ok();
		let mut tasks = std::mem::take(&mut self.tasks);
		tasks.abort_all();
		self.server
			.cleanup
			.spawn(self.server.runtime(), async move {
				while let Some(result) = tasks.join_next().await {
					match result {
						| Ok(Ok(())) => (),
						| Ok(Err(error)) => error!(%error, "Listener failed during cleanup"),
						| Err(error) if error.is_cancelled() => (),
						| Err(error) => error!(%error, "Listener panic during cleanup"),
					}
				}
			});
	}
}
