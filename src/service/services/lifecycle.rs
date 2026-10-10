//! Own rollback across dropped startup and shutdown futures.

use std::{pin::Pin, sync::Arc, task::Poll};

use futures::future::poll_fn;
use tuwunel_core::error;

use super::Services;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Lifecycle {
	Built,
	Starting,
	Running,
	Stopping,
	Stopped,
}

pub(super) struct CleanupOnDrop {
	services: Option<Arc<Services>>,
}

impl CleanupOnDrop {
	pub(super) fn new(services: &Arc<Services>) -> Self {
		Self { services: Some(services.clone()) }
	}

	pub(super) fn disarm(&mut self) { self.services.take(); }
}

impl Drop for CleanupOnDrop {
	fn drop(&mut self) {
		let Some(services) = self.services.take() else {
			return;
		};
		let mut slot = services.cleanup.lock().expect("locked");
		if slot
			.as_ref()
			.is_some_and(|handle| !handle.is_finished())
		{
			return;
		}
		let owner = services.clone();
		// The root retains the cleanup handle. The cleanup future owns the root
		// only until it has joined workers and closed the backend. Its result
		// contains no root reference; completion therefore cannot form a cycle.
		*slot = Some(services.server.runtime().spawn(async move {
			let mut lifecycle = owner.lifecycle.lock().await;
			if *lifecycle != Lifecycle::Stopped {
				*lifecycle = Lifecycle::Stopping;
				owner.stop_inner().await;
				*lifecycle = Lifecycle::Stopped;
			}
		}));
	}
}

impl Services {
	pub(super) async fn join_cleanup(&self) {
		poll_fn(|context| {
			let mut slot = self.cleanup.lock().expect("locked");
			let Some(handle) = slot.as_mut() else {
				return Poll::Ready(());
			};
			match Pin::new(handle).poll(context) {
				| Poll::Pending => Poll::Pending,
				| Poll::Ready(result) => {
					slot.take();
					if let Err(error) = result {
						error!(%error, "Service lifecycle cleanup task failed");
					}
					Poll::Ready(())
				},
			}
		})
		.await;
	}
}
