//! Deliveries must progress while the sender awaits queue mutations. Startup
//! stages them without HTTP; polling starts owned tasks, joined before exit.

use std::collections::HashMap;

use tokio::{
	runtime::Handle,
	task::{Id, JoinSet},
};
use tuwunel_core::Error;

use super::{Delivery, Destination, SendingFuture, SendingResult};

#[derive(Default)]
pub(super) struct SendingFutures {
	queued: Vec<(Destination, SendingFuture, Handle)>,
	tasks: JoinSet<SendingResult>,
	owners: HashMap<Id, Destination>,
}

impl SendingFutures {
	pub(super) fn new() -> Self { Self::default() }

	pub(super) fn push(&mut self, owner: Destination, delivery: SendingFuture, runtime: &Handle) {
		self.queued
			.push((owner, delivery, runtime.clone()));
	}

	pub(super) fn len(&self) -> usize { self.queued.len().saturating_add(self.tasks.len()) }

	pub(super) fn is_empty(&self) -> bool { self.queued.is_empty() && self.tasks.is_empty() }

	/// Only startup's unpolled staging buffer may be cleared for
	/// reconstruction.
	pub(super) fn clear(&mut self) {
		assert!(self.tasks.is_empty(), "cannot reconstruct running deliveries");
		self.queued.clear();
	}

	pub(super) async fn next(&mut self) -> Option<SendingResult> {
		for (owner, delivery, runtime) in self.queued.drain(..) {
			let task = self.tasks.spawn_on(delivery, &runtime);
			self.owners.insert(task.id(), owner);
		}
		match self.tasks.join_next_with_id().await? {
			| Ok((id, outcome)) => {
				self.owners.remove(&id);
				Some(outcome)
			},
			| Err(error) => {
				let owner = self
					.owners
					.remove(&error.id())
					.expect("owned delivery task");
				Some(Ok(Delivery::LocalFailure(owner, Box::new(Error::from(error)))))
			},
		}
	}

	/// Aborting leaves unacknowledged durable rows/journals for recovery. Join
	/// every task so no delivery holds a database/registration guard after
	/// exit.
	pub(super) async fn cancel_and_join(&mut self) {
		self.queued.clear();
		self.tasks.abort_all();
		while self.tasks.join_next().await.is_some() {}
		self.owners.clear();
	}
}

#[cfg(test)]
mod tests {
	use std::{sync::Arc, time::Duration};

	use futures::{FutureExt, poll};
	use tokio::sync::{Mutex, Notify, oneshot};

	use super::{Destination, Handle, SendingFutures};
	use crate::sending::sender::{Delivery, transport_acknowledged};

	#[tokio::test]
	async fn queue_mutation_can_progress_without_polling_delivery_completion() {
		let lock = Arc::new(Mutex::new(()));
		let held = Arc::new(Notify::new());
		let (release, released) = oneshot::channel();
		let destination = Destination::Appservice("owned-progress".into());
		let owner = destination.clone();
		let task_lock = lock.clone();
		let task_held = held.clone();
		let mut deliveries = SendingFutures::new();
		deliveries.push(
			destination,
			async move {
				let guard = task_lock.lock().await;
				task_held.notify_one();
				released.await.expect("owned release");
				drop(guard);
				Ok(transport_acknowledged(owner))
			}
			.boxed(),
			&Handle::current(),
		);
		let response = {
			let completion = deliveries.next();
			futures::pin_mut!(completion);
			assert!(poll!(&mut completion).is_pending());
			held.notified().await;
			release
				.send(())
				.expect("release delivery's queue guard");
			// This is the sender's inline queue wait. Completion is deliberately
			// not polled while it waits for a guard owned by the delivery.
			let guard = tokio::time::timeout(Duration::from_secs(1), lock.lock())
				.await
				.expect("delivery must progress independently of the sender");
			drop(guard);
			completion.await
		};
		assert!(matches!(response, Some(Ok(Delivery::Acknowledged(..)))));
		deliveries.cancel_and_join().await;
	}

	#[tokio::test]
	async fn cancellation_joins_deliveries_and_releases_their_guards() {
		let lock = Arc::new(Mutex::new(()));
		let held = Arc::new(Notify::new());
		let task_lock = lock.clone();
		let task_held = held.clone();
		let mut deliveries = SendingFutures::new();
		deliveries.push(
			Destination::Appservice("owned-cancel".into()),
			async move {
				let _guard = task_lock.lock().await;
				task_held.notify_one();
				std::future::pending().await
			}
			.boxed(),
			&Handle::current(),
		);
		let pending = {
			let completion = deliveries.next();
			futures::pin_mut!(completion);
			poll!(&mut completion).is_pending()
		};
		assert!(pending);
		held.notified().await;
		tokio::time::timeout(Duration::from_secs(1), deliveries.cancel_and_join())
			.await
			.expect("bounded owned task join");
		assert!(lock.try_lock().is_ok(), "no task retains its guard after exit");
		assert!(deliveries.is_empty());
		assert!(deliveries.next().await.is_none());
	}

	#[tokio::test]
	async fn panicked_task_is_a_local_failure_with_its_destination() {
		let destination =
			Destination::Federation(ruma::server_name!("task-failure.example").to_owned());
		let mut deliveries = SendingFutures::new();
		deliveries.push(
			destination.clone(),
			futures::future::poll_fn(|_| -> std::task::Poll<super::SendingResult> {
				panic!("owned fixture task panic")
			})
			.boxed(),
			&Handle::current(),
		);
		let outcome = tokio::time::timeout(Duration::from_secs(1), deliveries.next())
			.await
			.expect("bounded panic completion");
		assert!(
			matches!(outcome, Some(Ok(Delivery::LocalFailure(owner, error)))
			if owner == destination && matches!(error.as_ref(), super::Error::JoinError(join) if join.is_panic())),
			"a local task panic is not a remote transport failure"
		);
		assert!(deliveries.is_empty());
		assert!(deliveries.owners.is_empty());
	}

	#[tokio::test]
	async fn cancelled_task_is_a_local_failure_with_its_destination() {
		let destination = Destination::Appservice("task-cancel".into());
		let mut deliveries = SendingFutures::new();
		deliveries.push(destination.clone(), std::future::pending().boxed(), &Handle::current());
		let pending = {
			let completion = deliveries.next();
			futures::pin_mut!(completion);
			poll!(&mut completion).is_pending()
		};
		assert!(pending);
		deliveries.tasks.abort_all();
		let outcome = tokio::time::timeout(Duration::from_secs(1), deliveries.next())
			.await
			.expect("bounded cancelled-task completion");
		assert!(
			matches!(outcome, Some(Ok(Delivery::LocalFailure(owner, error)))
			if owner == destination && matches!(error.as_ref(), super::Error::JoinError(join) if join.is_cancelled())),
			"a locally cancelled task is not a remote transport failure"
		);
		assert!(deliveries.is_empty());
		assert!(deliveries.owners.is_empty());
	}
}
