//! Retains shutdown task completions across cancelled and concurrent joiners.

#[cfg(test)]
mod tests;

use std::{
	pin::Pin,
	sync::{Mutex, PoisonError},
	task::{Poll, Waker},
};

use futures::future::poll_fn;
use tokio::{
	runtime::Handle,
	sync::Mutex as AsyncMutex,
	task::{AbortHandle, JoinHandle},
};

use crate::{Error, Result};

/// Owns tasks whose resources must be released before shutdown returns.
/// Tasks may enqueue further cleanup, but must not join their own registry.
#[derive(Default)]
pub struct Tasks {
	state: Mutex<State>,
	joining: AsyncMutex<()>,
}

#[derive(Default)]
struct State {
	pending: Vec<JoinHandle<()>>,
	error: Option<Error>,
	spawning: usize,
	waker: Option<Waker>,
}

struct Registration<'a>(&'a Tasks);

impl<'a> Registration<'a> {
	fn new(tasks: &'a Tasks) -> Self {
		let mut state = tasks
			.state
			.lock()
			.unwrap_or_else(PoisonError::into_inner);
		state.spawning = state
			.spawning
			.checked_add(1)
			.expect("registration capacity");
		Self(tasks)
	}
}

impl Drop for Registration<'_> {
	fn drop(&mut self) {
		let wake = {
			let mut state = self
				.0
				.state
				.lock()
				.unwrap_or_else(PoisonError::into_inner);
			state.spawning = state
				.spawning
				.checked_sub(1)
				.expect("owned registration");
			state.waker.take()
		};
		if let Some(wake) = wake {
			wake.wake();
		}
	}
}

impl Tasks {
	/// Starts and retains a task, returning a handle that can request its
	/// cancellation.
	pub fn spawn<F>(&self, runtime: &Handle, future: F) -> AbortHandle
	where
		F: Future<Output = ()> + Send + 'static,
	{
		let registration = Registration::new(self);
		let task = runtime.spawn(future);
		let abort = task.abort_handle();
		// Spawning on an already closed runtime can drop the future immediately.
		// Its destructor may register cleanup; do not hold our mutex during spawn.
		self.state
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.pending
			.push(task);
		drop(registration);
		abort
	}

	/// Joins all retained tasks, including cleanup they enqueue while
	/// finishing. Cancelled joiners leave handles and failures available to
	/// the next caller. Task cancellation is expected during shutdown; panics
	/// are returned after draining.
	pub async fn join(&self) -> Result {
		let _joining = self.joining.lock().await;
		poll_fn(|context| {
			let mut state = self
				.state
				.lock()
				.unwrap_or_else(PoisonError::into_inner);
			state.waker = Some(context.waker().clone());
			loop {
				let Some(task) = state.pending.last_mut() else {
					if state.spawning != 0 {
						return Poll::Pending;
					}
					state.waker.take();
					return Poll::Ready(state.error.take().map_or(Ok(()), Err));
				};
				let Poll::Ready(result) = Pin::new(task).poll(context) else {
					return Poll::Pending;
				};
				state.pending.pop();
				if let Err(error) = result
					&& !error.is_cancelled()
					&& state.error.is_none()
				{
					state.error = Some(error.into());
				}
			}
		})
		.await
	}
}
