//! Native cleanup runs in the process-resident core, independently of Tokio.
//! An idle executor retains no callback from an unloadable service module.

use std::{
	panic::{AssertUnwindSafe, catch_unwind},
	sync::{Mutex, OnceLock, mpsc},
	thread,
};

use futures::channel::oneshot;

use super::Tasks;
use crate::{Error, Result};

/// Handle to the process-wide native teardown executor. Prepare it before
/// acquiring resources whose destruction cannot safely start a new thread.
#[derive(Clone)]
pub struct Executor(mpsc::Sender<Cleanup>);

struct Cleanup {
	run: Box<dyn FnOnce() -> Result + Send>,
	completed: oneshot::Sender<Result>,
}

static EXECUTOR: OnceLock<Mutex<Option<Executor>>> = OnceLock::new();

impl Executor {
	/// Starts the resident executor once, or obtains its existing handle.
	pub fn prepare() -> Result<Self> {
		let mut executor = EXECUTOR
			.get_or_init(Mutex::default)
			.lock()
			.expect("native executor initialization");
		if let Some(executor) = &*executor {
			return Ok(executor.clone());
		}
		let (send, recv) = mpsc::channel::<Cleanup>();
		thread::Builder::new()
			.name("tuwunel:cleanup".into())
			.stack_size(1_048_576)
			.spawn(move || {
				while let Ok(Cleanup { run, completed }) = recv.recv() {
					let result = catch_unwind(AssertUnwindSafe(run))
						.map_err(Error::from_panic)
						.and_then(std::convert::identity);
					_ = completed.send(result);
				}
			})?;
		let prepared = Self(send);
		*executor = Some(prepared.clone());
		Ok(prepared)
	}

	/// Transfers cleanup and retains its completion before exposing it to the
	/// executor. Callback captures are released before the receipt completes.
	pub fn submit<F>(&self, tasks: &Tasks, cleanup: F)
	where
		F: FnOnce() -> Result + Send + 'static,
	{
		let (completed, completion) = oneshot::channel();
		tasks.retain_native(completion);
		self.0
			.send(Cleanup { run: Box::new(cleanup), completed })
			.expect("native executor remains alive");
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use futures::executor::block_on;

	use super::{Executor, Tasks};

	#[test]
	fn native_executor_survives_panic_and_joins_without_runtime() {
		let executor = Executor::prepare().expect("resident executor");
		let tasks = Tasks::default();
		let owner = Arc::new(());
		let weak = Arc::downgrade(&owner);
		executor.submit(&tasks, || panic!("injected native callback panic"));
		executor.submit(&tasks, move || {
			drop(owner);
			Ok(())
		});
		let error = block_on(tasks.join()).expect_err("callback panic retained");
		assert!(error.is_panic());
		assert_eq!(weak.strong_count(), 0, "later callback was also joined");
		block_on(tasks.join()).expect("receipts drained without a Tokio runtime");
	}
}
