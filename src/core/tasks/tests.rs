use std::sync::Arc;

use futures::poll;
use tokio::{runtime::Handle, sync::oneshot, task::yield_now};

use super::Tasks;

#[tokio::test]
async fn cancelled_join_retains_completion_and_owner() {
	let tasks = Tasks::default();
	let owner = Arc::new(());
	let weak = Arc::downgrade(&owner);
	let (sender, receiver) = oneshot::channel();
	tasks.spawn(&Handle::current(), async move {
		receiver.await.ok();
		drop(owner);
	});
	let mut join = Box::pin(tasks.join());
	assert!(poll!(join.as_mut()).is_pending());
	drop(join);
	assert_eq!(weak.strong_count(), 1);
	sender.send(()).expect("task retained");
	tasks
		.join()
		.await
		.expect("retained completion joined");
	assert_eq!(weak.strong_count(), 0);
}

#[tokio::test]
async fn concurrent_joiners_both_wait_for_completion() {
	let tasks = Arc::new(Tasks::default());
	let (sender, receiver) = oneshot::channel();
	tasks.spawn(&Handle::current(), async move {
		receiver.await.ok();
	});
	let first = tasks.clone();
	let second = tasks.clone();
	let first = tokio::spawn(async move { first.join().await });
	let second = tokio::spawn(async move { second.join().await });
	yield_now().await;
	assert!(!first.is_finished());
	assert!(!second.is_finished());
	sender.send(()).expect("task retained");
	first.await.expect("joiner").expect("completion");
	second.await.expect("joiner").expect("completion");
}

#[tokio::test]
async fn nested_cleanup_is_also_joined() {
	let tasks = Arc::new(Tasks::default());
	let nested = tasks.clone();
	let owner = Arc::new(());
	let weak = Arc::downgrade(&owner);
	tasks.spawn(&Handle::current(), async move {
		nested.spawn(&Handle::current(), async move {
			yield_now().await;
			drop(owner);
		});
	});
	tasks
		.join()
		.await
		.expect("nested completions joined");
	assert_eq!(weak.strong_count(), 0);
}

#[tokio::test]
async fn cancelled_join_preserves_panic_and_drains_survivor() {
	let tasks = Tasks::default();
	let owner = Arc::new(());
	let weak = Arc::downgrade(&owner);
	let (sender, receiver) = oneshot::channel();
	tasks.spawn(&Handle::current(), async move {
		receiver.await.ok();
		drop(owner);
	});
	tasks.spawn(&Handle::current(), async {
		panic!("owned task panic");
	});
	yield_now().await;
	let mut join = Box::pin(tasks.join());
	assert!(poll!(join.as_mut()).is_pending());
	drop(join);
	sender.send(()).expect("survivor retained");
	let error = tasks
		.join()
		.await
		.expect_err("panic retained across cancelled join");
	assert!(error.is_panic());
	assert_eq!(weak.strong_count(), 0);
}

#[tokio::test]
async fn closed_runtime_spawn_keeps_registration_visible_and_allows_nested_cleanup() {
	use std::{sync::mpsc, thread, time::Duration};
	struct DropCleanup {
		tasks: Arc<Tasks>,
		runtime: Handle,
		entered: Option<oneshot::Sender<()>>,
		release: mpsc::Receiver<()>,
	}
	impl Drop for DropCleanup {
		fn drop(&mut self) {
			self.tasks.spawn(&self.runtime, async {});
			self.entered
				.take()
				.expect("drop gate")
				.send(())
				.ok();
			self.release
				.recv_timeout(Duration::from_secs(10))
				.expect("destructor released");
		}
	}
	let closed = thread::spawn(|| {
		let runtime = tokio::runtime::Builder::new_current_thread()
			.build()
			.expect("fixture runtime");
		let closed = runtime.handle().clone();
		drop(runtime);
		closed
	})
	.join()
	.expect("closed runtime fixture");
	let tasks = Arc::new(Tasks::default());
	let (entered, dropping) = oneshot::channel();
	let (release, blocked) = mpsc::channel();
	let guard = DropCleanup {
		tasks: tasks.clone(),
		runtime: closed.clone(),
		entered: Some(entered),
		release: blocked,
	};
	let owner = tasks.clone();
	let spawned = thread::spawn(move || {
		owner.spawn(&closed, async move {
			let _guard = guard;
			std::future::pending::<()>().await;
		});
	});
	let entered = tokio::time::timeout(Duration::from_secs(5), dropping).await;
	if entered.is_err() {
		release.send(()).ok();
	}
	entered
		.expect("nested cleanup does not deadlock")
		.expect("drop gate sender");
	let mut join = Box::pin(tasks.join());
	let premature = poll!(join.as_mut()).is_ready();
	release.send(()).expect("destructor retained");
	spawned.join().expect("registration finishes");
	if !premature {
		join.await
			.expect("closed-runtime completions drained");
	}
	assert!(!premature, "join ignored an in-progress task registration");
}
