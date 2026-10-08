//! Close must join renewal completion even when its first caller is cancelled.

use std::{
	future::pending,
	sync::{Arc, mpsc},
	time::Duration,
};

use tokio::{sync::oneshot, time::timeout};
use tuwunel_core::Result;

use super::{
	Backend,
	tests::{Fake, remote_server},
};

struct HeldOnDrop {
	entered: Option<oneshot::Sender<()>>,
	release: mpsc::Receiver<()>,
	_owner: Arc<()>,
}

impl Drop for HeldOnDrop {
	fn drop(&mut self) {
		self.entered
			.take()
			.expect("owned drop barrier")
			.send(())
			.ok();
		self.release
			.recv_timeout(Duration::from_secs(10))
			.expect("fixture releases renewal destructor");
	}
}

async fn close_joins(cancel: bool) -> Result {
	let fake = Fake::start().await?;
	let server = remote_server(&fake.url, 2, 0)?;
	let backend = Backend::open(&server).await?;
	// Retire the real renewal loop, then substitute a deterministic completion
	// barrier in the same owned task slot. No provider or durable data is used.
	let prior = backend
		.renewals
		.lock()
		.expect("renewal lock")
		.take()
		.expect("real renewal task");
	prior.abort();
	assert!(
		prior
			.await
			.expect_err("real renewal cancelled")
			.is_cancelled()
	);
	let owner = Arc::new(());
	let weak = Arc::downgrade(&owner);
	let (entered, dropping) = oneshot::channel();
	let (release, blocked) = mpsc::channel();
	let barrier = HeldOnDrop {
		entered: Some(entered),
		release: blocked,
		_owner: owner,
	};
	let (started, running) = oneshot::channel();
	let task = tokio::spawn(async move {
		let _barrier = barrier;
		started
			.send(())
			.expect("fixture awaits renewal startup");
		pending::<()>().await;
	});
	*backend.renewals.lock().expect("renewal lock") = Some(task);
	timeout(Duration::from_secs(5), running)
		.await
		.expect("renewal starts")
		.expect("renewal startup sender");
	let mut close = Box::pin(backend.close());
	let mut returned_before_join = false;
	let mut dropping = Box::pin(dropping);
	timeout(Duration::from_secs(5), async {
		loop {
			if !returned_before_join && futures::poll!(&mut close).is_ready() {
				returned_before_join = true;
			}
			if futures::poll!(&mut dropping).is_ready() {
				break;
			}
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("close interrupts renewal");
	if !returned_before_join {
		returned_before_join = timeout(Duration::from_millis(100), &mut close)
			.await
			.is_ok();
	}
	let resumed_before_join = if cancel && !returned_before_join {
		drop(close);
		close = Box::pin(backend.close());
		futures::poll!(&mut close).is_ready()
	} else {
		false
	};
	release
		.send(())
		.expect("blocked renewal destructor");
	if !returned_before_join && !resumed_before_join {
		timeout(Duration::from_secs(5), close)
			.await
			.expect("close joins the released renewal");
	}
	timeout(Duration::from_secs(5), async {
		while weak.strong_count() != 0 {
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("renewal ownership is gone before assertions");
	backend.close().await;
	assert!(!backend.lease_status().held);
	assert!(!returned_before_join, "database close returned before renewal completion");
	assert!(!resumed_before_join, "cancelled close lost its renewal join completion");
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_joins_interrupted_renewal_before_returning() -> Result { close_joins(false).await }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_close_retains_renewal_join_completion() -> Result { close_joins(true).await }

#[tokio::test]
async fn cancelled_close_retains_dispatched_release_completion() -> Result {
	let fake = Fake::start().await?;
	let server = remote_server(&fake.url, 2, 0)?;
	let backend = Backend::open(&server).await?;
	let gate = fake.pause_release();
	let mut entered = Box::pin(gate.entered.notified());
	let mut close = Box::pin(backend.close());
	timeout(Duration::from_secs(5), async {
		loop {
			assert!(futures::poll!(&mut close).is_pending(), "release response is blocked");
			if futures::poll!(&mut entered).is_ready() {
				break;
			}
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("release reaches fixture barrier");
	drop(close);
	let mut resumed = Box::pin(backend.close());
	let returned_before_release = futures::poll!(&mut resumed).is_ready();
	gate.release.notify_one();
	if !returned_before_release {
		timeout(Duration::from_secs(5), resumed)
			.await
			.expect("resumed close waits for release acknowledgement");
	}
	assert!(
		!returned_before_release,
		"cancelled close lost its dispatched release completion"
	);
	Ok(())
}

#[tokio::test]
async fn dropped_backend_retains_dispatched_close_for_shutdown_join() -> Result {
	let fake = Fake::start().await?;
	let server = remote_server(&fake.url, 2, 0)?;
	let backend = Backend::open(&server).await?;
	let root = Arc::downgrade(&backend);
	let gate = fake.pause_release();
	let mut entered = Box::pin(gate.entered.notified());
	let mut close = Box::pin(backend.close());
	timeout(Duration::from_secs(5), async {
		loop {
			assert!(futures::poll!(&mut close).is_pending(), "release response blocked");
			if futures::poll!(&mut entered).is_ready() {
				break;
			}
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("release dispatched");
	drop(close);
	drop(backend);
	assert_eq!(root.strong_count(), 0, "cleanup does not retain backend");
	let mut join = Box::pin(server.cleanup.join());
	let premature = futures::poll!(&mut join).is_ready();
	gate.release.notify_one();
	if !premature {
		timeout(Duration::from_secs(5), join)
			.await
			.expect("retained release joined")?;
	}
	assert!(!premature, "backend drop detached its dispatched close");
	let successor = Backend::open(&server).await?;
	assert!(successor.lease_status().held, "successor acquires released lease");
	successor.close().await;
	Ok(())
}
