//! Explicit close must wait for accepted reads without invalidating later lazy
//! queries. Dropped and concurrent close futures must preserve that boundary.

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, var},
	fs::DirBuilder,
	process::Command,
	sync::{
		Arc, Mutex,
		atomic::{AtomicUsize, Ordering},
		mpsc,
	},
	time::Duration,
};

use futures::{FutureExt, TryFutureExt, poll};
use tokio::{sync::oneshot, time::timeout};
use tuwunel_core::{Result, config::Figment, utils::rand};

use super::{Audit, Directory};
use crate::{Database, pool::Get};

const CASE: &str = "TUWUNEL_NATIVE_DRAIN_CASE";

#[derive(Clone, Copy)]
enum Close {
	CancelledRead,
	ActiveRead,
	CancelledJoin,
	ConcurrentJoin,
	FencedAdmission,
}

impl Close {
	fn name(self) -> &'static str {
		match self {
			| Self::CancelledRead => "native_close_waits_for_cancelled_accepted_read",
			| Self::ActiveRead => "native_close_waits_for_active_read",
			| Self::CancelledJoin => "cancelled_native_close_is_rejoinable",
			| Self::ConcurrentJoin => "concurrent_native_closers_wait_for_accepted_read",
			| Self::FencedAdmission => "native_drain_fences_later_read_admission",
		}
	}
}

async fn isolated(mode: Close) -> Result {
	if var(CASE).as_deref() == Ok(mode.name()) {
		return drain(mode).await;
	}
	let name = format!("pool::owner_teardown_tests::drain_tests::{}", mode.name());
	let output = Command::new(current_exe()?)
		.args(["--exact", &name, "--nocapture"])
		.env(CASE, mode.name())
		.output()?;
	assert!(
		output.status.success(),
		"{} child failed ({:?}):\n{}\n{}",
		mode.name(),
		output.status,
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
	Ok(())
}

fn fixture() -> Result<(Directory, Arc<tuwunel_core::Server>)> {
	let directory = Directory(
		std::env::temp_dir().join(format!("tuwunel-native-drain-{}", rand::string(20))),
	);
	let mut builder = DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&directory.0)?;
	let server = crate::tests::test_server(
		&Figment::new()
			.merge(("server_name", "localhost"))
			.merge(("database_path", directory.0.join("database")))
			.merge(("db_pool_workers", 2))
			.merge(("db_pool_max_workers", 4))
			.merge(("db_pool_affinity", false))
			.merge(("stream_width_scale", 0.0)),
	)?;
	Ok((directory, server))
}

async fn drain(mode: Close) -> Result {
	let (_directory, server) = fixture()?;
	let database = Database::open(&server).await?;
	let map = database.get("global")?.clone();
	map.insert(b"drain-marker", b"durable").await?;
	let engine = Arc::downgrade(map.engine());
	let context = Arc::downgrade(&map.engine().ctx);
	let pool = map.engine().pool.clone();
	let (entered, waiting) = oneshot::channel();
	let (release, held) = mpsc::channel();
	let audit = Arc::new(Audit {
		before: Mutex::new(Some(Box::new(move || {
			entered
				.send(())
				.expect("fixture waits for acceptance");
			held.recv_timeout(Duration::from_secs(10))
				.expect("fixture releases accepted read");
		}))),
		finished: AtomicUsize::new(0),
		panics: AtomicUsize::new(0),
		admissions: AtomicUsize::new(0),
		commands_finished: AtomicUsize::new(0),
	});
	*pool.audit.lock().expect("worker audit") = Some(audit.clone());
	let request = pool.execute_get(Get {
		map: map.clone(),
		key: [b"drain-marker".as_slice().into()].into(),
		res: None,
	});
	let mut request = request.map_ok(drop).boxed();
	assert!(poll!(request.as_mut()).is_pending());
	timeout(Duration::from_secs(5), waiting)
		.await
		.expect("accepted command reaches barrier")
		.expect("command barrier sender");
	let mut request = if matches!(mode, Close::ActiveRead) {
		Some(request)
	} else {
		drop(request);
		None
	};
	assert_eq!(audit.admissions.load(Ordering::Acquire), 1);
	assert_eq!(audit.commands_finished.load(Ordering::Acquire), 0);
	let mut closing = Box::pin(database.close());
	let mut premature = poll!(closing.as_mut()).is_ready();
	if matches!(mode, Close::CancelledJoin) {
		drop(closing);
		closing = Box::pin(database.close());
		premature |= poll!(closing.as_mut()).is_ready();
	}
	let mut other = matches!(mode, Close::ConcurrentJoin).then(|| Box::pin(database.close()));
	let other_premature = if let Some(other) = other.as_mut() {
		poll!(other.as_mut()).is_ready()
	} else {
		false
	};
	let mut later = matches!(mode, Close::FencedAdmission).then(|| {
		pool.execute_get(Get {
			map: map.clone(),
			key: [b"drain-marker".as_slice().into()].into(),
			res: None,
		})
		.map_ok(drop)
		.boxed()
	});
	let later_ready = if let Some(later) = later.as_mut() {
		match poll!(later.as_mut()) {
			| std::task::Poll::Ready(result) => Some(result),
			| std::task::Poll::Pending => None,
		}
	} else {
		None
	};
	let later_admitted = audit.admissions.load(Ordering::Acquire) > 1;
	release.send(()).expect("accepted read retained");
	if !premature {
		timeout(Duration::from_secs(10), closing.as_mut())
			.await
			.expect("close joins accepted read");
	}
	drop(closing);
	if let Some(other) = other.as_mut()
		&& !other_premature
	{
		timeout(Duration::from_secs(10), other.as_mut())
			.await
			.expect("second close also joins accepted read");
	}
	drop(other);
	if let Some(request) = request.take() {
		timeout(Duration::from_secs(5), request)
			.await
			.expect("active response remains usable")?;
	}
	drop(request);
	if let Some(result) = later_ready {
		result?;
	} else if let Some(later) = later.take() {
		timeout(Duration::from_secs(5), later)
			.await
			.expect("later admission resumes after drain")?;
	}
	drop(later);
	timeout(Duration::from_secs(5), async {
		while audit.commands_finished.load(Ordering::Acquire) == 0 {
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("accepted command finishes");
	// Explicit close drains workers without closing the pool. A caller's lazy
	// query can still submit work after the service graph has stopped.
	let values = pool
		.execute_get(Get {
			map: map.clone(),
			key: [b"drain-marker".as_slice().into()].into(),
			res: None,
		})
		.await?;
	assert_eq!(
		values[0]
			.as_ref()
			.expect("post-close native read")
			.as_ref(),
		b"durable"
	);
	drop(values);
	drop(map);
	drop(database);
	server.cleanup.join().await?;
	pool.close();
	assert_eq!(audit.panics.load(Ordering::Acquire), 0);
	assert_eq!(engine.strong_count(), 0);
	assert_eq!(context.strong_count(), 0);
	let pool_root = Arc::downgrade(&pool);
	drop(pool);
	assert_eq!(pool_root.strong_count(), 0);
	verify_marker(&server).await?;
	if matches!(mode, Close::FencedAdmission) {
		assert!(!later_admitted, "later read bypassed the pending native drain");
	}
	assert!(!premature, "native close returned while an accepted read was held");
	assert!(!other_premature, "concurrent native close returned before the read finished");
	Ok(())
}

async fn verify_marker(server: &Arc<tuwunel_core::Server>) -> Result {
	let reopened = Database::open(server).await?;
	assert_eq!(
		reopened["global"]
			.get(b"drain-marker")
			.await?
			.as_ref(),
		b"durable"
	);
	drop(reopened);
	assert_eq!(Arc::strong_count(server), 1);
	Ok(())
}

#[tokio::test]
async fn native_close_waits_for_cancelled_accepted_read() -> Result {
	isolated(Close::CancelledRead).await
}

#[tokio::test]
async fn native_close_waits_for_active_read() -> Result { isolated(Close::ActiveRead).await }

#[tokio::test]
async fn cancelled_native_close_is_rejoinable() -> Result { isolated(Close::CancelledJoin).await }

#[tokio::test]
async fn concurrent_native_closers_wait_for_accepted_read() -> Result {
	isolated(Close::ConcurrentJoin).await
}

#[tokio::test]
async fn native_drain_fences_later_read_admission() -> Result {
	isolated(Close::FencedAdmission).await
}
