//! Cancelled accepted reads must release the last map without joining their own
//! worker.

mod drain_tests;

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, var},
	fs::{DirBuilder, remove_dir_all},
	path::PathBuf,
	process::Command,
	sync::{
		Arc, Mutex,
		atomic::{AtomicUsize, Ordering},
		mpsc,
	},
	thread,
	time::Duration,
};

use futures::{FutureExt, TryFutureExt, poll};
use rocksdb::Direction;
use tokio::{sync::oneshot, time::timeout};
use tuwunel_core::{Result, config::Figment, utils::rand};

use super::{Get, Pool, Seek, into_send_seek};
use crate::{Database, map::iter_options_default, stream};

const CASE: &str = "TUWUNEL_POOL_OWNER_CASE";

pub(super) struct Audit {
	before: Mutex<Option<Box<dyn FnOnce() + Send>>>,
	finished: AtomicUsize,
	panics: AtomicUsize,
	admissions: AtomicUsize,
	commands_finished: AtomicUsize,
}

pub(super) struct WorkerAudit<'a>(pub(super) &'a Pool);

impl Drop for WorkerAudit<'_> {
	fn drop(&mut self) {
		if let Some(audit) = self
			.0
			.audit
			.lock()
			.expect("worker audit")
			.as_ref()
		{
			if thread::panicking() {
				audit.panics.fetch_add(1, Ordering::Release);
			}
			audit.finished.fetch_add(1, Ordering::Release);
		}
	}
}

pub(super) fn before_command(pool: &Pool) {
	let audit = pool.audit.lock().expect("worker audit").clone();
	if let Some(audit) = audit {
		let callback = audit
			.before
			.lock()
			.expect("command barrier")
			.take();
		if let Some(callback) = callback {
			callback();
		}
	}
}

pub(super) fn after_admission(pool: &Pool) {
	if let Some(audit) = pool.audit.lock().expect("worker audit").as_ref() {
		audit.admissions.fetch_add(1, Ordering::Release);
	}
}

pub(super) fn after_command(pool: &Pool) {
	if let Some(audit) = pool.audit.lock().expect("worker audit").as_ref() {
		audit
			.commands_finished
			.fetch_add(1, Ordering::Release);
	}
}

struct Directory(PathBuf);
impl Drop for Directory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[derive(Clone, Copy)]
enum Read {
	Point,
	Batch,
	Seek,
	PointClosedRuntime,
	SeekWhileClosing,
}

impl Read {
	fn name(self) -> &'static str {
		match self {
			| Self::Point => "cancelled_point_read_releases_last_native_owner",
			| Self::Batch => "cancelled_batch_read_releases_last_native_owner",
			| Self::Seek => "cancelled_seek_releases_last_native_owner",
			| Self::PointClosedRuntime => "last_native_owner_releases_after_runtime_shutdown",
			| Self::SeekWhileClosing => "last_native_owner_does_not_deadlock_an_existing_closer",
		}
	}
}

async fn isolated(read: Read) -> Result {
	if var(CASE).is_ok() {
		return last_owner(read).await;
	}
	let name = format!("pool::owner_teardown_tests::{}", read.name());
	let output = Command::new(current_exe()?)
		.args(["--exact", &name, "--nocapture"])
		.env(CASE, read.name())
		.output()?;
	assert!(
		output.status.success(),
		"{} child failed ({:?}):\n{}\n{}",
		read.name(),
		output.status,
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
	Ok(())
}

async fn last_owner(read: Read) -> Result {
	let directory =
		Directory(std::env::temp_dir().join(format!("tuwunel-pool-owner-{}", rand::string(20))));
	let mut builder = DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&directory.0)?;
	let mut server = crate::tests::test_server(
		&Figment::new()
			.merge(("server_name", "localhost"))
			.merge(("database_path", directory.0.join("database")))
			.merge(("db_pool_workers", 2))
			.merge(("db_pool_max_workers", 4))
			.merge(("db_pool_affinity", false))
			.merge(("stream_width_scale", 0.0)),
	)?;
	if matches!(read, Read::PointClosedRuntime) {
		let closed = thread::spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.build()
				.expect("fixture runtime");
			let handle = runtime.handle().clone();
			drop(runtime);
			handle
		})
		.join()
		.expect("closed runtime fixture");
		Arc::get_mut(&mut server)
			.expect("fixture owns server")
			.runtime = Some(closed);
	}
	let database = Database::open(&server).await?;
	let map = database.get("global")?.clone();
	map.insert(b"owner-marker", b"durable").await?;
	let engine = Arc::downgrade(map.engine());
	let context = Arc::downgrade(&map.engine().ctx);
	let pool = map.engine().pool.clone();
	let workers = pool
		.workers
		.lock()
		.expect("worker inventory")
		.len();
	let (entered, waiting) = oneshot::channel();
	let (release, held) = mpsc::channel();
	let audit = Arc::new(Audit {
		before: Mutex::new(Some(Box::new(move || {
			entered
				.send(())
				.expect("fixture waits for command acceptance");
			held.recv_timeout(Duration::from_secs(10))
				.expect("fixture releases accepted command");
		}))),
		finished: AtomicUsize::new(0),
		panics: AtomicUsize::new(0),
		admissions: AtomicUsize::new(0),
		commands_finished: AtomicUsize::new(0),
	});
	*pool.audit.lock().expect("worker audit") = Some(audit.clone());
	let mut request = match read {
		| Read::Point | Read::Batch | Read::PointClosedRuntime => {
			let keys: &[&[u8]] = if matches!(read, Read::Point | Read::PointClosedRuntime) {
				&[b"owner-marker"]
			} else {
				&[b"owner-marker", b"missing"]
			};
			pool.execute_get(Get {
				map,
				key: keys.iter().map(|key| (*key).into()).collect(),
				res: None,
			})
			.map_ok(drop)
			.boxed()
		},
		| Read::Seek | Read::SeekWhileClosing => {
			let state =
				into_send_seek(stream::State::new(&map, iter_options_default(map.engine())));
			pool.execute_iter(Seek {
				map,
				state,
				dir: Direction::Forward,
				key: None,
				res: None,
			})
			.map_ok(drop)
			.boxed()
		},
	};
	assert!(poll!(request.as_mut()).is_pending());
	timeout(Duration::from_secs(5), waiting)
		.await
		.expect("accepted command reaches barrier")
		.expect("command barrier sender");
	drop(request);
	drop(database);
	assert!(engine.strong_count() > 0, "accepted command owns the final native engine");
	let closer = if matches!(read, Read::SeekWhileClosing) {
		let owned = pool.clone();
		let closer = thread::spawn(move || owned.close());
		timeout(Duration::from_secs(5), async {
			while !pool
				.queues
				.iter()
				.all(async_channel::Sender::is_closed)
			{
				tokio::task::yield_now().await;
			}
		})
		.await
		.expect("other closer owns worker inventory before final owner drops");
		Some(closer)
	} else {
		None
	};
	release
		.send(())
		.expect("accepted command retained");
	timeout(Duration::from_secs(10), async {
		while audit.finished.load(Ordering::Acquire) != workers {
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("native workers terminate");
	server.cleanup.join().await?;
	if let Some(closer) = closer {
		closer.join().expect("concurrent closer joined");
	}
	pool.close();
	let failures = audit.panics.load(Ordering::Acquire);
	assert_eq!(failures, 0, "last owner drop must not panic while joining its own worker");
	assert_eq!(engine.strong_count(), 0);
	assert_eq!(context.strong_count(), 0);
	let pool_root = Arc::downgrade(&pool);
	drop(pool);
	assert_eq!(pool_root.strong_count(), 0, "all worker owners joined");
	let reopened = Database::open(&server).await?;
	assert_eq!(
		reopened["global"]
			.get(b"owner-marker")
			.await?
			.as_ref(),
		b"durable"
	);
	drop(reopened);
	assert_eq!(Arc::strong_count(&server), 1);
	Ok(())
}

#[tokio::test]
async fn cancelled_point_read_releases_last_native_owner() -> Result {
	isolated(Read::Point).await
}

#[tokio::test]
async fn cancelled_batch_read_releases_last_native_owner() -> Result {
	isolated(Read::Batch).await
}

#[tokio::test]
async fn cancelled_seek_releases_last_native_owner() -> Result { isolated(Read::Seek).await }

#[tokio::test]
async fn last_native_owner_releases_after_runtime_shutdown() -> Result {
	isolated(Read::PointClosedRuntime).await
}

#[tokio::test]
async fn last_native_owner_does_not_deadlock_an_existing_closer() -> Result {
	isolated(Read::SeekWhileClosing).await
}
