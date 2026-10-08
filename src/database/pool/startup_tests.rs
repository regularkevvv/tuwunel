//! Partial startup and simultaneous close must finish owned worker joins.

use std::{
	cell::{Cell, RefCell},
	panic::{AssertUnwindSafe, catch_unwind},
	sync::{Arc, Weak, mpsc},
	thread,
	time::Duration,
};

use tuwunel_core::{Result, Server, config::Figment};

use super::Pool;

thread_local! {
	static FAULT: Cell<Option<(usize, bool)>> = const { Cell::new(None) };
	static OBSERVED: RefCell<Weak<Pool>> = const { RefCell::new(Weak::new()) };
}

pub(crate) fn before_spawn(pool: &Arc<Pool>, id: usize) -> Result {
	OBSERVED.with(|observed| *observed.borrow_mut() = Arc::downgrade(pool));
	if let Some((after, panic)) = FAULT.get()
		&& after == id
	{
		assert!(!panic, "injected native pool spawn panic");
		return Err(std::io::Error::other("injected native pool spawn error").into());
	}

	Ok(())
}

pub(crate) fn clear_observation() {
	OBSERVED.with(|observed| *observed.borrow_mut() = Weak::new());
}

pub(crate) fn take_observation() -> Weak<Pool> { OBSERVED.with(RefCell::take) }

struct Fault;

impl Drop for Fault {
	fn drop(&mut self) { FAULT.set(None); }
}

pub(crate) fn server() -> Result<Arc<Server>> {
	crate::tests::test_server(
		&Figment::new()
			.merge(("server_name", "localhost"))
			.merge(("database_path", std::env::temp_dir()))
			.merge(("db_pool_workers", 4))
			.merge(("db_pool_max_workers", 16))
			.merge(("db_pool_affinity", false))
			.merge(("stream_width_scale", 0.0)),
	)
}

fn spawn_failure(after: usize, panic: bool) -> Result {
	let server = server()?;
	clear_observation();
	FAULT.set(Some((after, panic)));
	let fault = Fault;
	let result = catch_unwind(AssertUnwindSafe(|| Pool::new(&server)));
	drop(fault);
	if panic {
		assert!(result.is_err(), "the original spawn panic must propagate");
	} else {
		let result = result.expect("error injection does not panic");
		assert!(
			result
				.err()
				.expect("thread creation failed")
				.to_string()
				.contains("injected native pool spawn error")
		);
	}
	let observed = take_observation();
	let workers_retained = observed.strong_count() != 0;
	let server_refs = Arc::strong_count(&server);
	// An unchanged control leaks workers. Close them before asserting so this
	// test never leaves blocked worker threads in the rest of the test process.
	if let Some(pool) = observed.upgrade() {
		pool.workers.clear_poison();
		pool.close();
	}
	assert!(!workers_retained, "failed startup retained its pool workers");
	assert_eq!(server_refs, 1, "failed startup retained the server");
	assert_eq!(observed.strong_count(), 0);
	let reopened = Pool::new(&server)?;
	reopened.close();
	drop(reopened);
	assert_eq!(Arc::strong_count(&server), 1);
	Ok(())
}

#[tokio::test]
async fn first_spawn_failure_releases_the_unstarted_pool() -> Result { spawn_failure(0, false) }

#[tokio::test]
async fn later_spawn_failure_joins_all_started_workers() -> Result { spawn_failure(2, false) }

#[tokio::test]
async fn spawn_panic_joins_workers_despite_the_poisoned_inventory() -> Result {
	spawn_failure(2, true)
}

#[tokio::test]
async fn simultaneous_close_waits_for_the_original_worker_joins() -> Result {
	let server = server()?;
	let pool = Pool::new(&server)?;
	let (release, held) = mpsc::channel();
	let (ready, running) = mpsc::channel();
	let owner = pool.clone();
	let worker = thread::spawn(move || {
		ready
			.send(())
			.expect("fixture waits for worker startup");
		held.recv_timeout(Duration::from_secs(10))
			.expect("fixture releases worker");
		drop(owner);
	});
	pool.workers
		.lock()
		.expect("worker inventory")
		.push(worker);
	running
		.recv_timeout(Duration::from_secs(5))
		.expect("worker starts");
	let owner = pool.clone();
	let first = thread::spawn(move || owner.close());
	tokio::time::timeout(Duration::from_secs(5), async {
		while !pool
			.queues
			.iter()
			.all(async_channel::Sender::is_closed)
		{
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("first close closes queues");
	let (started, starting) = mpsc::channel();
	let (returned, done) = mpsc::channel();
	let owner = pool.clone();
	let second = thread::spawn(move || {
		started
			.send(())
			.expect("fixture waits for second close");
		owner.close();
		returned
			.send(())
			.expect("fixture checks second close completion");
	});
	starting
		.recv_timeout(Duration::from_secs(5))
		.expect("second close starts");
	let returned_before_join = done
		.recv_timeout(Duration::from_millis(100))
		.is_ok();
	release.send(()).expect("blocked worker");
	first.join().expect("first close joins");
	second.join().expect("second close joins");
	assert!(
		!returned_before_join,
		"second close returned while the original worker was alive"
	);
	assert!(
		pool.workers
			.lock()
			.expect("worker inventory")
			.is_empty()
	);
	drop(pool);
	assert_eq!(Arc::strong_count(&server), 1);
	Ok(())
}
