//! One process-wide teardown thread. It retains no database or server while
//! idle, and is prepared before worker creation so destruction is infallible.

use std::{
	panic::{AssertUnwindSafe, catch_unwind},
	sync::{Mutex, OnceLock, mpsc},
	thread,
};

use futures::channel::oneshot;
use tuwunel_core::{Error, Result};

use super::Resources;

pub(super) type Sender = mpsc::Sender<Cleanup>;

pub(super) struct Cleanup {
	resources: Box<Resources>,
	completed: oneshot::Sender<Result>,
}

static REAPER: OnceLock<Mutex<Option<Sender>>> = OnceLock::new();

pub(super) fn acquire() -> Result<Sender> {
	let mut sender = REAPER
		.get_or_init(Mutex::default)
		.lock()
		.expect("teardown executor initialization");
	if let Some(sender) = &*sender {
		return Ok(sender.clone());
	}
	let (send, recv) = mpsc::channel::<Cleanup>();
	thread::Builder::new()
		.name("tuwunel:db:close".into())
		.stack_size(1_048_576)
		.spawn(move || {
			while let Ok(Cleanup { resources, completed }) = recv.recv() {
				let result = catch_unwind(AssertUnwindSafe(move || {
					let result = resources.close();
					drop(resources);
					result
				}))
				.map_err(Error::from_panic)
				.and_then(std::convert::identity);
				_ = completed.send(result);
			}
		})?;
	*sender = Some(send.clone());
	Ok(send)
}

pub(super) fn submit(sender: &Sender, resources: Box<Resources>) {
	let (completed, completion) = oneshot::channel();
	// Register before exposing the work. The receipt owns no server, resources,
	// or runtime, so it cannot create a server -> cleanup -> server cycle.
	resources
		.server
		.cleanup
		.retain_native(async move {
			completion
				.await
				.map_err(|error| tuwunel_core::err!("native teardown completion lost: {error}"))?
		});
	sender
		.send(Cleanup { resources, completed })
		.expect("native teardown executor remains alive");
}
