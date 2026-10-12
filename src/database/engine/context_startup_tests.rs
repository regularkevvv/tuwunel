//! Environment refusal must occur before frontend worker creation.

use std::{cell::Cell, sync::Arc};

use tuwunel_core::{Result, err};

use super::Context;
use crate::pool::startup_tests::{clear_observation, server, take_observation};

#[tokio::test]
async fn environment_failure_does_not_start_or_retain_frontend_workers() -> Result {
	let server = server()?;
	clear_observation();
	let observed_refs = Cell::new(0);
	let error = Context::new_with_env(&server, || {
		observed_refs.set(Arc::strong_count(&server));
		Err(err!("injected native environment failure"))
	})
	.err()
	.expect("environment creation refused");
	let observation = take_observation();
	let workers_started = observation.strong_count() != 0;
	let retained_refs = Arc::strong_count(&server);
	if let Some(pool) = observation.upgrade() {
		pool.close();
	}
	assert!(
		error
			.to_string()
			.contains("injected native environment failure")
	);
	assert_eq!(observed_refs.get(), 1, "workers started before environment acquisition");
	assert!(!workers_started, "environment failure retained frontend workers");
	assert_eq!(retained_refs, 1);
	let reopened = Context::new(&server)?;
	drop(reopened);
	assert_eq!(Arc::strong_count(&server), 1);
	Ok(())
}
