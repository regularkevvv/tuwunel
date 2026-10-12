//! Identifies an owning worker without taking the inventory lock that a closer
//! can hold while joining that worker.

use std::{cell::Cell, ptr};

use super::Pool;

thread_local! {
	static CURRENT: Cell<*const Pool> = const { Cell::new(ptr::null()) };
}

pub(super) struct Current(*const Pool);

impl Current {
	pub(super) fn enter(pool: &Pool) -> Self { Self(CURRENT.replace(ptr::from_ref(pool))) }
}

impl Drop for Current {
	fn drop(&mut self) { CURRENT.set(self.0); }
}

impl Pool {
	pub(crate) fn is_worker_thread(&self) -> bool {
		CURRENT.with(|current| ptr::eq(current.get(), self))
	}
}
