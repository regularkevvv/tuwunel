//! Bounded hints for one owned worker. Delivery remains owned by durable rows.
use std::collections::VecDeque;

use ruma::OwnedUserId;
use tuwunel_core::{Result, warn};

use super::{Msg, SendingEvent, Service};

const MAX_USERS: usize = 128;
const MAX_USER_BYTES: usize = 16 * 1024;
pub(super) const MAX_CURSOR_BYTES: usize = 1024;

#[derive(Default)]
pub(super) struct PushWakes {
	queue: VecDeque<Wake>,
	user_bytes: usize,
	pub(super) stopped: bool,
}

#[derive(Clone)]
pub(super) struct Wake {
	pub(super) user: OwnedUserId,
	pub(super) reason: &'static str,
	pub(super) active: bool,
	pub(super) after: Option<Vec<u8>>,
	started: bool,
	again: bool,
}

pub(super) enum Page {
	Next(Vec<u8>),
	End,
	Refused,
}

impl Service {
	/// Coalesce presence/read hints without taking ownership from storage.
	pub fn schedule_resume_pushes_for_user(&self, user: OwnedUserId, reason: &'static str) {
		if self.server.is_running()
			&& self
				.push_wakes
				.lock()
				.expect("locked")
				.schedule(user, reason)
		{
			self.push_wake_signal.notify_one();
		}
	}

	pub(super) async fn push_wake_worker(&self) -> Result {
		while self.server.is_running() {
			let next = {
				let mut state = self.push_wakes.lock().expect("locked");
				if state.stopped {
					return Ok(());
				}
				state.next()
			};
			let Some(wake) = next else {
				self.push_wake_signal.notified().await;
				continue;
			};
			let page = match self.wake_push_page(&wake).await {
				| Ok(Some(next)) => Page::Next(next),
				| Ok(None) => Page::End,
				| Err(error) => {
					warn!(user = ?wake.user, reason = wake.reason, ?error,
						"Push wake failed; durable rows remain owed");
					Page::Refused
				},
			};
			self.push_wakes
				.lock()
				.expect("locked")
				.finish(page);
			tokio::task::yield_now().await;
		}
		Ok(())
	}

	async fn wake_push_page(&self, wake: &Wake) -> Result<Option<Vec<u8>>> {
		let (destinations, next) = self
			.db
			.push_destinations_for_user_after(&wake.user, wake.active, wake.after.as_deref())
			.await?;
		for dest in destinations {
			self.dispatch(Msg {
				dest,
				event: SendingEvent::Flush,
				queue_id: Vec::new(),
			})?;
		}
		Ok(next)
	}
}

impl PushWakes {
	/// A full queue drops only the hint. Sender retry timers and startup scans
	/// continue to own recovery of the original durable delivery rows.
	pub(super) fn schedule(&mut self, user: OwnedUserId, reason: &'static str) -> bool {
		if self.stopped {
			return false;
		}
		if let Some(wake) = self
			.queue
			.iter_mut()
			.find(|wake| wake.user == user)
		{
			wake.again |= wake.started;
			return true;
		}
		let bytes = self
			.user_bytes
			.saturating_add(user.as_bytes().len());
		if self.queue.len() >= MAX_USERS || bytes > MAX_USER_BYTES {
			return false;
		}
		self.user_bytes = bytes;
		self.queue.push_back(Wake {
			user,
			reason,
			active: true,
			after: None,
			started: false,
			again: false,
		});
		true
	}

	/// Keep the current job queued until its page completes. A cancelled or
	/// restarted worker cannot strand a user outside the queue.
	pub(super) fn next(&mut self) -> Option<Wake> {
		let wake = self.queue.front_mut()?;
		wake.started = true;
		Some(wake.clone())
	}

	/// Rotate after one page so another user's hint can run before this user's
	/// next page. Coalesced hints arriving during a scan request one fresh
	/// pass.
	pub(super) fn finish(&mut self, page: Page) {
		let Some(mut wake) = self.queue.pop_front() else {
			return;
		};
		match page {
			| Page::Next(cursor) if cursor.len() <= MAX_CURSOR_BYTES => {
				wake.after = Some(cursor);
			},
			| Page::End if wake.active => {
				wake.active = false;
				wake.after = None;
			},
			| Page::End if wake.again => {
				wake.active = true;
				wake.after = None;
				wake.started = false;
				wake.again = false;
			},
			| Page::End | Page::Next(_) | Page::Refused => {
				self.user_bytes = self
					.user_bytes
					.saturating_sub(wake.user.as_bytes().len());
				return;
			},
		}
		self.queue.push_back(wake);
	}

	pub(super) fn stop(&mut self) {
		self.stopped = true;
		self.queue.clear();
		self.user_bytes = 0;
	}
}

#[cfg(test)]
mod tests {
	use ruma::OwnedUserId;

	use super::{MAX_CURSOR_BYTES, MAX_USER_BYTES, MAX_USERS, Page, PushWakes};

	fn user(index: usize) -> OwnedUserId {
		format!("@wake{index}:example.org")
			.try_into()
			.expect("user")
	}

	#[test]
	fn repeated_hints_and_row_overflow_do_not_grow_the_queue() {
		let mut state = PushWakes::default();
		for index in 0..MAX_USERS {
			assert!(state.schedule(user(index), "receipt"));
		}
		for _ in 0..10_000 {
			assert!(state.schedule(user(0), "presence"));
		}
		assert_eq!(state.queue.len(), MAX_USERS);
		assert!(!state.schedule(user(MAX_USERS), "overflow"));
		assert!(!state.queue.front().expect("first").again, "no scan has started");
		state.finish(Page::Refused);
		assert!(state.schedule(user(MAX_USERS), "released capacity"));
		assert_eq!(state.queue.len(), MAX_USERS);
	}

	#[test]
	fn user_byte_budget_refuses_before_the_row_limit() {
		let mut state = PushWakes::default();
		let mut accepted = 0_usize;
		for index in 0..MAX_USERS {
			let user: OwnedUserId = format!("@wake{index}{}:example.org", "u".repeat(226))
				.try_into()
				.expect("bounded user");
			let next_bytes = state
				.user_bytes
				.saturating_add(user.as_bytes().len());
			if next_bytes > MAX_USER_BYTES {
				assert!(!state.schedule(user, "byte overflow"));
				break;
			}
			assert!(state.schedule(user, "receipt"));
			accepted = accepted.saturating_add(1);
		}
		assert!(accepted > 0 && accepted < MAX_USERS);
		assert!(state.user_bytes <= MAX_USER_BYTES);
		assert_eq!(state.queue.len(), accepted);
	}

	#[test]
	fn pages_rotate_fairly_and_an_inflight_hint_requests_a_fresh_pass() {
		let mut state = PushWakes::default();
		assert!(state.schedule(user(0), "receipt"));
		assert!(state.schedule(user(1), "presence"));
		assert_eq!(state.next().expect("first page").user, user(0));
		assert!(state.schedule(user(0), "later receipt"));
		state.finish(Page::Next(vec![7; MAX_CURSOR_BYTES]));
		assert_eq!(
			state
				.next()
				.expect("other user before continuation")
				.user,
			user(1)
		);
		state.finish(Page::Refused);
		let continued = state.next().expect("saved cursor");
		assert_eq!(continued.after.as_deref(), Some([7; MAX_CURSOR_BYTES].as_slice()));
		assert!(continued.active);
		state.finish(Page::End);
		assert!(!state.next().expect("pending rows").active);
		state.finish(Page::End);
		let fresh = state.next().expect("coalesced follow-up");
		assert!(fresh.active);
		assert!(fresh.after.is_none());
		state.finish(Page::End);
		state.finish(Page::End);
		assert!(state.next().is_none());
		assert_eq!(state.user_bytes, 0);
	}

	#[test]
	fn cancellation_preserves_the_current_job_and_oversized_cursors_are_refused() {
		let mut state = PushWakes::default();
		assert!(state.schedule(user(0), "receipt"));
		let interrupted = state.next().expect("page");
		let restarted = state
			.next()
			.expect("same job survives worker cancellation");
		assert_eq!(restarted.user, interrupted.user);
		assert_eq!(restarted.after, interrupted.after);
		state.finish(Page::Next(vec![0; MAX_CURSOR_BYTES.saturating_add(1)]));
		assert!(state.next().is_none());
		assert_eq!(state.user_bytes, 0);
	}

	#[test]
	fn shutdown_rejects_new_hints_and_releases_only_volatile_jobs() {
		let mut state = PushWakes::default();
		assert!(state.schedule(user(0), "receipt"));
		let _inflight = state.next().expect("in flight");
		state.stop();
		state.finish(Page::End);
		assert!(state.stopped);
		assert!(!state.schedule(user(1), "after stop"));
		assert!(state.next().is_none());
		assert_eq!(state.user_bytes, 0);
	}
}
