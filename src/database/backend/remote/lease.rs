//! The writer lease (ADR-0003, ADR-0012 "Lease").
//!
//! The process acquires the D1 writer lease before the database is usable,
//! renews it every `ttl / 3`, and carries `(holder, epoch)` on every commit
//! and scan page. Time is the Worker's: each lease reply returns
//! `expires_at_ms` and `now_ms` on the Worker's clock, and only their
//! difference is kept, counted down on the local monotonic clock. The local
//! wall clock is never compared with D1's.
//!
//! States: *held* (renewals confirmed), *uncertain* (a renewal failed or
//! timed out; commits fail fast until a later renewal confirms), *lost* (the
//! countdown reached zero without confirmation, or the Worker answered
//! `StaleLease`; writes stop for good and the server shuts down), and
//! *released* (explicitly expired at close).

use std::{
	sync::{Arc, Mutex, PoisonError},
	time::{Duration, Instant},
};

use tokio::time::sleep;
use tuwunel_bridge::{self as bridge, Request, Response};
use tuwunel_core::{Result, debug, err, error, utils, warn};

use super::client::{CallError, Client, database_error};

/// Lease state as reported by `GET /_tuwunel/readiness`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LeaseStatus {
	/// The lease is ours and has not been lost or released.
	pub held: bool,
	/// Fencing epoch granted at acquisition.
	pub epoch: u64,
	/// Milliseconds of confirmed validity left, counted down locally from
	/// the Worker's last reply.
	pub expires_in_ms: u64,
	/// The last renewal failed; writes are refused until one confirms.
	pub uncertain: bool,
}

/// Owner-side view of the lease.
pub(crate) struct Lease {
	holder: String,
	ttl: Duration,
	client: Arc<Client>,
	state: Mutex<State>,
}

/// Mutable lease state behind the lock.
struct State {
	epoch: u64,
	held: bool,
	/// Local instant of the last confirmed reply.
	confirmed_at: Instant,
	/// `expires_at_ms - now_ms` of that reply.
	expires_in: Duration,
	uncertain: bool,
	lost: bool,
}

impl Lease {
	/// Acquires the lease, waiting out a competing unexpired holder.
	///
	/// A rollout can leave the previous Container's lease unexpired for up
	/// to one `ttl`; rather than fail the start, acquisition sleeps for the
	/// remaining validity the Worker reports and retries, giving up after
	/// four `ttl`s so a stuck competitor still surfaces as an error.
	pub(crate) async fn acquire(
		client: Arc<Client>,
		holder: String,
		ttl: Duration,
	) -> Result<Arc<Self>> {
		let request = Request::LeaseAcquire {
			holder: holder.clone(),
			ttl_ms: millis(ttl),
		};
		let started = Instant::now();
		let patience = ttl.saturating_mul(4);
		loop {
			match client.call(&request, None).await {
				| Ok(Response::Leased { epoch, expires_at_ms, now_ms }) => {
					debug!(epoch, "acquired the writer lease");
					return Ok(Arc::new(Self {
						holder,
						ttl,
						client,
						state: Mutex::new(State {
							epoch,
							held: true,
							confirmed_at: Instant::now(),
							expires_in: Duration::from_millis(
								expires_at_ms.saturating_sub(now_ms),
							),
							uncertain: false,
							lost: false,
						}),
					}));
				},
				| Ok(_) => return Err(err!(Database("bridge lease acquire: unexpected reply"))),
				| Err(CallError::Bridge(bridge::Error::LeaseHeld { current })) => {
					let remaining = competitor_remaining(&client, current.expires_at_ms).await;
					if started.elapsed().saturating_add(remaining) > patience {
						return Err(err!(Database(
							"bridge lease acquire: another writer holds the lease (epoch {}) \
							 beyond the acquisition patience",
							current.epoch
						)));
					}

					warn!(
						epoch = current.epoch,
						wait_ms = millis(remaining),
						"writer lease held by another process; waiting for its expiry"
					);
					sleep(remaining).await;
				},
				| Err(error) => return Err(database_error("lease acquire", &error)),
			}
		}
	}

	/// The identity carried on commits and scan pages.
	///
	/// Present once an epoch has been granted, including after loss: a
	/// stale identity must reach the fence and be refused, not skip it.
	/// Absent only after an explicit release.
	pub(crate) fn current(&self) -> Option<bridge::Lease> {
		let state = self.lock();
		state.held.then(|| bridge::Lease {
			holder: self.holder.clone(),
			epoch: state.epoch,
		})
	}

	/// Snapshot for readiness reporting.
	pub(crate) fn status(&self) -> LeaseStatus {
		let state = self.lock();
		LeaseStatus {
			held: state.held && !state.lost,
			epoch: state.epoch,
			expires_in_ms: millis(remaining(&state)),
			uncertain: state.uncertain,
		}
	}

	/// Whether commits may proceed right now.
	pub(crate) fn writable(&self) -> bool {
		let state = self.lock();
		state.held && !state.lost && !state.uncertain && !remaining(&state).is_zero()
	}

	/// Whether the lease has been lost for good.
	pub(crate) fn is_lost(&self) -> bool { self.lock().lost }

	/// The lease's configured lifetime.
	pub(crate) fn ttl(&self) -> Duration { self.ttl }

	/// Records the loss; returns whether this call made the transition.
	pub(crate) fn mark_lost(&self) -> bool {
		let mut state = self.lock();
		let first = !state.lost;
		state.lost = true;
		first
	}

	/// Records a failed renewal.
	fn mark_uncertain(&self) { self.lock().uncertain = true; }

	/// Renews the lease once, with transport retries bounded by the
	/// remaining validity.
	///
	/// A stale-lease refusal marks the lease lost; any other failure marks
	/// it uncertain, and the caller decides whether the countdown ran out.
	pub(crate) async fn renew(&self) -> Result<(), CallError> {
		let Some(lease) = self.current() else {
			return Ok(());
		};

		let deadline = {
			let state = self.lock();
			state.confirmed_at.checked_add(state.expires_in)
		};

		let request = Request::LeaseRenew { lease, ttl_ms: millis(self.ttl) };
		match self.client.call(&request, deadline).await {
			| Ok(Response::Leased { epoch, expires_at_ms, now_ms }) => {
				let mut state = self.lock();
				state.epoch = epoch;
				state.confirmed_at = Instant::now();
				state.expires_in = Duration::from_millis(expires_at_ms.saturating_sub(now_ms));
				state.uncertain = false;
				Ok(())
			},
			| Ok(_) => {
				self.mark_uncertain();
				Err(CallError::Transport {
					class: "decode",
					detail: "unexpected reply to lease renewal".into(),
					retryable: false,
				})
			},
			| Err(error) => {
				if error.is_stale_lease() {
					self.mark_lost();
				} else {
					self.mark_uncertain();
				}
				Err(error)
			},
		}
	}

	/// Takes the identity for a release, marking the lease unheld.
	///
	/// Returns `None` when there is nothing to release, so a drop path can
	/// skip the call entirely. Marking happens here rather than after the
	/// reply so a second release can never be issued.
	pub(crate) fn releasable(&self) -> Option<bridge::Lease> {
		let mut state = self.lock();
		let lease = state.held.then(|| bridge::Lease {
			holder: self.holder.clone(),
			epoch: state.epoch,
		});

		state.held = false;

		lease
	}

	/// Expires the lease at the Worker so a successor need not wait it out.
	///
	/// Best effort: a failure only means the successor waits for the
	/// natural expiry. The lease reports unheld afterwards either way.
	pub(crate) async fn release(&self) {
		let Some(lease) = self.releasable() else {
			return;
		};

		match self
			.client
			.call(&Request::LeaseRelease { lease }, None)
			.await
		{
			| Ok(Response::Released) => debug!("released the writer lease"),
			| Ok(_) => warn!("unexpected reply to lease release"),
			| Err(error) => warn!(%error, "lease release failed; it expires on its own"),
		}
	}

	fn lock(&self) -> std::sync::MutexGuard<'_, State> {
		self.state
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
	}
}

/// Runs the renewal loop until the lease is lost or the task is aborted.
///
/// Renewal happens every `ttl / 3`. A renewal that fails leaves the lease
/// uncertain; when the countdown then reaches zero, or the Worker refuses
/// the renewal as stale, `on_lost` runs once and the loop ends.
pub(crate) async fn renewals(lease: Arc<Lease>, on_lost: impl FnOnce() + Send + 'static) {
	let period = lease
		.ttl()
		.checked_div(3)
		.unwrap_or(Duration::from_secs(1))
		.max(Duration::from_millis(100));

	loop {
		sleep(period).await;
		if lease.is_lost() {
			break;
		}

		match lease.renew().await {
			| Ok(()) => {},
			| Err(error) if error.is_stale_lease() => {
				error!(%error, "writer lease refused as stale; stopping writes");
				break;
			},
			| Err(error) => {
				let status = lease.status();
				if status.expires_in_ms == 0 && lease.mark_lost() {
					error!(%error, "writer lease expired without a confirmed renewal");
					break;
				}
				warn!(
					%error,
					expires_in_ms = status.expires_in_ms,
					"writer lease renewal failed; lease uncertain"
				);
			},
		}
	}

	on_lost();
}

/// Process identity for the lease row: hostname (or a fixed tag), the pid,
/// and a random suffix, within the protocol's 128-byte holder limit.
pub(crate) fn holder_id() -> String {
	let host = std::env::var("HOSTNAME")
		.ok()
		.filter(|host| !host.is_empty())
		.unwrap_or_else(|| "tuwunel".to_owned());
	let host: String = host.chars().take(64).collect();

	format!("{host}-{}-{}", std::process::id(), utils::rand::string(8))
}

/// Confirmed validity left on the local countdown.
fn remaining(state: &State) -> Duration {
	if state.lost || !state.held {
		return Duration::ZERO;
	}

	state
		.expires_in
		.saturating_sub(state.confirmed_at.elapsed())
}

/// How long a competing lease has left, on the Worker's clock.
///
/// `LeaseHeld` carries the competitor's expiry but not the Worker's clock,
/// so one `Hello` supplies `now_ms`; if that fails, one full `ttl` is the
/// conservative wait. A small margin absorbs clock granularity.
async fn competitor_remaining(client: &Client, expires_at_ms: u64) -> Duration {
	let now_ms = match client.call(&Request::Hello, None).await {
		| Ok(Response::Hello { now_ms, .. }) => Some(now_ms),
		| _ => None,
	};

	now_ms.map_or(Duration::from_secs(15), |now_ms| {
		Duration::from_millis(
			expires_at_ms
				.saturating_sub(now_ms)
				.saturating_add(100),
		)
	})
}

/// Milliseconds of a duration, saturating.
pub(crate) fn millis(duration: Duration) -> u64 {
	u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
