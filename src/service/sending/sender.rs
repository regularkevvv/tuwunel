use std::{
	cmp::Reverse,
	collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, btree_map::Entry},
	fmt::Debug,
	iter::once,
	str::from_utf8,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::{Duration, Instant, SystemTime},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::{
	FutureExt, StreamExt, TryFutureExt, TryStreamExt,
	future::{BoxFuture, join, join3, try_join3},
	pin_mut,
	stream::FuturesUnordered,
};
use ruma::{
	MilliSecondsSinceUnixEpoch, OneTimeKeyAlgorithm, OwnedDeviceId, OwnedRoomId, OwnedServerName,
	OwnedUserId, RoomId, ServerName, UInt, UserId,
	api::{
		appservice::event::push_events::v1::{
			DeviceLists, EphemeralData, Request as PushEventsRequest,
		},
		client::push::Pusher,
		error::ErrorKind,
		federation::transactions::{
			edu::{
				DeviceListUpdateContent, Edu, PresenceContent, PresenceUpdate, ReceiptContent,
				ReceiptData, ReceiptMap,
			},
			send_transaction_message,
		},
	},
	device_id,
	events::{
		AnySyncEphemeralRoomEvent, GlobalAccountDataEventType, push_rules::PushRulesEvent,
		receipt::ReceiptType,
	},
	presence::PresenceState,
	push::Ruleset,
	serde::Raw,
	uint,
};
use serde::Deserialize;
use tokio::time::Instant as TokioInstant;
use tuwunel_core::{
	Error, Event, Result, debug, debug_warn, err, error,
	error::error_chain,
	extract_variant, implement,
	smallvec::SmallVec,
	trace,
	utils::{
		BoolExt, ReadyExt, calculate_hash, exponential_backoff_remaining_secs,
		future::TryExtExt,
		rand::secs as rand_secs,
		stream::{BroadbandExt, IterStream, WidebandExt},
	},
	warn,
};

use super::{
	Destination, EduBuf, EduVec, Msg, SendingEvent, Service, TAG_PREFIX_LEN, data::QueueItem,
	reap_flushes,
};
use crate::{federation::ShouldAttempt, rooms::timeline::RawPduId};

#[cfg(test)]
mod edu_tests;

/// In-flight bookkeeping for one `Destination`. Cross-attempt backoff lives
/// in `peer_status` (federation only); appservice/push paths keep their own
/// status because they are not server-keyed.
#[derive(Debug)]
enum TransactionStatus {
	Running,
	RunningForceRetry,
	Failed(u32, Instant), // push backoff: tries, last failure
	Retrying(u32),        // number of times failed
}

enum RetryAction {
	None,
	Force,
}

type SendingError = (Destination, Error);
type SendingResult = Result<Destination, SendingError>;
type SendingFuture<'a> = BoxFuture<'a, SendingResult>;
type SendingFutures<'a> = FuturesUnordered<SendingFuture<'a>>;
type CurTransactionStatus = HashMap<Destination, TransactionStatus>;
type FailedPushIds = SmallVec<[RawPduId; 1]>;

/// MSC3202 `device_one_time_keys_count`: unclaimed one-time-key counts per
/// algorithm, keyed by user then device. Matches the ruma request field type.
type OtkCounts =
	BTreeMap<OwnedUserId, BTreeMap<OwnedDeviceId, BTreeMap<OneTimeKeyAlgorithm, UInt>>>;

/// MSC3202 `device_unused_fallback_key_types`: algorithms with an unused
/// fallback key, keyed by user then device.
type FallbackTypes = BTreeMap<OwnedUserId, BTreeMap<OwnedDeviceId, Vec<OneTimeKeyAlgorithm>>>;

/// The MSC3202-interesting devices of one transaction: the appservice
/// sender's plus matched PDU senders' devices and the to-device recipients.
type Devices = SmallVec<[(OwnedUserId, OwnedDeviceId); 1]>;

/// Per-worker retry timer keyed by earliest-retry deadline and destination.
///
/// Every recorded federation or push failure arms an entry. Stale entries are
/// consumed by the destination's in-flight or newer failure generation. The
/// heap is bounded by concurrently failing destinations, transient federation
/// re-arms, and stale push entries.
type WakeQueue = BinaryHeap<Reverse<(TokioInstant, Destination)>>;

/// Local database backpressure is not a remote delivery failure. In particular,
/// once ACK cleanup completes, a retry must never clean up the unsent
/// successor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueueRecovery {
	CleanupAcknowledged,
	ResumePending,
}

impl QueueRecovery {
	async fn clean_acknowledged<F, Fut>(&mut self, cleanup: F) -> Result
	where
		F: FnOnce() -> Fut,
		Fut: Future<Output = Result>,
	{
		if *self == Self::CleanupAcknowledged {
			cleanup().await?;
			*self = Self::ResumePending;
		}
		Ok(())
	}
}

type QueueRetries = BTreeMap<Destination, (TokioInstant, QueueRecovery)>;
const QUEUE_RETRY_DELAY: Duration = Duration::from_millis(100);

fn defer_queue_error(
	retries: &mut QueueRetries,
	destination: Destination,
	stage: QueueRecovery,
	error: Error,
) -> Result {
	if error.status_code() != http::StatusCode::TOO_MANY_REQUESTS {
		return Err(error);
	}

	let (deadline, _) = wake_deadline(QUEUE_RETRY_DELAY);
	retries.insert(destination, (deadline, stage));
	Ok(())
}

/// Per-(room, user) bucket of `ReceiptData`. MSC3771 allows one receipt
/// per thread context per user per EDU window; the dominant case is
/// still a single receipt, so inline-1 fits without a heap touch.
type UserReceipts = SmallVec<[ReceiptData; 1]>;

/// Per-rank slice of receipt EDU output. Each entry becomes one
/// `Edu::Receipt` buffer; rank 0 carries each user's earliest receipt
/// in the window, rank 1 the next, and so on. Most windows produce a
/// single rank.
type RankedReceipts = SmallVec<[ReceiptMap; 1]>;

/// Per-room ranked receipts gathered for one federation EDU window. The
/// common case is a single room, so inline-1 avoids a heap touch.
type RoomReceipts = SmallVec<[(OwnedRoomId, RankedReceipts); 1]>;

/// Output of one EDU selector. `shipped` rides the current transaction up to
/// the shared budget; `overflow` past the budget is written as queued rows for
/// later transactions to drain.
#[derive(Default)]
struct Selected {
	shipped: EduVec,
	overflow: Vec<EduBuf>,
}

/// The appservice-injected recipient fields of a queued to-device event
/// (MSC4203), parsed to scope MSC3202 one-time-key counts to the addressed
/// devices.
#[derive(Deserialize)]
struct ToDeviceRecipient {
	to_user_id: OwnedUserId,
	to_device_id: OwnedDeviceId,
}

#[derive(Default)]
struct PushFailures {
	ids: FailedPushIds,
	error: Option<Error>,
}

impl PushFailures {
	fn retain(mut self, pdu_id: RawPduId, error: Error) -> Self {
		self.ids.push(pdu_id);
		self.error = self.error.or(Some(error));

		self
	}
}

const SELECT_PRESENCE_LIMIT: usize = 256;
const SELECT_RECEIPT_LIMIT: usize = 256;
const SELECT_DEVICE_CHANGE_LIMIT: usize = 256;
/// Global source counts are unique across update producers. A complete window
/// stays below the per-source caps without taking another source's larger
/// cursor.
const EDU_WINDOW_COUNTS: u64 = 128;
const EDU_ROOM_READ_CONCURRENCY: usize = 8;
const DEQUEUE_LIMIT: usize = 48;
const PUSH_FAILURE_STREAK: u32 = 4;
const WAKE_OVERFLOW_DELAY_SECS: u64 = 365 * 24 * 60 * 60;
const WAKE_OVERFLOW_DELAY: Duration = Duration::from_secs(WAKE_OVERFLOW_DELAY_SECS);

pub const PDU_LIMIT: usize = 50;
pub const EDU_LIMIT: usize = 100;

fn edu_window_end(since: u64, retired: u64) -> Result<u64> {
	if since > retired {
		return Err(Error::bad_database("Outgoing EDU watermark exceeds the retired counter"));
	}
	Ok(since
		.saturating_add(EDU_WINDOW_COUNTS)
		.min(retired))
}

impl Service {
	#[tracing::instrument(skip(self), level = "debug")]
	pub(super) async fn sender(self: Arc<Self>, id: usize) -> Result {
		let mut statuses: CurTransactionStatus = CurTransactionStatus::new();
		let mut futures: SendingFutures<'_> = FuturesUnordered::new();
		let mut wakes: WakeQueue = WakeQueue::new();

		loop {
			match self
				.startup_netburst(id, &mut futures, &mut statuses)
				.boxed()
				.await
			{
				| Ok(()) => break,
				| Err(error) if error.status_code() == http::StatusCode::TOO_MANY_REQUESTS => {
					// These futures have not been polled. Reconstruct from durable
					// active/queued rows before dispatching any startup transaction.
					futures.clear();
					statuses.clear();
					if !self.server.is_running() {
						return Ok(());
					}
					tokio::time::sleep(QUEUE_RETRY_DELAY).await;
				},
				| Err(error) => return Err(error),
			}
		}

		self.work_loop(id, &mut futures, &mut statuses, &mut wakes)
			.await?;

		if !futures.is_empty() {
			self.finish_responses(&mut futures)
				.boxed()
				.await?;
		}

		Ok(())
	}

	#[tracing::instrument(
		name = "work",
		level = "trace",
		skip_all,
		fields(
			futures = %futures.len(),
			statuses = %statuses.len(),
		),
	)]
	async fn work_loop<'a>(
		&'a self,
		id: usize,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
		wakes: &mut WakeQueue,
	) -> Result {
		use tokio::time::{Instant, sleep_until};

		let receiver = self
			.channels
			.get(id)
			.map(|(_, receiver)| receiver.clone())
			.expect("Missing channel for sender worker");
		let mut retries = QueueRetries::new();

		while !receiver.is_closed() {
			let next_due = wakes
				.peek()
				.map_or_else(Instant::now, |Reverse((instant, _))| *instant);
			let retry_due = retries
				.values()
				.map(|(due, _)| *due)
				.min()
				.unwrap_or_else(Instant::now);

			tokio::select! {
				Some(response) = futures.next() => {
					let (dest, mut stage) = match &response {
						Ok(dest) => (dest.clone(), QueueRecovery::CleanupAcknowledged),
						Err((dest, _)) => (dest.clone(), QueueRecovery::ResumePending),
					};
					if let Err(error) = self.handle_response(response, futures, statuses, wakes, &mut stage).await {
						defer_queue_error(&mut retries, dest, stage, error)?;
					}
				},
				request = receiver.recv_async() => match request {
					Ok(request) => {
						let dest = request.dest.clone();
						// Requests are durable queue rows (or coalescible empty-key
						// wakes). The retry owns this destination until it dispatches.
						if !retries.contains_key(&dest)
							&& let Err(error) = self.handle_request(request, futures, statuses).await {
							defer_queue_error(&mut retries, dest, QueueRecovery::ResumePending, error)?;
						}
					},
					Err(_) => return Ok(()),
				},
				() = sleep_until(next_due), if !wakes.is_empty() => {
					self.drain_due_wakes(futures, statuses, wakes, &mut retries).await?;
				},
				() = sleep_until(retry_due), if !retries.is_empty() => {
					self.retry_queue(futures, statuses, &mut retries).await?;
				},
			}
		}
		Ok(())
	}

	#[tracing::instrument(name = "response", level = "debug", skip_all)]
	async fn handle_response<'a>(
		&'a self,
		response: SendingResult,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
		wakes: &mut WakeQueue,
		stage: &mut QueueRecovery,
	) -> Result {
		match response {
			| Ok(dest) =>
				self.resume_queue(&dest, futures, statuses, stage)
					.await?,
			| Err((dest, e)) => {
				let retry_action = Self::handle_response_err(&dest, statuses, &e);

				match dest {
					| Destination::Federation(server) => {
						// Arm a one-shot retry at the destination's earliest-retry time.
						if let ShouldAttempt::No { earliest_retry } = self
							.services
							.federation
							.should_attempt(&server)
							.await
						{
							arm_wake(wakes, Destination::Federation(server), earliest_retry);
						}
					},
					| dest @ Destination::Push(..) => {
						let Some(status @ TransactionStatus::Failed(tries, _)) =
							statuses.get(&dest)
						else {
							return Ok(());
						};

						let tries = *tries;
						let delay = self
							.push_backoff_remaining(Some(status))
							.unwrap_or_default();
						let (deadline, retry_in) = wake_deadline(delay);

						Self::record_push_failure(&dest, &e, tries, retry_in);
						wakes.push(Reverse((deadline, dest)));
					},
					| dest if matches!(retry_action, RetryAction::Force) =>
						self.handle_force_retry(dest, futures, statuses)
							.await?,
					| _ => {},
				}
			},
		}
		Ok(())
	}
}

fn arm_wake_in(wakes: &mut WakeQueue, dest: Destination, delay: Duration) {
	let (deadline, _) = wake_deadline(delay);
	wakes.push(Reverse((deadline, dest)));
}

fn wake_deadline(delay: Duration) -> (TokioInstant, Duration) {
	// Floor the delay at 1s so clock steps and past deadlines wake promptly.
	let delay = delay.max(Duration::from_secs(1));

	// Spread the wake over another delay-width (3s minimum), so destinations
	// sharing a backoff tier trickle back rather than retrying in one burst.
	let jitter = rand_secs(0..delay.as_secs().max(3));
	let now = TokioInstant::now();
	let scheduled = delay.saturating_add(jitter);
	let deadline = now.checked_add(scheduled).unwrap_or_else(|| {
		now.checked_add(WAKE_OVERFLOW_DELAY)
			.unwrap_or(now)
	});
	let scheduled = deadline.saturating_duration_since(now);

	(deadline, scheduled)
}

#[implement(Service)]
fn record_push_failure(dest: &Destination, error: &Error, tries: u32, retry_in: Duration) {
	let Destination::Push(user_id, pushkey) = dest else {
		return;
	};

	match tries {
		| PUSH_FAILURE_STREAK => error!(
			%user_id,
			%pushkey,
			streak = tries,
			retry_in_seconds = retry_in.as_secs(),
			chain = %error_chain(error),
			"Push notifications for this pusher are not being delivered",
		),
		| _ => warn!(
			%user_id,
			%pushkey,
			streak = tries,
			retry_in_seconds = retry_in.as_secs(),
			chain = %error_chain(error),
			"Push transaction failed",
		),
	}
}

#[implement(Service)]
#[inline]
fn push_backoff_remaining(&self, status: Option<&TransactionStatus>) -> Option<Duration> {
	let Some(TransactionStatus::Failed(tries, time)) = status else {
		return None;
	};

	exponential_backoff_remaining_secs(
		self.server.config.sender_timeout,
		self.server.config.sender_retry_backoff_limit,
		time.elapsed(),
		*tries,
	)
}

impl Service {
	async fn handle_force_retry<'a>(
		&'a self,
		dest: Destination,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
	) -> Result {
		let Some(events) = self
			.select_events(&dest, Vec::new(), statuses)
			.await?
		else {
			return Ok(());
		};

		self.schedule_events(dest, events, futures, statuses);
		Ok(())
	}

	fn handle_response_err(
		dest: &Destination,
		statuses: &mut CurTransactionStatus,
		e: &Error,
	) -> RetryAction {
		debug!(?dest, "{e:?}");
		// Push backs off locally; federation defers to peer_status, appservice retries.
		let push = matches!(dest, Destination::Push(..));

		let Some(status) = statuses.get_mut(dest) else {
			return RetryAction::None;
		};

		let (tries, retry_action) = match status {
			| TransactionStatus::Running => (1, RetryAction::None),
			| TransactionStatus::RunningForceRetry => (1, RetryAction::Force),
			| TransactionStatus::Failed(n, _) | TransactionStatus::Retrying(n) =>
				(n.saturating_add(1), RetryAction::None),
		};

		*status = if push {
			TransactionStatus::Failed(tries, Instant::now())
		} else {
			TransactionStatus::Retrying(tries)
		};

		retry_action
	}

	#[expect(clippy::needless_pass_by_ref_mut)]
	async fn resume_queue<'a>(
		&'a self,
		dest: &Destination,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
		stage: &mut QueueRecovery,
	) -> Result {
		let _cork = self.db.db.cork();
		stage
			.clean_acknowledged(|| self.db.delete_all_active_requests_for(dest))
			.await?;

		// A prior attempt may have promoted queued rows or persisted EDUs before
		// backpressure interrupted selection. Replay that durable active set;
		// never delete it as though it belonged to the acknowledged transaction.
		let active = self
			.db
			.active_requests_for(dest)
			.try_collect::<Vec<_>>()
			.await?;
		if !active.is_empty() {
			statuses.insert(dest.clone(), TransactionStatus::Running);
			futures.push(
				self.send_events(
					dest.clone(),
					active
						.into_iter()
						.map(|(_, event)| event)
						.collect(),
				),
			);
			return Ok(());
		}

		// Find events that have been added since starting the last request
		let new_events = self
			.db
			.queued_requests(dest)
			.take(DEQUEUE_LIMIT)
			.try_collect::<Vec<_>>()
			.await?;

		if !new_events.is_empty() {
			self.db.mark_as_active(new_events.iter()).await?;
		}

		let mut events: Vec<SendingEvent> = new_events
			.into_iter()
			.map(|(_, event)| event)
			.collect();

		// Top up with EDUs that accrued while the transaction was in flight.
		if let Destination::Federation(server_name) = dest {
			let budget_used = events
				.iter()
				.filter(|event| matches!(event, SendingEvent::Edu(_)))
				.count();

			let selected = self.select_edus(server_name, budget_used).await?;
			events.extend(selected.into_iter().map(SendingEvent::Edu));
		}

		if events.is_empty() {
			statuses.remove(dest);
		} else {
			statuses.insert(dest.clone(), TransactionStatus::Running);

			futures.push(self.send_events(dest.clone(), events));
		}
		Ok(())
	}

	async fn retry_queue<'a>(
		&'a self,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
		retries: &mut QueueRetries,
	) -> Result {
		let now = TokioInstant::now();
		let Some(dest) = retries
			.iter()
			.find(|(_, (due, _))| *due <= now)
			.map(|(dest, _)| dest.clone())
		else {
			return Ok(());
		};
		let (_, mut stage) = retries
			.remove(&dest)
			.expect("selected queue retry");
		if let Err(error) = self
			.resume_queue(&dest, futures, statuses, &mut stage)
			.await
		{
			defer_queue_error(retries, dest, stage, error)?;
		}
		Ok(())
	}

	#[expect(
		clippy::needless_pass_by_ref_mut,
		reason = "mutable reference avoids requiring SendingFutures to be Sync"
	)]
	fn schedule_events<'a>(
		&'a self,
		dest: Destination,
		events: Vec<SendingEvent>,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
	) {
		if events.is_empty() {
			statuses.remove(&dest);
		} else {
			futures.push(self.send_events(dest, events));
		}
	}

	#[tracing::instrument(name = "request", level = "debug", skip_all)]
	async fn handle_request<'a>(
		&'a self,
		msg: Msg,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
	) -> Result {
		let synthetic_badge =
			msg.queue_id.is_empty() && matches!(&msg.event, SendingEvent::BadgeRefresh);

		let new_events = match (synthetic_badge, statuses.contains_key(&msg.dest)) {
			| (false, _) => vec![(msg.queue_id, msg.event)],
			| (true, true) => Vec::new(),
			| (true, false) =>
				self.db
					.queued_requests(&msg.dest)
					.take(DEQUEUE_LIMIT)
					.try_collect()
					.await?,
		};

		if let Some(events) = self
			.select_events(&msg.dest, new_events, statuses)
			.await?
		{
			self.schedule_events(msg.dest, events, futures, statuses);
		}
		Ok(())
	}

	async fn drain_due_wakes<'a>(
		&'a self,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
		wakes: &mut WakeQueue,
		retries: &mut QueueRetries,
	) -> Result {
		use tokio::time::Instant;

		let now = Instant::now();
		while wakes
			.peek()
			.is_some_and(|Reverse((due, _))| *due <= now)
		{
			let Reverse((_, dest)) = wakes.pop().expect("peeked entry");
			if !retries.contains_key(&dest)
				&& let Err(error) = self
					.handle_wake(dest.clone(), futures, statuses, wakes)
					.await
			{
				defer_queue_error(retries, dest, QueueRecovery::ResumePending, error)?;
			}
		}
		Ok(())
	}

	async fn handle_wake<'a>(
		&'a self,
		dest: Destination,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
		wakes: &mut WakeQueue,
	) -> Result {
		let status = statuses.get(&dest);

		if matches!(
			status,
			Some(TransactionStatus::Running | TransactionStatus::RunningForceRetry)
		) {
			return Ok(());
		}

		if matches!(
			(&dest, status),
			(Destination::Push(..), Some(TransactionStatus::Retrying(_)))
		) {
			trace!(?dest, "Dropping push wake while retry is in flight");
			return Ok(());
		}

		if let (Destination::Push(..), Some(remaining)) =
			(&dest, self.push_backoff_remaining(status))
		{
			if wakes
				.iter()
				.any(|Reverse((_, armed_dest))| armed_dest == &dest)
			{
				trace!(?dest, "Dropping stale push wake");
			} else {
				trace!(?dest, ?remaining, "Re-arming early push wake");
				arm_wake_in(wakes, dest, remaining);
			}

			return Ok(());
		}

		match dest {
			| Destination::Federation(server) => {
				let should_attempt = self
					.services
					.federation
					.should_attempt(&server)
					.await;

				let dest = Destination::Federation(server);

				match should_attempt {
					| ShouldAttempt::No { earliest_retry } =>
						arm_wake(wakes, dest, earliest_retry),
					| _ => {
						let msg = Msg {
							dest,
							event: SendingEvent::Flush,
							queue_id: Vec::new(),
						};

						self.handle_request(msg, futures, statuses)
							.await?;
					},
				}
			},
			| dest @ Destination::Push(..) => {
				self.handle_force_retry(dest, futures, statuses)
					.await?;
			},
			| Destination::Appservice(_) => {},
		}
		Ok(())
	}

	#[tracing::instrument(
		name = "finish",
		level = "info",
		skip_all,
		fields(futures = %futures.len()),
	)]
	async fn finish_responses<'a>(&'a self, futures: &mut SendingFutures<'a>) -> Result {
		use tokio::{
			select,
			time::{Instant, sleep_until},
		};

		let timeout = self.server.config.sender_shutdown_timeout;
		let timeout = Duration::from_secs(timeout);
		let now = Instant::now();
		let deadline = now.checked_add(timeout).unwrap_or(now);
		loop {
			trace!("Waiting for {} requests to complete...", futures.len());
			select! {
				() = sleep_until(deadline) => return Ok(()),
				response = futures.next() => match response {
					Some(Ok(dest)) => self.db.delete_all_active_requests_for(&dest).await?,
					Some(_) => {},
					None => return Ok(()),
				},
			}
		}
	}

	#[tracing::instrument(
		name = "netburst",
		level = "debug",
		skip_all,
		fields(futures = %futures.len()),
	)]
	async fn startup_netburst<'a>(
		&'a self,
		id: usize,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
	) -> Result {
		let keep =
			usize::try_from(self.server.config.startup_netburst_keep).unwrap_or(usize::MAX);

		let mut txns = HashMap::<Destination, Vec<SendingEvent>>::new();
		let active = self.db.active_requests();

		pin_mut!(active);
		while let Some((key, event, dest)) = active.try_next().await? {
			if self.shard_id(&dest) != id {
				continue;
			}

			let entry = txns.entry(dest.clone()).or_default();
			if self.server.config.startup_netburst_keep >= 0 && entry.len() >= keep {
				warn!("Dropping unsent event {dest:?} {:?}", String::from_utf8_lossy(&key));
				self.db.delete_active_request(&key).await?;
			} else {
				entry.push(event);
			}
		}

		for (dest, events) in txns {
			if self.server.config.startup_netburst && !events.is_empty() {
				statuses.insert(dest.clone(), TransactionStatus::Running);
				futures.push(self.send_events(dest.clone(), events));
			}
		}

		// Active transaction generations must own their queued successors before
		// queued-only badge destinations are woken.
		if !self.server.config.startup_netburst || keep == 0 {
			return Ok(());
		}

		let mut destinations = self
			.db
			.queued_badge_refresh_destinations()
			.try_filter(|dest| futures::future::ready(self.shard_id(dest) == id))
			.try_collect::<HashSet<_>>()
			.await?;
		destinations.extend(
			self.db
				.pending_edu_destinations(self.services.globals.current_count())
				.try_filter(|dest| futures::future::ready(self.shard_id(dest) == id))
				.try_collect::<Vec<_>>()
				.await?,
		);

		for dest in destinations {
			let event = match &dest {
				| Destination::Federation(_) => SendingEvent::Flush,
				| _ => SendingEvent::BadgeRefresh,
			};
			let msg = Msg { dest, event, queue_id: Vec::new() };

			self.handle_request(msg, futures, statuses)
				.await?;
		}
		Ok(())
	}

	#[tracing::instrument(
		name = "select",
		level = "debug",
		skip_all,
		fields(
			?dest,
			new_events = %new_events.len(),
		),
	)]
	async fn select_events(
		&self,
		dest: &Destination,
		new_events: Vec<QueueItem>, // Events we want to send: event and full key
		statuses: &mut CurTransactionStatus,
	) -> Result<Option<Vec<SendingEvent>>> {
		let retry_action = if matches!(dest, Destination::Appservice(_))
			&& new_events
				.iter()
				.any(|(_, event)| matches!(event, SendingEvent::Flush))
		{
			RetryAction::Force
		} else {
			RetryAction::None
		};

		let (allow, retry) = self
			.select_events_current(dest, statuses, retry_action)
			.await?;

		// Nothing can be done for this remote, bail out.
		if !allow {
			return Ok(None);
		}

		let mut events = Vec::new();

		// Must retry any previous transaction for this remote.
		if retry {
			let active = self
				.db
				.active_requests_for(dest)
				.try_collect::<Vec<_>>()
				.await?;
			events.extend(active.into_iter().map(|(_, event)| event));

			return Ok(Some(events));
		}

		// Compose the next transaction
		let _cork = self.db.db.cork();
		let queued = self.db.retain_queued(new_events);
		futures::pin_mut!(queued);
		while let Some(item) = queued.try_next().await? {
			{
				self.db.mark_as_active(once(&item)).await?;
				if !matches!(&item.1, SendingEvent::Flush) {
					events.push(item.1);
				}
			}
		}

		// Add EDU's into the transaction
		if let Destination::Federation(server_name) = dest {
			let budget_used = events
				.iter()
				.filter(|event| matches!(event, SendingEvent::Edu(_)))
				.count();

			let selected = self.select_edus(server_name, budget_used).await?;
			events.extend(selected.into_iter().map(SendingEvent::Edu));
		}

		Ok(Some(events))
	}

	async fn select_events_current(
		&self,
		dest: &Destination,
		statuses: &mut CurTransactionStatus,
		retry_action: RetryAction,
	) -> Result<(bool, bool)> {
		// peer_status gates federation only; appservice and push fall through.
		if let Destination::Federation(server) = dest {
			let should_attempt = self
				.services
				.federation
				.should_attempt(server)
				.await;

			if matches!(should_attempt, ShouldAttempt::No { .. }) {
				return Ok((false, false));
			}
		}

		let (mut allow, mut retry) = (true, false);
		statuses
			.entry(dest.clone())
			.and_modify(|e| match e {
				| TransactionStatus::Running | TransactionStatus::RunningForceRetry => {
					allow = false; // already running
					if matches!(retry_action, RetryAction::Force) {
						*e = TransactionStatus::RunningForceRetry;
					}
				},
				| TransactionStatus::Failed(tries, time) => {
					// Push backoff: hold off until the exponential window elapses.
					let min = self.server.config.sender_timeout;
					let max = self.server.config.sender_retry_backoff_limit;
					let remaining =
						exponential_backoff_remaining_secs(min, max, time.elapsed(), *tries);

					trace!(
						?dest,
						tries = *tries,
						?remaining,
						"Push destination remains in backoff",
					);

					if remaining.is_some() {
						allow = false;
					} else {
						retry = true;
						*e = TransactionStatus::Retrying(*tries);
					}
				},
				| TransactionStatus::Retrying(_) if matches!(dest, Destination::Push(..)) => {
					allow = false; // push retry already in flight
				},
				| TransactionStatus::Retrying(_) => {
					// Promote to Running so a concurrent select does not double-send.
					retry = true;
					*e = TransactionStatus::Running;
				},
			})
			.or_insert(TransactionStatus::Running);

		Ok((allow, retry))
	}

	#[tracing::instrument(name = "edus", level = "debug", skip_all)]
	async fn select_edus(&self, server_name: &ServerName, budget_used: usize) -> Result<EduVec> {
		// selection window
		let since = self.db.get_latest_educount(server_name).await?;
		let since_upper = edu_window_end(since, self.services.globals.current_count())?;

		// Nothing new since the last window: skip the scan and the watermark.
		if since == since_upper || budget_used >= EDU_LIMIT {
			return Ok(EduVec::new());
		}

		let batch = (since, since_upper);
		debug_assert!(batch.0 <= batch.1, "since range must not be negative");

		// Reserve presence's one EDU before the durable selectors share their
		// budget, so advancing a complete window never drops it for lack of a slot.
		let outgoing_presence = self.server.config.allow_outgoing_presence;
		let outgoing_receipts = self.server.config.allow_outgoing_read_receipts;
		let events_len =
			AtomicUsize::new(budget_used.saturating_add(usize::from(outgoing_presence)));
		let device_changes = self.select_edus_device_changes(server_name, batch, &events_len);

		let receipts = outgoing_receipts
			.then_async(|| self.select_edus_receipts(server_name, batch, &events_len));

		let presence =
			outgoing_presence.then_async(|| self.select_edus_presence(server_name, batch));

		let (device_changes, receipts, presence) =
			join3(device_changes, receipts, presence).await;

		// All selected sources must complete before persisting any selected row
		// or watermark. A missing optional source is not a failed source.
		let device_changes = device_changes?;
		let receipts = receipts.transpose()?.unwrap_or_default();
		let presence = presence.transpose()?.flatten();
		let mut events = device_changes.shipped;

		events.extend(receipts.shipped);

		// Presence rides last and is excluded from the durable prefix because
		// its content is compose-time-relative and regenerates fresh.
		let durable_len = events.len();

		events.extend(presence);
		debug_assert!(
			budget_used.saturating_add(events.len()) <= EDU_LIMIT,
			"exceeded edus limit"
		);

		// EDUs past the budget become queued rows drained by later transactions.
		let overflow: Vec<EduBuf> = device_changes
			.overflow
			.into_iter()
			.chain(receipts.overflow)
			.collect();

		self.db
			.persist_edus(server_name, &events[..durable_len], &overflow, since_upper)
			.await?;

		// Also continue empty windows: global counters include unrelated writes
		// and queue identifiers. The persisted watermark reconstructs this wake
		// at startup if the process stops before it can run.
		if since_upper < self.services.globals.current_count() {
			self.dispatch(Msg {
				dest: Destination::Federation(server_name.to_owned()),
				event: SendingEvent::Flush,
				queue_id: Vec::new(),
			})?;
		}

		Ok(events)
	}

	/// Look for device changes
	#[tracing::instrument(
		name = "device_changes",
		level = "trace",
		skip(self, server_name, events_len)
	)]
	async fn select_edus_device_changes(
		&self,
		server_name: &ServerName,
		since: (u64, u64),
		events_len: &AtomicUsize,
	) -> Result<Selected> {
		let mut selected = Selected::default();
		let server_rooms = self
			.services
			.state_cache
			.server_rooms_fallible(server_name);

		pin_mut!(server_rooms);
		let mut device_list_changes = HashSet::<OwnedUserId>::new();
		while let Some(room_id) = server_rooms.try_next().await? {
			let keys_changed =
				self.services
					.users
					.room_keys_changed_fallible(room_id, since.0, Some(since.1));

			pin_mut!(keys_changed);
			while let Some((user_id, count)) = keys_changed.try_next().await? {
				debug_assert!(count <= since.1, "exceeds upper-bound");

				if !self.services.globals.user_is_local(user_id) {
					continue;
				}
				if !device_list_changes.insert(user_id.into()) {
					continue;
				}
				if device_list_changes.len() > SELECT_DEVICE_CHANGE_LIMIT {
					return Err(Error::bad_database(
						"Device changes exceed the bounded counter window",
					));
				}

				// Empty prev id forces synapse to resync; because synapse resyncs,
				// we can just insert placeholder data
				let edu = Edu::DeviceListUpdate(DeviceListUpdateContent {
					user_id: user_id.into(),
					device_id: device_id!("placeholder").to_owned(),
					device_display_name: Some("Placeholder".to_owned()),
					stream_id: uint!(1),
					prev_id: Vec::new(),
					deleted: None,
					keys: None,
				});

				let mut buf = EduBuf::new();
				serde_json::to_writer(&mut buf, &edu)
					.expect("failed to serialize device list update to JSON");

				// Past the budget these rows overflow to the queue; replay is
				// benign because the placeholder content is user-id-only.
				if !selected.overflow.is_empty()
					|| events_len.fetch_add(1, Ordering::Relaxed) >= EDU_LIMIT
				{
					selected.overflow.push(buf);
				} else {
					selected.shipped.push(buf);
				}
			}
		}

		Ok(selected)
	}

	/// Look for read receipts in this room
	///
	/// MSC3771 lets a user emit multiple receipts in the same EDU window, one
	/// per thread context. The federation EDU shape allows only one
	/// `ReceiptData` per `(room, user)` slot, so a user with N parallel
	/// thread receipts ships across N parallel `Edu::Receipt` buffers within
	/// the same transaction. Each buffer is shape-compliant; receivers
	/// process them as independent receipt EDUs and our storage keeps each
	/// thread distinct.
	#[tracing::instrument(
		name = "receipts",
		level = "trace",
		skip(self, server_name, events_len)
	)]
	async fn select_edus_receipts(
		&self,
		server_name: &ServerName,
		since: (u64, u64),
		events_len: &AtomicUsize,
	) -> Result<Selected> {
		let num = AtomicUsize::new(0);
		let num = &num;
		let by_room: RoomReceipts = self
			.services
			.state_cache
			.server_rooms_fallible(server_name)
			.map_ok(ToOwned::to_owned)
			.map(|room_id| async move {
				let room_id = room_id?;
				let ranked = self
					.select_edus_receipts_room(&room_id, since, num)
					.await?;
				Ok::<_, Error>((room_id, ranked))
			})
			.buffer_unordered(EDU_ROOM_READ_CONCURRENCY)
			.try_collect()
			.boxed()
			.await?;

		let max_rank = by_room
			.iter()
			.map(|(_, maps)| maps.len())
			.max()
			.unwrap_or(0);

		let pivot_rank = |rank: usize| -> Option<BTreeMap<OwnedRoomId, ReceiptMap>> {
			let receipts: BTreeMap<_, _> = by_room
				.iter()
				.filter_map(|(room_id, maps)| {
					maps.get(rank)
						.cloned()
						.map(|map| (room_id.clone(), map))
				})
				.collect();

			receipts.is_empty().is_false().then_some(receipts)
		};

		let serialize_edu = |receipts: BTreeMap<OwnedRoomId, ReceiptMap>| -> EduBuf {
			let mut buf = EduBuf::new();
			serde_json::to_writer(&mut buf, &Edu::Receipt(ReceiptContent { receipts }))
				.expect("Failed to serialize Receipt EDU to JSON vec");

			buf
		};

		// Ranks reserve from the shared budget in order; those past the cap
		// overflow to the queue instead of truncating the tail.
		let mut selected = Selected::default();
		for receipts in (0..max_rank).filter_map(pivot_rank) {
			if !selected.overflow.is_empty()
				|| events_len.fetch_add(1, Ordering::Relaxed) >= EDU_LIMIT
			{
				selected.overflow.push(serialize_edu(receipts));
			} else {
				selected.shipped.push(serialize_edu(receipts));
			}
		}

		Ok(selected)
	}

	/// Look for read receipts in this room.
	///
	/// Returns a per-rank vector of [`ReceiptMap`]s. Each user's receipts in
	/// the window (one per thread context, count-ordered) are placed into
	/// successive ranks, so rank 0 carries each user's earliest receipt,
	/// rank 1 the next, and so on. The receipt-limit budget bounds distinct
	/// users only; subsequent thread receipts for an already-counted user do
	/// not consume additional budget.
	#[tracing::instrument(name = "receipts", level = "trace", skip(self, since))]
	async fn select_edus_receipts_room(
		&self,
		room_id: &RoomId,
		since: (u64, u64),
		num: &AtomicUsize,
	) -> Result<RankedReceipts> {
		let receipts = self
			.services
			.read_receipt
			.readreceipts_since_fallible(room_id, since.0, Some(since.1));

		pin_mut!(receipts);
		let mut by_user = BTreeMap::<OwnedUserId, UserReceipts>::new();
		while let Some((user_id, count, read_receipt)) = receipts.try_next().await? {
			debug_assert!(count <= since.1, "exceeds upper-bound");

			if !self.services.globals.user_is_local(user_id) {
				continue;
			}

			let event = serde_json::from_str(read_receipt.json().get())
				.map_err(|_| Error::bad_database("Invalid stored receipt JSON"))?;

			let AnySyncEphemeralRoomEvent::Receipt(r) = event else {
				return Err(Error::bad_database("Invalid stored receipt event type"));
			};

			let (event_id, mut receipt) = r
				.content
				.0
				.into_iter()
				.next()
				.ok_or_else(|| Error::bad_database("Stored receipt has no event"))?;

			let receipt = receipt
				.remove(&ReceiptType::Read)
				.ok_or_else(|| Error::bad_database("Stored receipt is missing read type"))?
				.remove(user_id)
				.ok_or_else(|| Error::bad_database("Stored receipt is missing its owner"))?;

			let receipt_data = ReceiptData { data: receipt, event_ids: vec![event_id] };

			match by_user.entry(user_id.to_owned()) {
				| Entry::Vacant(slot) => {
					slot.insert(SmallVec::from_buf([receipt_data]));
					let num = num.fetch_add(1, Ordering::Relaxed);
					if num >= SELECT_RECEIPT_LIMIT {
						return Err(Error::bad_database(
							"Receipts exceed the bounded counter window",
						));
					}
				},
				| Entry::Occupied(mut slot) => {
					slot.get_mut().push(receipt_data);
				},
			}
		}

		// Pivot per-user count-ordered receipts into rank-major
		// `RankedReceipts`. Rank 0 carries each user's earliest receipt in
		// the window, rank 1 the next, and so on.
		Ok(by_user
			.into_iter()
			.fold(RankedReceipts::new(), |mut acc, (user_id, receipts)| {
				for (rank, receipt_data) in receipts.into_iter().enumerate() {
					if rank >= acc.len() {
						acc.push(ReceiptMap { read: BTreeMap::new() });
					}

					acc[rank]
						.read
						.insert(user_id.clone(), receipt_data);
				}

				acc
			}))
	}

	/// Look for presence
	#[tracing::instrument(name = "presence", level = "trace", skip(self, server_name))]
	async fn select_edus_presence(
		&self,
		server_name: &ServerName,
		since: (u64, u64),
	) -> Result<Option<EduBuf>> {
		let presence_since = self
			.services
			.presence
			.presence_since_fallible(since.0, Some(since.1));

		pin_mut!(presence_since);
		let mut presence_updates = HashMap::<OwnedUserId, PresenceUpdate>::new();
		while let Some((user_id, count, presence_bytes)) = presence_since.try_next().await? {
			debug_assert!(count <= since.1, "exceeded upper-bound");

			if !self.services.globals.user_is_local(user_id) {
				continue;
			}

			if !self
				.services
				.state_cache
				.server_sees_user_fallible(server_name, user_id)
				.await?
			{
				continue;
			}

			let presence_event = self
				.services
				.presence
				.from_json_bytes_to_event(presence_bytes, user_id)
				.await?;

			let update = PresenceUpdate {
				user_id: user_id.into(),
				presence: presence_event.content.presence,
				currently_active: presence_event
					.content
					.currently_active
					.unwrap_or(false),
				status_msg: presence_event.content.status_msg,
				last_active_ago: presence_event
					.content
					.last_active_ago
					.unwrap_or_else(|| uint!(0)),
			};

			presence_updates.insert(user_id.into(), update);
			if presence_updates.len() > SELECT_PRESENCE_LIMIT {
				return Err(Error::bad_database("Presence exceeds the bounded counter window"));
			}
		}

		if presence_updates.is_empty() {
			return Ok(None);
		}

		let presence_content = Edu::Presence(PresenceContent {
			push: presence_updates.into_values().collect(),
		});

		let mut buf = EduBuf::new();
		serde_json::to_writer(&mut buf, &presence_content)
			.expect("failed to serialize Presence EDU to JSON");

		Ok(Some(buf))
	}

	fn send_events(&self, dest: Destination, events: Vec<SendingEvent>) -> SendingFuture<'_> {
		debug_assert!(!events.is_empty(), "sending empty transaction");
		match dest {
			| Destination::Federation(server) => self
				.send_events_dest_federation(server, events)
				.boxed(),
			| Destination::Appservice(id) => self
				.send_events_dest_appservice(id, events)
				.boxed(),
			| Destination::Push(user_id, pushkey) => self
				.send_events_dest_push(user_id, pushkey, events)
				.boxed(),
		}
	}

	#[tracing::instrument(
		name = "appservice",
		level = "debug",
		skip(self, events),
		fields(
			events = %events.len(),
		),
	)]
	async fn send_events_dest_appservice(
		&self,
		id: String,
		events: Vec<SendingEvent>,
	) -> SendingResult {
		let Some(info) = self
			.services
			.appservice
			.get_registration_info(&id)
			.await
		else {
			//TODO: appservice queue cleanup.
			return Err((
				Destination::Appservice(id.clone()),
				err!(Database(debug_warn!(?id, "Missing appservice registration"))),
			));
		};

		let msc3202 = info.registration.msc3202_transaction_extensions;

		let (pdu_count, edu_count, to_device_count, device_list_count) = events.iter().fold(
			(0_usize, 0_usize, 0_usize, 0_usize),
			|(pdus, edus, to_device, device_list), event| match event {
				| SendingEvent::Pdu(_) => (pdus.saturating_add(1), edus, to_device, device_list),
				| SendingEvent::Edu(_) => (pdus, edus.saturating_add(1), to_device, device_list),
				| SendingEvent::ToDevice(_) =>
					(pdus, edus, to_device.saturating_add(1), device_list),
				| SendingEvent::DeviceListChanged(_) =>
					(pdus, edus, to_device, device_list.saturating_add(1)),
				| SendingEvent::BadgeRefresh | SendingEvent::Flush =>
					(pdus, edus, to_device, device_list),
			},
		);

		let mut pdu_jsons = Vec::with_capacity(pdu_count);
		let mut edu_jsons: Vec<Raw<EphemeralData>> = Vec::with_capacity(edu_count);
		let mut to_device = Vec::with_capacity(to_device_count);
		let mut changed = Vec::with_capacity(device_list_count);

		// MSC3202 one-time-key scope: the appservice sender plus (below) the
		// namespace-matched PDU senders and to-device recipients of this txn.
		let mut otk_users = BTreeSet::new();
		let mut otk_recipients = BTreeSet::new();
		if msc3202 {
			otk_users.insert(info.sender.clone());
		}

		for event in &events {
			match event {
				| SendingEvent::Pdu(pdu_id) => {
					if let Ok(pdu) = self
						.services
						.timeline
						.get_pdu_from_id(pdu_id)
						.await
					{
						if msc3202 && info.is_user_match(pdu.sender()) {
							otk_users.insert(pdu.sender().to_owned());
						}

						pdu_jsons.push(pdu.to_format());
					}
				},
				| SendingEvent::Edu(edu) => {
					if info.registration.receive_ephemeral
						&& let Ok(edu) =
							serde_json::from_slice(edu).and_then(|edu| Raw::new(&edu))
					{
						edu_jsons.push(edu);
					}
				},
				| SendingEvent::ToDevice(buf) => {
					let Some(bytes) = buf.get(TAG_PREFIX_LEN..) else {
						debug_warn!("skipping malformed queued to-device event");
						continue;
					};

					if msc3202
						&& let Ok(recipient) = serde_json::from_slice::<ToDeviceRecipient>(bytes)
					{
						otk_recipients.insert((recipient.to_user_id, recipient.to_device_id));
					}

					if let Ok(raw) = serde_json::from_slice(bytes) {
						to_device.push(raw);
					} else {
						debug_warn!("skipping malformed queued to-device event");
					}
				},
				| SendingEvent::DeviceListChanged(buf) => {
					if msc3202
						&& let Some(bytes) = buf.get(TAG_PREFIX_LEN..)
						&& let Ok(user) = from_utf8(bytes)
						&& let Ok(user_id) = UserId::parse(user)
					{
						changed.push(user_id);
					}
				},
				| SendingEvent::BadgeRefresh | SendingEvent::Flush => {},
			}
		}

		let txn_hash = calculate_hash(events.iter().filter_map(|e| match e {
			| SendingEvent::Edu(b)
			| SendingEvent::ToDevice(b)
			| SendingEvent::DeviceListChanged(b) => Some(b.as_ref()),
			| SendingEvent::Pdu(b) => Some(b.as_ref()),
			| SendingEvent::BadgeRefresh | SendingEvent::Flush => None,
		}));

		let txn_id = &*URL_SAFE_NO_PAD.encode(txn_hash);

		let (device_lists, device_one_time_keys_count, device_unused_fallback_key_types) =
			if msc3202 {
				changed.sort_unstable();
				changed.dedup();

				let (counts, fallbacks) = self
					.msc3202_key_counts(otk_users, otk_recipients)
					.await;

				(DeviceLists { changed, left: Vec::new() }, counts, fallbacks)
			} else {
				(DeviceLists::new(), OtkCounts::new(), FallbackTypes::new())
			};

		if pdu_jsons.is_empty()
			&& edu_jsons.is_empty()
			&& to_device.is_empty()
			&& device_lists.is_empty()
			&& device_one_time_keys_count.is_empty()
			&& device_unused_fallback_key_types.is_empty()
		{
			return Ok(Destination::Appservice(id));
		}

		match self
			.services
			.appservice
			.send_request(info.registration, PushEventsRequest {
				txn_id: txn_id.into(),
				events: pdu_jsons,
				ephemeral: edu_jsons,
				to_device,
				device_lists,
				device_one_time_keys_count,
				device_unused_fallback_key_types,
			})
			.await
		{
			| Ok(_) => Ok(Destination::Appservice(id)),
			| Err(e) => Err((Destination::Appservice(id), e)),
		}
	}

	/// MSC3202 one-time-key counts and unused fallback key types over every
	/// device of `users` plus the specific `recipients`. Recomputed per build
	/// rather than snapshotted, so a retry ships fresh counts.
	async fn msc3202_key_counts(
		&self,
		users: BTreeSet<OwnedUserId>,
		recipients: BTreeSet<(OwnedUserId, OwnedDeviceId)>,
	) -> (OtkCounts, FallbackTypes) {
		let mut devices: Devices = users
			.into_iter()
			.stream()
			.broad_then(async |user_id: OwnedUserId| {
				self.services
					.users
					.all_device_ids(&user_id)
					.map(|device_id| (user_id.clone(), device_id.to_owned()))
					.collect()
					.await
			})
			.flat_map(|pairs: Vec<(OwnedUserId, OwnedDeviceId)>| pairs.into_iter().stream())
			.chain(recipients.into_iter().stream())
			.collect()
			.await;

		devices.sort_unstable();
		devices.dedup();

		devices
			.into_iter()
			.stream()
			.broad_then(async |(user_id, device_id): (OwnedUserId, OwnedDeviceId)| {
				let counts = self
					.services
					.users
					.count_one_time_keys(&user_id, &device_id);

				let fallbacks = self
					.services
					.users
					.unused_fallback_key_algorithms(&user_id, &device_id)
					.collect();

				let (counts, fallbacks) = join(counts, fallbacks).await;

				(user_id, device_id, counts, fallbacks)
			})
			.ready_fold(
				(OtkCounts::new(), FallbackTypes::new()),
				|(mut counts, mut fallbacks), (user_id, device_id, otk, fallback)| {
					counts
						.entry(user_id.clone())
						.or_default()
						.insert(device_id.clone(), otk);

					fallbacks
						.entry(user_id)
						.or_default()
						.insert(device_id, fallback);

					(counts, fallbacks)
				},
			)
			.await
	}

	#[tracing::instrument(
		name = "push",
		level = "info",
		skip(self, events),
		fields(
			events = %events.len(),
		),
	)]
	async fn send_events_dest_push(
		&self,
		user_id: OwnedUserId,
		pushkey: String,
		events: Vec<SendingEvent>,
	) -> SendingResult {
		let has_pdu = events
			.iter()
			.any(|event| matches!(event, SendingEvent::Pdu(_)));

		let destination = || Destination::Push(user_id.clone(), pushkey.clone());
		let suppressed = self.pushing_suppressed(&user_id).map(Ok);
		let pusher = self
			.services
			.pusher
			.get_pusher(&user_id, &pushkey)
			.map(|result| match result {
				| Ok(pusher) => Ok(Some(pusher)),
				| Err(error) if error.is_not_found() => {
					error!(%user_id, %pushkey, "Pusher disappeared before delivery");

					Ok(None)
				},
				| Err(error) => Err((destination(), error)),
			});

		let rules_for_user = has_pdu
			.then_async(async || {
				self.services
					.account_data
					.get_global::<PushRulesEvent>(&user_id, GlobalAccountDataEventType::PushRules)
					.await
					.map_or_else(|_| Ruleset::server_default(&user_id), |ev| ev.content.global)
			})
			.map(Ok);

		let (pusher, rules_for_user, suppressed) =
			try_join3(pusher, rules_for_user, suppressed).await?;

		let Some(pusher) = pusher else {
			return Ok(Destination::Push(user_id, pushkey));
		};

		// Reconciliation, not an alert: a suppressed drop strands a stale badge.
		if events.contains(&SendingEvent::BadgeRefresh) {
			let result = self
				.services
				.pusher
				.send_badge_notice(&user_id, &pusher)
				.await;

			match result {
				| Ok(()) => (),
				| Err(error) if is_permanent_push_error(&error) => warn!(
					%user_id,
					%pushkey,
					chain = %error_chain(&error),
					"Dropping a badge push with a permanent local error",
				),
				| Err(error) => return Err((destination(), error)),
			}
		}

		if suppressed {
			let queued = self
				.enqueue_suppressed_push_events(&user_id, &pushkey, &events)
				.await;

			debug!(
				?user_id,
				pushkey,
				queued,
				events = events.len(),
				"Push suppressed; queued events"
			);
			return Ok(Destination::Push(user_id, pushkey));
		}

		self.schedule_flush_suppressed_for_pushkey(
			user_id.clone(),
			pushkey.clone(),
			"non-suppressed push",
		);

		let failures = match rules_for_user {
			| None => PushFailures::default(),
			| Some(rules_for_user) =>
				events
					.iter()
					.stream()
					.ready_filter_map(|event| extract_variant!(event, SendingEvent::Pdu))
					.wide_filter_map(async |pdu_id| {
						self.services
							.timeline
							.get_pdu_from_id(pdu_id)
							.map_ok(|pdu| (*pdu_id, pdu))
							.await
							.ok()
					})
					.ready_filter(|(_, pdu)| !pdu.is_redacted())
					.wide_then(async |(pdu_id, pdu)| {
						let result = self
							.services
							.pusher
							.send_push_notice(&user_id, &pusher, &rules_for_user, &pdu)
							.await;

						(pdu_id, result)
					})
					.ready_fold(
						PushFailures::default(),
						|failures, (pdu_id, result)| match result {
							| Ok(()) => failures,
							| Err(error) if is_permanent_push_error(&error) => {
								warn!(
									%user_id,
									%pushkey,
									?pdu_id,
									chain = %error_chain(&error),
									"Dropping a push with a permanent local error",
								);

								failures
							},
							| Err(error) => failures.retain(pdu_id, error),
						},
					)
					.await,
		};

		let PushFailures { ids, error: Some(error) } = failures else {
			return Ok(Destination::Push(user_id, pushkey));
		};

		let dest = Destination::Push(user_id, pushkey);

		for pdu_id in events
			.iter()
			.filter_map(|event| extract_variant!(event, SendingEvent::Pdu))
			.filter(|pdu_id| !ids.contains(*pdu_id))
		{
			self.db
				.delete_active_request(&dest.event_key(pdu_id))
				.await
				.expect("database remove error");
		}

		Err((dest, error))
	}
}

#[inline]
fn is_permanent_push_error(error: &Error) -> bool {
	matches!(error, Error::Request(ErrorKind::InvalidParam, ..))
}

impl Service {
	/// Schedule a flush of the pushes suppressed for one pushkey.
	///
	/// The flush runs as a task this service owns, so the caller never waits on
	/// the push gateway.
	pub fn schedule_flush_suppressed_for_pushkey(
		&self,
		user_id: OwnedUserId,
		pushkey: String,
		reason: &'static str,
	) {
		let sending = self.services.sending.clone();

		self.spawn_flush(async move {
			sending
				.flush_suppressed_for_pushkey(user_id, pushkey, reason)
				.await;
		});
	}

	/// Schedule a flush of the pushes suppressed for every pushkey a user owns.
	///
	/// The flush runs as a task this service owns, so the caller never waits on
	/// the push gateway.
	pub fn schedule_flush_suppressed_for_user(&self, user_id: OwnedUserId, reason: &'static str) {
		let sending = self.services.sending.clone();

		self.spawn_flush(async move {
			sending
				.flush_suppressed_for_user(user_id, reason)
				.await;
		});
	}

	fn spawn_flush<F>(&self, flush: F)
	where
		F: Future<Output = ()> + Send + 'static,
	{
		// A flush scheduled during shutdown is dropped, not spawned.
		if !self.server.is_running() {
			return;
		}

		let mut flushes = self.flushes.lock().expect("locked");

		reap_flushes(&mut flushes);
		let _abort = flushes.spawn_on(flush, self.server.runtime());
	}

	async fn enqueue_suppressed_push_events(
		&self,
		user_id: &UserId,
		pushkey: &str,
		events: &[SendingEvent],
	) -> usize {
		let mut queued = 0_usize;
		for event in events {
			let SendingEvent::Pdu(pdu_id) = event else {
				continue;
			};

			let Ok(pdu) = self
				.services
				.timeline
				.get_pdu_from_id(pdu_id)
				.await
			else {
				debug!(?user_id, ?pdu_id, "Suppressing push but PDU is missing");
				continue;
			};

			if pdu.is_redacted() {
				trace!(?user_id, ?pdu_id, "Suppressing push for redacted PDU");
				continue;
			}

			if self.services.pusher.queue_suppressed_push(
				user_id,
				pushkey,
				pdu.room_id(),
				*pdu_id,
			) {
				queued = queued.saturating_add(1);
			}
		}

		queued
	}

	async fn flush_suppressed_rooms(
		&self,
		user_id: &UserId,
		pushkey: &str,
		pusher: &Pusher,
		rules_for_user: &Ruleset,
		rooms: Vec<(OwnedRoomId, Vec<RawPduId>)>,
		reason: &'static str,
	) {
		if rooms.is_empty() {
			return;
		}

		let mut sent = 0_usize;
		debug!(?user_id, pushkey, rooms = rooms.len(), "Flushing suppressed pushes ({reason})");

		for (room_id, pdu_ids) in rooms {
			let unread = self
				.services
				.pusher
				.notification_count(user_id, &room_id)
				.await;

			if unread == 0 {
				trace!(?user_id, ?room_id, "Skipping suppressed push flush: no unread");
				continue;
			}

			for pdu_id in pdu_ids {
				let Ok(pdu) = self
					.services
					.timeline
					.get_pdu_from_id(&pdu_id)
					.await
				else {
					debug!(?user_id, ?pdu_id, "Suppressed PDU missing during flush");
					continue;
				};

				if pdu.is_redacted() {
					trace!(?user_id, ?pdu_id, "Suppressed PDU redacted during flush");
					continue;
				}

				if let Err(error) = self
					.services
					.pusher
					.send_push_notice(user_id, pusher, rules_for_user, &pdu)
					.await
				{
					let requeued = self
						.services
						.pusher
						.queue_suppressed_push(user_id, pushkey, &room_id, pdu_id);

					warn!(
						?user_id,
						?room_id,
						?error,
						requeued,
						"Failed to send suppressed push notification"
					);
				} else {
					sent = sent.saturating_add(1);
				}
			}
		}

		debug!(?user_id, pushkey, sent, "Flushed suppressed push notifications");
	}

	async fn flush_suppressed_for_pushkey(
		&self,
		user_id: OwnedUserId,
		pushkey: String,
		reason: &'static str,
	) {
		let suppressed = self
			.services
			.pusher
			.take_suppressed_for_pushkey(&user_id, &pushkey);

		if suppressed.is_empty() {
			return;
		}

		let pusher = match self
			.services
			.pusher
			.get_pusher(&user_id, &pushkey)
			.await
		{
			| Ok(pusher) => pusher,
			| Err(error) => {
				warn!(?user_id, pushkey, ?error, "Missing pusher for suppressed flush");
				return;
			},
		};

		let rules_for_user = match self
			.services
			.account_data
			.get_global::<PushRulesEvent>(&user_id, GlobalAccountDataEventType::PushRules)
			.await
		{
			| Ok(ev) => ev.content.global,
			| Err(_) => Ruleset::server_default(&user_id),
		};

		self.flush_suppressed_rooms(
			&user_id,
			&pushkey,
			&pusher,
			&rules_for_user,
			suppressed,
			reason,
		)
		.await;
	}

	pub async fn flush_suppressed_for_user(&self, user_id: OwnedUserId, reason: &'static str) {
		let suppressed = self
			.services
			.pusher
			.take_suppressed_for_user(&user_id);

		if suppressed.is_empty() {
			return;
		}

		let rules_for_user = match self
			.services
			.account_data
			.get_global::<PushRulesEvent>(&user_id, GlobalAccountDataEventType::PushRules)
			.await
		{
			| Ok(ev) => ev.content.global,
			| Err(_) => Ruleset::server_default(&user_id),
		};

		for (pushkey, rooms) in suppressed {
			let pusher = match self
				.services
				.pusher
				.get_pusher(&user_id, &pushkey)
				.await
			{
				| Ok(pusher) => pusher,
				| Err(error) => {
					warn!(?user_id, pushkey, ?error, "Missing pusher for suppressed flush");
					continue;
				},
			};

			self.flush_suppressed_rooms(
				&user_id,
				&pushkey,
				&pusher,
				&rules_for_user,
				rooms,
				reason,
			)
			.await;
		}
	}

	// optional suppression: heuristic combining presence age and recent sync
	// activity.
	async fn pushing_suppressed(&self, user_id: &UserId) -> bool {
		if !self.services.config.suppress_push_when_active {
			debug!(?user_id, "push not suppressed: suppress_push_when_active disabled");
			return false;
		}

		let Ok(presence) = self.services.presence.get_presence(user_id).await else {
			debug!(?user_id, "push not suppressed: presence unavailable");
			return false;
		};

		if presence.content.presence != PresenceState::Online {
			debug!(
				?user_id,
				presence = ?presence.content.presence,
				"push not suppressed: presence not online"
			);
			return false;
		}

		let presence_age_ms = presence
			.content
			.last_active_ago
			.map(u64::from)
			.unwrap_or(u64::MAX);

		if presence_age_ms >= 65_000 {
			debug!(?user_id, presence_age_ms, "push not suppressed: presence too old");
			return false;
		}

		let sync_gap_ms = self
			.services
			.presence
			.last_sync_gap_ms(user_id)
			.await;

		let considered_active = sync_gap_ms.is_some_and(|gap| gap < 32_000);

		match sync_gap_ms {
			| Some(gap) if gap < 32_000 => debug!(
				?user_id,
				presence_age_ms,
				sync_gap_ms = gap,
				"suppressing push: active heuristic"
			),
			| Some(gap) => debug!(
				?user_id,
				presence_age_ms,
				sync_gap_ms = gap,
				"push not suppressed: sync gap too large"
			),
			| None => debug!(?user_id, presence_age_ms, "push not suppressed: no recent sync"),
		}

		considered_active
	}

	async fn send_events_dest_federation(
		&self,
		server: OwnedServerName,
		events: Vec<SendingEvent>,
	) -> SendingResult {
		let pdus: Vec<_> = events
			.iter()
			.filter_map(|event| extract_variant!(event, SendingEvent::Pdu))
			.stream()
			.wide_filter_map(|pdu_id| {
				self.services
					.timeline
					.get_pdu_json_from_id(pdu_id)
					.ok()
			})
			.wide_then(|pdu| {
				self.services
					.state_accessor
					.erased_for_server(&server, pdu)
			})
			.wide_then(|pdu| {
				self.services
					.federation
					.format_pdu_into(pdu, None)
			})
			.collect()
			.await;

		let edus: Vec<Raw<Edu>> = events
			.iter()
			.filter_map(|edu| match edu {
				| SendingEvent::Edu(edu) => Some(edu.as_ref()),
				| _ => None,
			})
			.map(serde_json::from_slice)
			.filter_map(Result::ok)
			.collect();

		if pdus.is_empty() && edus.is_empty() {
			return Ok(Destination::Federation(server));
		}

		let preimage = pdus
			.iter()
			.map(|raw| raw.get().as_bytes())
			.chain(edus.iter().map(|raw| raw.json().get().as_bytes()));

		let txn_hash = calculate_hash(preimage);
		let txn_id = &*URL_SAFE_NO_PAD.encode(txn_hash);
		let request = send_transaction_message::v1::Request {
			transaction_id: txn_id.into(),
			origin: self.server.name.clone(),
			origin_server_ts: MilliSecondsSinceUnixEpoch::now(),
			pdus,
			edus,
		};

		let result = self
			.services
			.federation
			.execute_on(&self.services.client.sender, &server, request)
			.await;

		for (event_id, result) in result.iter().flat_map(|resp| resp.pdus.iter()) {
			if let Err(e) = result {
				warn!(
					%txn_id, %server,
					"error sending PDU {event_id} to remote server: {e:?}"
				);
			}
		}

		match result {
			| Ok(_) => Ok(Destination::Federation(server)),
			| Err(error) => Err((Destination::Federation(server), error)),
		}
	}
}

fn arm_wake(wakes: &mut WakeQueue, dest: Destination, earliest_retry: SystemTime) {
	let delay = earliest_retry
		.duration_since(SystemTime::now())
		.unwrap_or_default();

	arm_wake_in(wakes, dest, delay);
}

#[cfg(test)]
mod queue_recovery_tests {
	use tuwunel_core::{
		Error, err,
		http::StatusCode,
		ruma::api::error::{ErrorKind, LimitExceededErrorData},
	};

	use super::{Destination, QueueRecovery, QueueRetries, defer_queue_error};

	fn busy() -> Error {
		Error::Request(
			ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
			"queue admission busy".into(),
			StatusCode::TOO_MANY_REQUESTS,
		)
	}

	#[tokio::test]
	async fn cleanup_retry_cannot_delete_an_unsent_successor() {
		let mut stage = QueueRecovery::CleanupAcknowledged;
		stage
			.clean_acknowledged(|| async { Err(busy()) })
			.await
			.expect_err("cleanup refused");
		assert_eq!(stage, QueueRecovery::CleanupAcknowledged);

		stage
			.clean_acknowledged(|| async { Ok(()) })
			.await
			.expect("acknowledged rows removed");
		assert_eq!(stage, QueueRecovery::ResumePending);

		// Selection may now promote the next batch and then fail. Retrying at
		// this stage must not run even one deletion against that successor.
		stage
			.clean_acknowledged(|| async { panic!("must not delete the successor") })
			.await
			.expect("cleanup skipped");
	}

	#[tokio::test(start_paused = true)]
	async fn retries_coalesce_per_destination_and_preserve_cleanup_stage() {
		let mut retries = QueueRetries::new();
		let dest = Destination::Appservice("test".into());
		defer_queue_error(&mut retries, dest.clone(), QueueRecovery::CleanupAcknowledged, busy())
			.expect("retry admitted");
		defer_queue_error(&mut retries, dest.clone(), QueueRecovery::CleanupAcknowledged, busy())
			.expect("retry coalesced");
		assert_eq!(retries.len(), 1);
		assert_eq!(retries[&dest].1, QueueRecovery::CleanupAcknowledged);
		assert!(retries[&dest].0 > tokio::time::Instant::now());

		defer_queue_error(&mut retries, dest.clone(), QueueRecovery::ResumePending, busy())
			.expect("selection retry");
		assert_eq!(retries[&dest].1, QueueRecovery::ResumePending);
	}

	#[test]
	fn storage_failure_is_not_relabelled_as_retryable_backpressure() {
		let mut retries = QueueRetries::new();
		let error = defer_queue_error(
			&mut retries,
			Destination::Appservice("test".into()),
			QueueRecovery::ResumePending,
			err!(Database("indeterminate commit")),
		)
		.expect_err("fatal storage failure reaches supervision");
		assert!(retries.is_empty());
		assert_ne!(error.status_code(), StatusCode::TOO_MANY_REQUESTS);
	}
}
