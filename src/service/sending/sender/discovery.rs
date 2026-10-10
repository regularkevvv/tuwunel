//! Recovery retains only a key cursor and one bounded page. A capacity stop
//! advances through processed rows only, so later destinations remain visible.
use std::time::Duration;

use tuwunel_core::Result;

use super::{
	CurTransactionStatus, DELIVERY_LIMIT, Msg, QueueRecovery, QueueRetries, SendingEvent,
	SendingFutures, Service, defer_queue_error,
};
use crate::sending::data::{DISCOVERY_PAGE_LIMIT, RecoverySource};

pub(super) const DISCOVERY_RETRY_INTERVAL: Duration = Duration::from_secs(1);
// Four empty indexes previously cost four one-second page delays. Keep that
// idle sweep budget, without delaying every page of accepted recovery work.
pub(super) const DISCOVERY_IDLE_INTERVAL: Duration = Duration::from_secs(4);

#[derive(Default)]
pub(super) struct Discovery {
	source: RecoverySource,
	after: Option<Vec<u8>>,
}

impl Discovery {
	fn next_source(&mut self) -> bool {
		self.after = None;
		self.source = match self.source {
			| RecoverySource::Active => RecoverySource::Queued,
			| RecoverySource::Queued => RecoverySource::Edus,
			| RecoverySource::Edus => RecoverySource::Rooms,
			| RecoverySource::Rooms => RecoverySource::Active,
		};
		self.source == RecoverySource::Active
	}
}

impl Service {
	pub(super) async fn discover_page(
		&self,
		id: usize,
		futures: &mut SendingFutures,
		statuses: &mut CurTransactionStatus,
		mut retries: Option<&mut QueueRetries>,
		discovery: &mut Discovery,
	) -> Result<bool> {
		let retired = self
			.services
			.get()
			.as_ref()
			.globals
			.current_count();
		let page = self
			.db
			.recovery_page(discovery.source, discovery.after.as_deref(), retired)
			.await?;
		let ended = page.len() < DISCOVERY_PAGE_LIMIT;
		for (key, destination) in page {
			if let Some(dest) = destination
				&& !matches!(&dest, super::Destination::Federation(server) if self.services.get().as_ref().globals.server_is_ours(server))
				&& self.shard_id(&dest) == id
				&& !futures.contains_destination(&dest)
				&& !retries
					.as_ref()
					.is_some_and(|owned| owned.contains_key(&dest))
			{
				if futures
					.len()
					.saturating_add(retries.as_ref().map_or(0, |owned| owned.len()))
					>= DELIVERY_LIMIT
				{
					return Ok(false);
				}
				// This is an ordinary durable wake. It must not force a push
				// out of its existing failure backoff.
				let msg = Msg {
					dest: dest.clone(),
					event: SendingEvent::BadgeRefresh,
					queue_id: Vec::new(),
				};
				if let Err(error) = self.handle_request(msg, futures, statuses).await {
					match retries.as_deref_mut() {
						| Some(owned) =>
							defer_queue_error(owned, dest, QueueRecovery::ResumePending, error)?,
						| None => return Err(error),
					}
				}
			}
			discovery.after = Some(key);
		}
		Ok(ended && discovery.next_source())
	}
}
