//! Presence update pipeline.
//!
//! This module centralizes the write path for presence updates. It keeps the
//! aggregation and timer logic in one place so the public `Service` surface
//! remains small and the update flow is easy to review.

use std::time::Duration;

use futures::TryFutureExt;
use ruma::{
	DeviceId, OwnedUserId, UInt, UserId, events::presence::PresenceEvent, presence::PresenceState,
};
use tokio::time::sleep;
use tuwunel_core::{
	Error, Result, debug,
	debug::INFO_SPAN_LEVEL,
	error,
	result::LogErr,
	trace,
	utils::{future::OptionFutureExt, option::OptionExt},
};

use super::{
	Ping, Service, TimerFired,
	aggregate::{self, StatusMsg},
};

impl Service {
	fn device_key(device_id: Option<&DeviceId>, is_remote: bool) -> aggregate::DeviceKey {
		if is_remote {
			return aggregate::DeviceKey::Remote;
		}

		match device_id {
			| Some(device_id) => aggregate::DeviceKey::Device(device_id.to_owned()),
			| None => aggregate::DeviceKey::UnknownLocal,
		}
	}

	fn schedule_presence_timer(
		&self,
		user_id: &UserId,
		presence_state: &PresenceState,
		count: u64,
	) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		if !(self.timeout_remote_users || services_root.globals.user_is_local(user_id))
			|| user_id == services_root.globals.server_user
		{
			return Ok(());
		}

		let timeout = match presence_state {
			| PresenceState::Online =>
				services_root
					.server
					.config
					.presence_idle_timeout_s,
			| _ =>
				services_root
					.server
					.config
					.presence_offline_timeout_s,
		};

		self.timer_channel
			.0
			.send((user_id.to_owned(), Duration::from_secs(timeout), count))
			.map_err(|e| {
				error!("Failed to add presence timer: {}", e);
				Error::bad_database("Failed to add presence timer")
			})
	}

	fn refresh_skip_decision(
		refresh_window_ms: Option<u64>,
		last_event: Option<&PresenceEvent>,
		last_count: Option<u64>,
	) -> Option<(u64, u64)> {
		let (Some(refresh_ms), Some(event), Some(count)) =
			(refresh_window_ms, last_event, last_count)
		else {
			return None;
		};

		let last_last_active_ago: u64 = event.content.last_active_ago?.into();

		(last_last_active_ago < refresh_ms).then_some((count, last_last_active_ago))
	}

	fn timer_is_stale(expected_count: u64, current_count: u64) -> bool {
		expected_count != current_count
	}

	#[tracing::instrument(
		name = "presence",
		level = INFO_SPAN_LEVEL,
		skip_all,
		fields(
			%user_id,
			?device_key,
			%state,
			?currently_active,
		),
	)]
	#[expect(clippy::too_many_arguments)]
	async fn apply_device_presence_update(
		&self,
		user_id: &UserId,
		device_key: aggregate::DeviceKey,
		state: &PresenceState,
		currently_active: Option<bool>,
		last_active_ago: Option<UInt>,
		status_msg: StatusMsg,
		refresh_window_ms: Option<u64>,
	) -> Result {
		let _update = self.update_mutex.lock(user_id).await;
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();
		let now = tuwunel_core::utils::millis_since_unix_epoch();
		let preserve_status = matches!(status_msg, StatusMsg::Unchanged);
		let last_presence = match self.db.get_presence(user_id).await {
			| Ok(presence) => Some(presence),
			| Err(error) if error.is_not_found() => None,
			| Err(error) => return Err(error),
		};
		let (last_count, last_event) = match last_presence {
			| Some((count, event)) => (Some(count), Some(event)),
			| None => (None, None),
		};
		self.device_presence
			.reconcile(user_id, last_count);
		let checkpoint = self
			.device_presence
			.checkpoint(user_id, &device_key);
		self.device_presence.update(
			user_id,
			device_key,
			state,
			currently_active,
			last_active_ago,
			status_msg,
			now,
		);
		let aggregated =
			self.device_presence
				.aggregate(user_id, now, self.idle_timeout, self.offline_timeout);
		let last_state = last_event
			.as_ref()
			.map(|event| event.content.presence.clone());
		let state_changed = last_state.as_ref() != Some(&aggregated.state);
		if !state_changed
			&& let Some((count, _)) =
				Self::refresh_skip_decision(refresh_window_ms, last_event.as_ref(), last_count)
		{
			self.schedule_presence_timer(user_id, &aggregated.state, count)
				.log_err()
				.ok();
			checkpoint.accept(last_count);
			return Ok(());
		}
		let fallback_status = || {
			last_event
				.and_then(|event| event.content.status_msg)
				.filter(|msg| !msg.is_empty())
		};
		let status_msg = aggregated
			.status_msg
			.or_else(|| preserve_status.then(fallback_status).flatten());
		let last_active_ago =
			Some(UInt::new_saturating(now.saturating_sub(aggregated.last_active_ts)));
		let count = self
			.persist_presence(
				user_id,
				&aggregated.state,
				Some(aggregated.currently_active),
				last_active_ago,
				status_msg,
			)
			.await?;
		checkpoint.accept(count.or(last_count));
		if matches!(last_state, Some(PresenceState::Online))
			&& aggregated.state != PresenceState::Online
		{
			services_root
				.sending
				.schedule_resume_pushes_for_user(
					user_id.to_owned(),
					"presence->inactive (aggregate)",
				);
		}
		Ok(())
	}

	/// Pings the presence of the given user, defaulting the state to online.
	///
	/// Requests authenticated with an appservice token do not imply user
	/// activity. In particular, they must not update presence or device
	/// last-seen data. Explicit appservice presence updates use
	/// [`Self::set_presence_for_device`] instead.
	pub async fn maybe_ping_presence(&self, user_id: &UserId, args: Ping<'_>) -> Result {
		const REFRESH_TIMEOUT: u64 = 30 * 1000;

		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		if args.appservice.is_some()
			|| !services_root.server.config.allow_local_presence
			|| services_root.db.is_read_only()
		{
			return Ok(());
		}

		let update_device_seen = args.device_id.map_async(|device_id| {
			services_root
				.users
				.update_device_last_seen(user_id, device_id, args.client_ip, None)
		});

		let new_state = args.new_state.unwrap_or(&PresenceState::Online);
		let currently_active = *new_state == PresenceState::Online;
		let set_presence = self.apply_device_presence_update(
			user_id,
			Self::device_key(args.device_id, false),
			new_state,
			Some(currently_active),
			UInt::new(0),
			StatusMsg::Unchanged,
			Some(REFRESH_TIMEOUT),
		);

		debug!(?user_id, ?new_state, currently_active, "Presence ping accepted");

		futures::future::try_join(set_presence, update_device_seen.unwrap_or(Ok(())))
			.map_ok(|_| ())
			.await
	}

	/// Applies an explicit presence update for a local device.
	pub async fn set_presence_for_device(
		&self,
		user_id: &UserId,
		device_id: Option<&DeviceId>,
		state: &PresenceState,
		status_msg: Option<String>,
	) -> Result {
		let currently_active = *state == PresenceState::Online;
		self.apply_device_presence_update(
			user_id,
			Self::device_key(device_id, false),
			state,
			Some(currently_active),
			None,
			StatusMsg::Set(status_msg),
			None,
		)
		.await
	}

	/// Applies a presence update received over federation.
	pub async fn set_presence_from_federation(
		&self,
		user_id: &UserId,
		state: &PresenceState,
		currently_active: bool,
		last_active_ago: UInt,
		status_msg: Option<String>,
	) -> Result {
		self.apply_device_presence_update(
			user_id,
			Self::device_key(None, true),
			state,
			Some(currently_active),
			Some(last_active_ago),
			StatusMsg::Set(status_msg),
			None,
		)
		.await
	}

	/// Adds a presence event which will be saved until a new event replaces it.
	pub async fn set_presence(
		&self,
		user_id: &UserId,
		state: &PresenceState,
		currently_active: Option<bool>,
		last_active_ago: Option<UInt>,
		status_msg: Option<String>,
	) -> Result {
		let _update = self.update_mutex.lock(user_id).await;
		if self
			.persist_presence(user_id, state, currently_active, last_active_ago, status_msg)
			.await?
			.is_some()
		{
			self.device_presence.invalidate(user_id);
		}
		Ok(())
	}

	async fn persist_presence(
		&self,
		user_id: &UserId,
		state: &PresenceState,
		currently_active: Option<bool>,
		last_active_ago: Option<UInt>,
		status_msg: Option<String>,
	) -> Result<Option<u64>> {
		let presence_state = match state.as_str() {
			| "" => &PresenceState::Offline,
			| _ => state,
		};
		let count = self
			.db
			.set_presence(user_id, presence_state, currently_active, last_active_ago, status_msg)
			.await?;
		if let Some(count) = count {
			// A timer is a post-commit hint; its failure cannot turn a committed
			// presence mutation into a reported persistence rejection.
			self.schedule_presence_timer(user_id, presence_state, count)
				.log_err()
				.ok();
		}
		Ok(count)
	}

	pub(super) async fn process_presence_timer(
		&self,
		user_id: &OwnedUserId,
		expected_count: u64,
	) -> Result {
		let _update = self.update_mutex.lock(user_id).await;
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let Ok((current_count, presence)) = self.db.get_presence_raw(user_id).await else {
			return Ok(());
		};

		if Self::timer_is_stale(expected_count, current_count) {
			trace!(?user_id, expected_count, current_count, "Skipping stale presence timer");
			return Ok(());
		}

		self.device_presence
			.reconcile(user_id, Some(current_count));
		let presence_state = presence.state.clone();
		let now = tuwunel_core::utils::millis_since_unix_epoch();
		let aggregated =
			self.device_presence
				.aggregate(user_id, now, self.idle_timeout, self.offline_timeout);

		if aggregated.device_count == 0 {
			let last_active_ago =
				Some(UInt::new_saturating(now.saturating_sub(presence.last_active_ts)));
			let status_msg = presence.status_msg;

			let new_state = match (&presence_state, last_active_ago.map(u64::from)) {
				| (PresenceState::Online, Some(ago)) if ago >= self.idle_timeout =>
					Some(PresenceState::Unavailable),
				| (PresenceState::Unavailable, Some(ago)) if ago >= self.offline_timeout =>
					Some(PresenceState::Offline),
				| _ => None,
			};

			debug!(
				"Processed presence timer for user '{user_id}': Old state = {presence_state}, \
				 New state = {new_state:?}"
			);

			if let Some(new_state) = new_state {
				self.persist_presence(
					user_id,
					&new_state,
					Some(false),
					last_active_ago,
					status_msg,
				)
				.await?;
				if matches!(new_state, PresenceState::Unavailable | PresenceState::Offline) {
					services_root
						.sending
						.schedule_resume_pushes_for_user(
							user_id.to_owned(),
							"presence->inactive",
						);
				}
			}

			return Ok(());
		}

		if aggregated.state == presence_state {
			self.schedule_presence_timer(user_id, &presence_state, current_count)
				.log_err()
				.ok();
			return Ok(());
		}

		let status_msg = aggregated.status_msg.or(presence.status_msg);
		let last_active_ago =
			Some(UInt::new_saturating(now.saturating_sub(aggregated.last_active_ts)));

		let count = self
			.persist_presence(
				user_id,
				&aggregated.state,
				Some(aggregated.currently_active),
				last_active_ago,
				status_msg,
			)
			.await?;
		self.device_presence
			.committed_count(user_id, count.or(Some(current_count)));
		if matches!(aggregated.state, PresenceState::Unavailable | PresenceState::Offline) {
			services_root
				.sending
				.schedule_resume_pushes_for_user(user_id.to_owned(), "presence->inactive");
		}

		Ok(())
	}
}

pub(super) async fn presence_timer(
	user_id: OwnedUserId,
	timeout: Duration,
	count: u64,
) -> TimerFired {
	sleep(timeout).await;

	(user_id, count)
}

#[cfg(test)]
mod tests {
	use ruma::{uint, user_id};

	use super::*;

	#[test]
	fn refresh_window_skip_decision() {
		let user_id = user_id!("@alice:example.com");
		let event = PresenceEvent {
			sender: user_id.to_owned(),
			content: ruma::events::presence::PresenceEventContent {
				presence: PresenceState::Online,
				status_msg: None,
				currently_active: Some(true),
				last_active_ago: Some(uint!(10)),
				avatar_url: None,
				displayname: None,
			},
		};

		let decision = Service::refresh_skip_decision(Some(20), Some(&event), Some(5));
		assert_eq!(decision, Some((5, 10)));

		let decision = Service::refresh_skip_decision(Some(5), Some(&event), Some(5));
		assert_eq!(decision, None);

		let event_missing_ago = PresenceEvent {
			sender: user_id.to_owned(),
			content: ruma::events::presence::PresenceEventContent {
				presence: PresenceState::Online,
				status_msg: None,
				currently_active: Some(true),
				last_active_ago: None,
				avatar_url: None,
				displayname: None,
			},
		};

		let decision =
			Service::refresh_skip_decision(Some(20), Some(&event_missing_ago), Some(5));
		assert_eq!(decision, None);

		let decision = Service::refresh_skip_decision(Some(20), None, Some(5));
		assert_eq!(decision, None);
	}

	#[test]
	fn timer_stale_detection() {
		assert!(Service::timer_is_stale(2, 3));
		assert!(!Service::timer_is_stale(2, 2));
	}
}
