//! Typed history-purge requests with mutation-coupled progress. Generic task
//! futures retain their explicit interruption failure policy.

use std::{collections::BTreeSet, future::pending, sync::Arc};

use ruma::{OwnedEventId, OwnedRoomId, RoomId};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tuwunel_core::{
	Error, Result, error, implement,
	matrix::pdu::{PduCount, RawPduId},
	utils::{hash::sha256, rand::string_array},
};

use super::{MAX_RUNNING, Service, Status, TaskId, matches_nonterminal};
use crate::rooms::timeline::RoomMutexGuard;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct History {
	pub executor: Executor,
	pub boundary: i64,
	pub delete_local_events: bool,
	pub shortroomid: u64,
	pub after: Option<Vec<u8>>,
	pub purged: u64,
	pub current: Option<Target>,
	pub done: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) enum Executor {
	#[serde(rename = "history-v1")]
	HistoryV1,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Target {
	pub key: Vec<u8>,
	pub event_id: OwnedEventId,
	pub canonical: sha256::Digest,
	pub original: Option<sha256::Digest>,
	pub phase: Phase,
	pub after: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
	SearchCurrent,
	SearchOriginal,
	LegacyRelations,
	TypedRelations,
	Notifications,
	Final,
}

impl History {
	pub(crate) fn decode(parameters: &Value) -> Result<Option<Self>> {
		if parameters.get("executor").is_none() {
			return Ok(None);
		}
		let history: Self = serde_json::from_value(parameters.clone())
			.map_err(|_| Error::bad_database("Invalid typed history request"))?;
		history.validate()?;
		Ok(Some(history))
	}

	fn validate(&self) -> Result {
		if self.shortroomid == 0
			|| (self.purged > 0 && self.after.is_none())
			|| (self.done && self.current.is_some())
		{
			return Err(Error::bad_database("Invalid history progress"));
		}
		let check_key = |key: &[u8]| -> Result {
			let raw = RawPduId::from_bytes(key)?;
			if raw.shortroomid() != self.shortroomid.to_be_bytes()
				|| raw.pdu_count() >= PduCount::from(self.boundary)
			{
				return Err(Error::bad_database("Invalid history cursor binding"));
			}
			Ok(())
		};
		if let Some(after) = &self.after {
			check_key(after)?;
		}
		if let Some(target) = &self.current {
			check_key(&target.key)?;
			if self
				.after
				.as_ref()
				.is_some_and(|after| after >= &target.key)
			{
				return Err(Error::bad_database("History target precedes cursor"));
			}
			if let Some(after) = &target.after {
				let valid = match target.phase {
					| Phase::SearchCurrent | Phase::SearchOriginal =>
						after.len() <= 256
							&& after.len() > 9_usize.saturating_add(target.key.len())
							&& after[after
								.len()
								.saturating_sub(target.key.len().saturating_add(1))]
								== tuwunel_database::SEP
							&& std::str::from_utf8(
								&after[8..after
									.len()
									.saturating_sub(target.key.len().saturating_add(1))],
							)
							.is_ok() && after.starts_with(&self.shortroomid.to_be_bytes())
							&& after.ends_with(&target.key),
					| Phase::LegacyRelations =>
						after.len() == 16
							&& after.starts_with(&RawPduId::from_bytes(&target.key)?.count()),
					| Phase::TypedRelations =>
						after.len() == 33
							&& after.starts_with(&self.shortroomid.to_be_bytes())
							&& after[8..16] == RawPduId::from_bytes(&target.key)?.count(),
					| Phase::Notifications => {
						let mut prefix = target.key.clone();
						prefix.push(tuwunel_database::SEP);
						after.starts_with(&prefix)
							&& after.len() <= tuwunel_bridge::MAX_KEY_BYTES
							&& std::str::from_utf8(&after[prefix.len()..])
								.ok()
								.and_then(|user| ruma::UserId::parse(user).ok())
								.is_some()
					},
					| Phase::Final => false,
				};
				if !valid {
					return Err(Error::bad_database("Invalid history cleanup cursor"));
				}
			}
		}
		Ok(())
	}
}

pub(super) fn validate_parameters(
	action: &str,
	_resource: &str,
	parameters: &Value,
	status: Status,
	result: Option<&Value>,
) -> Result {
	let Some(history) = History::decode(parameters)? else {
		return Ok(());
	};
	let valid = action == "purge_history"
		&& match status {
			| Status::Scheduled =>
				!history.done
					&& history.current.is_none()
					&& history.after.is_none()
					&& history.purged == 0,
			| Status::Active => !history.done,
			// A partially executed typed job cannot release its exclusions as
			// a terminal failure. Repair/restart must retain its active intent.
			| Status::Failed => false,
			| Status::Complete =>
				history.done
					&& result.is_some_and(|result| {
						result.get("purged").and_then(Value::as_u64) == Some(history.purged)
					}),
		};
	if !valid {
		return Err(Error::bad_database("Invalid typed history outcome"));
	}
	Ok(())
}

#[implement(Service)]
pub async fn spawn_history(
	self: &Arc<Self>,
	room: OwnedRoomId,
	boundary: PduCount,
	delete_local_events: bool,
) -> Result<TaskId> {
	// Room -> insertion -> journal; never wait for a room under the journal.
	let Ok(state) = self.services.state.mutex.try_lock(&room) else {
		return super::data::limit();
	};
	let Ok(insert) = self
		.services
		.timeline
		.mutex_insert
		.try_lock(&room)
	else {
		return super::data::limit();
	};
	let shortroomid = self.services.short.get_shortroomid(&room).await?;
	let history = History {
		executor: Executor::HistoryV1,
		boundary: boundary.into_signed(),
		delete_local_events,
		shortroomid,
		after: None,
		purged: 0,
		current: None,
		done: false,
	};
	let _originals = self.services.retention.lock_originals().await;
	let _journal = self.journal.lock().await;
	let admission = self
		.admit("purge_history", room.to_string(), serde_json::to_value(history)?)
		.await;
	let id = match admission {
		| Ok(id) => id,
		| Err(error) => {
			if self.db.is_uncertain() {
				// A native flush can fail after Scheduled was written. Preserve
				// exclusion even though the caller cannot safely receive its id.
				self.hold_uncertain_history_room(state, insert);
			}
			return Err(error);
		},
	};
	self.launch_history(id, room, state, insert);
	Ok(id)
}

#[implement(Service)]
fn hold_uncertain_history_room(&self, state: RoomMutexGuard, insert: RoomMutexGuard) {
	let mut handles = self.handles.lock().expect("locked");
	let mut id = string_array::<{ super::TASK_ID_LEN }>();
	while handles.contains_key(&id) {
		id = string_array::<{ super::TASK_ID_LEN }>();
	}
	// This handle owns only an exclusion, not a fabricated journal record.
	// abort_all joins it during shutdown before dropping database services.
	handles.insert(
		id,
		self.services.server.runtime().spawn(async move {
			let _state = state;
			let _insert = insert;
			pending::<()>().await;
		}),
	);
}

#[implement(Service)]
fn launch_history(
	self: &Arc<Self>,
	id: TaskId,
	room: OwnedRoomId,
	state: RoomMutexGuard,
	insert: RoomMutexGuard,
) {
	let this = self.clone();
	let handle = self.services.server.runtime().spawn(async move {
		let _state = state;
		let _insert = insert;
		if let Err(error) = this.run_history(&id, &room).await {
			// A commit can have an ambiguous outcome. Keep canonical progress;
			// never replace it with a speculative Failed/Complete transition.
			error!(%id, %error, "History request stopped with durable progress; restart or repair required");
			this.services.server.until_shutdown().await;
		}
		this.handles.lock().expect("locked").remove(&id);
	});
	self.handles
		.lock()
		.expect("locked")
		.insert(id, handle);
}

#[implement(Service)]
async fn run_history(&self, id: &TaskId, room: &RoomId) -> Result {
	self.set_active(id).await?;
	loop {
		let mut task = {
			let _journal = self.journal.lock().await;
			self.db
				.get(id)
				.await?
				.ok_or_else(|| Error::bad_database("History task disappeared"))?
				.1
		};
		let old_parameters = task.parameters.clone();
		let history = History::decode(&old_parameters)?
			.ok_or_else(|| Error::bad_database("History handler is missing"))?;
		// Capture original bytes and their durable pin under the same exclusion
		// used by retention. Subsequent expiry reads these canonical job pins.
		let _originals = self.services.retention.lock_originals().await;
		let (txn, next) = self
			.services
			.timeline
			.prepare_history_step(room, history)
			.await?;
		task.parameters = serde_json::to_value(&next)?;
		if next.done {
			task.status = Status::Complete;
			task.result = Some(json!({"purged": next.purged}));
		}
		let _journal = self.journal.lock().await;
		let canonical = self
			.db
			.get(id)
			.await?
			.ok_or_else(|| Error::bad_database("History task disappeared"))?
			.1;
		if canonical.status != Status::Active || canonical.parameters != old_parameters {
			return Err(Error::bad_database("History task changed outside its executor"));
		}
		self.db.put_with_txn(id, &task, txn).await?;
		if next.done {
			return Ok(());
		}
	}
}

/// Validate the complete journal and frozen targets before changing any task.
/// Preflight every interrupted job before another startup recovery mutates
/// storage. Notification completion must precede retained history room locks.
#[implement(Service)]
pub(crate) async fn preflight_interrupted(&self) -> Result {
	let tasks = {
		let _journal = self.journal.lock().await;
		self.db.load().await?
	};
	let pending: Vec<_> = tasks
		.values()
		.filter(|task| !task.status.is_terminal())
		.collect();
	if pending.len() > MAX_RUNNING {
		return super::data::limit();
	}
	for (index, task) in pending.iter().enumerate() {
		if pending
			.iter()
			.skip(index.saturating_add(1))
			.any(|other| matches_nonterminal(other, task.action, &task.resource_id))
		{
			return Err(Error::bad_database("Conflicting interrupted admin requests"));
		}
		if let Some(history) = History::decode(&task.parameters)? {
			let room = OwnedRoomId::try_from(task.resource_id.as_str())?;
			self.services
				.timeline
				.validate_history_progress(&room, &history)
				.await?;
		}
	}
	Ok(())
}

/// Restore room exclusion before workers/readiness, then resume in background.
#[implement(Service)]
pub(crate) async fn restore_interrupted(self: &Arc<Self>) -> Result {
	let tasks = {
		let _journal = self.journal.lock().await;
		self.db.load().await?
	};
	let pending: Vec<_> = tasks
		.iter()
		.filter(|(_, task)| !task.status.is_terminal())
		.collect();
	if pending.len() > MAX_RUNNING {
		return super::data::limit();
	}
	for (i, (_, task)) in pending.iter().enumerate() {
		if pending
			.iter()
			.skip(i.saturating_add(1))
			.any(|(_, other)| matches_nonterminal(other, task.action, &task.resource_id))
		{
			return Err(Error::bad_database("Conflicting interrupted admin requests"));
		}
	}
	let mut resumed = Vec::new();
	for (id, task) in &tasks {
		if !task.status.is_terminal()
			&& let Some(history) = History::decode(&task.parameters)?
		{
			let room = OwnedRoomId::try_from(task.resource_id.as_str())?;
			let state = self.services.state.mutex.lock(&room).await;
			let insert = self
				.services
				.timeline
				.mutex_insert
				.lock(&room)
				.await;
			self.services
				.timeline
				.validate_history_progress(&room, &history)
				.await?;
			resumed.push((*id, room, state, insert));
		}
	}
	let _journal = self.journal.lock().await;
	for (id, mut task) in tasks {
		if !task.status.is_terminal() && History::decode(&task.parameters)?.is_none() {
			task.status = Status::Failed;
			task.error = Some("Interrupted by server restart; partial changes may exist".into());
			self.db.put(&id, &task).await?;
		}
	}
	for (id, room, state, insert) in resumed {
		self.launch_history(id, room, state, insert);
	}
	Ok(())
}

#[implement(Service)]
pub(crate) async fn pinned_history_rooms(&self) -> Result<BTreeSet<OwnedRoomId>> {
	let _journal = self.journal.lock().await;
	let mut rooms = BTreeSet::new();
	for task in self.db.load().await?.values() {
		if let Some(history) = History::decode(&task.parameters)?
			&& (!task.status.is_terminal() || history.current.is_some())
		{
			rooms.insert(OwnedRoomId::try_from(task.resource_id.as_str())?);
		}
	}
	Ok(rooms)
}
