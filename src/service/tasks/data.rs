use std::{
	collections::BTreeMap,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use futures::{TryStreamExt, pin_mut};
use ruma::OwnedEventId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tuwunel_core::{
	Error, Result,
	ruma::api::error::{ErrorKind, LimitExceededErrorData},
};
use tuwunel_database::{Database, Map, Txn};

use super::{CAPACITY, Status, TASK_ID_LEN, Task, TaskId};

pub(super) const REQUEST_BYTES: usize = 16 * 1024;
const RECORD_BYTES: usize = 128 * 1024;
const INVENTORY_BYTES: usize = 4 * 1024 * 1024;

pub(super) struct Data {
	db: Arc<Database>,
	records: Arc<Map>,
	uncertain: AtomicBool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
	version: u8,
	id: String,
	action: String,
	resource_id: String,
	parameters: Value,
	status: Status,
	timestamp_ms: u64,
	result: Option<Value>,
	error: Option<String>,
}

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		Self {
			db: args.db.clone(),
			records: args.db["adminjobid_record"].clone(),
			uncertain: AtomicBool::new(false),
		}
	}

	pub(super) async fn load(&self) -> Result<BTreeMap<TaskId, Task>> {
		self.ensure_healthy()?;
		let stream = self.records.raw_stream();
		pin_mut!(stream);
		let mut tasks = BTreeMap::new();
		let mut bytes = 0_usize;
		while let Some((key, value)) = stream.try_next().await? {
			if tasks.len() >= CAPACITY {
				return limit();
			}
			bytes = bytes
				.saturating_add(key.len())
				.saturating_add(value.len());
			if bytes > INVENTORY_BYTES {
				return limit();
			}
			let (id, task) = decode(key, value)?;
			tasks.insert(id, task);
		}
		Ok(tasks)
	}

	pub(super) async fn get(&self, id: &str) -> Result<Option<(TaskId, Task)>> {
		self.ensure_healthy()?;
		match self.records.get(id.as_bytes()).await {
			| Ok(value) => decode(id.as_bytes(), &value).map(Some),
			| Err(error) if error.is_not_found() => Ok(None),
			| Err(error) => Err(error),
		}
	}

	pub(super) async fn put(&self, id: &TaskId, task: &Task) -> Result {
		self.put_with_txn(id, task, self.db.txn()).await
	}

	pub(super) async fn put_with_txn(&self, id: &TaskId, task: &Task, mut txn: Txn) -> Result {
		self.ensure_healthy()?;
		let value = encode(id, task)?;
		// Enforce the same complete-inventory bounds for writes and startup.
		// The service serializes every journal mutation under its admission lock.
		{
			let stream = self.records.raw_stream();
			pin_mut!(stream);
			let mut count = 0_usize;
			let mut found = false;
			let mut bytes = id
				.len()
				.saturating_add(if task.status.is_terminal() {
					value.len()
				} else {
					RECORD_BYTES
				});
			while let Some((key, existing_value)) = stream.try_next().await? {
				let (existing_id, existing) = decode(key, existing_value)?;
				count = count.saturating_add(1);
				if count > CAPACITY {
					return limit();
				}
				if existing_id == *id {
					found = true;
				} else {
					// Reserve each running task's maximum terminal record, so an
					// admitted request can finish without competing for bytes.
					bytes = bytes.saturating_add(key.len()).saturating_add(
						if existing.status.is_terminal() {
							existing_value.len()
						} else {
							RECORD_BYTES
						},
					);
				}
				if bytes > INVENTORY_BYTES {
					return limit();
				}
			}
			if bytes > INVENTORY_BYTES || (!found && count >= CAPACITY) {
				return limit();
			}
		}

		txn.insert_raw(&self.records, id.as_bytes(), &value);
		crate::rooms::timeline::check_purge_batch(&txn)?;
		let outcome = txn.execute().await;
		// Task acceptance/activation must survive a process kill even if a
		// different service has a native batching cork open. Remote commits
		// already acknowledge their durable backend boundary.
		if self.db.backend() == "rocksdb"
			&& let Err(error) = self.db.engine().and_then(|engine| engine.flush())
		{
			// The batch may be visible in native memory despite an
			// uncertain WAL flush. Refuse polling and further admission
			// until a fresh process can read the durable database.
			self.uncertain.store(true, Ordering::Release);
			return Err(error);
		}
		outcome
	}

	fn ensure_healthy(&self) -> Result {
		if self.uncertain.load(Ordering::Acquire) {
			return Err(Error::bad_database(
				"Native admin task WAL outcome is uncertain; restart required",
			));
		}
		Ok(())
	}

	pub(super) fn is_uncertain(&self) -> bool { self.uncertain.load(Ordering::Acquire) }

	pub(super) async fn remove(&self, ids: &[TaskId]) -> Result {
		self.ensure_healthy()?;
		// Bound each batch below the bridge's operation/byte budget. A later
		// refusal may leave additional expired records, never a hidden task.
		for batch in ids.chunks(256) {
			let mut txn = self.db.txn();
			for id in batch {
				txn.del_raw(&self.records, id.as_bytes());
			}
			txn.execute().await?;
		}
		Ok(())
	}
}

pub(super) fn action(value: &str) -> Result<&'static str> {
	match value {
		| "shutdown_and_purge_room" => Ok("shutdown_and_purge_room"),
		| "purge_history" => Ok("purge_history"),
		| "redact_all_events" => Ok("redact_all_events"),
		| _ => Err(Error::bad_database("Invalid admin task action")),
	}
}

pub(super) fn validate_parameters(parameters: &Value) -> Result {
	if !parameters.is_object() {
		return Err(Error::bad_database("Invalid admin task parameters"));
	}
	if serde_json::to_vec(parameters)?.len() > REQUEST_BYTES {
		return limit();
	}
	Ok(())
}

pub(super) fn validate_record(id: &TaskId, task: &Task) -> Result { encode(id, task).map(|_| ()) }

fn encode(id: &TaskId, task: &Task) -> Result<Vec<u8>> {
	validate_parameters(&task.parameters)?;
	let value = serde_json::to_vec(&Record {
		// Older executors must refuse resumable requests rather than marking
		// them failed and releasing their frozen room/original exclusions.
		version: if task.parameters.get("executor").is_some() {
			2
		} else {
			1
		},
		id: id.to_string(),
		action: task.action.into(),
		resource_id: task.resource_id.clone(),
		parameters: task.parameters.clone(),
		status: task.status,
		timestamp_ms: task.timestamp_ms,
		result: task.result.clone(),
		error: task.error.clone(),
	})?;
	if value.len() > RECORD_BYTES {
		return limit();
	}
	decode(id.as_bytes(), &value)?;
	Ok(value)
}

fn decode(key: &[u8], value: &[u8]) -> Result<(TaskId, Task)> {
	if key.len() != TASK_ID_LEN || !key.iter().all(u8::is_ascii_alphanumeric) {
		return Err(Error::bad_database("Invalid admin task identifier"));
	}
	if value.len() > RECORD_BYTES {
		return limit();
	}
	let record: Record = serde_json::from_slice(value)
		.map_err(|_| Error::bad_database("Invalid admin task record"))?;
	let expected_version = if record.parameters.get("executor").is_some() {
		2
	} else {
		1
	};
	if record.version != expected_version || record.id.as_bytes() != key {
		return Err(Error::bad_database("Invalid admin task record binding"));
	}
	let id = TaskId::from(record.id.as_str())
		.map_err(|_| Error::bad_database("Invalid admin task identifier"))?;
	let action = action(&record.action)?;
	match action {
		| "redact_all_events" => {
			ruma::UserId::parse(&record.resource_id)
				.map_err(|_| Error::bad_database("Invalid admin task user"))?;
		},
		| _ => {
			ruma::RoomId::parse(&record.resource_id)
				.map_err(|_| Error::bad_database("Invalid admin task room"))?;
		},
	}
	validate_parameters(&record.parameters)?;
	super::history::validate_parameters(
		action,
		&record.resource_id,
		&record.parameters,
		record.status,
		record.result.as_ref(),
	)?;
	let valid_outcome = match record.status {
		| Status::Scheduled | Status::Active => record.result.is_none() && record.error.is_none(),
		| Status::Complete => record.result.is_some() && record.error.is_none(),
		| Status::Failed => record.result.is_none() && record.error.is_some(),
	};
	if !valid_outcome {
		return Err(Error::bad_database("Invalid admin task outcome"));
	}
	if let Some(result) = &record.result {
		validate_result(action, result)?;
	}
	Ok((id, Task {
		action,
		resource_id: record.resource_id,
		parameters: record.parameters,
		status: record.status,
		timestamp_ms: record.timestamp_ms,
		result: record.result,
		error: record.error,
	}))
}

pub(super) fn limit<T>() -> Result<T> {
	Err(Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Admin task journal limit exceeded".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	))
}

fn validate_result(action: &str, result: &Value) -> Result {
	let valid = match action {
		| "shutdown_and_purge_room" =>
			serde_json::from_value::<crate::rooms::delete::ShutdownRoom>(result.clone()).is_ok(),
		| "purge_history" => result.as_object().is_some_and(|object| {
			object.len() == 1
				&& object
					.get("purged")
					.is_some_and(|count| count.as_u64().is_some())
		}),
		| "redact_all_events" => result.as_object().is_some_and(|object| {
			object.len() == 1
				&& object
					.get("failed_redactions")
					.is_some_and(|failed| {
						serde_json::from_value::<BTreeMap<OwnedEventId, String>>(failed.clone())
							.is_ok()
					})
		}),
		| _ => false,
	};
	if !valid {
		return Err(Error::bad_database("Invalid admin task result"));
	}
	Ok(())
}
