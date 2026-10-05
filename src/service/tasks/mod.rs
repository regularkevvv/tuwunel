//! Durable admission and status records for long-running admin requests.
//!
//! Requests are stored before an id is returned and outcomes before they are
//! exposed as complete. Interrupted work is retained with an explicit failure;
//! replaying partially completed destructive operations requires progress
//! records coupled to their individual commits.

mod data;

use std::{
	collections::BTreeMap,
	panic::AssertUnwindSafe,
	sync::{Arc, Mutex as StdMutex},
	time::Duration,
};

use async_trait::async_trait;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::{sync::Mutex, task::JoinHandle, time::sleep};
use tuwunel_core::{
	Result,
	arrayvec::ArrayString,
	err, error, implement,
	utils::{rand::string_array, time::now_millis},
};

use self::data::Data;

const TASK_ID_LEN: usize = 16;
const RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;
/// Bound the complete retained journal, including accepted running work.
const CAPACITY: usize = 1024;
const MAX_RUNNING: usize = 16;
const GC_INTERVAL: Duration = Duration::from_hours(1);

type TaskId = ArrayString<TASK_ID_LEN>;

pub struct Service {
	services: Arc<crate::services::OnceServices>,
	db: Data,
	journal: Mutex<()>,
	handles: StdMutex<BTreeMap<TaskId, JoinHandle<()>>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
	Scheduled,
	Active,
	Complete,
	Failed,
}

#[derive(Clone, Debug)]
pub struct TaskInfo {
	pub id: TaskId,
	pub action: &'static str,
	pub resource_id: String,
	pub status: Status,
	pub timestamp_ms: u64,
	pub result: Option<JsonValue>,
	pub error: Option<String>,
}

struct Task {
	action: &'static str,
	resource_id: String,
	parameters: JsonValue,
	status: Status,
	timestamp_ms: u64,
	result: Option<JsonValue>,
	error: Option<String>,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: args.services.clone(),
			db: Data::new(args),
			journal: Mutex::new(()),
			handles: StdMutex::new(BTreeMap::new()),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		if self.services.server.config.maintenance {
			self.services.server.until_shutdown().await;
			return Ok(());
		}
		loop {
			self.prune().await?;
			tokio::select! {
				() = sleep(GC_INTERVAL) => {},
				() = self.services.server.until_shutdown() => return Ok(()),
			}
		}
	}

	async fn interrupt(&self) { self.abort_all().await; }

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Admit a request durably, then run its future. Admission is serialized with
/// the duplicate check and retention. No future is polled until Active is
/// durable, and a refused admission starts no work.
#[implement(Service)]
pub async fn spawn<F>(
	self: &Arc<Self>,
	action: &'static str,
	resource_id: String,
	parameters: JsonValue,
	work: F,
) -> Result<TaskId>
where
	F: Future<Output = Result<JsonValue>> + Send + 'static,
{
	let _guard = self.journal.lock().await;
	if self.services.server.config.maintenance {
		return Err(err!("Admin tasks are unavailable in maintenance mode"));
	}
	data::action(action)?;
	data::validate_parameters(&parameters)?;
	let tasks = self.db.load().await?;
	if tasks
		.values()
		.any(|task| matches_nonterminal(task, action, &resource_id))
	{
		return Err(err!(Request(InvalidParam("Admin task already in progress for resource"))));
	}
	if tasks
		.values()
		.filter(|task| !task.status.is_terminal())
		.count()
		>= MAX_RUNNING
	{
		return data::limit();
	}
	let mut id = string_array::<TASK_ID_LEN>();
	while tasks.contains_key(&id) {
		id = string_array::<TASK_ID_LEN>();
	}
	let task = Task {
		action,
		resource_id,
		parameters,
		status: Status::Scheduled,
		timestamp_ms: now_millis(),
		result: None,
		error: None,
	};
	// Validate the prospective record before any retention deletion.
	data::validate_record(&id, &task)?;
	self.db
		.remove(&prune_ids(&tasks, now_millis(), 1))
		.await?;
	self.db.put(&id, &task).await?;

	let this = Arc::clone(self);
	let handle = self.services.server.runtime().spawn(async move {
		let result = async {
			this.set_active(&id).await?;
			let outcome = AssertUnwindSafe(work)
				.catch_unwind()
				.await
				.unwrap_or_else(|_| Err(err!("Admin task panicked; partial changes may exist")));
			this.finish(&id, outcome).await
		}
		.await;
		if let Err(error) = result {
			error!(%id, %error, "Admin task journal transition failed; task remains unresolved");
		}
		this.handles.lock().expect("locked").remove(&id);
	});
	self.handles
		.lock()
		.expect("locked")
		.insert(id, handle);
	Ok(id)
}

/// Canonical storage errors propagate; an unavailable journal is not a 404.
#[implement(Service)]
pub async fn get(&self, id: &str) -> Result<Option<TaskInfo>> {
	let _guard = self.journal.lock().await;
	Ok(self
		.db
		.get(id)
		.await?
		.map(|(id, task)| task.info(&id)))
}

#[implement(Service)]
pub async fn by_resource(&self, resource_id: &str) -> Result<Vec<TaskInfo>> {
	Ok(self
		.list()
		.await?
		.into_iter()
		.filter(|task| task.resource_id == resource_id)
		.collect())
}

fn matches_nonterminal(task: &Task, action: &str, resource_id: &str) -> bool {
	task.action == action && task.resource_id == resource_id && !task.status.is_terminal()
}

#[implement(Service)]
pub async fn list(&self) -> Result<Vec<TaskInfo>> {
	let _guard = self.journal.lock().await;
	Ok(self
		.db
		.load()
		.await?
		.iter()
		.map(|(id, task)| task.info(id))
		.collect())
}

/// Validate the entire inventory before changing it. Preserve interrupted
/// requests rather than disappearing or blindly repeating partial changes.
/// This is a journal foundation; operation-level resumable receipts remain
/// necessary for automatic completion after a kill.
#[implement(Service)]
pub async fn restore_interrupted(&self) -> Result {
	let _guard = self.journal.lock().await;
	let tasks = self.db.load().await?;
	for (id, mut task) in tasks {
		if !task.status.is_terminal() {
			task.status = Status::Failed;
			task.error = Some("Interrupted by server restart; partial changes may exist".into());
			self.db.put(&id, &task).await?;
		}
	}
	Ok(())
}

#[implement(Service)]
async fn set_active(&self, id: &TaskId) -> Result {
	let _guard = self.journal.lock().await;
	let (_, mut task) = self
		.db
		.get(id)
		.await?
		.ok_or_else(|| err!("Accepted admin task is missing"))?;
	task.status = Status::Active;
	self.db.put(id, &task).await
}

#[implement(Service)]
async fn finish(&self, id: &TaskId, outcome: Result<JsonValue>) -> Result {
	let _guard = self.journal.lock().await;
	let (_, mut task) = self
		.db
		.get(id)
		.await?
		.ok_or_else(|| err!("Accepted admin task is missing"))?;
	match outcome {
		| Ok(value) => {
			task.status = Status::Complete;
			task.result = Some(value);
		},
		| Err(error) => {
			task.status = Status::Failed;
			task.error = Some(error.to_string());
		},
	}
	self.db.put(id, &task).await
}

#[implement(Service)]
async fn prune(&self) -> Result {
	let _guard = self.journal.lock().await;
	let tasks = self.db.load().await?;
	self.db
		.remove(&prune_ids(&tasks, now_millis(), 0))
		.await
}

#[implement(Service)]
async fn abort_all(&self) {
	let handles = std::mem::take(&mut *self.handles.lock().expect("locked"));
	for handle in handles.values() {
		handle.abort();
	}
	// Release the handle lock before joining; cancelled tasks may remove
	// their handle and must drop their database references before shutdown.
	for (id, handle) in handles {
		if let Err(error) = handle.await
			&& !error.is_cancelled()
		{
			error!(%id, %error, "Admin task failed while shutting down");
		}
	}
}

impl Status {
	#[must_use]
	pub fn is_terminal(self) -> bool { matches!(self, Self::Complete | Self::Failed) }

	#[must_use]
	pub fn as_str(self) -> &'static str {
		match self {
			| Self::Scheduled => "scheduled",
			| Self::Active => "active",
			| Self::Complete => "complete",
			| Self::Failed => "failed",
		}
	}
}

impl Task {
	fn info(&self, id: &TaskId) -> TaskInfo {
		TaskInfo {
			id: *id,
			action: self.action,
			resource_id: self.resource_id.clone(),
			status: self.status,
			timestamp_ms: self.timestamp_ms,
			result: self.result.clone(),
			error: self.error.clone(),
		}
	}
}

/// Retention and total admission cap have a deterministic id tie-break.
/// Active requests are never selected, including beyond their retention age.
fn prune_ids(tasks: &BTreeMap<TaskId, Task>, now_ms: u64, reserve: usize) -> Vec<TaskId> {
	let mut terminal: Vec<_> = tasks
		.iter()
		.filter(|(_, task)| task.status.is_terminal())
		.map(|(id, task)| (task.timestamp_ms, *id))
		.collect();
	terminal.sort_unstable();
	let minimum = tasks
		.len()
		.saturating_add(reserve)
		.saturating_sub(CAPACITY);
	terminal
		.into_iter()
		.enumerate()
		.filter(|(index, (timestamp, _))| {
			*index < minimum || now_ms.saturating_sub(*timestamp) >= RETENTION_MS
		})
		.map(|(_, (_, id))| id)
		.collect()
}

#[cfg(test)]
mod tests;
