//! Resumable secondary-index backfill. Every 64-key page and its resume cursor
//! share one commit; the original notification metadata remains unchanged.

use ruma::UserId;
use serde::{Deserialize, Serialize};
use tuwunel_core::{
	Error, Result,
	matrix::{Event, pdu::PduId},
};
use tuwunel_database::{deserialize_from_slice as deserialize_key, serialize_key};

use crate::Services;

const DONE: &[u8] = b"notification_index_v1";
const CURSOR: &[u8] = b"notification_index_cursor_v1";

#[cfg(all(feature = "notification_recovery_tests", debug_assertions))]
pub use self::test_pause::NotificationIndexMigrationPause;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
	version: u8,
	after: Option<Vec<u8>>,
}

fn decode_key(key: &[u8]) -> Result<(&UserId, u64)> {
	let (user, count): (&UserId, u64) = deserialize_key(key)?;
	if count == 0
		|| count > i64::MAX.unsigned_abs()
		|| serialize_key((user, count))?.as_slice() != key
	{
		return Err(Error::bad_database("Invalid notification index migration key"));
	}
	Ok((user, count))
}

pub(super) async fn migrate(services: &Services) -> Result {
	let global = &services.db["global"];
	match global.get(DONE).await {
		| Ok(value) if value.is_empty() => {
			return match global.get(CURSOR).await {
				| Err(error) if error.is_not_found() => Ok(()),
				| Err(error) => Err(error),
				| Ok(_) =>
					Err(Error::bad_database("Completed notification index retains cursor")),
			};
		},
		| Ok(_) =>
			return Err(Error::bad_database("Invalid notification index completion marker")),
		| Err(error) if error.is_not_found() => {},
		| Err(error) => return Err(error),
	}
	let mut cursor = match global.get(CURSOR).await {
		| Ok(value) => {
			if value.len() > 24 * 1024 {
				return Err(Error::bad_database("Oversized notification index cursor"));
			}
			let cursor: Cursor = serde_json::from_slice(&value)
				.map_err(|_| Error::bad_database("Invalid notification index cursor"))?;
			if cursor.version != 1 {
				return Err(Error::bad_database("Unsupported notification index cursor"));
			}
			if let Some(key) = &cursor.after {
				decode_key(key)?;
			}
			cursor
		},
		| Err(error) if error.is_not_found() => Cursor { version: 1, after: None },
		| Err(error) => return Err(error),
	};
	loop {
		let keys = services.db["useridcount_notification"]
			.raw_keys_after(cursor.after.as_deref(), 64)
			.await?;
		let mut txn = services.db.txn();
		for key in &keys {
			let (user, count) = decode_key(key)?;
			let value = services.db["useridcount_notification"]
				.get(key)
				.await?;
			let notified = crate::pusher::parse_notified(&value)?;
			let raw = PduId {
				shortroomid: notified.sroomid,
				count: count.into(),
			}
			.into();
			let pdu = match services.timeline.get_pdu_from_id(&raw).await {
				| Ok(pdu) => pdu,
				| Err(error) if error.is_not_found() => continue,
				| Err(error) => return Err(error),
			};
			if services
				.timeline
				.get_pdu_id(pdu.event_id())
				.await? != raw
				|| services
					.short
					.get_shortroomid(pdu.room_id())
					.await? != notified.sroomid
			{
				return Err(Error::bad_database(
					"Notification index migration PDU binding mismatch",
				));
			}
			services
				.pusher
				.stage_notification_index_backfill(&mut txn, raw, pdu.room_id(), user)
				.await?;
		}
		if let Some(last) = keys.last() {
			cursor.after = Some(last.clone());
			txn.insert_raw(global, CURSOR, serde_json::to_vec(&cursor)?);
		} else {
			txn.insert_raw(global, DONE, []);
			txn.del_raw(global, CURSOR);
		}
		txn.execute().await?;
		#[cfg(all(feature = "notification_recovery_tests", debug_assertions))]
		if let Some(after) = keys.last() {
			pause_after_commit(services, after).await;
		}
		if keys.is_empty() {
			return Ok(());
		}
	}
}

#[cfg(all(feature = "notification_recovery_tests", debug_assertions))]
mod test_pause {
	use std::sync::{Arc, Mutex};

	use tokio::sync::{Notify, oneshot};
	use tuwunel_core::{Error, Result};

	use crate::Services;

	pub(super) static PAUSE: Mutex<Option<Gate>> = Mutex::new(None);
	pub(super) struct Gate {
		pub entered: oneshot::Sender<Vec<u8>>,
		pub release: Arc<Notify>,
	}
	pub struct NotificationIndexMigrationPause {
		entered: oneshot::Receiver<Vec<u8>>,
		release: Arc<Notify>,
	}
	impl NotificationIndexMigrationPause {
		pub async fn entered(&mut self) -> Result<Vec<u8>> {
			(&mut self.entered)
				.await
				.map_err(|_| Error::bad_database("Notification index migration pause abandoned"))
		}
	}
	impl Drop for NotificationIndexMigrationPause {
		fn drop(&mut self) { self.release.notify_one(); }
	}
	impl Services {
		/// Pause the next actual backfill page after its index and cursor
		/// commit. Process-owned debug fixture only; dropping the owner
		/// releases the page.
		pub fn pause_notification_index_migration_for_test() -> NotificationIndexMigrationPause {
			let (entered, receiver) = oneshot::channel();
			let release = Arc::new(Notify::new());
			let mut gate = PAUSE.lock().expect("owned migration gate");
			assert!(gate.is_none(), "one process-owned migration pause");
			*gate = Some(Gate { entered, release: release.clone() });
			NotificationIndexMigrationPause { entered: receiver, release }
		}
	}
}

#[cfg(all(feature = "notification_recovery_tests", debug_assertions))]
async fn pause_after_commit(services: &Services, after: &[u8]) {
	let pause = test_pause::PAUSE
		.lock()
		.expect("owned migration gate")
		.take();
	if let Some(pause) = pause {
		pause.entered.send(after.to_vec()).ok();
		tokio::select! {
			() = pause.release.notified() => {},
			() = services.server.until_shutdown() => {},
		}
	}
}
