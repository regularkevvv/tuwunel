//! Bounded push cancellation staged with credential revocation.
use std::sync::Arc;

use ruma::UserId;
use tuwunel_core::{Error, Result};
use tuwunel_database::{Map, Txn};

use super::{Destination, Service};

pub(crate) struct PreparedPushCleanup {
	keys: Vec<(Arc<Map>, Vec<u8>)>,
	owners: Vec<Destination>,
}

impl Service {
	pub(crate) async fn prepare_device_push_cleanup(
		&self,
		user: &UserId,
		pushkeys: &[String],
		admission: Option<&crate::users::DeviceKeyUpdatePlan>,
	) -> Result<PreparedPushCleanup> {
		if !pushkeys.is_empty() && admission.is_none() {
			return Err(Error::bad_database("Missing push cleanup admission guard"));
		}
		// Caller retains DeviceKeyUpdatePlan sender admission through this preparation
		// and commit.
		let mut plan = PreparedPushCleanup { keys: Vec::new(), owners: Vec::new() };
		let mut bytes = 0_usize;
		for pushkey in pushkeys {
			let destination = Destination::Push(user.to_owned(), pushkey.clone());
			let prefix = destination.get_prefix();
			for map in [&self.db.servercurrentevent_data, &self.db.servernameevent_data] {
				let keys = map
					.raw_keys_prefix_after(
						&prefix,
						None,
						901_usize.saturating_sub(plan.keys.len()),
					)
					.await?;
				for key in keys {
					bytes = bytes.saturating_add(key.len());
					if plan.keys.len() >= 900 || bytes > 512 * 1024 {
						return Err(Error::bad_database(
							"Push revocation cleanup exceeds inventory budget",
						));
					}
					plan.keys.push((map.clone(), key));
				}
			}
			plan.owners.push(destination);
		}
		Ok(plan)
	}

	pub(crate) fn append_device_push_cleanup(
		&self,
		plan: &PreparedPushCleanup,
		txn: &mut Txn,
	) -> Result {
		for (map, key) in &plan.keys {
			txn.del_raw(map, key);
		}
		for destination in &plan.owners {
			self.db
				.stage_clear_push_backoff(txn, destination)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use futures::TryStreamExt;
	use tuwunel_core::ruma::{DeviceId, user_id};

	use super::*;
	use crate::{
		sending::{
			SendingEvent,
			data::{ActiveAcknowledgement, PushBackoff},
		},
		users::device_removal_tests::{device, fixture},
	};

	async fn activate_push(
		services: &crate::Services,
		destination: &Destination,
		event: &SendingEvent,
	) -> Result<ActiveAcknowledgement> {
		let data = &services.sending.db;
		data.queue_requests(std::iter::once((event, destination)))
			.await?;
		let queued = data
			.queued_requests(destination)
			.try_collect::<Vec<_>>()
			.await?;
		data.mark_as_active(queued.iter()).await?;
		let (_, acknowledgement) = data.active_batch(destination).await?;
		Ok(acknowledgement)
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn revoked_push_late_ack_and_failure_cannot_change_readmitted_delivery() -> Result {
		let services = fixture().await?;
		let user = user_id!("@revoke:localhost");
		let id: &DeviceId = "DEVICE".into();
		device(&services, user, id).await?;
		let destination = Destination::Push(user.to_owned(), "pushkey".to_owned());
		let mut raw = [0_u8; 16];
		raw[7] = 1;
		raw[15] = 1;
		let raw = crate::rooms::timeline::RawPduId::from_bytes(&raw)?;
		services.db["pduid_pdu"]
			.insert(raw.as_ref(), b"{}".as_slice())
			.await?;
		let event = SendingEvent::Pdu(raw);

		let old = activate_push(&services, &destination, &event).await?;
		assert!(
			services
				.sending
				.db
				.persist_push_backoff(&destination, &old, PushBackoff::failed(1)?)
				.await?
		);
		let plan = services
			.users
			.prepare_all_device_removals(user)
			.await?;
		let mut txn = services.db.txn();
		services
			.users
			.append_device_removals(&plan, &mut txn)?;
		txn.execute().await?;
		drop(plan);
		assert!(
			services
				.sending
				.db
				.push_backoff(&destination)
				.await?
				.is_none()
		);
		assert!(
			services
				.sending
				.db
				.active_requests_for(&destination)
				.try_collect::<Vec<_>>()
				.await?
				.is_empty()
		);
		assert!(
			services
				.sending
				.db
				.queued_requests(&destination)
				.try_collect::<Vec<_>>()
				.await?
				.is_empty()
		);
		// Reuse the exact logical event and destination. The persisted active
		// identity, not its payload or key, must distinguish the successor.
		let current = activate_push(&services, &destination, &event).await?;
		assert_eq!(
			old.selected_rows()[0].0,
			current.selected_rows()[0].0,
			"the same physical key was reused"
		);
		assert_ne!(
			old.selected_rows()[0].1,
			current.selected_rows()[0].1,
			"successor owns a new admission identity"
		);

		assert!(
			services
				.sending
				.db
				.persist_push_backoff(&destination, &current, PushBackoff::failed(2)?)
				.await?
		);
		let before = services
			.sending
			.db
			.active_requests_for(&destination)
			.try_collect::<Vec<_>>()
			.await?;
		services
			.sending
			.db
			.acknowledge_active(&destination, &old)
			.await?;
		assert!(
			!services
				.sending
				.db
				.persist_push_backoff(&destination, &old, PushBackoff::failed(3)?)
				.await?
		);
		assert_eq!(
			services
				.sending
				.db
				.active_requests_for(&destination)
				.try_collect::<Vec<_>>()
				.await?,
			before
		);
		assert!(
			services
				.sending
				.db
				.push_backoff(&destination)
				.await?
				.is_some()
		);
		services
			.sending
			.db
			.acknowledge_active(&destination, &current)
			.await?;
		assert!(
			services
				.sending
				.db
				.active_requests_for(&destination)
				.try_collect::<Vec<_>>()
				.await?
				.is_empty()
		);
		assert!(
			services
				.sending
				.db
				.push_backoff(&destination)
				.await?
				.is_none()
		);
		services.stop().await;
		Ok(())
	}
}
