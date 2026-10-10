//! Journal identities follow the durably persisted dispatch frontier, while
//! an unrelated in-flight canonical write may hold reader visibility back.

use std::iter::once;

use ruma::server_name;
use tuwunel_core::Result;
use tuwunel_database::refusal;

use super::super::{Destination, SendingEvent, active};
use crate::{Services, sending::EduBuf, services::startup_tests};

#[test]
fn persisted_active_attempt_can_acknowledge_while_an_earlier_counter_permit_is_held() -> Result {
	startup_tests::isolated(
		"sending::data::ack::frontier_tests::persisted_active_attempt_can_acknowledge_while_an_earlier_counter_permit_is_held",
		async |root| {
			let services = startup_tests::services(root).await?;
			let outcome = exercise(&services).await;
			services.stop().await;
			outcome
		},
	)
}

async fn exercise(services: &Services) -> Result {
	let held = services.globals.next_count().await?;
	let retired = services.globals.current_count();
	assert!(retired < *held);
	let before_refusal = services.globals.pending_count().end;
	refusal::refuse_next("global");
	assert!(
		services.globals.next_count().await.is_err(),
		"failed counter persistence refuses dispatch"
	);
	assert_eq!(refusal::pending(), 0);
	assert_eq!(
		services.globals.pending_count().end,
		before_refusal,
		"no uncommitted reservation becomes dispatched"
	);

	let destination = Destination::Federation(server_name!("frontier.invalid").to_owned());
	let event = SendingEvent::Edu(EduBuf::from_slice(b"{}"));
	let data = &services.sending.db;
	let keys = data
		.queue_requests(once((&event, &destination)))
		.await?;
	let item = (keys[0].clone(), event.clone());
	data.mark_as_active(once(&item)).await?;
	assert_eq!(
		services.globals.current_count(),
		retired,
		"earlier permit still owns visibility"
	);
	let (events, selected) = data.active_batch(&destination).await?;
	assert_eq!(events, vec![event.clone()]);
	let valid = selected.rows[0].1.clone();
	let identity = active::identity(&valid)?.expect("new promotion owns an identity");
	assert!(identity > retired);
	assert!(identity <= services.globals.pending_count().end);
	let persisted = services.db["global"].get(b"c").await?;
	assert_eq!(
		u64::from_be_bytes(
			persisted
				.as_ref()
				.try_into()
				.expect("counter width")
		),
		services.globals.pending_count().end
	);

	let attempt = data
		.persist_attempt(&destination, selected, b"{}".to_vec(), None)
		.await?;
	let loaded = data
		.load_attempt(&destination)
		.await?
		.expect("persisted HTTP attempt reloads");
	assert_eq!(loaded.body, attempt.body);
	data.require_current_attempt(&destination, &attempt)
		.await?;
	data.acknowledge_active(&destination, &attempt.acknowledgement)
		.await?;
	assert_eq!(data.active_batch(&destination).await?.0.len(), 0);
	assert_eq!(
		services.db["sendingtransaction_record"]
			.count()
			.await,
		0
	);
	assert_eq!(
		services.globals.current_count(),
		retired,
		"ACK does not retire another writer's permit"
	);

	// The persisted frontier is not an excuse to accept a future incarnation
	// or a truncated identity envelope, nor to delete corrupt owed rows.
	let active_rows = &services.db["servercurrentevent_data"];
	let future = active::encode(
		&event,
		services
			.globals
			.pending_count()
			.end
			.checked_add(1)
			.expect("fixture counter fits"),
	)?;
	active_rows.insert(&keys[0], &future).await?;
	assert!(data.active_batch(&destination).await.is_err());
	assert_eq!(active_rows.get(&keys[0]).await?.as_ref(), future);
	let malformed = &valid[..9];
	active_rows.insert(&keys[0], malformed).await?;
	assert!(data.active_batch(&destination).await.is_err());
	assert_eq!(active_rows.get(&keys[0]).await?.as_ref(), malformed);
	active_rows.remove(&keys[0]).await?;
	drop(held);
	assert_eq!(services.globals.current_count(), services.globals.pending_count().end);
	Ok(())
}
