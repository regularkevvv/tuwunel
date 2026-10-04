//! Actual service operations share the membership exclusion. No workers,
//! listeners or cloud resources are started; the test runner owns scratch.

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{fs, sync::Arc};

use futures::FutureExt;
use tokio::{runtime::Handle, time::timeout};
use tracing::subscriber::NoSubscriber;
use tuwunel_core::{
	Error, Result, Server,
	config::{Config, Figment, Sources},
	log::{LogLevelReloadHandles, Logging, capture::State},
	metrics::Metrics,
	ruma::{RoomId, room_id, user_id},
	utils::{rand, sys},
};

use super::Service;
use crate::Services;

async fn services() -> Result<Arc<Services>> {
	sys::maximize_fd_limit()?;
	let root =
		std::env::temp_dir().join(format!("matrix-recount-exclusion-{}", rand::string(20)));
	let mut directory = fs::DirBuilder::new();
	#[cfg(unix)]
	directory.mode(0o700);
	directory.create(&root)?;
	let raw = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_backend", "rocksdb"))
		.merge(("database_path", root.join("database")));
	let config = Config::new(&raw)?;
	let runtime = Handle::current();
	let log = Logging {
		subscriber: Arc::new(NoSubscriber::new()),
		reload: LogLevelReloadHandles::default(),
		capture: Arc::new(State::new()),
	};
	let server = Arc::new(Server::new(
		config,
		Sources::default(),
		Some(&runtime),
		log,
		Metrics::new(Some(&runtime)),
	));
	Services::build(server).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_recount_and_count_reads_share_the_actual_room_exclusion() -> Result {
	let services = services().await?;
	let outcome = timeout(std::time::Duration::from_secs(120), exclusion(&services)).await;
	services.stop().await;
	outcome.expect("membership exclusion fixture deadline")
}

async fn exclusion(services: &Services) -> Result {
	let room = room_id!("!recount-exclusion:localhost");
	let other = room_id!("!recount-independent:localhost");
	let user = user_id!("@recount-exclusion:localhost");
	let state = &services.state_cache;
	state.update_joined_count(room).await?;
	state.update_joined_count(other).await?;
	let guard = state.membership_mutex.lock(room).await;
	let mut txn = services.db.txn();
	txn.put_raw(&services.db["roomuserid_joined"], (room, user), 1_u64.to_be_bytes());
	txn.put_raw(&services.db["userroomid_joined"], (user, room), 1_u64.to_be_bytes());
	let mut write = Box::pin(state.commit_membership(room, txn));
	let mut recount = Box::pin(state.update_joined_count(room));
	let mut joined = Box::pin(state.room_joined_count(room));
	let mut invited = Box::pin(state.room_invited_count(room));
	let mut knocked = Box::pin(state.room_knocked_count(room));
	let mut deletion = Box::pin(state.delete_room_join_counts(room, true));
	assert!(write.as_mut().now_or_never().is_none(), "membership cannot overtake recount");
	assert!(
		recount.as_mut().now_or_never().is_none(),
		"aggregate repair waits for exclusion"
	);
	assert!(joined.as_mut().now_or_never().is_none(), "joined read waits for exclusion");
	assert!(invited.as_mut().now_or_never().is_none(), "invited read waits for exclusion");
	assert!(knocked.as_mut().now_or_never().is_none(), "knocked read waits for exclusion");
	assert!(
		deletion.as_mut().now_or_never().is_none(),
		"deletion waits before preparing its batch"
	);
	drop(deletion);
	assert_eq!(state.room_joined_count(other).await?, 0, "other rooms remain independent");
	assert!(
		services.db["roomuserid_joined"]
			.qry(&(room, user))
			.await
			.expect_err("blocked membership has not committed")
			.is_not_found(),
		"exclusion precedes the durable membership batch"
	);
	drop(guard);
	write.await?;
	assert!(
		services.db["global"]
			.qry(&("membership_recount_pending", room))
			.await?
			.is_empty(),
		"membership published its obligation before waiting repair runs"
	);
	recount.await?;
	assert_eq!(joined.await?, 1, "serialized recount includes the newly committed member");
	assert_eq!(invited.await?, 0, "serialized invited count remains complete");
	assert_eq!(knocked.await?, 0, "serialized knocked count remains complete");
	assert!(
		services.db["global"]
			.qry(&("membership_recount_pending", room))
			.await
			.expect_err("completed recount clears marker")
			.is_not_found(),
		"no new membership obligation was lost"
	);
	strict_counts(services, room).await
}

async fn read_count(state: &Service, room: &RoomId, kind: usize) -> Result<u64> {
	match kind {
		| 0 => state.room_joined_count(room).await,
		| 1 => state.room_invited_count(room).await,
		| 2 => state.room_knocked_count(room).await,
		| _ => unreachable!("owned counter kind"),
	}
}

async fn strict_counts(services: &Services, room: &RoomId) -> Result {
	for (kind, (map, expected)) in [
		("roomid_joinedcount", 1_u64),
		("roomid_invitedcount", 0),
		("roomid_knockedcount", 0),
	]
	.into_iter()
	.enumerate()
	{
		for length in [0_usize, 7, 9] {
			let bytes = vec![0_u8; length];
			services.db[map]
				.raw_put(room.as_bytes(), bytes.as_slice())
				.await?;
			assert!(
				matches!(
					read_count(&services.state_cache, room, kind)
						.await
						.expect_err("malformed aggregate refuses public read"),
					Error::Database(_)
				),
				"every counter requires an exact stored encoding"
			);
			assert_eq!(
				services.db[map]
					.get(room.as_bytes())
					.await?
					.as_ref(),
				bytes,
				"read refusal preserves the malformed stored count"
			);
		}
		services.db[map]
			.raw_put(room.as_bytes(), expected.to_be_bytes())
			.await?;
		assert_eq!(
			read_count(&services.state_cache, room, kind).await?,
			expected,
			"restored encoding restores the public count"
		);
	}
	Ok(())
}
