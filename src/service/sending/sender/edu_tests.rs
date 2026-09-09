//! Reference-backend consumer regressions. No workers, listeners, or cloud
//! resources are started; each case owns an exclusively created scratch root.

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{collections::HashSet, fs, sync::Arc};

use futures::TryStreamExt;
use tokio::runtime::Handle;
use tracing::subscriber::NoSubscriber;
use tuwunel_core::{
	Result, Server,
	config::{Config, Figment, Sources},
	log::{LogLevelReloadHandles, Logging, capture::State},
	metrics::Metrics,
	ruma::{ServerName, UserId, room_id, server_name, user_id},
	utils::{rand, sys},
};
use tuwunel_database::serialize_key;

use super::{
	CurTransactionStatus, Destination, EDU_WINDOW_COUNTS, EduBuf, SendingFutures, edu_window_end,
};
use crate::Services;

struct Fixture {
	services: Arc<Services>,
}

impl Fixture {
	async fn new() -> Result<Self> {
		// Match the main runtime's per-process descriptor setup. A service
		// graph is process-lifetime (OnceServices has strong references), so
		// the outer test runner owns scratch cleanup after this process exits.
		sys::maximize_fd_limit()?;
		let root =
			std::env::temp_dir().join(format!("matrix-edu-reference-{}", rand::string(20)));
		let mut directory = fs::DirBuilder::new();
		#[cfg(unix)]
		directory.mode(0o700);
		directory.create(&root)?;
		let raw = Figment::new()
			.merge(("server_name", "localhost"))
			.merge(("database_backend", "rocksdb"))
			.merge(("database_path", root.join("database")))
			.merge(("allow_outgoing_presence", true))
			.merge(("allow_outgoing_read_receipts", true))
			.merge(("startup_netburst", true))
			.merge(("startup_netburst_keep", -1));
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
		let services = Services::build(server).await?;
		Ok(Self { services })
	}

	async fn finish(self) { self.services.stop().await; }
}

async fn assert_empty_outgoing(services: &Services, server: &ServerName) -> Result {
	let destination = Destination::Federation(server.to_owned());
	assert_eq!(
		services
			.sending
			.db
			.get_latest_educount(server)
			.await?,
		0
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
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_overflow_and_watermark_share_the_persistence_operation() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let server = server_name!("remote.example");
	let destination = Destination::Federation(server.to_owned());
	let active = EduBuf::from_slice(br#"{"selected":true}"#);
	let overflow = EduBuf::from_slice(br#"{"overflow":true}"#);
	services
		.sending
		.db
		.persist_edus(server, std::slice::from_ref(&active), std::slice::from_ref(&overflow), 37)
		.await?;
	assert_eq!(
		services
			.sending
			.db
			.get_latest_educount(server)
			.await?,
		37
	);
	let active_rows = services
		.sending
		.db
		.active_requests_for(&destination)
		.try_collect::<Vec<_>>()
		.await?;
	let queued_rows = services
		.sending
		.db
		.queued_requests(&destination)
		.try_collect::<Vec<_>>()
		.await?;
	assert_eq!(active_rows.len(), 1);
	assert_eq!(queued_rows.len(), 1);
	assert_eq!(active_rows[0].1.value_bytes(), active.as_slice());
	assert_eq!(queued_rows[0].1.value_bytes(), overflow.as_slice());
	assert_ne!(active_rows[0].0, queued_rows[0].0);
	services
		.sending
		.db
		.pending_edu_destinations(36)
		.try_collect::<Vec<_>>()
		.await
		.expect_err("startup must not silently ignore a cursor beyond retired writes");
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_source_rows_never_advance_the_edu_watermark() -> Result {
	for source in ["rooms", "device_changes", "receipts", "presence"] {
		let fixture = Fixture::new().await?;
		let services = &fixture.services;
		let server = server_name!("remote.example");
		let room = room_id!("!edu:localhost");
		let user = user_id!("@edu:localhost");
		let permit = services.globals.next_count().await?;
		let count = *permit;
		drop(permit);
		services.db["serverroomids"]
			.put_raw((server, room), [])
			.await?;
		match source {
			| "rooms" => {
				services.db["serverroomids"]
					.put_raw((server, "not-a-room"), [])
					.await?;
			},
			| "device_changes" => {
				services.db["keychangeid_userid"]
					.put_raw((room, count), b"not-a-user")
					.await?;
			},
			| "receipts" => {
				let key = serialize_key((room, count, user, ""))?;
				services.db["readreceiptid_readreceipt"]
					.raw_put(key, b"not-json")
					.await?;
			},
			| "presence" => {
				services.db["userroomid_joined"]
					.put_raw((user, room), [])
					.await?;
				let mut key = count.to_be_bytes().to_vec();
				key.extend_from_slice(user.as_bytes());
				services.db["presenceid_presence"]
					.raw_put(key, b"not-json")
					.await?;
			},
			| _ => unreachable!("declared source"),
		}
		services
			.sending
			.select_edus(server, 0)
			.await
			.expect_err(source);
		assert_empty_outgoing(services, server).await?;
		fixture.finish().await;
	}
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_later_device_change_cannot_skip_the_presence_tail() -> Result {
	for budget_used in [0, 99] {
		verify_presence_tail(budget_used).await?;
	}
	Ok(())
}

async fn verify_presence_tail(budget_used: usize) -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let server = server_name!("remote.example");
	let room = room_id!("!edu-window:localhost");
	services.db["serverroomids"]
		.put_raw((server, room), [])
		.await?;
	let mut expected = HashSet::new();
	for index in 0..300 {
		let user = UserId::parse(format!("@presence{index}:localhost"))?;
		services.db["userroomid_joined"]
			.put_raw((&user, room), [])
			.await?;
		let permit = services.globals.next_count().await?;
		let mut key = permit.to_be_bytes().to_vec();
		key.extend_from_slice(user.as_bytes());
		services.db["presenceid_presence"].raw_put(key,
			br#"{"state":"online","currently_active":false,"last_active_ts":0,"status_msg":null}"#.as_slice()).await?;
		drop(permit);
		expected.insert(user.to_string());
	}
	// This other source is beyond the presence selector's former 256-user cap.
	// Taking the maximum cursor across sources would acknowledge unseen rows.
	let permit = services.globals.next_count().await?;
	services.db["keychangeid_userid"]
		.put_raw((room, *permit), user_id!("@device:localhost"))
		.await?;
	drop(permit);

	let mut received = HashSet::new();
	for _ in 0..16 {
		if services
			.sending
			.db
			.get_latest_educount(server)
			.await? >= services.globals.current_count()
		{
			break;
		}
		let edus = services
			.sending
			.select_edus(server, budget_used)
			.await?;
		assert!(edus.len() <= super::EDU_LIMIT.saturating_sub(budget_used));
		for edu in edus {
			let value: serde_json::Value = serde_json::from_slice(&edu)?;
			if value["edu_type"] == "m.presence" {
				for update in value["content"]["push"]
					.as_array()
					.expect("presence updates")
				{
					received.insert(
						update["user_id"]
							.as_str()
							.expect("presence user")
							.to_owned(),
					);
				}
			}
		}
	}
	assert_eq!(
		received.len(),
		expected.len(),
		"a shared cursor must not skip the capped source's tail"
	);
	assert_eq!(received, expected);
	fixture.finish().await;
	Ok(())
}

#[test]
fn counter_windows_are_bounded_complete_and_overflow_safe() {
	assert_eq!(edu_window_end(0, 1_000).expect("first window"), EDU_WINDOW_COUNTS);
	assert_eq!(edu_window_end(100, 101).expect("partial window"), 101);
	assert_eq!(edu_window_end(100, 100).expect("caught up"), 100);
	assert_eq!(edu_window_end(u64::MAX - 1, u64::MAX).expect("last count"), u64::MAX);
	edu_window_end(101, 100).expect_err("a cursor beyond retired writes is corrupt");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_reconstructs_an_empty_window_wake_from_the_durable_cursor() -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let server = server_name!("remote.example");
	let destination = Destination::Federation(server.to_owned());
	for _ in 0..300 {
		drop(services.globals.next_count().await?);
	}
	services
		.sending
		.db
		.persist_edus(server, &[], &[], 0)
		.await?;
	let pending = services
		.sending
		.db
		.pending_edu_destinations(services.globals.current_count())
		.try_collect::<Vec<_>>()
		.await?;
	assert_eq!(pending, vec![destination.clone()]);
	let id = services.sending.shard_id(&destination);
	let mut futures = SendingFutures::new();
	let mut statuses = CurTransactionStatus::new();
	services
		.sending
		.startup_netburst(id, &mut futures, &mut statuses)
		.await?;
	assert!(futures.is_empty(), "counter holes never require a remote transaction");
	assert_eq!(
		services
			.sending
			.db
			.get_latest_educount(server)
			.await?,
		EDU_WINDOW_COUNTS
	);
	let wake = services.sending.channels[id]
		.1
		.try_recv()
		.expect("next window wake");
	assert_eq!(wake.dest, destination);
	// Discard the sole process-local wake/bookkeeping and reconstruct again.
	statuses.clear();
	services
		.sending
		.startup_netburst(id, &mut futures, &mut statuses)
		.await?;
	assert_eq!(
		services
			.sending
			.db
			.get_latest_educount(server)
			.await?,
		EDU_WINDOW_COUNTS.saturating_mul(2)
	);
	assert!(futures.is_empty());
	drop(futures);
	fixture.finish().await;
	Ok(())
}
