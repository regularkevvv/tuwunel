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

pub(super) struct Fixture {
	pub(super) services: Arc<Services>,
}

impl Fixture {
	/// Queue identity controls use opaque canonical rows and never decode PDUs.
	/// Retain the referenced row so they exercise the real admission
	/// precondition.
	pub(super) async fn retain_pdu(&self, event: &super::SendingEvent) -> Result {
		if let Some(raw) = event.pdu_id() {
			self.services.db["pduid_pdu"]
				.insert(raw.as_ref(), b"{}".as_slice())
				.await?;
		}
		Ok(())
	}

	pub(super) async fn new() -> Result<Self> {
		// Match the main runtime's descriptor setup. The outer test runner
		// owns scratch cleanup after this process exits.
		sys::maximize_fd_limit()?;
		let root =
			std::env::temp_dir().join(format!("matrix-edu-reference-{}", rand::string(20)));
		let mut directory = fs::DirBuilder::new();
		#[cfg(unix)]
		directory.mode(0o700);
		directory.create(&root)?;
		Self::open(&root).await
	}

	async fn new_key_fanout() -> Result<Self> {
		let root = std::env::temp_dir().join(format!("matrix-key-fanout-{}", rand::string(20)));
		let mut directory = fs::DirBuilder::new();
		#[cfg(unix)]
		directory.mode(0o700);
		directory.create(&root)?;
		Self::open_config(&root, false, None, true).await
	}

	/// Open only a scratch root owned by the invoking fixture. Cold-start
	/// controls reopen it in a new process after the preceding child exits.
	pub(super) async fn open(root: &std::path::Path) -> Result<Self> {
		Self::open_options(root, false).await
	}

	/// Exercise the real service startup order on a current-schema scratch DB.
	/// No migration or outbound federation is part of this control.
	pub(super) async fn open_startup(root: &std::path::Path) -> Result<Self> {
		Self::open_options(root, true).await
	}

	async fn open_options(root: &std::path::Path, startup: bool) -> Result<Self> {
		Self::open_config(root, startup, None, false).await
	}

	/// Open only the owned loopback bridge oracle for remote service controls.
	pub(super) async fn open_remote(root: &std::path::Path, url: &str) -> Result<Self> {
		Self::open_config(root, false, Some(url), false).await
	}

	async fn open_config(
		root: &std::path::Path,
		startup: bool,
		remote: Option<&str>,
		all_key_rooms: bool,
	) -> Result<Self> {
		sys::maximize_fd_limit()?;
		let mut raw = Figment::new()
			.merge(("server_name", "localhost"))
			.merge(("database_backend", "rocksdb"))
			.merge(("database_path", root.join("database")))
			.merge(("device_key_update_encrypted_rooms_only", !all_key_rooms))
			.merge(("allow_outgoing_presence", true))
			.merge(("allow_outgoing_read_receipts", true))
			.merge(("startup_netburst", true))
			.merge(("startup_netburst_keep", -1));
		if let Some(url) = remote {
			raw = raw
				.merge(("database_backend", "d1"))
				.merge(("d1_bridge_url", url))
				.merge(("d1_bridge_token", crate::bridge_fixture::TOKEN))
				.merge(("d1_read_cache_mb", 1))
				.merge(("d1_lease_ttl_ms", 15_000));
		}
		if startup {
			raw = raw
				.merge(("database_migrations", false))
				.merge(("allow_federation", false))
				.merge(("startup_netburst", false));
		}
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
		// This lower-level fixture does not run startup migrations. Declare the
		// supported schema before exercising production active-row writers.
		services
			.globals
			.db
			.bump_database_version(crate::migrations::DATABASE_VERSION)
			.await?;
		Ok(Self { services })
	}

	pub(super) async fn finish(self) { self.services.stop().await; }
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
		.pending_edu_destinations(36, None, 256)
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
	let (pending, next) = services
		.sending
		.db
		.pending_edu_destinations(services.globals.current_count(), None, 256)
		.await?;
	assert_eq!(pending, vec![destination.clone()]);
	assert!(next.is_none(), "one watermark is a whole batch's end");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_key_fanout_proves_complete_sources_before_marking_change() -> Result {
	for source in ["corrupt_room", "corrupt_server", "server_overflow", "server_bytes_overflow"] {
		let fixture = Fixture::new_key_fanout().await?;
		let services = &fixture.services;
		let user = user_id!("@fanout:localhost");
		let room = room_id!("!fanout:localhost");
		services.db["userroomid_joined"]
			.put_raw((user, room), 1_u64.to_be_bytes())
			.await?;
		match source {
			| "corrupt_room" =>
				services.db["userroomid_joined"]
					.put_raw((user, "not-a-room"), [])
					.await?,
			| "corrupt_server" =>
				services.db["roomserverids"]
					.put_raw((room, "not a server"), [])
					.await?,
			| "server_overflow" => {
				let mut txn = services.db.txn();
				for index in 0..4097 {
					txn.put_raw(
						&services.db["roomserverids"],
						(room, format!("s{index}.example")),
						[],
					);
				}
				txn.execute().await?;
			},
			| "server_bytes_overflow" => {
				let mut txn = services.db.txn();
				for index in 0..1100 {
					let server = format!(
						"s{index:04}.{}.{}.{}.{}",
						"a".repeat(60),
						"b".repeat(60),
						"c".repeat(60),
						"d".repeat(60)
					);
					txn.put_raw(&services.db["roomserverids"], (room, server), []);
				}
				txn.execute().await?;
			},
			| _ => unreachable!("declared source"),
		}
		services
			.users
			.mark_device_key_update(user)
			.await
			.expect_err("incomplete fanout must refuse");
		assert!(
			services.db["keychangeid_userid"]
				.raw_stream()
				.map_ok(|_| ())
				.try_collect::<Vec<_>>()
				.await?
				.is_empty()
		);
		assert!(
			services.db["servernameevent_data"]
				.raw_stream()
				.map_ok(|_| ())
				.try_collect::<Vec<_>>()
				.await?
				.is_empty()
		);
		fixture.finish().await;
	}
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_key_fanout_and_appservice_queue_share_mutation_transaction() -> Result {
	let fixture = Fixture::new_key_fanout().await?;
	let services = &fixture.services;
	let user = user_id!("@fanout:localhost");
	let room = room_id!("!fanout:localhost");
	services.db["userroomid_joined"]
		.put_raw((user, room), 1_u64.to_be_bytes())
		.await?;
	let plan = services
		.users
		.prepare_device_key_update(user)
		.await?;
	let mut txn = services.db.txn();
	services
		.users
		.append_device_key_update(user, &plan, &mut txn);
	// An abandoned cleanup transaction cannot publish any notification marker.
	drop(txn);
	assert!(
		services.db["keychangeid_userid"]
			.raw_stream()
			.map_ok(|_| ())
			.try_collect::<Vec<_>>()
			.await?
			.is_empty()
	);
	let mut txn = services.db.txn();
	services
		.users
		.append_device_key_update(user, &plan, &mut txn);
	txn.check_bridge_admission()?;
	txn.execute().await?;
	assert_eq!(
		services.db["keychangeid_userid"]
			.raw_stream()
			.map_ok(|_| ())
			.try_collect::<Vec<_>>()
			.await?
			.len(),
		2
	);
	drop(plan);
	let count = services.globals.next_count().await?;
	let admission = services
		.sending
		.prepare_device_list_admission(user, *count, crate::sending::PreparedDeviceListTargets {
			ids: vec!["bridge".to_owned()],
			registrations: services.appservice.read_owned().await,
		})
		.await?;
	let mut txn = services.db.txn();
	admission.append(&services.db, &mut txn);
	drop(txn);
	assert!(
		services.db["servernameevent_data"]
			.raw_stream()
			.map_ok(|_| ())
			.try_collect::<Vec<_>>()
			.await?
			.is_empty()
	);
	let mut txn = services.db.txn();
	admission.append(&services.db, &mut txn);
	txn.check_bridge_admission()?;
	txn.execute().await?;
	assert_eq!(
		services.db["servernameevent_data"]
			.raw_stream()
			.map_ok(|_| ())
			.try_collect::<Vec<_>>()
			.await?
			.len(),
		1
	);
	drop(admission);
	drop(count);
	let count = services.globals.next_count().await?;
	match services
		.sending
		.prepare_device_list_admission(user, *count, crate::sending::PreparedDeviceListTargets {
			ids: vec!["a".repeat(tuwunel_bridge::MAX_KEY_BYTES)],
			registrations: services.appservice.read_owned().await,
		})
		.await
	{
		| Ok(_) => panic!("over-width queue identity must be refused before cleanup"),
		| Err(_) => {},
	}
	drop(count);
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removed_device_cannot_readmit_key_material() -> Result {
	let fixture = Fixture::new_key_fanout().await?;
	let services = &fixture.services;
	let user = user_id!("@removed:localhost");
	let device = ruma::device_id!("REMOVED");
	let keys: ruma::serde::Raw<ruma::encryption::DeviceKeys> = serde_json::from_str("{}")?;
	services
		.users
		.add_device_keys(user, device, &keys)
		.await
		.expect_err("removed metadata must prevent key reinsertion");
	services
		.users
		.add_one_time_keys(user, device, &std::collections::BTreeMap::new(), 0)
		.await
		.expect_err("removed metadata must prevent one-time admission");
	services
		.users
		.add_fallback_keys(
			user,
			device,
			std::iter::empty::<(
				&ruma::OneTimeKeyId,
				&ruma::serde::Raw<ruma::encryption::OneTimeKey>,
			)>(),
		)
		.await
		.expect_err("removed metadata must prevent fallback admission");
	for map in [
		"keyid_key",
		"onetimekeyid4225_otk",
		"userdeviceidalgorithm_fallback",
		"keychangeid_userid",
	] {
		assert!(
			services.db[map]
				.raw_stream()
				.map_ok(|_| ())
				.try_collect::<Vec<_>>()
				.await?
				.is_empty()
		);
	}
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_key_admission_excludes_appservice_unregister_until_commit() -> Result {
	use std::time::Duration;

	use ruma::api::appservice::{Namespaces, Registration, RegistrationInit};
	use tokio::{sync::oneshot, time::timeout};

	let fixture = Fixture::new_key_fanout().await?;
	let services = &fixture.services;
	<crate::appservice::Service as crate::Service>::worker(services.appservice.clone()).await?;
	let make_registration = |suffix: &str| -> Result<Registration> {
		let mut registration: Registration = RegistrationInit {
			id: "key-admission-owner".into(),
			url: Some("http://127.0.0.1:1".into()),
			as_token: format!("disposable-key-admission-as-{suffix}"),
			hs_token: format!("disposable-key-admission-hs-{suffix}"),
			sender_localpart: "key-admission-owner".into(),
			namespaces: serde_json::from_value::<Namespaces>(serde_json::json!({
				"users": [{"exclusive": false, "regex": "^@fanout:localhost$"}]
			}))?,
			rate_limited: None,
			protocols: None,
		}
		.into();
		registration.msc3202_transaction_extensions = true;
		Ok(registration)
	};
	services
		.appservice
		.register_appservice(make_registration("first")?)
		.await?;
	let user = user_id!("@fanout:localhost");
	let targets = services
		.sending
		.prepare_device_list_appservices(user, &[])
		.await?;
	let (entered_tx, entered_rx) = oneshot::channel();
	let appservice = services.appservice.clone();
	let mut removal = tokio::spawn(async move {
		entered_tx
			.send(())
			.expect("owned entry observation");
		appservice
			.unregister_appservice("key-admission-owner")
			.await
	});
	entered_rx.await.expect("unregister task entered");
	timeout(Duration::from_millis(50), &mut removal)
		.await
		.expect_err("unregister must wait for the selected registration's admission");
	let count = services.globals.next_count().await?;
	let admission = services
		.sending
		.prepare_device_list_admission(user, *count, targets)
		.await?;
	let mut txn = services.db.txn();
	admission.append(&services.db, &mut txn);
	txn.check_bridge_admission()?;
	txn.execute().await?;
	assert_eq!(
		services.db["servernameevent_data"]
			.raw_stream()
			.map_ok(|_| ())
			.try_collect::<Vec<_>>()
			.await?
			.len(),
		1
	);
	timeout(Duration::from_millis(50), &mut removal)
		.await
		.expect_err("unregister must remain excluded through the coupled commit");
	drop(admission);
	drop(count);
	timeout(Duration::from_secs(10), removal)
		.await
		.expect("unregister completes after admission releases")
		.expect("unregister task completed")?;
	assert!(
		services.db["servernameevent_data"]
			.raw_stream()
			.map_ok(|_| ())
			.try_collect::<Vec<_>>()
			.await?
			.is_empty()
	);
	services
		.appservice
		.register_appservice(make_registration("replacement")?)
		.await?;
	let destination = Destination::Appservice("key-admission-owner".into());
	assert!(
		services
			.sending
			.db
			.queued_requests(&destination)
			.try_collect::<Vec<_>>()
			.await?
			.is_empty(),
		"replacement registration must not inherit a stale device-list marker"
	);
	fixture.finish().await;
	Ok(())
}
