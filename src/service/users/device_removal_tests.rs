//! The actual native store must atomically invalidate every token and refuse
//! corrupt/oversized inventories before deleting any of the account's data.
#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{fs, sync::Arc, time::Duration};

use tokio::runtime::Handle;
use tracing::subscriber::NoSubscriber;
use tuwunel_core::{
	Result, Server,
	config::{Config, Figment, Sources},
	log::{LogLevelReloadHandles, Logging, capture::State},
	metrics::Metrics,
	ruma::{DeviceId, UserId, user_id},
	utils::{rand, sys},
};
use tuwunel_database::{Json, serialize_key};

use crate::Services;

pub(crate) async fn fixture() -> Result<Arc<Services>> {
	sys::maximize_fd_limit()?;
	let root =
		std::env::temp_dir().join(format!("matrix-device-revocation-{}", rand::string(20)));
	let mut directory = fs::DirBuilder::new();
	#[cfg(unix)]
	directory.mode(0o700);
	directory.create(&root)?;
	let config = Config::new(
		&Figment::new()
			.merge(("server_name", "localhost"))
			.merge(("database_backend", "rocksdb"))
			.merge(("database_path", root.join("database"))),
	)?;
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
	services
		.globals
		.db
		.bump_database_version(crate::migrations::DATABASE_VERSION)
		.await?;
	Ok(services)
}

const OLD: &str = "old_access_token_12345678901234567890";
const NEW: &str = "new_access_token_12345678901234567890";
const REFRESH: &str = "refresh_123456789012345678901234567890";

pub(crate) async fn device(services: &Services, user: &UserId, id: &DeviceId) -> Result {
	services.db["userid_password"]
		.insert(user, b"*".as_slice())
		.await?;
	services
		.users
		.create_device(user, Some(id), (Some(OLD), None), None, None, None)
		.await?;
	services
		.users
		.set_access_token(user, id, NEW, None, Some(REFRESH))
		.await?;
	services.db["todeviceid_events"]
		.put((user, id, 9_u64), Json(serde_json::json!({"type":"m.test","content":{}})))
		.await?;
	services.db["userdeviceidalgorithm_fallback"]
		.put((user, id, "signed_curve25519"), Json(serde_json::json!({})))
		.await?;
	services.db["senderkey_pusher"]
		.put((user, "pushkey"), Json(serde_json::json!({})))
		.await?;
	services.db["pushkey_deviceid"]
		.insert("pushkey", id)
		.await?;
	Ok(())
}

async fn assert_present(services: &Services, user: &UserId, id: &DeviceId) -> Result {
	for token in [OLD, NEW, REFRESH] {
		assert_eq!(
			services
				.users
				.find_from_token(token)
				.await?
				.0
				.as_str(),
			user.as_str()
		);
	}
	assert!(services.users.device_exists(user, id).await);
	services.db["todeviceid_events"]
		.qry(&(user, id, 9_u64))
		.await?;
	services.db["senderkey_pusher"]
		.qry(&(user, "pushkey"))
		.await?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normal_revocation_removes_rotated_legacy_and_refresh_tokens_together() -> Result {
	let services = fixture().await?;
	let user = user_id!("@revoke:localhost");
	let id: &DeviceId = "DEVICE".into();
	device(&services, user, id).await?;
	services.users.remove_device(user, id).await?;
	for token in [OLD, NEW, REFRESH] {
		assert!(
			services
				.users
				.find_from_token(token)
				.await
				.expect_err("token invalidated")
				.is_not_found()
		);
	}
	assert!(!services.users.device_exists(user, id).await);
	for (map, key) in [
		("todeviceid_events", serialize_key((user, id, 9_u64))?),
		(
			"userdeviceidalgorithm_fallback",
			serialize_key((user, id, "signed_curve25519"))?,
		),
		("senderkey_pusher", serialize_key((user, "pushkey"))?),
	] {
		assert!(
			services.db[map]
				.get(&key)
				.await
				.expect_err("cleanup committed")
				.is_not_found()
		);
	}
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_pointer_refuses_without_partial_device_or_token_cleanup() -> Result {
	let services = fixture().await?;
	let user = user_id!("@revoke:localhost");
	let id: &DeviceId = "DEVICE".into();
	device(&services, user, id).await?;
	services.db["userdeviceid_token"]
		.put_raw((user, id), [0xFF])
		.await?;
	services
		.users
		.remove_device(user, id)
		.await
		.expect_err("malformed pointer is not absence");
	assert_present(&services, user, id).await?;
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_second_device_inventory_preserves_first_devices_tokens() -> Result {
	let services = fixture().await?;
	let user = user_id!("@revoke:localhost");
	let id: &DeviceId = "A".into();
	let second: &DeviceId = "B".into();
	device(&services, user, id).await?;
	services
		.users
		.create_device(user, Some(second), (None, None), None, None, None)
		.await?;
	for count in 0..=u64::try_from(super::device_removal::ROWS)? {
		services.db["todeviceid_events"]
			.put_raw((user, second, count), [])
			.await?;
	}
	services
		.users
		.remove_all_devices(user)
		.await
		.expect_err("complete aggregate inventory is required");
	assert_present(&services, user, id).await?;
	assert!(services.users.device_exists(user, second).await);
	services.stop().await;
	Ok(())
}

#[test]
fn refused_cleanup_commit_preserves_tokens_and_device_metadata() -> Result {
	const CHILD: &str = "TUWUNEL_DEVICE_REFUSAL_TEST_CHILD";
	const TEST: &str = "users::device_removal_tests::refused_cleanup_commit_preserves_tokens_and_device_metadata";
	if std::env::var(CHILD).as_deref() != Ok("refusal") {
		let status = std::process::Command::new(std::env::current_exe()?)
			.args(["--exact", TEST, "--nocapture", "--test-threads=1"])
			.env(CHILD, "refusal")
			.status()?;
		assert!(status.success(), "isolated actual commit refusal case");
		return Ok(());
	}
	tokio::runtime::Builder::new_multi_thread()
		.worker_threads(2)
		.enable_all()
		.build()?
		.block_on(verify_refused_cleanup())
}

async fn verify_refused_cleanup() -> Result {
	let services = fixture().await?;
	let user = user_id!("@revoke:localhost");
	let id: &DeviceId = "DEVICE".into();
	device(&services, user, id).await?;
	let plan = services
		.users
		.prepare_all_device_removals(user)
		.await?;
	let mut txn = services.db.txn();
	services
		.users
		.append_device_removals(&plan, &mut txn)?;
	tuwunel_database::refusal::refuse_next("userdeviceid_metadata");
	txn.execute()
		.await
		.expect_err("backend refused the one atomic cleanup");
	drop(plan);
	assert_present(&services, user, id).await?;
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_token_admission_waits_and_cannot_resurrect_a_removed_device() -> Result {
	let services = fixture().await?;
	let user = user_id!("@revoke:localhost");
	let id: &DeviceId = "DEVICE".into();
	device(&services, user, id).await?;
	let plan = services
		.users
		.prepare_all_device_removals(user)
		.await?;
	let cloned = services.clone();
	let mut setter = tokio::spawn(async move {
		cloned
			.users
			.set_access_token(user, id, "concurrent_access_12345678901234567890", None, None)
			.await
	});
	tokio::time::timeout(Duration::from_millis(20), &mut setter)
		.await
		.expect_err("token admission waits for the revocation plan");
	let mut txn = services.db.txn();
	services
		.users
		.append_device_removals(&plan, &mut txn)?;
	txn.execute().await?;
	drop(plan);
	setter
		.await
		.expect("setter task")
		.expect_err("a removed device cannot issue a new token");
	assert!(
		services
			.users
			.find_from_token("concurrent_access_12345678901234567890")
			.await
			.expect_err("concurrent token remains invalid")
			.is_not_found()
	);
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_pusher_ownership_refuses_without_partial_cleanup() -> Result {
	let services = fixture().await?;
	let user = user_id!("@revoke:localhost");
	let id: &DeviceId = "DEVICE".into();
	device(&services, user, id).await?;
	services.db["pushkey_deviceid"]
		.remove("pushkey")
		.await?;
	services
		.users
		.remove_device(user, id)
		.await
		.expect_err("missing pusher ownership pointer is not absence");
	assert_present(&services, user, id).await?;
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_inbox_and_fallback_keys_refuse_before_any_deletion() -> Result {
	let services = fixture().await?;
	let user = user_id!("@revoke:localhost");
	let id: &DeviceId = "DEVICE".into();
	device(&services, user, id).await?;
	let mut inbox = serialize_key((user, id, tuwunel_database::Interfix))?;
	inbox.push(1); // The mandatory big-endian count is truncated.
	services.db["todeviceid_events"]
		.insert(&inbox, [])
		.await?;
	services
		.users
		.remove_device(user, id)
		.await
		.expect_err("malformed inbox key");
	assert_present(&services, user, id).await?;
	services.db["todeviceid_events"]
		.remove(&inbox)
		.await?;
	let mut fallback = serialize_key((user, id, tuwunel_database::Interfix))?;
	fallback.push(0xFE); // An invalid UTF-8 algorithm cannot be skipped.
	services.db["userdeviceidalgorithm_fallback"]
		.insert(&fallback, [])
		.await?;
	services
		.users
		.remove_device(user, id)
		.await
		.expect_err("malformed fallback key");
	assert_present(&services, user, id).await?;
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_index_cannot_delete_another_devices_access_token() -> Result {
	let services = fixture().await?;
	let user = user_id!("@revoke:localhost");
	let foreign = user_id!("@foreign:localhost");
	let id: &DeviceId = "DEVICE".into();
	device(&services, user, id).await?;
	let foreign_token = "foreign_access_token_12345678901234567890";
	services.db["token_userdeviceid"]
		.raw_put(foreign_token, (foreign, id, None::<u64>))
		.await?;
	services.db["userdeviceidtoken_index"]
		.put_raw((user, id, foreign_token), [])
		.await?;
	services
		.users
		.remove_device(user, id)
		.await
		.expect_err("foreign index target is corruption");
	assert_present(&services, user, id).await?;
	assert_eq!(
		services
			.users
			.find_from_token(foreign_token)
			.await?
			.0
			.as_str(),
		foreign.as_str()
	);
	services.stop().await;
	Ok(())
}
