#![cfg(test)]

use std::{env::var, fs::remove_dir_all, path::PathBuf, process::id as process_id};

use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_admin::{fini, init};
use tuwunel_core::{
	Result, http,
	ruma::{UserId, api::client::device::Device},
};
use tuwunel_service::{
	Services,
	users::{MAX_ADMIN_DEVICE_BYTES, MAX_ADMIN_DEVICE_ROWS},
};

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn local_user_activity_preserves_order_and_refuses_incomplete_inventories() -> Result {
	let root = var("TMPDIR").unwrap_or_else(|_| "/tmp".into());
	let db_path = DatabasePath(
		PathBuf::from(root).join(format!("tuwunel-local-user-activity-{}", process_id())),
	);
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.push(format!("database_path={:?}", db_path.0));
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		init(&services.admin);
		let outcome = exercise(&services).await;
		fini(&services.admin);
		let shutdown = server.server.shutdown();
		drop(services);
		let run = async_run(&server).await;
		let stop = async_stop(&server).await;
		outcome.and(shutdown).and(run).and(stop)
	});
	drop(runtime);
	result
}

async fn device(services: &Services, user: &UserId, json: serde_json::Value) -> Result {
	let device: Device = serde_json::from_value(json)?;
	services
		.users
		.put_device_metadata(user, false, &device)
		.await
}

async fn exercise(services: &Services) -> Result {
	let users = &services.db["userid_password"];
	let metadata = &services.db["userdeviceid_metadata"];
	// Only this fresh disposable fixture's database is modified.
	users.clear().await?;
	metadata.clear().await?;
	assert!(
		services
			.users
			.recent_local_activity(48)
			.await?
			.is_empty()
	);
	let alice = UserId::parse("@alice:localhost").expect("valid user");
	let bob = UserId::parse("@bob:localhost").expect("valid user");
	let disabled = UserId::parse("@disabled:localhost").expect("valid user");
	let unrecorded = UserId::parse("@alice-other:localhost").expect("valid user");
	users.insert(&alice, "*").await?;
	users.insert(&bob, "hash").await?;
	users.insert(&disabled, "").await?;
	users.insert("@unseen:localhost", "*").await?;
	device(
		services,
		&alice,
		serde_json::json!({"device_id":"A", "last_seen_ts":100,
		"last_seen_ip":"192.0.2.1"}),
	)
	.await?;
	device(
		services,
		&alice,
		serde_json::json!({"device_id":"Z", "last_seen_ts":100,
		"last_seen_ip":"192.0.2.2"}),
	)
	.await?;
	device(services, &bob, serde_json::json!({"device_id":"B", "last_seen_ts":200})).await?;
	device(services, &disabled, serde_json::json!({"device_id":"D", "last_seen_ts":999})).await?;
	device(
		services,
		&unrecorded,
		serde_json::json!({"device_id":"OTHER", "last_seen_ts":1000}),
	)
	.await?;
	let activity = services.users.recent_local_activity(48).await?;
	assert_eq!(
		activity
			.iter()
			.map(|item| item.user_id.as_str())
			.collect::<Vec<_>>(),
		vec!["@bob:localhost", "@alice:localhost"]
	);
	assert_eq!(activity[1].last_seen_ip.as_deref(), Some("192.0.2.2"));
	assert_eq!(services.users.recent_local_activity(1).await?[0].user_id, bob);
	let inventory = services
		.users
		.bounded_devices_metadata(&alice, MAX_ADMIN_DEVICE_ROWS, MAX_ADMIN_DEVICE_BYTES)
		.await?;
	assert_eq!(inventory.devices.len(), 2);
	assert_eq!(inventory.examined, 2);
	assert!(inventory.encoded_bytes > 0);
	for (rows, bytes) in [(0, MAX_ADMIN_DEVICE_BYTES), (1, MAX_ADMIN_DEVICE_BYTES), (128, 1)] {
		assert_eq!(
			services
				.users
				.bounded_devices_metadata(&alice, rows, bytes)
				.await
				.expect_err("an incomplete device inventory must be refused")
				.status_code(),
			http::StatusCode::TOO_MANY_REQUESTS
		);
	}
	assert_eq!(
		services
			.users
			.bounded_devices_metadata(&unrecorded, 0, 0)
			.await
			.expect_err("zero budget cannot hide an existing device")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	let empty = UserId::parse("@empty:localhost").expect("valid user");
	assert!(
		services
			.users
			.bounded_devices_metadata(&empty, 0, 0)
			.await?
			.devices
			.is_empty()
	);
	for limit in [0, 65, usize::MAX] {
		assert_eq!(
			services
				.users
				.recent_local_activity(limit)
				.await
				.expect_err("invalid output limit")
				.status_code(),
			http::StatusCode::BAD_REQUEST
		);
	}
	let Ok(Some(output)) = services
		.admin
		.command_in_place("users last-active --limit 2".into(), None)
		.await
	else {
		panic!("activity command must succeed");
	};
	let lines = output
		.as_str()
		.lines()
		.filter(|line| line.ends_with("bob") || line.ends_with("alice"))
		.collect::<Vec<_>>();
	assert_eq!(lines.len(), 2);
	assert!(lines[0].ends_with("bob"));
	assert!(lines[1].ends_with("alice"));

	metadata
		.put((&alice, "CORRUPT"), b"not-json")
		.await?;
	services
		.users
		.recent_local_activity(1)
		.await
		.expect_err("the output limit cannot hide a malformed device row");
	let Err(output) = services
		.admin
		.command_in_place("query users list-devices-metadata @alice:localhost".into(), None)
		.await
	else {
		panic!("metadata command must preserve a decoding failure");
	};
	assert!(!output.as_str().contains("Query completed"));
	metadata.clear().await?;
	metadata
		.put(
			(&alice, "KEY"),
			tuwunel_database::Json(serde_json::json!({"device_id":"OTHER"})),
		)
		.await?;
	assert!(
		services
			.users
			.bounded_devices_metadata(&alice, 128, MAX_ADMIN_DEVICE_BYTES)
			.await
			.expect_err("metadata must match the stored key")
			.to_string()
			.contains("does not match")
	);

	metadata.clear().await?;
	for index in 0..=MAX_ADMIN_DEVICE_ROWS {
		device(services, &alice, serde_json::json!({"device_id":format!("D{index:03}")})).await?;
	}
	assert_eq!(
		services
			.users
			.bounded_devices_metadata(&alice, usize::MAX, usize::MAX)
			.await
			.expect_err("caller limits cannot remove the hard per-user cap")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	services
		.users
		.recent_local_activity(1)
		.await
		.expect_err("last-active cannot return a top-N from a truncated user's devices");

	metadata.clear().await?;
	device(
		services,
		&alice,
		serde_json::json!({"device_id":"LARGE",
		"display_name":"x".repeat(MAX_ADMIN_DEVICE_BYTES)}),
	)
	.await?;
	assert_eq!(
		services
			.users
			.bounded_devices_metadata(&alice, 128, MAX_ADMIN_DEVICE_BYTES)
			.await
			.expect_err("raw byte budget must precede decoding a large row")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);

	users.clear().await?;
	metadata.clear().await?;
	for user_index in 0..33 {
		let user =
			UserId::parse(format!("@aggregate-{user_index:03}:localhost")).expect("valid user");
		users.insert(&user, "*").await?;
		for index in 0..MAX_ADMIN_DEVICE_ROWS {
			device(services, &user, serde_json::json!({"device_id":format!("D{index:03}")}))
				.await?;
		}
	}
	assert_eq!(
		services
			.users
			.recent_local_activity(1)
			.await
			.expect_err("device rows share a budget across users")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);

	users.clear().await?;
	metadata.clear().await?;
	for index in 0..5 {
		let user = UserId::parse(format!("@bytes-{index}:localhost")).expect("valid user");
		users.insert(&user, "*").await?;
		device(
			services,
			&user,
			serde_json::json!({"device_id":"BIG", "last_seen_ts":100,
			"display_name":"x".repeat(60 * 1024)}),
		)
		.await?;
	}
	assert_eq!(
		services
			.users
			.recent_local_activity(1)
			.await
			.expect_err("metadata bytes share a budget across users")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	let Err(output) = services
		.admin
		.command_in_place("users last-active --limit 1".into(), None)
		.await
	else {
		panic!("aggregate budget refusal must fail the command");
	};
	assert!(
		output
			.as_str()
			.contains("inventory limit reached")
	);
	assert!(
		!output.as_str().contains("bytes-0"),
		"no activity rows may be emitted before refusal"
	);

	users.clear().await?;
	metadata.clear().await?;
	for index in 0..64 {
		let user = UserId::parse(format!("@reply-{index:03}{}:localhost", "x".repeat(220)))
			.expect("valid long user id");
		users.insert(&user, "*").await?;
		device(services, &user, serde_json::json!({"device_id":"D", "last_seen_ts":100})).await?;
	}
	assert_eq!(
		services
			.users
			.recent_local_activity(1)
			.await?
			.len(),
		1
	);
	assert_eq!(
		services
			.users
			.recent_local_activity(64)
			.await
			.expect_err("complete inputs do not permit an oversized response")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	Ok(())
}
