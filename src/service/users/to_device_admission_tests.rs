//! Actual inbox writes and transaction records remain unchanged on admission
//! refusal. The existing route-level retry/wildcard assertions stay intact.
use tuwunel_core::ruma::user_id;
use tuwunel_database::Row;

use super::*;
use crate::{Services, users::device_removal_tests::fixture};

#[test]
fn bounded_body_preserves_existing_stored_json_bytes() -> Result {
	let sender = user_id!("@sender:localhost");
	let kind = "m.test\"escaped\\kind";
	let content = json!({
		"z": [null, true, 1, "line\nquote\"slash\\unicode-🦊"],
		"a": {"nested": {"b": 2, "a": 1}},
	});
	let existing = serde_json::to_vec(&json!({
		"type": kind,
		"sender": sender,
		"content": content,
	}))?;
	assert_eq!(
		encode_to_device_body(sender, kind, &content)?,
		existing,
		"bounded encoding preserves the stored JSON codec exactly"
	);
	Ok(())
}

type Snapshot = (Vec<Row>, Vec<Row>, u64);

async fn snapshot(services: &Services) -> Result<Snapshot> {
	Ok((
		services.db["todeviceid_events"]
			.raw_rows_after(None, 2_000)
			.await?,
		services.db["userdevicetxnid_response"]
			.raw_rows_after(None, 2_000)
			.await?,
		services.globals.pending_count().end,
	))
}

async fn seed_canaries(services: &Services) -> Result {
	services.db["todeviceid_events"]
		.put_raw((user_id!("@canary:localhost"), "CANARY", 1_u64), b"preserved")
		.await?;
	services.db["userdevicetxnid_response"]
		.insert("canary-transaction", b"preserved")
		.await
}

async fn register_devices(
	services: &Services,
	user: &UserId,
	count: usize,
) -> Result<Vec<OwnedDeviceId>> {
	services.db["userid_password"]
		.insert(user, b"*".as_slice())
		.await?;
	let mut devices = Vec::new();
	for index in 0..count {
		let id: OwnedDeviceId = format!("DEVICE-{index}").into();
		devices.push(
			services
				.users
				.create_device(user, Some(&id), (None, None), None, None, None)
				.await?,
		);
	}
	Ok(devices)
}

fn wildcard(user: &UserId, content: serde_json::Value) -> ToDeviceTarget {
	ToDeviceTarget {
		user_id: user.to_owned(),
		device: DeviceIdOrAllDevices::AllDevices,
		content,
	}
}

async fn refuse_unchanged(
	services: &Services,
	targets: &[ToDeviceTarget],
	txnid: &[u8],
) -> Result {
	let before = snapshot(services).await?;
	services
		.users
		.deliver_to_device(user_id!("@sender:localhost"), "m.test", targets, Some(txnid))
		.await
		.expect_err("complete admission must refuse");
	assert_eq!(
		snapshot(services).await?,
		before,
		"no inbox, transaction record or counter permit changed"
	);
	assert!(
		services.db["userdevicetxnid_response"]
			.get(txnid)
			.await
			.expect_err("refusal leaves transaction retryable")
			.is_not_found()
	);
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_device_wildcard_keeps_both_inboxes_and_atomic_transaction_record() -> Result {
	let services = fixture().await?;
	let user = user_id!("@recipient:localhost");
	let devices = register_devices(&services, user, 2).await?;
	let target = wildcard(user, json!({"n":1}));
	services
		.users
		.deliver_to_device(
			user_id!("@sender:localhost"),
			"m.test",
			&[target],
			Some(b"two-device-wildcard"),
		)
		.await?;
	for device in &devices {
		let prefix = serialize_key((user, &**device, Interfix))?;
		let rows = services.db["todeviceid_events"]
			.raw_rows_prefix_after(&prefix, None, 2)
			.await?;
		assert_eq!(rows.len(), 1, "every addressed device receives the message");
		let message: serde_json::Value = serde_json::from_slice(&rows[0].1)?;
		assert_eq!(
			message,
			json!({"type":"m.test","sender":"@sender:localhost","content":{"n":1}})
		);
	}
	assert!(
		services.db["userdevicetxnid_response"]
			.get(b"two-device-wildcard")
			.await?
			.is_empty()
	);
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_overflow_and_corrupt_device_index_refuse_complete_delivery() -> Result {
	let services = fixture().await?;
	seed_canaries(&services).await?;
	let user = user_id!("@overflow:localhost");
	// The production creation API has no new device ceiling: existing larger
	// accounts are preserved while wildcard work is refused as a whole.
	register_devices(&services, user, 129).await?;
	refuse_unchanged(&services, &[wildcard(user, json!({}))], b"wildcard-overflow").await?;
	let corrupt = user_id!("@corrupt:localhost");
	register_devices(&services, corrupt, 1).await?;
	let mut bad = serialize_key((corrupt, Interfix))?.to_vec();
	bad.push(0xFE);
	services.db["userdeviceid_metadata"]
		.insert(&bad, b"{}")
		.await?;
	refuse_unchanged(&services, &[wildcard(corrupt, json!({}))], b"wildcard-corrupt").await?;
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_wildcard_expansion_refuses_before_any_counter_or_inbox_commit() -> Result {
	let services = fixture().await?;
	seed_canaries(&services).await?;
	let mut targets = Vec::new();
	for index in 0..8 {
		let user = OwnedUserId::try_from(format!("@aggregate-{index}:localhost"))?;
		register_devices(&services, &user, 128).await?;
		targets.push(wildcard(&user, json!({})));
	}
	refuse_unchanged(&services, &targets, b"aggregate-device-overflow").await?;
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expanded_encoded_bytes_and_single_oversized_content_refuse_atomically() -> Result {
	let services = fixture().await?;
	seed_canaries(&services).await?;
	let user = user_id!("@payload:localhost");
	register_devices(&services, user, 3).await?;
	let expanded =
		wildcard(user, json!({"data":"x".repeat(tuwunel_bridge::request::MAX_BYTES / 3)}));
	refuse_unchanged(&services, &[expanded], b"expanded-byte-overflow").await?;
	let oversized = wildcard(
		user,
		json!({"data":"x".repeat(tuwunel_bridge::MAX_VALUE_BYTES.saturating_add(1))}),
	);
	refuse_unchanged(&services, &[oversized], b"single-content-overflow").await?;
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_target_and_identifier_budgets_refuse_without_cloning_or_delivery() -> Result {
	let services = fixture().await?;
	seed_canaries(&services).await?;
	let user = user_id!("@targets:localhost");
	register_devices(&services, user, 1).await?;
	let targets = (0..=tuwunel_bridge::MAX_COMMIT_OPS)
		.map(|index| ToDeviceTarget {
			user_id: user.to_owned(),
			device: DeviceIdOrAllDevices::DeviceId(format!("DEVICE-{index}").into()),
			content: json!({}),
		})
		.collect::<Vec<_>>();
	refuse_unchanged(&services, &targets, b"initial-target-overflow").await?;
	let huge_id = ToDeviceTarget {
		user_id: user.to_owned(),
		device: DeviceIdOrAllDevices::DeviceId(
			"x".repeat(tuwunel_bridge::MAX_KEY_BYTES.saturating_add(1))
				.into(),
		),
		content: json!({}),
	};
	refuse_unchanged(&services, &[huge_id], b"initial-id-width-overflow").await?;
	let targets = (0..tuwunel_bridge::MAX_COMMIT_OPS)
		.map(|index| ToDeviceTarget {
			user_id: user.to_owned(),
			device: DeviceIdOrAllDevices::DeviceId(
				format!("{index}{}", "x".repeat(5_000)).into(),
			),
			content: json!({}),
		})
		.collect::<Vec<_>>();
	refuse_unchanged(&services, &targets, b"initial-aggregate-id-overflow").await?;
	services.stop().await;
	Ok(())
}

async fn diagnostic_refuses_without_output(
	services: &Services,
	user: &UserId,
	device: &DeviceId,
) -> Result {
	let before = snapshot(services).await?;
	let context = crate::admin::Context {
		services,
		body: &[],
		timer: SystemTime::now(),
		output: futures::lock::Mutex::new("existing output".into()),
	};
	context
		.write_timed_query_try(
			services
				.users
				.bounded_to_device_events(user, device),
		)
		.await
		.expect_err("diagnostic refuses its entire corrupt/oversized inventory");
	assert_eq!(
		&*context.output.lock().await,
		"existing output",
		"failure emits no partial success output"
	);
	assert_eq!(snapshot(services).await?, before, "diagnostic is read only");
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diagnostic_complete_inbox_preserves_counts_content_and_prefix() -> Result {
	let services = fixture().await?;
	let user = user_id!("@diagnostic:localhost");
	let device: &DeviceId = "DIAGNOSTIC".into();
	for count in [1_u64, 2] {
		services.db["todeviceid_events"]
			.put_raw(
				(user, device, count),
				encode_to_device_body(user, "m.test", &json!({"n":count}))?,
			)
			.await?;
	}
	services.db["todeviceid_events"]
		.put_raw((user, "DIAGNOSTIC-OTHER", 3_u64), b"invalid foreign JSON")
		.await?;
	let events = services
		.users
		.bounded_to_device_events(user, device)
		.await?;
	assert_eq!(events.len(), 2, "complete matching inventory only");
	for (index, (count, event)) in events.iter().enumerate() {
		assert_eq!(*count, u64::try_from(index)?.saturating_add(1), "stored sequence retained");
		let content: serde_json::Value = serde_json::from_str(event.json().get())?;
		assert_eq!(content["content"]["n"], json!(count), "stored content retained");
	}
	let context = crate::admin::Context {
		services: &services,
		body: &[],
		timer: SystemTime::now(),
		output: futures::lock::Mutex::new(String::new()),
	};
	context
		.write_timed_query_try(
			services
				.users
				.bounded_to_device_events(user, device),
		)
		.await?;
	assert!(
		context
			.output
			.lock()
			.await
			.contains("Query completed"),
		"complete inventory renders"
	);
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diagnostic_row_and_byte_overflow_refuse_without_partial_output() -> Result {
	let services = fixture().await?;
	let user = user_id!("@diagnostic:localhost");
	let rows: &DeviceId = "ROW-OVERFLOW".into();
	for count in 0..=super::super::MAX_ADMIN_DEVICE_ROWS {
		services.db["todeviceid_events"]
			.put_raw((user, rows, u64::try_from(count)?), b"{}")
			.await?;
	}
	diagnostic_refuses_without_output(&services, user, rows).await?;
	let bytes: &DeviceId = "BYTE-OVERFLOW".into();
	let body = encode_to_device_body(user, "m.test", &json!({"data":"x".repeat(40_000)}))?;
	for count in [1_u64, 2] {
		services.db["todeviceid_events"]
			.put_raw((user, bytes, count), &body)
			.await?;
	}
	diagnostic_refuses_without_output(&services, user, bytes).await?;
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diagnostic_corrupt_key_and_json_propagate_without_partial_output() -> Result {
	let services = fixture().await?;
	let user = user_id!("@diagnostic:localhost");
	let json: &DeviceId = "BAD-JSON".into();
	services.db["todeviceid_events"]
		.put_raw((user, json, 1_u64), b"{}")
		.await?;
	services.db["todeviceid_events"]
		.put_raw((user, json, 2_u64), b"{invalid")
		.await?;
	diagnostic_refuses_without_output(&services, user, json).await?;
	let key: &DeviceId = "BAD-KEY".into();
	services.db["todeviceid_events"]
		.put_raw((user, key, 1_u64), b"{}")
		.await?;
	let mut corrupt = serialize_key((user, key, Interfix))?.to_vec();
	corrupt.push(0xFE);
	services.db["todeviceid_events"]
		.insert(&corrupt, b"{}")
		.await?;
	diagnostic_refuses_without_output(&services, user, key).await?;
	services.stop().await;
	Ok(())
}
