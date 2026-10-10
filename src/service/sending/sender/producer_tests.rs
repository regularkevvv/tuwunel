//! Producer refusal must happen before copying an unbounded inventory or
//! committing a prefix of a logical appservice fanout.
use std::{collections::BTreeMap, iter::repeat_n};

use futures::TryStreamExt;
use ruma::{
	api::appservice::{Namespaces, Registration, RegistrationInit},
	device_id, user_id,
};
use tuwunel_core::Result;

use super::{
	super::{sender_count, to_device_payloads},
	edu_tests::Fixture,
};

#[test]
fn process_sender_count_has_a_fixed_ceiling_and_safe_zero_inputs() {
	for configured in [0, 1, 2, 4, usize::MAX] {
		for runtime in [0, 1, 2, 4, usize::MAX] {
			for cores in [0, 1, 2, 4, usize::MAX] {
				let count = sender_count(configured, runtime, cores);
				assert!((1..=4).contains(&count));
				if configured == 0 {
					assert_eq!(count, 1);
				}
			}
		}
	}
}

#[test]
fn to_device_input_has_exact_single_row_and_recipient_boundaries() {
	let user = user_id!("@producer:localhost");
	let device = device_id!("PRODUCER");
	let baseline = to_device_payloads(
		user,
		user,
		[(device, 1)].into_iter(),
		"example",
		&serde_json::json!({"value":""}),
	)
	.unwrap();
	let maximum = tuwunel_bridge::MAX_VALUE_BYTES - 10;
	let content = serde_json::json!({"value": "x".repeat(maximum - baseline[0].len())});
	let boundary =
		to_device_payloads(user, user, [(device, 1)].into_iter(), "example", &content).unwrap();
	assert_eq!(boundary[0].len(), maximum);
	let oversized = serde_json::json!({"value": "x".repeat(maximum - baseline[0].len() + 1)});
	to_device_payloads(user, user, [(device, 1)].into_iter(), "example", &oversized)
		.expect_err("single row leaves active identity room");
	assert_eq!(
		to_device_payloads(
			user,
			user,
			repeat_n((device, 1), 900),
			"example",
			&serde_json::json!({})
		)
		.unwrap()
		.len(),
		900
	);
	to_device_payloads(user, user, repeat_n((device, 1), 901), "example", &serde_json::json!({}))
		.expect_err("recipient overflow");
	to_device_payloads(user, user, repeat_n((device, 1), 4), "example", &content)
		.expect_err("aggregate payload overflow");
}

async fn physical(fixture: &Fixture, map: &str) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
	fixture.services.db[map]
		.raw_stream()
		.map_ok(|(key, value)| (key.to_vec(), value.to_vec()))
		.try_collect()
		.await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appservice_fanout_refusal_preserves_every_queue_and_counter() -> Result {
	let fixture = Fixture::new().await?;
	for index in 0..2 {
		let registration: Registration = RegistrationInit {
			id: format!("producer-{index}"),
			url: None,
			as_token: format!("disposable-producer-{index}-as"),
			hs_token: format!("disposable-producer-{index}-hs"),
			sender_localpart: format!("producer-{index}"),
			namespaces: serde_json::from_value::<Namespaces>(
				serde_json::json!({"users":[{"exclusive": false, "regex": ".*"}]}),
			)?,
			rate_limited: None,
			protocols: None,
		}
		.into();
		fixture
			.services
			.appservice
			.load_appservice(registration)
			.await?;
	}
	let before = physical(&fixture, "servernameevent_data").await?;
	let active = physical(&fixture, "servercurrentevent_data").await?;
	let count = fixture.services.globals.current_count();
	let user = user_id!("@producer:localhost");
	let device = device_id!("PRODUCER");
	fixture
		.services
		.sending
		.send_to_device_appservices(
			user,
			user,
			repeat_n((device, 1), 451),
			"example",
			&serde_json::json!({}),
		)
		.await
		.expect_err("902 operations must reject complete fanout");
	assert_eq!(physical(&fixture, "servernameevent_data").await?, before);
	assert_eq!(physical(&fixture, "servercurrentevent_data").await?, active);
	assert_eq!(fixture.services.globals.current_count(), count);
	fixture
		.services
		.sending
		.send_to_device_appservices(
			user,
			user,
			[(device, 1)].into_iter(),
			"example",
			&serde_json::json!({"ok":true}),
		)
		.await?;
	assert_eq!(
		physical(&fixture, "servernameevent_data")
			.await?
			.len(),
		before.len() + 2
	);
	fixture.finish().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn key_metadata_refuses_complete_oversized_device_inventory() -> Result {
	use std::collections::BTreeSet;

	use tuwunel_database::serialize_key;
	let fixture = Fixture::new().await?;
	let user = user_id!("@metadata:localhost");
	for index in 0..128 {
		let device = ruma::OwnedDeviceId::from(format!("DEVICE{index:03}"));
		let key = serialize_key((user, &*device))?;
		fixture.services.db["userdeviceid_metadata"]
			.insert(&key, b"{}".as_slice())
			.await?;
	}
	let (counts, _) = fixture
		.services
		.sending
		.msc3202_key_counts(BTreeSet::from([user.to_owned()]), BTreeSet::new())
		.await?;
	assert_eq!(counts[user].len(), 128, "complete boundary metadata");
	let key = serialize_key((user, device_id!("OVERFLOW")))?;
	fixture.services.db["userdeviceid_metadata"]
		.insert(&key, b"{}".as_slice())
		.await?;
	let before = physical(&fixture, "userdeviceid_metadata").await?;
	fixture
		.services
		.sending
		.msc3202_key_counts(BTreeSet::from([user.to_owned()]), BTreeSet::new())
		.await
		.expect_err("no partial metadata after129th device");
	assert_eq!(physical(&fixture, "userdeviceid_metadata").await?, before);
	fixture.finish().await;
	Ok(())
}

#[test]
fn aggregate_device_metadata_has_shared_row_and_identifier_budgets() {
	use std::collections::BTreeSet;
	let user = user_id!("@metadata:localhost");
	let mut devices = BTreeSet::new();
	let mut bytes = 0;
	for index in 0..256 {
		super::admit_metadata_device(
			&mut devices,
			&mut bytes,
			user.to_owned(),
			ruma::OwnedDeviceId::from(format!("D{index}")),
		)
		.unwrap();
	}
	let old_bytes = bytes;
	super::admit_metadata_device(
		&mut devices,
		&mut bytes,
		user.to_owned(),
		ruma::OwnedDeviceId::from("D0"),
	)
	.unwrap();
	assert_eq!(bytes, old_bytes, "duplicates retain no second identifier");
	super::admit_metadata_device(
		&mut devices,
		&mut bytes,
		user.to_owned(),
		ruma::OwnedDeviceId::from("OVERFLOW"),
	)
	.expect_err("shared device count");
	assert_eq!(devices.len(), 256);
	assert_eq!(bytes, old_bytes);
	let mut devices = BTreeSet::new();
	let mut bytes = 0;
	let exact = ruma::OwnedDeviceId::from("x".repeat(64 * 1024 - user.as_bytes().len()));
	super::admit_metadata_device(&mut devices, &mut bytes, user.to_owned(), exact).unwrap();
	assert_eq!(bytes, 64 * 1024);
	super::admit_metadata_device(
		&mut devices,
		&mut bytes,
		user.to_owned(),
		ruma::OwnedDeviceId::from("x"),
	)
	.expect_err("shared identifier bytes");
	assert_eq!(bytes, 64 * 1024);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_flush_hint_is_not_retained() -> Result {
	let fixture = Fixture::new().await?;
	fixture
		.services
		.sending
		.flush_appservice("x".repeat(tuwunel_bridge::MAX_KEY_BYTES + 1))
		.expect_err("hint byte boundary");
	assert_eq!(fixture.services.sending.channels[0].1.len(), 0);
	fixture.finish().await;
	Ok(())
}
