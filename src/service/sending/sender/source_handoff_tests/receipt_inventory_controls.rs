//! Real shared rooms and public receipt writes, without workers or transport.
//! Cold children preserve exact source bytes and active delivery envelopes.

use std::{fs, path::Path, sync::atomic::Ordering};

use ruma::{
	OwnedEventId, RoomId,
	events::{receipt::ReceiptEvent, room::message::RoomMessageEventContent},
	room_id, server_name, user_id,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tuwunel_core::{Result, matrix::pdu::PduBuilder};
use tuwunel_database::serialize_key;

use super::super::{EDU_WINDOW_COUNTS, RECEIPT_ROOM_GROUPS, edu_window_end};
use crate::Services;

type Rows = Vec<(Vec<u8>, Vec<u8>)>;
type Receipt = (String, String, String);

#[derive(Serialize, Deserialize)]
struct Expected {
	receipts: Vec<Receipt>,
	sources: Rows,
	active: Rows,
	cursor: u64,
	corrupt_key: Option<Vec<u8>>,
}

pub(super) async fn child(services: &Services, root: &Path, phase: &str) -> Result {
	if matches!(phase, "receipt-restart" | "receipt-again") {
		let expected = load(root)?;
		assert_eq!(snapshot(services, "readreceiptid_readreceipt").await?, expected.sources);
		assert_eq!(snapshot(services, "servercurrentevent_data").await?, expected.active);
		assert_eq!(cursor(services).await?, expected.cursor);
		let before = cursor(services).await?;
		let upper = edu_window_end(before, services.globals.current_count())?;
		RECEIPT_ROOM_GROUPS.store(usize::MAX, Ordering::Relaxed);
		assert!(
			services
				.sending
				.select_edus(server_name!("handoff.invalid"), 0)
				.await?
				.is_empty(),
			"cold restart must not replay receipts"
		);
		if upper > before {
			assert_eq!(
				RECEIPT_ROOM_GROUPS.load(Ordering::Relaxed),
				0,
				"empty windows retain no room groups"
			);
		}
		assert_eq!(cursor(services).await?, upper);
		assert_eq!(snapshot(services, "servercurrentevent_data").await?, expected.active);
		save(root, &Expected { cursor: upper, ..expected })?;
		return Ok(());
	}
	if phase == "receipt-repair" {
		let mut expected = load(root)?;
		assert_eq!(snapshot(services, "readreceiptid_readreceipt").await?, expected.sources);
		assert_eq!(cursor(services).await?, expected.cursor);
		assert!(
			snapshot(services, "servercurrentevent_data")
				.await?
				.is_empty()
		);
		if let Some(key) = expected.corrupt_key.take() {
			services
				.sending
				.select_edus(server_name!("handoff.invalid"), 0)
				.await
				.expect_err("cold corrupt receipt remains refused");
			assert_unchanged(services, &expected).await?;
			services.db["readreceiptid_readreceipt"]
				.remove(&key)
				.await?;
			expected.sources = snapshot(services, "readreceiptid_readreceipt").await?;
		}
		select(services, &mut expected).await?;
		save(root, &expected)?;
		return Ok(());
	}

	super::setup(services).await?;
	let early = room_id!("!aa-receipt-first:localhost");
	let late = room_id!("!zz-receipt-late:localhost");
	super::setup_room(services, early).await?;
	for index in 0..16 {
		let room = RoomId::parse(format!("!receipt-empty-{index:02}:localhost"))?;
		super::setup_room(services, &room).await?;
	}
	super::setup_room(services, late).await?;
	let corrupt_room = room_id!("!zzzz-receipt-corrupt:localhost");
	if phase == "receipt-corrupt" {
		super::setup_room(services, corrupt_room).await?;
	}
	let early_target = append(services, early, "early receipt target").await?;
	let root_one = append(services, late, "first thread root").await?;
	let root_two = append(services, late, "second thread root").await?;
	let late_target = append(services, late, "late receipt target").await?;
	let start = services.globals.current_count();
	services
		.sending
		.db
		.persist_edus(server_name!("handoff.invalid"), &[], &[], start)
		.await?;
	let mut receipts = Vec::new();
	for (room, target, thread) in [
		(early, &early_target, None),
		(late, &late_target, None),
		(late, &late_target, Some("main")),
		(late, &late_target, Some(root_one.as_str())),
		(late, &late_target, Some(root_two.as_str())),
	] {
		let mut data = json!({"ts": 42});
		if let Some(thread) = thread {
			data["thread_id"] = json!(thread);
		}
		let event: ReceiptEvent = serde_json::from_value(json!({
			"type": "m.receipt", "room_id": room,
			"content": { (target.as_str()): { "m.read": { "@source:localhost": data } } }
		}))?;
		assert!(
			services
				.read_receipt
				.readreceipt_update(user_id!("@source:localhost"), room, &event)
				.await?
		);
		receipts.push((room.to_string(), target.to_string(), thread.unwrap_or("").to_owned()));
	}
	receipts.sort();
	let corrupt_key = if phase == "receipt-corrupt" {
		let count = services.globals.next_count().await?;
		let key = serialize_key((corrupt_room, *count, user_id!("@source:localhost"), ""))?;
		drop(count);
		services.db["readreceiptid_readreceipt"]
			.raw_put(&key, b"not-json")
			.await?;
		Some(key.to_vec())
	} else {
		None
	};
	assert!(
		services.globals.current_count() <= start.saturating_add(EDU_WINDOW_COUNTS),
		"all receipts fit in one complete counter window"
	);
	let mut expected = Expected {
		receipts,
		sources: snapshot(services, "readreceiptid_readreceipt").await?,
		active: Vec::new(),
		cursor: start,
		corrupt_key,
	};
	assert_eq!(expected.sources.len(), if phase == "receipt-corrupt" { 6 } else { 5 });
	if phase == "receipt-corrupt" || phase == "receipt-refuse" {
		if phase == "receipt-refuse" {
			tuwunel_database::refusal::refuse_next("servercurrentevent_data");
		}
		services
			.sending
			.select_edus(server_name!("handoff.invalid"), 0)
			.await
			.expect_err("selection or persistence must refuse without consuming sources");
		if phase == "receipt-refuse" {
			assert_eq!(tuwunel_database::refusal::pending(), 0);
		}
		assert_unchanged(services, &expected).await?;
	} else {
		assert_eq!(phase, "receipt-prepare");
		select(services, &mut expected).await?;
	}
	save(root, &expected)
}

async fn append(services: &Services, room: &RoomId, body: &str) -> Result<OwnedEventId> {
	let lock = services.state.mutex.lock(room).await;
	services
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(body)),
			user_id!("@source:localhost"),
			room,
			&lock,
		)
		.await
}

async fn select(services: &Services, expected: &mut Expected) -> Result {
	let upper = edu_window_end(cursor(services).await?, services.globals.current_count())?;
	RECEIPT_ROOM_GROUPS.store(usize::MAX, Ordering::Relaxed);
	let selected = services
		.sending
		.select_edus(server_name!("handoff.invalid"), 0)
		.await?;
	let mut receipts = Vec::new();
	let mut receipt_edus = 0_usize;
	for bytes in &selected {
		let edu: Value = serde_json::from_slice(bytes)?;
		if edu["edu_type"] != "m.receipt" {
			continue;
		}
		receipt_edus = receipt_edus
			.checked_add(1)
			.expect("receipt EDU count");
		for (room, content) in edu["content"].as_object().expect("receipt rooms") {
			let users = content["m.read"]
				.as_object()
				.expect("read receipt users");
			assert_eq!(users.len(), 1);
			for (user, receipt) in users {
				assert_eq!(user, "@source:localhost");
				assert_eq!(receipt["data"]["ts"], 42);
				for target in receipt["event_ids"]
					.as_array()
					.expect("receipt event IDs")
				{
					receipts.push((
						room.clone(),
						target.as_str().expect("target ID").to_owned(),
						receipt["data"]["thread_id"]
							.as_str()
							.unwrap_or("")
							.to_owned(),
					));
				}
			}
		}
	}
	receipts.sort();
	assert_eq!(
		receipts, expected.receipts,
		"all early, late and parallel thread receipts survive"
	);
	assert_eq!(receipt_edus, 4);
	assert_eq!(cursor(services).await?, upper);
	assert_eq!(snapshot(services, "readreceiptid_readreceipt").await?, expected.sources);
	expected.active = snapshot(services, "servercurrentevent_data").await?;
	assert_eq!(expected.active.len(), 4);
	expected.cursor = upper;
	assert_eq!(
		RECEIPT_ROOM_GROUPS.load(Ordering::Relaxed),
		2,
		"retain only the two nonempty receipt room groups"
	);
	Ok(())
}

async fn cursor(services: &Services) -> Result<u64> {
	services
		.sending
		.db
		.get_latest_educount(server_name!("handoff.invalid"))
		.await
}

async fn assert_unchanged(services: &Services, expected: &Expected) -> Result {
	assert_eq!(cursor(services).await?, expected.cursor);
	assert_eq!(snapshot(services, "readreceiptid_readreceipt").await?, expected.sources);
	assert!(
		snapshot(services, "servercurrentevent_data")
			.await?
			.is_empty()
	);
	Ok(())
}

async fn snapshot(services: &Services, map: &str) -> Result<Rows> {
	let keys = services.db[map].raw_keys_after(None, 128).await?;
	assert!(keys.len() < 128, "owned fixture inventory fits the snapshot");
	let mut rows = Vec::new();
	for key in keys {
		let value = services.db[map].get(&key).await?.to_vec();
		rows.push((key, value));
	}
	Ok(rows)
}

fn load(root: &Path) -> Result<Expected> {
	Ok(serde_json::from_slice(&fs::read(root.join("receipt-expected.json"))?)?)
}

fn save(root: &Path, expected: &Expected) -> Result {
	fs::write(root.join("receipt-expected.json"), serde_json::to_vec(expected)?)?;
	Ok(())
}
