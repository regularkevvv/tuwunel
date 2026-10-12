//! Admission must validate existing obligations before accepting another event.
//! Only the native owned database is corrupted; no service workers or
//! transport.

use std::{fs, path::Path};

use ruma::{
	EventId, OwnedEventId, events::room::message::RoomMessageEventContent, room_id, user_id,
};
use tuwunel_core::{Result, matrix::pdu::PduBuilder, utils::hash::sha256::hash};

use crate::{Services, transaction_ids};

const PLAN: &str = "pduid_federationplan";

pub(super) async fn child(services: &Services, root: &Path, phase: &str) -> Result {
	if phase == "inventory-recover" {
		return recover(services, root).await;
	}
	if phase == "inventory-again" {
		return again(services, root).await;
	}
	super::setup(services).await?;
	tuwunel_database::refusal::refuse_next("servernameevent_data");
	let original = append(services, false).await?;
	assert_eq!(
		tuwunel_database::refusal::pending(),
		0,
		"source stays pending after queue refusal"
	);
	let raw = services.timeline.get_pdu_id(&original).await?;
	assert!(
		services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	let mut key = vec![0x08];
	key.extend_from_slice(raw.as_ref());
	let body = services.db[PLAN]
		.get(raw.as_ref())
		.await?
		.to_vec();
	let witness = services.db["global"].get(&key).await?.to_vec();
	fs::write(root.join("inventory-original-event"), original.as_str())?;
	fs::write(root.join("inventory-original-body"), &body)?;
	fs::write(root.join("inventory-original-witness"), &witness)?;
	let mut corrupt_body = body;
	let mut corrupt_witness = witness;
	match phase {
		| "inventory-size" => corrupt_witness[..4].copy_from_slice(&1_u32.to_be_bytes()),
		| "inventory-hash" => corrupt_witness[4] ^= 1,
		| "inventory-codec" => {
			*corrupt_body.last_mut().expect("nonempty source") = 0xFF;
			corrupt_witness[4..].copy_from_slice(&hash(&corrupt_body));
		},
		| "inventory-body" => *corrupt_body.last_mut().expect("nonempty source") = b'e',
		| _ => unreachable!("owned inventory phase"),
	}
	let mut txn = services.db.txn();
	txn.insert_raw(&services.db[PLAN], raw.as_ref(), &corrupt_body);
	txn.insert_raw(&services.db["global"], &key, &corrupt_witness);
	txn.execute_flushed().await?;
	fs::write(root.join("inventory-corrupt-body"), &corrupt_body)?;
	fs::write(root.join("inventory-corrupt-witness"), &corrupt_witness)?;
	let state = services
		.state
		.get_room_shortstatehash(room_id!("!source-handoff:localhost"))
		.await?;
	append(services, true)
		.await
		.expect_err("corrupt existing source must refuse canonical admission");
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room_id!("!source-handoff:localhost"))
			.await?,
		state
	);
	assert_refused(services, root, &original).await
}

async fn append(services: &Services, record_retry: bool) -> Result<OwnedEventId> {
	let room = room_id!("!source-handoff:localhost");
	let alice = user_id!("@source:localhost");
	let lock = services.state.mutex.lock(room).await;
	let key = transaction_ids::key(
		alice,
		Some(ruma::device_id!("source-device")),
		"source-message".into(),
	);
	services
		.timeline
		.build_and_append_pdu_with_txnid(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(
				"source inventory admission",
			)),
			alice,
			room,
			record_retry.then_some(key.as_slice()),
			&lock,
		)
		.await
}

async fn assert_refused(services: &Services, root: &Path, original: &EventId) -> Result {
	let raw = services.timeline.get_pdu_id(original).await?;
	let mut key = vec![0x08];
	key.extend_from_slice(raw.as_ref());
	assert_eq!(
		services
			.timeline
			.latest_pdu_in_room(room_id!("!source-handoff:localhost"))
			.await?
			.event_id
			.as_str(),
		original.as_str()
	);
	assert_eq!(
		super::retry_record(services).await?,
		None,
		"refused event has no retry acknowledgement"
	);
	assert_eq!(services.db[PLAN].raw_keys_after(None, 2).await?, vec![raw.as_ref().to_vec()]);
	assert_eq!(
		services.db[PLAN]
			.get(raw.as_ref())
			.await?
			.as_ref(),
		fs::read(root.join("inventory-corrupt-body"))?
	);
	assert_eq!(
		services.db["global"].get(&key).await?.as_ref(),
		fs::read(root.join("inventory-corrupt-witness"))?
	);
	Ok(())
}

async fn recover(services: &Services, root: &Path) -> Result {
	let original = EventId::parse(fs::read_to_string(root.join("inventory-original-event"))?)?;
	assert_refused(services, root, &original).await?;
	let raw = services.timeline.get_pdu_id(&original).await?;
	let mut key = vec![0x08];
	key.extend_from_slice(raw.as_ref());
	let mut txn = services.db.txn();
	txn.insert_raw(
		&services.db[PLAN],
		raw.as_ref(),
		fs::read(root.join("inventory-original-body"))?,
	);
	txn.insert_raw(
		&services.db["global"],
		key,
		fs::read(root.join("inventory-original-witness"))?,
	);
	txn.execute_flushed().await?;
	services
		.sending
		.resume_federation_source(raw)
		.await?;
	assert!(
		!services
			.sending
			.db
			.has_federation_plan(&raw)
			.await?
	);
	assert!(
		super::owns(services, &original).await?,
		"original delivery survives refused admission"
	);
	let admitted = append(services, true).await?;
	assert_eq!(super::retry_record(services).await?, Some(admitted.clone()));
	assert!(super::owns(services, &admitted).await?);
	fs::write(root.join("inventory-admitted-event"), admitted.as_str())?;
	Ok(())
}

async fn again(services: &Services, root: &Path) -> Result {
	let admitted = EventId::parse(fs::read_to_string(root.join("inventory-admitted-event"))?)?;
	assert_eq!(super::retry_record(services).await?, Some(admitted.clone()));
	for event in [
		EventId::parse(fs::read_to_string(root.join("inventory-original-event"))?)?,
		admitted,
	] {
		assert_eq!(services.timeline.get_pdu(&event).await?.event_id, event);
		assert!(super::owns(services, &event).await?);
		let raw = services.timeline.get_pdu_id(&event).await?;
		assert!(
			!services
				.sending
				.db
				.has_federation_plan(&raw)
				.await?
		);
	}
	Ok(())
}
