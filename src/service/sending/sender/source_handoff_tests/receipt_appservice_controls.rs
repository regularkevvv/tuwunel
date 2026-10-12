//! Actual public receipt writes must share their commit with owed appservice
//! EDUs. The existing process-isolated reference fixture reopens durable rows
//! twice; no sender transport or external resources are started.

use std::{collections::BTreeMap, fs, path::Path};

use futures::TryStreamExt;
use ruma::{
	OwnedEventId,
	api::appservice::{Namespaces, Registration, RegistrationInit},
	events::{receipt::ReceiptEvent, room::message::RoomMessageEventContent},
	room_id, user_id,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tuwunel_core::{Result, matrix::pdu::PduBuilder};
use tuwunel_database::{Row, refusal};

use crate::{
	Services,
	sending::{Destination, SendingEvent},
};

const MAPS: [&str; 8] = [
	"readreceiptid_readreceipt",
	"roomuserid_notificationcutoff",
	"userroomid_notificationcount",
	"userroomid_highlightcount",
	"roomuserid_lastnotificationread",
	"servernameevent_data",
	"servercurrentevent_data",
	"notificationreceiptid_record",
];

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Snapshot(BTreeMap<String, Vec<Row>>);

#[derive(Serialize, Deserialize)]
struct Expected {
	event: ReceiptEvent,
	rows: Snapshot,
}

pub(super) async fn child(services: &Services, root: &Path, phase: &str) -> Result {
	// This one-shot native loader only populates the real registry. It starts
	// no transport and makes the production registration APIs available.
	crate::Service::worker(services.appservice.clone()).await?;
	if matches!(phase, "appreceipt-restart" | "appreceipt-again") {
		let expected: Expected =
			serde_json::from_slice(&fs::read(root.join("appreceipt.json"))?)?;
		assert_eq!(
			snapshot(services).await?,
			expected.rows,
			"cold restart preserves every composite row"
		);
		assert_delivery(services, &expected.event).await?;
		let count = services.globals.current_count();
		assert!(!update(services, &expected.event).await?, "unchanged cold receipt is a no-op");
		assert_eq!(services.globals.current_count(), count, "no-op allocates no stream position");
		assert_eq!(snapshot(services).await?, expected.rows, "no duplicate cold queue work");
		return Ok(());
	}

	super::setup(services).await?;
	let first = append(services, "earlier receipt target").await?;
	let target = append(services, "owed receipt target").await?;
	assert!(update(services, &receipt(&first)?).await?);
	let registrations = if phase == "appreceipt-capacity" { 129 } else { 1 };
	for index in 0..registrations {
		register(services, index).await?;
	}
	let event = receipt(&target)?;
	let before = snapshot(services).await?;
	let count = services.globals.current_count();
	if phase == "appreceipt-refuse" {
		refusal::refuse_next("servernameevent_data");
	} else {
		assert_eq!(phase, "appreceipt-capacity");
	}
	update(services, &event)
		.await
		.expect_err("sender admission cannot acknowledge an unqueued receipt");
	assert_eq!(
		snapshot(services).await?,
		before,
		"refusal preserves receipt, notification state and every queue byte"
	);
	if phase == "appreceipt-refuse" {
		assert_eq!(refusal::pending(), 0, "actual composite queue commit was refused");
		// Sequence reservation is intentionally durable before the composite
		// commit; refusal may leave this one unused sequence, never a receipt.
		assert_eq!(services.globals.current_count(), count.saturating_add(1));
	} else {
		assert_eq!(
			services.globals.current_count(),
			count,
			"capacity preflight precedes stream allocation"
		);
		for index in 1..registrations {
			services
				.appservice
				.unregister_appservice(&format!("receipt-atomic-{index}"))
				.await?;
		}
	}
	assert_eq!(
		services.globals.pending_count().start,
		services.globals.pending_count().end,
		"no leaked counter permit"
	);
	assert!(
		update(services, &event).await?,
		"identical refused receipt succeeds after capacity returns"
	);
	assert_delivery(services, &event).await?;
	let accepted = snapshot(services).await?;
	assert_ne!(accepted, before, "accepted receipt changes durable composite state");
	let count = services.globals.current_count();
	assert!(!update(services, &event).await?, "unchanged receipt does not queue twice");
	assert_eq!(services.globals.current_count(), count);
	assert_eq!(snapshot(services).await?, accepted);
	fs::write(
		root.join("appreceipt.json"),
		serde_json::to_vec(&Expected { event, rows: accepted })?,
	)?;
	Ok(())
}

async fn register(services: &Services, index: usize) -> Result {
	let mut registration: Registration = RegistrationInit {
		id: format!("receipt-atomic-{index}"),
		url: None,
		as_token: format!("disposable-receipt-{index}-as"),
		hs_token: format!("disposable-receipt-{index}-hs"),
		sender_localpart: format!("receipt-bot-{index}"),
		namespaces: serde_json::from_value::<Namespaces>(json!({
			"rooms": [{"exclusive": false, "regex": "^!source-handoff:localhost$"}]
		}))?,
		rate_limited: None,
		protocols: None,
	}
	.into();
	registration.receive_ephemeral = true;
	services
		.appservice
		.register_appservice(registration)
		.await
}

async fn append(services: &Services, body: &str) -> Result<OwnedEventId> {
	let room = room_id!("!source-handoff:localhost");
	let state = services.state.mutex.lock(room).await;
	services
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(body)),
			user_id!("@source:localhost"),
			room,
			&state,
		)
		.await
}

fn receipt(event: &ruma::EventId) -> Result<ReceiptEvent> {
	Ok(serde_json::from_value(json!({
		"type": "m.receipt", "room_id": "!source-handoff:localhost",
		"content": {(event.as_str()): {"m.read": {"@source:localhost": {"ts": 42}}}}
	}))?)
}

async fn update(services: &Services, event: &ReceiptEvent) -> Result<bool> {
	services
		.read_receipt
		.readreceipt_update(
			user_id!("@source:localhost"),
			room_id!("!source-handoff:localhost"),
			event,
		)
		.await
}

async fn snapshot(services: &Services) -> Result<Snapshot> {
	let mut maps = BTreeMap::new();
	for map in MAPS {
		let rows = services.db[map]
			.raw_rows_after(None, 4096)
			.await?;
		assert!(rows.len() < 4096, "complete owned fixture snapshot");
		maps.insert(map.to_owned(), rows);
	}
	Ok(Snapshot(maps))
}

async fn assert_delivery(services: &Services, event: &ReceiptEvent) -> Result {
	let rows = services
		.sending
		.db
		.queued_requests(&Destination::Appservice("receipt-atomic-0".into()))
		.try_collect::<Vec<_>>()
		.await?;
	assert_eq!(rows.len(), 1, "exactly one owed appservice receipt");
	let SendingEvent::Edu(body) = &rows[0].1 else {
		panic!("receipt must queue an EDU");
	};
	let queued: Value = serde_json::from_slice(body)?;
	let expected: Value = serde_json::to_value(event)?;
	assert_eq!(queued, expected, "queued receipt retains exact event content and room");
	let stored = services.db["readreceiptid_readreceipt"]
		.raw_rows_after(None, 2)
		.await?;
	assert_eq!(stored.len(), 1, "new receipt replaces its earlier canonical row");
	assert_eq!(serde_json::from_slice::<Value>(&stored[0].1)?, expected);
	Ok(())
}
