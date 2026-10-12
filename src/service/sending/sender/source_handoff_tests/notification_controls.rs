//! Accepted-state decisions, pending membership overlay, and cold recovery.
//! Synthetic state fixtures exercise admission; they do not prove peer auth.

use std::{fs, path::Path, sync::Arc};

use futures::TryStreamExt;
use ruma::{
	EventId, UserId,
	events::{
		StateEventType,
		room::{
			member::{MembershipState, RoomMemberEventContent},
			message::RoomMessageEventContent,
		},
	},
	room_id, user_id,
};
use serde_json::{Value, json};
use tuwunel_core::{Result, matrix::pdu::PduBuilder, utils::to_canonical_object};
use tuwunel_database::{refusal, serialize_key};

use crate::{
	Services,
	rooms::state_compressor::{CompressedState, compress_state_event},
	users::Register,
};

const PLAN: &str = "pduid_notificationplan";
const RECEIPT: &str = "notificationreceiptid_record";

pub(super) async fn child(services: &Services, root: &Path, phase: &str) -> Result {
	if phase == "notification-restart" || phase == "notification-again" {
		return restart(services, root, phase).await;
	}
	super::setup(services).await?;
	match phase {
		| "notification-snapshot" => snapshot(services, root).await,
		| "notification-overlay" => overlay(services).await,
		| "notification-corrupt" => corrupt(services).await,
		| "notification-budget" => budget(services).await,
		| _ => unreachable!("owned notification phase"),
	}
}

async fn register(services: &Services, user: &UserId) -> Result {
	services
		.users
		.full_register(Register {
			user_id: Some(user),
			// These exclusively owned native accounts must be active; None marks
			// password-origin accounts deactivated and suppresses notifications.
			password: Some("owned-native-notification-fixture"),
			..Default::default()
		})
		.await?;
	assert!(
		services
			.users
			.notification_recipient_active(user)
			.await?
	);
	Ok(())
}

async fn membership(services: &Services, user: &UserId, membership: MembershipState) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let lock = services.state.mutex.lock(room).await;
	services
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(user.to_string(), &RoomMemberEventContent::new(membership)),
			user,
			room,
			&lock,
		)
		.await?;
	Ok(())
}

async fn rules(services: &Services, user: &UserId, count: Option<usize>, notify: bool) -> Result {
	let conditions = count.map_or_else(
		|| json!([]),
		|count| {
			json!([
				{"kind":"room_member_count","is":count.to_string()}
			])
		},
	);
	let actions = if notify { json!(["notify"]) } else { json!([]) };
	services
		.account_data
		.update(
			None,
			user,
			"m.push_rules".into(),
			&json!({
				"type":"m.push_rules", "content":{"global":{
					"override":[{"rule_id":"accepted-state-count", "default":false,
						"enabled":true,"conditions":conditions,"actions":actions}],
					"content":[],"room":[],"sender":[],"underride":[]
				}}
			}),
		)
		.await
}

async fn receipt(services: &Services, event: &EventId, user: &UserId) -> Result<Option<Value>> {
	let raw = services.timeline.get_pdu_id(event).await?;
	let key = serialize_key((raw.as_ref(), user))?;
	match services.db[RECEIPT].get(&key).await {
		| Ok(value) => Ok(Some(serde_json::from_slice(&value)?)),
		| Err(error) if error.is_not_found() => Ok(None),
		| Err(error) => Err(error),
	}
}

async fn snapshot(services: &Services, root: &Path) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let bob = user_id!("@notification-bob:localhost");
	let carol = user_id!("@notification-carol:localhost");
	for user in [bob, carol] {
		register(services, user).await?;
		membership(services, user, MembershipState::Join).await?;
	}
	membership(services, bob, MembershipState::Leave).await?;
	rules(services, bob, None, true).await?;
	rules(services, carol, Some(3), true).await?;
	// Accepted state has Alice, Carol and the remote member. The derived cache
	// omits Carol and still includes departed Bob plus a never-joined remote.
	services.db["roomuserid_joined"]
		.remove(&serialize_key((room, carol))?)
		.await?;
	for user in [bob, user_id!("@phantom:stale.invalid")] {
		services.db["roomuserid_joined"]
			.insert(&serialize_key((room, user))?, 1_u64.to_be_bytes())
			.await?;
	}
	assert_eq!(
		services
			.state_cache
			.bounded_room_members(room)
			.await?
			.len(),
		4
	);
	let lock = services.state.mutex.lock(room).await;
	// Retain the actual plan after the canonical commit, before its first
	// recipient transaction, so a new process must finish the frozen decision.
	refusal::refuse_next(RECEIPT);
	let event = services
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(
				"accepted state notification",
			)),
			user_id!("@source:localhost"),
			room,
			&lock,
		)
		.await?;
	assert_eq!(refusal::pending(), 0, "recipient commit refusal fired");
	let raw = services.timeline.get_pdu_id(&event).await?;
	let bytes = services.db[PLAN].get(&raw).await?;
	let plan: Value = serde_json::from_slice(&bytes)?;
	assert_eq!(plan["recipients"].as_array().unwrap().len(), 1);
	assert_eq!(plan["recipients"][0]["user"], carol.as_str());
	assert_eq!(plan["recipients"][0]["actions"], json!(["notify"]));
	assert_eq!(plan["next"], 0);
	fs::write(root.join("notification-event"), event.as_str())?;
	fs::write(root.join("notification-plan"), bytes.as_ref())?;
	drop(lock);
	// Subsequent rules must not change the already accepted work.
	rules(services, carol, None, false).await?;
	Ok(())
}

async fn restart(services: &Services, root: &Path, phase: &str) -> Result {
	let event = EventId::parse(fs::read_to_string(root.join("notification-event"))?)?;
	let raw = services.timeline.get_pdu_id(&event).await?;
	let carol = user_id!("@notification-carol:localhost");
	let bob = user_id!("@notification-bob:localhost");
	let count_key = serialize_key((carol, room_id!("!source-handoff:localhost")))?;
	if phase == "notification-restart" {
		assert_eq!(
			services.db[PLAN].get(&raw).await?.as_ref(),
			fs::read(root.join("notification-plan"))?
		);
		services.pusher.restore_notifications().await?;
		let count = services.db["userroomid_notificationcount"]
			.get(&count_key)
			.await?;
		fs::write(root.join("notification-count"), count.as_ref())?;
	} else {
		services.pusher.restore_notifications().await?;
		assert_eq!(
			services.db["userroomid_notificationcount"]
				.get(&count_key)
				.await?
				.as_ref(),
			fs::read(root.join("notification-count"))?
		);
	}
	assert!(
		services.db[PLAN]
			.get(&raw)
			.await
			.is_err_and(|error| error.is_not_found())
	);
	let saved = receipt(services, &event, carol)
		.await?
		.expect("accepted member receives receipt");
	assert_eq!(saved["actions"], json!(["notify"]));
	assert_eq!(saved["canceled"], false);
	assert!(
		receipt(services, &event, bob).await?.is_none(),
		"departed member receives no work"
	);
	Ok(())
}

async fn overlay(services: &Services) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let bob = user_id!("@notification-bob:localhost");
	let carol = user_id!("@notification-carol:localhost");
	register(services, bob).await?;
	membership(services, bob, MembershipState::Join).await?;
	register(services, carol).await?;
	// A sender never receives their own push. Bob's rule must see Carol's
	// not-yet-canonical join in addition to the three existing members.
	rules(services, bob, Some(4), true).await?;
	assert_eq!(
		services
			.state_cache
			.bounded_room_members(room)
			.await?
			.len(),
		3
	);
	let lock = services.state.mutex.lock(room).await;
	let event = services
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				carol.to_string(),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
			carol,
			room,
			&lock,
		)
		.await?;
	assert_eq!(
		receipt(services, &event, bob)
			.await?
			.expect("pending join counted")["actions"],
		json!(["notify"])
	);
	assert!(
		receipt(services, &event, carol).await?.is_none(),
		"self notification stays suppressed"
	);
	Ok(())
}

async fn corrupt(services: &Services) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let hash = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let mut cells = services
		.state_accessor
		.state_full_shortids(hash)
		.map_ok(|(key, event)| compress_state_event(key, event))
		.try_collect::<CompressedState>()
		.await?;
	let member = services
		.state_accessor
		.room_state_get(room, &StateEventType::RoomMember, "@source:localhost")
		.await?;
	// Both dictionaries are valid, but this key falsely binds Alice's event to
	// another user. The notification path must refuse even with no recipients.
	let key = services
		.short
		.get_or_create_shortstatekey(&StateEventType::RoomMember, "@wrong:localhost")
		.await?;
	cells.insert(
		services
			.state_compressor
			.compress_state_event(key, &member.event_id)
			.await?,
	);
	assert_refused(services, cells, "Append membership binding is invalid").await
}

async fn budget(services: &Services) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let original = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let base = services
		.state_accessor
		.state_full_shortids(original)
		.map_ok(|(key, event)| compress_state_event(key, event))
		.try_collect::<CompressedState>()
		.await?;
	let prototype = services
		.state_accessor
		.room_state_get(room, &StateEventType::RoomMember, "@remote:handoff.invalid")
		.await?;
	for (prefix, count, padding) in [("rows", 1023, 0), ("bytes", 700, 200)] {
		let mut cells = base.clone();
		for index in 0..count {
			let mut pdu = prototype.clone();
			pdu.event_id = EventId::parse(format!("$notification-{prefix}-{index}"))?;
			let user =
				UserId::parse(format!("@member-{index}{}:budget.invalid", "x".repeat(padding)))?;
			pdu.state_key = Some(user.to_string().into());
			pdu.sender = user;
			services
				.timeline
				.add_pdu_outlier(&pdu.event_id, &to_canonical_object(&pdu)?)
				.await?;
			let key = services
				.short
				.get_or_create_shortstatekey(
					&StateEventType::RoomMember,
					pdu.state_key.as_ref().unwrap().as_str(),
				)
				.await?;
			cells.insert(
				services
					.state_compressor
					.compress_state_event(key, &pdu.event_id)
					.await?,
			);
		}
		assert_refused(services, cells, "Notification member inventory limit reached").await?;
	}
	Ok(())
}

async fn assert_refused(services: &Services, cells: CompressedState, expected: &str) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let before = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let frontier = services
		.timeline
		.latest_pdu_in_room(room)
		.await?
		.event_id;
	let lock = services.state.mutex.lock(room).await;
	let (pdu, json) = services
		.timeline
		.create_hash_and_sign_event(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(expected)),
			user_id!("@source:localhost"),
			room,
			&lock,
		)
		.await?;
	let saved = services
		.state_compressor
		.save_state(room, Arc::new(cells))
		.await?;
	let error = services
		.timeline
		.append_pdu_with_txnid(
			&pdu,
			json,
			std::iter::once(&*pdu.event_id),
			None,
			Some(saved.shortstatehash),
			&lock,
		)
		.await
		.expect_err("invalid notification state refuses canonical admission");
	assert!(error.to_string().contains(expected), "unexpected refusal: {error}");
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
		before
	);
	assert_eq!(
		services
			.timeline
			.latest_pdu_in_room(room)
			.await?
			.event_id,
		frontier
	);
	assert!(
		services
			.timeline
			.get_pdu_id(&pdu.event_id)
			.await
			.is_err_and(|error| error.is_not_found())
	);
	assert!(
		services.db[PLAN]
			.raw_keys_after(None, 1)
			.await?
			.is_empty()
	);
	Ok(())
}
