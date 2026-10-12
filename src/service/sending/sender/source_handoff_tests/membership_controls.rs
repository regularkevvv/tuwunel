//! A refused or killed derived update must not outlive startup recovery.
//! Either atomic canonical refusal or recovered accepted membership is safe.

use std::{collections::BTreeSet, fs, path::Path, sync::Arc, time::Duration};

use futures::TryStreamExt;
use ruma::{
	OwnedUserId,
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
	room_id, user_id,
};
use serde::{Deserialize, Serialize};
use tuwunel_core::{
	Result,
	matrix::{Event, pdu::PduBuilder},
};
use tuwunel_database::refusal;

use crate::Services;

#[derive(Deserialize, Serialize)]
struct Expected {
	state: u64,
	joined: bool,
	event: String,
	members: Vec<OwnedUserId>,
	count: u64,
	forgotten: bool,
}

pub(super) async fn child(services: &Arc<Services>, root: &Path, phase: &str) -> Result {
	if phase.starts_with("membership-wide-") {
		return super::recount_controls::child(services, root, phase).await;
	}
	if phase == "membership-next" {
		return next_publication(services, root).await;
	}
	if phase == "membership-stripped-restart" || phase == "membership-stripped-again" {
		return stripped_restart(services, root).await;
	}
	if phase == "membership-initial-restart" || phase == "membership-initial-again" {
		return initial_restart(services, root, phase).await;
	}
	if phase == "membership-restart" || phase == "membership-again" {
		return restart(services, root).await;
	}
	if phase == "membership-force-restart" || phase == "membership-force-again" {
		return force_restart(services, root).await;
	}
	super::setup(services).await?;
	if phase == "membership-stripped-prepare" {
		return stripped_prepare(services, root).await;
	}
	if phase == "membership-initial-prepare" {
		return initial_prepare(services, root).await;
	}
	if phase == "membership-force-refuse" {
		return force_refuse(services, root).await;
	}
	if phase == "membership-corrupt-forget" {
		return corrupt_and_forget(services, root).await;
	}
	if phase == "membership-crash" {
		return crash(services.clone(), root).await;
	}
	assert_eq!(phase, "membership-refuse");
	let before = authoritative(services).await?;
	assert!(before.joined);
	refusal::refuse_next("roomuserid_leftcount");
	let result = leave(services).await;
	assert_eq!(refusal::pending(), 0, "actual membership-index refusal fired");
	let after = authoritative(services).await?;
	if result.is_err() {
		// A future atomic implementation may refuse the whole acceptance.
		assert_eq!(after.state, before.state);
		assert_eq!(after.event, before.event);
		assert!(after.joined);
	} else {
		assert!(!after.joined, "accepted departure is authoritative");
	}
	fs::write(root.join("membership-expected.json"), serde_json::to_vec(&after)?)?;
	Ok(())
}

async fn leave(services: &Services) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let alice = user_id!("@source:localhost");
	let lock = services.state.mutex.lock(room).await;
	services
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				alice.to_string(),
				&RoomMemberEventContent::new(MembershipState::Leave),
			),
			alice,
			room,
			&lock,
		)
		.await?;
	Ok(())
}

// The second process publishes without calling the startup repair helper.
// Canonical admission itself must finish the earlier writer's durable work.
async fn next_publication(services: &Services, root: &Path) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let remote = user_id!("@remote:handoff.invalid");
	let first: Expected =
		serde_json::from_slice(&fs::read(root.join("membership-expected.json"))?)?;
	assert!(!first.joined, "first process accepted Alice's departure");
	assert!(
		services
			.state_cache
			.is_joined(user_id!("@source:localhost"), room)
			.await,
		"replacement writer observes the earlier unprojected index"
	);
	let lock = services.state.mutex.lock(room).await;
	services
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				remote.to_string(),
				&RoomMemberEventContent::new(MembershipState::Leave),
			),
			remote,
			room,
			&lock,
		)
		.await?;
	assert_no_projection(services).await?;
	let expected = authoritative(services).await?;
	assert_ne!(expected.state, first.state, "second canonical state was accepted");
	assert_eq!(expected.event, first.event, "Alice's accepted departure stays selected");
	assert_eq!(expected.count, first.count, "first writer's saved position stays fixed");
	assert!(expected.members.is_empty());
	fs::write(root.join("membership-expected.json"), serde_json::to_vec(&expected)?)?;
	Ok(())
}

async fn authoritative(services: &Services) -> Result<Expected> {
	let room = room_id!("!source-handoff:localhost");
	let pdu = services
		.state_accessor
		.room_state_get(room, &StateEventType::RoomMember, "@source:localhost")
		.await?;
	let member: RoomMemberEventContent = pdu.get_content()?;
	let state = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let mut members = services
		.state_accessor
		.notification_members_for_append(state, &pdu)
		.await?;
	members.sort();
	Ok(Expected {
		state,
		joined: member.membership == MembershipState::Join,
		event: pdu.event_id.to_string(),
		members,
		count: services
			.timeline
			.get_pdu_id(&pdu.event_id)
			.await?
			.pdu_count()
			.into_unsigned(),
		forgotten: false,
	})
}

async fn crash(services: Arc<Services>, root: &Path) -> Result {
	let mut pause = refusal::pause_next("roomuserid_leftcount");
	let writer = services.clone();
	let _writer = tokio::spawn(async move { leave(&writer).await });
	tokio::time::timeout(Duration::from_secs(20), pause.entered())
		.await
		.expect("owned membership commit reaches its actual pause")?;
	// An atomic implementation can still have the original state here. A
	// post-acceptance projector must retain the accepted departure on restart.
	let expected = authoritative(&services).await?;
	fs::write(root.join("membership-expected.json"), serde_json::to_vec(&expected)?)?;
	fs::write(
		root.join("source.ready"),
		b"membership index dispatch paused; canonical state recorded",
	)?;
	std::future::pending::<()>().await;
	Ok(())
}

async fn restart(services: &Services, root: &Path) -> Result {
	let expected: Expected =
		serde_json::from_slice(&fs::read(root.join("membership-expected.json"))?)?;
	let before = authoritative(services).await?;
	assert_eq!(before.state, expected.state, "canonical state survives the process boundary");
	assert_eq!(before.event, expected.event, "canonical event survives the process boundary");
	assert_eq!(before.joined, expected.joined);
	// This is the current startup membership recovery entry point. It must
	// recover the indexes themselves, as well as aggregates derived from them.
	services
		.state_cache
		.restore_pending_recounts()
		.await?;
	let members = services
		.state_cache
		.bounded_room_members(room_id!("!source-handoff:localhost"))
		.await?;
	assert_eq!(
		members
			.iter()
			.any(|user| user == user_id!("@source:localhost")),
		expected.joined,
		"startup recovery must reconcile derived membership with accepted state"
	);
	let mut members = members;
	members.sort();
	assert_eq!(members, expected.members, "all membership indexes match accepted state");
	assert_eq!(
		services
			.state_cache
			.room_joined_count(room_id!("!source-handoff:localhost"))
			.await?,
		u64::try_from(expected.members.len()).unwrap()
	);
	let servers = services
		.state_cache
		.room_servers_fallible(room_id!("!source-handoff:localhost"))
		.map_ok(ToOwned::to_owned)
		.try_collect::<BTreeSet<_>>()
		.await?;
	assert_eq!(
		servers,
		expected
			.members
			.iter()
			.map(|user| user.server_name().to_owned())
			.collect()
	);
	let map = if expected.joined {
		"roomuserid_joined"
	} else {
		"roomuserid_leftcount"
	};
	if expected.forgotten {
		assert!(
			!services.db[map]
				.contains_checked(&(
					room_id!("!source-handoff:localhost"),
					user_id!("@source:localhost")
				))
				.await?
		);
	} else {
		assert_eq!(
			services.db[map]
				.qry(&(room_id!("!source-handoff:localhost"), user_id!("@source:localhost")))
				.await?
				.as_ref(),
			expected.count.to_be_bytes()
		);
	}
	assert_no_projection(services).await?;
	Ok(())
}

async fn assert_no_projection(services: &Services) -> Result {
	for prefix in [
		"membership_projection_v1",
		"membership_projection_witness_v1",
		"membership_projection_cursor_v1",
		"membership_recount_pending",
	] {
		assert!(
			!services.db["global"]
				.contains_checked(&(prefix, room_id!("!source-handoff:localhost")))
				.await?
		);
	}
	Ok(())
}

async fn force_refuse(services: &Services, root: &Path) -> Result {
	use crate::rooms::state_compressor::{CompressedState, compress_state_event};
	let room = room_id!("!source-handoff:localhost");
	let lock = services.state.mutex.lock(room).await;
	let state = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let entries = services
		.state_accessor
		.state_full_shortids(state)
		.try_collect::<Vec<_>>()
		.await?;
	let mut cells = CompressedState::new();
	for (key, event) in entries {
		let (kind, _) = services
			.short
			.get_statekey_from_short(key)
			.await?;
		if kind != StateEventType::RoomMember {
			cells.insert(compress_state_event(key, event));
		}
	}
	let saved = services
		.state_compressor
		.save_state(room, Arc::new(cells))
		.await?;
	refusal::refuse_next("roomuserid_leftcount");
	services
		.state
		.force_state(room, saved.shortstatehash, saved.added, saved.removed, &lock)
		.await
		.expect_err("accepted forced state retains failed member projection");
	assert_eq!(refusal::pending(), 0);
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
		saved.shortstatehash
	);
	assert_eq!(
		services
			.state_cache
			.bounded_room_members(room)
			.await?
			.len(),
		2
	);
	fs::write(root.join("membership-force-state"), saved.shortstatehash.to_string())?;
	Ok(())
}

async fn force_restart(services: &Services, root: &Path) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let expected: u64 = fs::read_to_string(root.join("membership-force-state"))?
		.parse()
		.unwrap();
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
		expected
	);
	for (key, _) in services
		.state_accessor
		.state_full_shortids(expected)
		.try_collect::<Vec<_>>()
		.await?
	{
		assert_ne!(
			services
				.short
				.get_statekey_from_short(key)
				.await?
				.0,
			StateEventType::RoomMember
		);
	}
	services
		.state_cache
		.restore_pending_recounts()
		.await?;
	assert!(
		services
			.state_cache
			.bounded_room_members(room)
			.await?
			.is_empty()
	);
	assert_eq!(
		services
			.state_cache
			.room_joined_count(room)
			.await?,
		0
	);
	assert!(
		services
			.state_cache
			.room_servers_fallible(room)
			.try_collect::<Vec<_>>()
			.await?
			.is_empty()
	);
	assert_no_projection(services).await
}

// Synthetic send_join state exercises the publication/recovery boundary, not
// remote peer signatures or authorization. This room has no canonical PDU.
async fn initial_prepare(services: &Services, root: &Path) -> Result {
	use crate::rooms::state_compressor::CompressedState;
	let room = room_id!("!membership-initial:localhost");
	services
		.short
		.get_or_create_shortroomid(room)
		.await?;
	let mut cells = CompressedState::new();
	for (kind, key, event) in [
		(StateEventType::RoomCreate, "", "$initial-create"),
		(StateEventType::RoomMember, "@remote:handoff.invalid", "$initial-member"),
	] {
		let mut pdu = services
			.state_accessor
			.room_state_get(room_id!("!source-handoff:localhost"), &kind, key)
			.await?;
		pdu.room_id = room.to_owned();
		pdu.event_id = ruma::EventId::parse(event)?;
		pdu.prev_events.clear();
		pdu.auth_events.clear();
		pdu.unsigned = None;
		services
			.timeline
			.add_pdu_outlier(&pdu.event_id, &tuwunel_core::utils::to_canonical_object(&pdu)?)
			.await?;
		let shortkey = services
			.short
			.get_or_create_shortstatekey(&kind, key)
			.await?;
		cells.insert(
			services
				.state_compressor
				.compress_state_event(shortkey, &pdu.event_id)
				.await?,
		);
	}
	let lock = services.state.mutex.lock(room).await;
	let saved = services
		.state_compressor
		.save_state(room, Arc::new(cells))
		.await?;
	refusal::refuse_next("roomuserid_joined");
	services
		.state
		.force_state(room, saved.shortstatehash, saved.added, saved.removed, &lock)
		.await
		.expect_err("initial accepted state retains refused membership work");
	assert_eq!(refusal::pending(), 0);
	assert!(!services.metadata.exists_checked(room).await?, "no local timeline was created");
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
		saved.shortstatehash
	);
	let plan: serde_json::Value = serde_json::from_slice(
		&services.db["global"]
			.qry(&("membership_projection_v1", room))
			.await?,
	)?;
	fs::write(
		root.join("membership-initial.json"),
		serde_json::to_vec(
			&serde_json::json!({"state":saved.shortstatehash,"count":plan["count"]}),
		)?,
	)?;
	Ok(())
}

async fn initial_restart(services: &Services, root: &Path, phase: &str) -> Result {
	let room = room_id!("!membership-initial:localhost");
	let expected: serde_json::Value =
		serde_json::from_slice(&fs::read(root.join("membership-initial.json"))?)?;
	assert!(!services.metadata.exists_checked(room).await?);
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
		expected["state"].as_u64().unwrap()
	);
	services
		.state_cache
		.restore_pending_recounts()
		.await?;
	assert_eq!(
		services
			.state_cache
			.bounded_room_members(room)
			.await?,
		vec![user_id!("@remote:handoff.invalid").to_owned()]
	);
	assert_eq!(
		services
			.state_cache
			.room_joined_count(room)
			.await?,
		1
	);
	assert_eq!(
		services.db["roomuserid_joined"]
			.qry(&(room, user_id!("@remote:handoff.invalid")))
			.await?
			.as_ref(),
		expected["count"].as_u64().unwrap().to_be_bytes()
	);
	for prefix in [
		"membership_projection_v1",
		"membership_projection_witness_v1",
		"membership_projection_cursor_v1",
		"membership_recount_pending",
	] {
		assert!(
			!services.db["global"]
				.contains_checked(&(prefix, room))
				.await?
		);
	}
	if phase == "membership-initial-restart" {
		// Also recover a standalone aggregate obligation for this initial
		// room: the second process must not require a canonical timeline.
		services.db["global"]
			.put_raw(("membership_recount_pending", room), [])
			.await?;
	}
	Ok(())
}

async fn corrupt_and_forget(services: &Services, root: &Path) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let key = ("membership_projection_v1", room);
	refusal::refuse_next("roomuserid_leftcount");
	leave(services).await?;
	assert_eq!(refusal::pending(), 0);
	let original = services.db["global"].qry(&key).await?.to_vec();
	for field in ["count", "next", "cursor"] {
		let corrupt_key = if field == "cursor" {
			("membership_projection_cursor_v1", room)
		} else {
			key
		};
		let value = if field == "cursor" {
			1_u64.to_be_bytes().to_vec()
		} else {
			let mut corrupt: serde_json::Value = serde_json::from_slice(&original)?;
			corrupt[field] = if field == "next" {
				serde_json::json!(1)
			} else {
				serde_json::json!(99999)
			};
			serde_json::to_vec(&corrupt)?
		};
		services.db["global"]
			.put_raw(corrupt_key, &value)
			.await?;
		services
			.state_cache
			.restore_pending_recounts()
			.await
			.expect_err("corrupt immutable work or advanced cursor refuses");
		assert_eq!(
			services.db["global"]
				.qry(&corrupt_key)
				.await?
				.as_ref(),
			value
		);
		assert_eq!(
			services
				.state_cache
				.bounded_room_members(room)
				.await?
				.len(),
			2,
			"refusal did not project any membership"
		);
		let restored = if field == "cursor" {
			0_u64.to_be_bytes().to_vec()
		} else {
			original.clone()
		};
		services.db["global"]
			.put_raw(corrupt_key, &restored)
			.await?;
	}

	services
		.state_cache
		.forget(room, user_id!("@source:localhost"))
		.await?;
	let mut expected = authoritative(services).await?;
	expected.forgotten = true;
	fs::write(root.join("membership-expected.json"), serde_json::to_vec(&expected)?)?;
	Ok(())
}

// A synthetic resolved snapshot selects an invite for Bob while retaining
// Alice's original join. The accepted incoming leave for Alice loses that
// state choice. This tests the canonical handoff, not federation
// resolution/auth.
async fn stripped_prepare(services: &Services, root: &Path) -> Result {
	use crate::rooms::state_compressor::{CompressedState, compress_state_event};
	let room = room_id!("!source-handoff:localhost");
	let alice = user_id!("@source:localhost");
	let bob = user_id!("@invited:invite.invalid");
	let lock = services.state.mutex.lock(room).await;
	let state = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let before = Arc::new(
		services
			.state_accessor
			.state_full_shortids(state)
			.map_ok(|(key, event)| compress_state_event(key, event))
			.try_collect::<CompressedState>()
			.await?,
	);
	let selected = services
		.state_accessor
		.room_state_get(room, &StateEventType::RoomMember, alice.as_str())
		.await?;
	let mut invited = selected.clone();
	invited.event_id = ruma::EventId::parse("$projection-resolved-invite")?;
	invited.state_key = Some(bob.to_string().into());
	invited.content =
		PduBuilder::state(bob.to_string(), &RoomMemberEventContent::new(MembershipState::Invite))
			.content;
	invited.prev_events.clear();
	invited.auth_events.clear();
	invited.unsigned = None;
	services
		.timeline
		.add_pdu_outlier(&invited.event_id, &tuwunel_core::utils::to_canonical_object(&invited)?)
		.await?;
	let (pending, json) = services
		.timeline
		.create_hash_and_sign_event(
			PduBuilder::state(
				alice.to_string(),
				&RoomMemberEventContent::new(MembershipState::Leave),
			),
			alice,
			room,
			&lock,
		)
		.await?;
	let mut cells = (*before).clone();
	let key = services
		.short
		.get_or_create_shortstatekey(&StateEventType::RoomMember, bob.as_str())
		.await?;
	cells.insert(
		services
			.state_compressor
			.compress_state_event(key, &invited.event_id)
			.await?,
	);
	let saved = services
		.state_compressor
		.save_state(room, Arc::new(cells))
		.await?;
	refusal::refuse_next("roomuserid_invitecount");
	let raw = services
		.timeline
		.append_incoming_pdu(
			&pending,
			json,
			std::iter::once(pending.event_id()),
			before,
			false,
			Some(saved.shortstatehash),
			false,
			&lock,
		)
		.await?
		.expect("accepted incoming event with supplied resolved state");
	assert_eq!(refusal::pending(), 0);
	assert!(
		services.state_cache.is_joined(alice, room).await,
		"losing incoming leave must not project over accepted join"
	);
	let plan: serde_json::Value = serde_json::from_slice(
		&services.db["global"]
			.qry(&("membership_projection_v1", room))
			.await?,
	)?;
	assert_eq!(plan["members"].as_array().unwrap().len(), 1);
	assert_eq!(plan["members"][0]["user"], bob.as_str());
	assert_stripped_join(plan["members"][0]["stripped"].as_array().unwrap());
	fs::write(
		root.join("membership-stripped.json"),
		serde_json::to_vec(
			&serde_json::json!({"state":saved.shortstatehash,"count":raw.pdu_count().into_unsigned(),"event":pending.event_id.as_str(),"alice_event":selected.event_id.as_str()}),
		)?,
	)?;
	Ok(())
}

fn assert_stripped_join(state: &[serde_json::Value]) {
	let alice = state
		.iter()
		.find(|event| {
			event["type"] == "m.room.member" && event["state_key"] == "@source:localhost"
		})
		.expect("accepted sender membership is in stripped state");
	assert_eq!(
		alice["content"]["membership"], "join",
		"frozen summary follows selected state, not the losing incoming leave"
	);
}

async fn stripped_restart(services: &Services, root: &Path) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let bob = user_id!("@invited:invite.invalid");
	let expected: serde_json::Value =
		serde_json::from_slice(&fs::read(root.join("membership-stripped.json"))?)?;
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
		expected["state"].as_u64().unwrap()
	);
	assert_eq!(
		services
			.timeline
			.get_pdu_id(&ruma::EventId::parse(expected["event"].as_str().unwrap())?)
			.await?
			.pdu_count()
			.into_unsigned(),
		expected["count"].as_u64().unwrap()
	);
	services
		.state_cache
		.restore_pending_recounts()
		.await?;
	assert_eq!(
		services
			.state_accessor
			.room_state_get(room, &StateEventType::RoomMember, "@source:localhost")
			.await?
			.event_id
			.as_str(),
		expected["alice_event"].as_str().unwrap()
	);
	assert!(
		services
			.state_cache
			.is_joined(user_id!("@source:localhost"), room)
			.await
	);
	assert_eq!(
		services
			.state_cache
			.room_joined_count(room)
			.await?,
		2
	);
	assert_eq!(
		services
			.state_cache
			.room_invited_count(room)
			.await?,
		1
	);
	assert_eq!(
		services.db["roomuserid_invitecount"]
			.qry(&(room, bob))
			.await?
			.as_ref(),
		expected["count"].as_u64().unwrap().to_be_bytes()
	);
	let state: Vec<serde_json::Value> = serde_json::from_slice(
		&services.db["userroomid_invitestate"]
			.qry(&(bob, room))
			.await?,
	)?;
	assert_stripped_join(&state);
	assert_no_projection(services).await
}
