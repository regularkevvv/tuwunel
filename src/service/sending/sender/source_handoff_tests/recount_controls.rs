//! Native multi-page recovery through actual forced-state publication. The
//! supplied outlier state tests durable projection, not peer authentication.

use std::{collections::BTreeSet, fs, path::Path, sync::Arc, time::Duration};

use futures::TryStreamExt;
use ruma::{OwnedUserId, events::StateEventType, room_id, user_id};
use serde::{Deserialize, Serialize};
use tuwunel_core::Result;
use tuwunel_database::{Interfix, refusal};

use crate::{
	Services,
	rooms::state_compressor::{CompressedState, HashSetCompressStateEvent},
};

const WIDE_MEMBERS: usize = 425;

#[derive(Deserialize, Serialize)]
struct Expected {
	state: u64,
	count: u64,
	removed: bool,
}

pub(super) async fn child(services: &Arc<Services>, root: &Path, phase: &str) -> Result {
	if phase == "membership-wide-restart" || phase == "membership-wide-again" {
		return restart(services, root).await;
	}
	super::setup(services).await?;
	let removed = phase == "membership-wide-remove";
	let saved = prepare(services, removed).await?;
	if phase == "membership-wide-crash" {
		return crash(services.clone(), root, saved).await;
	}
	assert!(matches!(phase, "membership-wide-add" | "membership-wide-remove"));
	refusal::refuse_after("roomserverids", 1);
	force(services, saved.clone())
		.await
		.expect_err("second server-pair page refuses");
	assert_eq!(refusal::pending(), 0, "second actual page consumed the refusal");
	let expected = record(services, root, saved.shortstatehash, removed).await?;
	assert_eq!(
		raw_count(services).await?,
		if removed { 427 } else { 2 },
		"partial recount cannot publish final aggregates"
	);
	assert_eq!(
		server_rows(services).await?.len(),
		if removed { 227 } else { 202 },
		"exactly one 200-server page committed before refusal"
	);
	assert_projected_members(services, &expected).await
}

async fn prepare(services: &Services, removed: bool) -> Result<HashSetCompressStateEvent> {
	let room = room_id!("!source-handoff:localhost");
	let state = services
		.state
		.get_room_shortstatehash(room)
		.await?;
	let original = Arc::new(
		services
			.state_accessor
			.state_full_shortids(state)
			.map_ok(|(key, event)| {
				crate::rooms::state_compressor::compress_state_event(key, event)
			})
			.try_collect::<CompressedState>()
			.await?,
	);
	let mut cells = (*original).clone();
	let selected = services
		.state_accessor
		.room_state_get(room, &StateEventType::RoomMember, "@source:localhost")
		.await?;
	for index in 0..WIDE_MEMBERS {
		let user = wide_user(index)?;
		let mut pdu = selected.clone();
		pdu.event_id = ruma::EventId::parse(format!("$membership-page-{index}"))?;
		pdu.sender = user.clone();
		pdu.state_key = Some(user.to_string().into());
		pdu.prev_events.clear();
		pdu.auth_events.clear();
		pdu.unsigned = None;
		services
			.timeline
			.add_pdu_outlier(&pdu.event_id, &tuwunel_core::utils::to_canonical_object(&pdu)?)
			.await?;
		let key = services
			.short
			.get_or_create_shortstatekey(&StateEventType::RoomMember, user.as_str())
			.await?;
		cells.insert(
			services
				.state_compressor
				.compress_state_event(key, &pdu.event_id)
				.await?,
		);
	}
	let wide = services
		.state_compressor
		.save_state(room, Arc::new(cells))
		.await?;
	if !removed {
		return Ok(wide);
	}
	force(services, wide).await?;
	assert_eq!(
		services
			.state_cache
			.room_joined_count(room)
			.await?,
		427
	);
	assert_eq!(server_rows(services).await?.len(), 427);
	services
		.state_compressor
		.save_state(room, original)
		.await
}

async fn force(services: &Services, saved: HashSetCompressStateEvent) -> Result {
	let room = room_id!("!source-handoff:localhost");
	let lock = services.state.mutex.lock(room).await;
	services
		.state
		.force_state(room, saved.shortstatehash, saved.added, saved.removed, &lock)
		.await
}

async fn crash(services: Arc<Services>, root: &Path, saved: HashSetCompressStateEvent) -> Result {
	let mut pause = refusal::pause_next("roomid_joinedcount");
	let writer = services.clone();
	let state = saved.shortstatehash;
	let mut task = tokio::spawn(async move { force(&writer, saved).await });
	tokio::select! {
		result = &mut task => panic!("forced state ended before final aggregate pause: {result:?}"),
		result = tokio::time::timeout(Duration::from_secs(30), pause.entered()) => {
			result.expect("actual final aggregate commit reaches owned pause")?;
		},
	}
	let expected = record(&services, root, state, false).await?;
	assert_projected_members(&services, &expected).await?;
	assert_eq!(server_rows(&services).await?.len(), 402, "two intermediate pages are durable");
	assert_eq!(raw_count(&services).await?, 2, "final aggregate remains uncommitted");
	fs::write(
		root.join("source.ready"),
		b"intermediate pages committed; final server pairs and counts paused",
	)?;
	std::future::pending::<()>().await;
	Ok(())
}

async fn record(services: &Services, root: &Path, state: u64, removed: bool) -> Result<Expected> {
	let room = room_id!("!source-handoff:localhost");
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
		state
	);
	let plan: serde_json::Value = serde_json::from_slice(
		&services.db["global"]
			.qry(&("membership_projection_v1", room))
			.await?,
	)?;
	assert_eq!(plan["members"].as_array().unwrap().len(), WIDE_MEMBERS);
	assert_eq!(
		services.db["global"]
			.qry(&("membership_projection_cursor_v1", room))
			.await?
			.as_ref(),
		u64::try_from(WIDE_MEMBERS).unwrap().to_be_bytes(),
		"every membership index committed before recount"
	);
	assert!(
		services.db["global"]
			.qry(&("membership_recount_pending", room))
			.await?
			.is_empty()
	);
	let expected = Expected {
		state,
		count: plan["count"].as_u64().unwrap(),
		removed,
	};
	fs::write(root.join("membership-wide.json"), serde_json::to_vec(&expected)?)?;
	Ok(expected)
}

async fn restart(services: &Services, root: &Path) -> Result {
	let expected: Expected =
		serde_json::from_slice(&fs::read(root.join("membership-wide.json"))?)?;
	let room = room_id!("!source-handoff:localhost");
	assert_eq!(
		services
			.state
			.get_room_shortstatehash(room)
			.await?,
		expected.state
	);
	services
		.state_cache
		.restore_pending_recounts()
		.await?;
	assert_projected_members(services, &expected).await?;
	let mut members: BTreeSet<_> = [
		user_id!("@source:localhost").to_owned(),
		user_id!("@remote:handoff.invalid").to_owned(),
	]
	.into_iter()
	.collect();
	if !expected.removed {
		for index in 0..WIDE_MEMBERS {
			members.insert(wide_user(index)?);
		}
	}
	assert_eq!(
		services
			.state_cache
			.bounded_room_members(room)
			.await?
			.into_iter()
			.collect::<BTreeSet<_>>(),
		members
	);
	let servers: BTreeSet<_> = members
		.iter()
		.map(|user| user.server_name().to_owned())
		.collect();
	assert_eq!(server_rows(services).await?, servers);
	for server in &servers {
		assert!(
			services.db["serverroomids"]
				.contains_checked(&(server, room))
				.await?
		);
	}
	for index in 0..WIDE_MEMBERS {
		assert_eq!(
			services.db["serverroomids"]
				.contains_checked(&(wide_user(index)?.server_name(), room))
				.await?,
			!expected.removed
		);
	}
	assert_eq!(
		services
			.state_cache
			.room_joined_count(room)
			.await?,
		u64::try_from(members.len()).unwrap()
	);
	assert_eq!(
		services
			.state_cache
			.room_invited_count(room)
			.await?,
		0
	);
	assert_eq!(
		services
			.state_cache
			.room_knocked_count(room)
			.await?,
		0
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
	Ok(())
}

async fn assert_projected_members(services: &Services, expected: &Expected) -> Result {
	let room = room_id!("!source-handoff:localhost");
	for index in 0..WIDE_MEMBERS {
		let user = wide_user(index)?;
		let map = if expected.removed {
			"roomuserid_leftcount"
		} else {
			"roomuserid_joined"
		};
		assert_eq!(
			services.db[map]
				.qry(&(room, &user))
				.await?
				.as_ref(),
			expected.count.to_be_bytes(),
			"recount recovery never changes the accepted membership position"
		);
		assert_eq!(
			services.db["userroomid_joined"]
				.contains_checked(&(&user, room))
				.await?,
			!expected.removed
		);
		if !expected.removed {
			assert_eq!(
				services.db["userroomid_joined"]
					.qry(&(&user, room))
					.await?
					.as_ref(),
				expected.count.to_be_bytes()
			);
		}
	}
	Ok(())
}

fn wide_user(index: usize) -> Result<OwnedUserId> {
	Ok(ruma::UserId::parse(format!("@member:page-{index:04}.invalid"))?)
}

async fn raw_count(services: &Services) -> Result<u64> {
	Ok(u64::from_be_bytes(
		services.db["roomid_joinedcount"]
			.get(room_id!("!source-handoff:localhost").as_bytes())
			.await?
			.as_ref()
			.try_into()
			.unwrap(),
	))
}

async fn server_rows(services: &Services) -> Result<BTreeSet<ruma::OwnedServerName>> {
	services.db["roomserverids"]
		.keys_prefix::<(tuwunel_database::Ignore, ruma::OwnedServerName), _>(&(
			room_id!("!source-handoff:localhost"),
			Interfix,
		))
		.map_ok(|(_, server)| server)
		.try_collect()
		.await
}
