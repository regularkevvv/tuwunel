//! Real public device presence calls against an owned native database.
//! No workers or transport. Refusals must preserve accepted state and avoid
//! cache/push effects; concurrent updates must retire the superseded bodies.

use std::{fs, path::Path, sync::Arc};

use futures::future::try_join_all;
use ruma::{device_id, presence::PresenceState, user_id};
use serde::{Deserialize, Serialize};
use tokio::sync::Barrier;
use tuwunel_core::Result;

use crate::{Services, presence::Ping};

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Serialize, Deserialize)]
struct Expected {
	pointers: Rows,
	bodies: Rows,
	status: String,
	state: String,
}

pub(super) async fn child(services: &Arc<Services>, root: &Path, phase: &str) -> Result {
	if matches!(phase, "presence-restart" | "presence-again") {
		return Box::pin(restart(services, root, phase)).await;
	}
	Box::pin(super::setup(services)).await?;
	services
		.users
		.create_device(
			user_id!("@source:localhost"),
			Some(device_id!("presence-device")),
			(None, None),
			None,
			Some("native presence control"),
			None,
		)
		.await?;
	drain_setup_pushes(services);
	set(services, &PresenceState::Online, "accepted status").await?;
	let expected = capture(services).await?;
	assert!(next_push(services).is_none(), "owned fixture starts without push wakes");
	if phase == "presence-timer" {
		crate::Service::interrupt(services.presence.as_ref()).await;
		set(services, &PresenceState::Offline, "accepted without timer").await?;
		let committed = capture(services).await?;
		assert_eq!(committed.state, "offline");
		assert_eq!(committed.status, "accepted without timer");
		assert_eq!(committed.pointers.len(), 1);
		assert_eq!(committed.bodies.len(), 1);
		assert_eq!(next_push(services).as_deref(), Some("@source:localhost"));
		return save(root, &committed);
	}
	if phase == "presence-corrupt" {
		Box::pin(corrupt(services, &expected)).await?;
		return save(root, &expected);
	}
	#[cfg(debug_assertions)]
	if phase == "presence-cancel" {
		Box::pin(cancel(services, &expected)).await?;
		return save(root, &capture(services).await?);
	}
	if phase == "presence-concurrent" {
		Box::pin(concurrent(services)).await?;
		return save(root, &capture(services).await?);
	}
	let map = if phase == "presence-body" {
		"presenceid_presence"
	} else {
		"userid_presenceid"
	};
	tuwunel_database::refusal::refuse_next(map);
	set(services, &PresenceState::Offline, "refused status")
		.await
		.expect_err("injected persistence refusal must reach public setter");
	assert_eq!(tuwunel_database::refusal::pending(), 0);
	assert_saved(services, &expected).await?;
	if phase == "presence-cache" {
		services
			.presence
			.maybe_ping_presence(user_id!("@source:localhost"), Ping {
				device_id: Some(device_id!("presence-device")),
				new_state: Some(&PresenceState::Unavailable),
				..Default::default()
			})
			.await?;
		let event = services
			.presence
			.get_presence(user_id!("@source:localhost"))
			.await?;
		assert_eq!(
			event.content.status_msg.as_deref(),
			Some("accepted status"),
			"rejected device status leaked into a later ping"
		);
		assert_eq!(event.content.presence, PresenceState::Unavailable);
	} else if phase == "presence-hint" {
		assert!(
			next_push(services).is_none(),
			"rejected presence transition scheduled a push wake before commit"
		);
	} else {
		assert_eq!(phase, "presence-body");
	}
	save(root, &capture(services).await?)
}

async fn corrupt(services: &Services, expected: &Expected) -> Result {
	assert_eq!(expected.bodies.len(), 1);
	let (key, original) = &expected.bodies[0];
	for missing in [false, true] {
		if missing {
			services.db["presenceid_presence"]
				.remove(key)
				.await?;
		} else {
			services.db["presenceid_presence"]
				.raw_put(key, b"not-json")
				.await?;
		}
		let damaged = snapshot(services, "presenceid_presence").await?;
		set(services, &PresenceState::Offline, "must not replace damaged presence")
			.await
			.expect_err("malformed or missing saved body must refuse before mutation");
		assert_eq!(snapshot(services, "userid_presenceid").await?, expected.pointers);
		assert_eq!(snapshot(services, "presenceid_presence").await?, damaged);
		assert!(next_push(services).is_none());
		services.db["presenceid_presence"]
			.insert(key, original)
			.await?;
		assert_saved(services, expected).await?;
	}
	Ok(())
}

#[cfg(debug_assertions)]
async fn cancel(services: &Arc<Services>, expected: &Expected) -> Result {
	let mut pause = tuwunel_database::refusal::pause_next("userid_presenceid");
	let owned = services.clone();
	let task =
		tokio::spawn(
			async move { set(&owned, &PresenceState::Offline, "cancelled status").await },
		);
	tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered())
		.await
		.expect("owned presence transaction reaches pre-dispatch pause")?;
	assert_saved(services, expected).await?;
	task.abort();
	assert!(
		task.await
			.expect_err("owned update was aborted")
			.is_cancelled()
	);
	drop(pause);
	assert_saved(services, expected).await?;
	assert!(next_push(services).is_none(), "cancelled presence cannot wake pushes");
	tokio::time::timeout(
		std::time::Duration::from_secs(5),
		services
			.presence
			.maybe_ping_presence(user_id!("@source:localhost"), Ping {
				device_id: Some(device_id!("presence-device")),
				new_state: Some(&PresenceState::Unavailable),
				..Default::default()
			}),
	)
	.await
	.expect("cancelled update releases user exclusion")?;
	let event = services
		.presence
		.get_presence(user_id!("@source:localhost"))
		.await?;
	assert_eq!(event.content.status_msg.as_deref(), Some("accepted status"));
	Ok(())
}

async fn concurrent(services: &Arc<Services>) -> Result {
	let barrier = Arc::new(Barrier::new(16));
	let tasks = (0..16).map(|index| {
		let services = services.clone();
		let barrier = barrier.clone();
		tokio::spawn(async move {
			barrier.wait().await;
			set(&services, &PresenceState::Online, &format!("concurrent status {index}")).await
		})
	});
	for result in try_join_all(tasks).await? {
		result?;
	}
	let current = capture(services).await?;
	assert!(current.status.starts_with("concurrent status "));
	assert_eq!(current.pointers.len(), 1);
	assert_eq!(current.bodies.len(), 1, "concurrent updates retained orphaned presence bodies");
	Ok(())
}

async fn restart(services: &Services, root: &Path, phase: &str) -> Result {
	let expected: Expected =
		serde_json::from_slice(&fs::read(root.join("presence-expected.json"))?)?;
	assert_saved(services, &expected).await?;
	if phase == "presence-restart" {
		set(services, &PresenceState::Offline, "accepted cold retry").await?;
		let updated = capture(services).await?;
		assert_eq!(updated.status, "accepted cold retry");
		assert_eq!(updated.state, "offline");
		assert_eq!(updated.pointers.len(), 1);
		assert_eq!(updated.bodies.len(), 1);
		if expected.state == "online" {
			let wake =
				next_push(services).expect("committed inactive transition schedules a push wake");
			assert_eq!(wake, "@source:localhost");
		} else {
			assert!(next_push(services).is_none());
		}
		save(root, &updated)?;
	}
	Ok(())
}

fn drain_setup_pushes(services: &Services) {
	let mut wakes = services
		.sending
		.push_wakes
		.lock()
		.expect("locked");
	while wakes.next().is_some() {
		wakes.finish(crate::sending::wakes::Page::Refused);
	}
}

fn next_push(services: &Services) -> Option<String> {
	services
		.sending
		.push_wakes
		.lock()
		.expect("locked")
		.next()
		.map(|wake| wake.user.to_string())
}

async fn set(services: &Services, state: &PresenceState, status: &str) -> Result {
	services
		.presence
		.set_presence_for_device(
			user_id!("@source:localhost"),
			Some(device_id!("presence-device")),
			state,
			Some(status.to_owned()),
		)
		.await
}

async fn capture(services: &Services) -> Result<Expected> {
	let event = services
		.presence
		.get_presence(user_id!("@source:localhost"))
		.await?;
	Ok(Expected {
		pointers: snapshot(services, "userid_presenceid").await?,
		bodies: snapshot(services, "presenceid_presence").await?,
		status: event.content.status_msg.expect("owned status"),
		state: event.content.presence.to_string(),
	})
}

async fn assert_saved(services: &Services, expected: &Expected) -> Result {
	assert_eq!(
		snapshot(services, "userid_presenceid").await?,
		expected.pointers,
		"refused presence update changed saved pointer"
	);
	assert_eq!(
		snapshot(services, "presenceid_presence").await?,
		expected.bodies,
		"refused presence update changed saved bodies"
	);
	let event = services
		.presence
		.get_presence(user_id!("@source:localhost"))
		.await?;
	assert_eq!(event.content.status_msg.as_deref(), Some(expected.status.as_str()));
	assert_eq!(event.content.presence.as_str(), expected.state);
	Ok(())
}

async fn snapshot(services: &Services, map: &str) -> Result<Rows> {
	let keys = services.db[map].raw_keys_after(None, 128).await?;
	assert!(keys.len() < 128, "owned presence fixture inventory fits snapshot");
	let mut rows = Vec::new();
	for key in keys {
		let value = services.db[map].get(&key).await?.to_vec();
		rows.push((key, value));
	}
	Ok(rows)
}

fn save(root: &Path, expected: &Expected) -> Result {
	fs::write(root.join("presence-expected.json"), serde_json::to_vec(expected)?)?;
	Ok(())
}
