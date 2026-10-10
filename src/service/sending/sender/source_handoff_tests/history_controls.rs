//! Actual service startup with accepted membership work and a durably admitted
//! interrupted history job in the same room. History preserves state events.

use std::{fs, path::Path, sync::Arc, time::Duration};

use ruma::{room_id, user_id};
use tuwunel_core::{Error, Result, matrix::PduCount};
use tuwunel_database::refusal;

use crate::{Services, tasks::Status};

pub(super) async fn child(services: &Arc<Services>, root: &Path, phase: &str) -> Result {
	if phase == "membership-history-crash" {
		return crash(services, root).await;
	}
	assert!(matches!(phase, "membership-history-start" | "membership-history-again"));
	let room = room_id!("!source-handoff:localhost");
	let id = fs::read_to_string(root.join("membership-history-task"))?;
	let pending = services.db["global"]
		.contains_checked(&("membership_projection_v1", room))
		.await?;
	assert_eq!(pending, phase == "membership-history-start");
	assert_eq!(
		services
			.tasks
			.get(&id)
			.await?
			.expect("accepted history task")
			.status,
		if pending { Status::Scheduled } else { Status::Complete }
	);
	// This calls migrations' schema gate, preflight, notification recovery,
	// membership recovery, history restoration and Manager::start in order.
	// Fixture config skips migration work and disables outbound federation.
	tokio::time::timeout(Duration::from_secs(20), services.start())
		.await
		.expect("startup must not wait on a history guard restored before membership")?;
	for prefix in [
		"membership_projection_v1",
		"membership_projection_witness_v1",
		"membership_projection_cursor_v1",
		"membership_recount_pending",
	] {
		assert!(
			!services.db["global"]
				.contains_checked(&(prefix, room))
				.await?,
			"actual startup completed membership before returning"
		);
	}
	assert!(
		!services
			.state_cache
			.is_joined(user_id!("@source:localhost"), room)
			.await
	);
	assert_eq!(
		services
			.state_cache
			.room_joined_count(room)
			.await?,
		1
	);
	tokio::time::timeout(Duration::from_secs(20), async {
		loop {
			let task = services
				.tasks
				.get(&id)
				.await?
				.expect("retained history task");
			if task.status == Status::Complete {
				assert_eq!(
					task.result,
					Some(serde_json::json!({"purged":0})),
					"history retains state events"
				);
				return Ok::<(), Error>(());
			}
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.expect("restored history completes after membership recovery")?;
	assert!(services.server.is_running(), "the worker graph remains healthy");
	super::membership_controls::child(services, root, "membership-restart").await
}

async fn crash(services: &Arc<Services>, root: &Path) -> Result {
	super::membership_controls::child(services, root, "membership-refuse").await?;
	let room = room_id!("!source-handoff:localhost");
	let expected: serde_json::Value =
		serde_json::from_slice(&fs::read(root.join("membership-expected.json"))?)?;
	assert_eq!(expected["joined"], false);
	// Scheduled commits, then Active refuses. The real executor retains the
	// room exclusion until shutdown; both durable obligations survive SIGKILL.
	refusal::refuse_after("adminjobid_record", 1);
	let id = services
		.tasks
		.spawn_history(
			room.to_owned(),
			PduCount::Normal(expected["count"].as_u64().unwrap()),
			true,
		)
		.await?;
	tokio::time::timeout(Duration::from_secs(20), async {
		while refusal::pending() != 0 {
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.expect("actual history activation refusal fires");
	assert_eq!(
		services
			.tasks
			.get(&id)
			.await?
			.expect("durable admitted job")
			.status,
		Status::Scheduled
	);
	assert!(
		services.db["global"]
			.contains_checked(&("membership_projection_v1", room))
			.await?
	);
	assert!(
		services.state.mutex.try_lock(room).is_err(),
		"interrupted history owns the room"
	);
	fs::write(root.join("membership-history-task"), id.as_bytes())?;
	fs::write(
		root.join("source.ready"),
		b"membership pending; interrupted history owns the same room",
	)?;
	std::future::pending::<()>().await;
	Ok(())
}
