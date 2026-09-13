#![cfg(test)]
//! An event whose commit fails is never published beside the state it
//! replaced.
//!
//! A local event used to be stored in one commit and made the room's current
//! state in a later one. A remote commit can fail without the process
//! stopping. When the state commit failed, `append_pdu` returned early, and
//! the event's count retired as its permit dropped. So sync delivered the
//! event in its timeline beside the state it had replaced, and the pointer
//! stayed stale across a restart.
//!
//! The event, its frontier and its state now land in one commit. Here that
//! commit is refused for a leave, just as the D1 bridge refuses a batch SQLite
//! rejected. Then nothing of the leave is visible: not to sync, not in current
//! state, and not in the frontier the room's next event builds on.

use std::{env::var, net::TcpListener, path::PathBuf, process::id as process_id, time::Duration};

use futures::{StreamExt, future::join, pin_mut};
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	matrix::{Event, PduEvent, pdu::PduCount},
	pdu::PduBuilder,
	ruma::{
		OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
		events::{
			StateEventType, TimelineEventType,
			room::member::{MembershipState, RoomMemberEventContent},
		},
	},
};
use tuwunel_database::refusal;
use tuwunel_service::{Services, users::Register};

const ALICE_TOKEN: &str = "room-state-commit-refusal-alice-token";
const BOB_TOKEN: &str = "room-state-commit-refusal-bob-token";
const PASSWORD: &str = "room-state-commit-refusal-password";
const DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[test]
fn a_refused_append_publishes_nothing() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path =
		PathBuf::from(root).join(format!("tuwunel-room-state-commit-refusal-{}", process_id()));

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={db_path:?}"),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"log=\"warn\"".to_owned(),
	]);

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);

		let exercise = async {
			let outcome = exercise(&services, &base).await;
			let shutdown = server.server.shutdown();

			outcome.and(shutdown)
		};
		let (run_result, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run_result?;

		outcome
	});

	drop(runtime);

	result
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	register(services, "refusalalice", ALICE_TOKEN).await?;
	let bob = register(services, "refusalbob", BOB_TOKEN).await?;
	let room = create_public_room(services, base).await?;
	join_room(services, base, &room).await?;

	let joined = published(services, &room).await?;
	if !is_membership(&joined, &bob, &MembershipState::Join) {
		return Err!("setup: the newest published event is not bob's join");
	}
	if membership(services, &room, &bob).await? != MembershipState::Join {
		return Err!("setup: bob is not joined to {room}");
	}
	let state_before = services
		.state
		.get_room_shortstatehash(&room)
		.await?;
	let frontier_before = frontier(services, &room).await;

	// The leave's one commit is refused.
	refusal::refuse_next("roomid_shortstatehash");
	if leave_room(services, &bob, &room).await.is_ok() {
		return Err!("a leave whose commit was refused reported success");
	}
	if refusal::pending() != 0 {
		return Err!("the leave never reached its commit");
	}

	// Nothing of the leave is visible, and nothing is out of step.
	let newest = published(services, &room).await?;
	if newest.event_id != joined.event_id {
		return Err!(
			"sync can deliver {} ({}) after a refused commit, beside the state it replaced",
			newest.event_id,
			newest.kind
		);
	}
	if membership(services, &room, &bob).await? != MembershipState::Join {
		return Err!("current state took a leave that was never stored");
	}
	if services
		.state
		.get_room_shortstatehash(&room)
		.await?
		!= state_before
	{
		return Err!("the room's current state moved without its event");
	}
	if frontier(services, &room).await != frontier_before {
		return Err!("the room's frontier names an event that was never stored");
	}
	if !services.state_cache.is_joined(&bob, &room).await {
		return Err!("the membership cache applied a leave that was never stored");
	}

	// The room goes on: the next leave publishes with its state, on the join.
	leave_room(services, &bob, &room).await?;
	let left = published(services, &room).await?;
	if !is_membership(&left, &bob, &MembershipState::Leave) {
		return Err!("the retried leave was not published");
	}
	if membership(services, &room, &bob).await? != MembershipState::Leave {
		return Err!("the published leave is not current state");
	}
	let prev_events: Vec<OwnedEventId> = left
		.prev_events()
		.map(ToOwned::to_owned)
		.collect();
	if prev_events != frontier_before {
		return Err!("the retried leave builds on {prev_events:?}, not {frontier_before:?}");
	}

	// The commit that stores an event is the one holding the room's new state.
	let pdu_id = services
		.timeline
		.get_pdu_id(&left.event_id)
		.await?;
	let json = services
		.timeline
		.get_pdu_json(&left.event_id)
		.await?;
	let state = services
		.state
		.get_room_shortstatehash(&room)
		.await?;
	let state_lock = services.state.mutex.lock(&room).await;
	let txn =
		services
			.timeline
			.append_pdu_txn(&pdu_id, &left, &json, None, Some((state, &state_lock)));

	let keys: Vec<(String, Vec<u8>)> = txn
		.keys()
		.map(|(map, key)| (map.name().to_owned(), key.to_vec()))
		.collect();
	drop(txn);
	drop(state_lock);

	let pdu_key: &[u8] = pdu_id.as_ref();
	for (map, key) in [
		("pduid_pdu", pdu_key),
		("eventid_pduid", left.event_id.as_bytes()),
		("roomid_shortstatehash", room.as_bytes()),
	] {
		if !keys
			.iter()
			.any(|(name, queued)| name == map && queued == key)
		{
			return Err!("the event's commit lacks its {map} key");
		}
	}

	Ok(())
}

/// The newest event a sync may deliver now: the timeline up to the retired
/// count, the bound sync uses.
async fn published(services: &Services, room: &RoomId) -> Result<PduEvent> {
	let retired = services.globals.wait_pending().await?;
	let until = PduCount::Normal(retired).saturating_add(1);
	let pdus = services
		.timeline
		.pdus_rev(None, room, Some(until));
	pin_mut!(pdus);

	let (_, pdu) = pdus
		.next()
		.await
		.ok_or_else(|| err!("{room} has no published event"))??;

	Ok(pdu)
}

fn is_membership(pdu: &PduEvent, user: &UserId, expected: &MembershipState) -> bool {
	*pdu.kind() == TimelineEventType::RoomMember
		&& pdu.state_key() == Some(user.as_str())
		&& pdu
			.get_content::<RoomMemberEventContent>()
			.is_ok_and(|content| content.membership == *expected)
}

/// The membership the room's current state gives `user`.
async fn membership(
	services: &Services,
	room: &RoomId,
	user: &UserId,
) -> Result<MembershipState> {
	services
		.state_accessor
		.room_state_get_content::<RoomMemberEventContent>(
			room,
			&StateEventType::RoomMember,
			user.as_str(),
		)
		.await
		.map(|content| content.membership)
}

/// The room's forward extremities, which its next local event builds on.
async fn frontier(services: &Services, room: &RoomId) -> Vec<OwnedEventId> {
	services
		.state
		.get_forward_extremities(room)
		.map(ToOwned::to_owned)
		.collect()
		.await
}

async fn leave_room(services: &Services, user: &UserId, room: &RoomId) -> Result {
	let content = RoomMemberEventContent::new(MembershipState::Leave);
	let builder = PduBuilder::state(user.to_string(), &content);
	let state_lock = services.state.mutex.lock(room).await;

	services
		.timeline
		.build_and_append_pdu(builder, user, room, &state_lock)
		.await
		.map(drop)
}

async fn register(services: &Services, localpart: &str, token: &str) -> Result<OwnedUserId> {
	let user_id = UserId::parse_with_server_name(localpart, services.globals.server_name())?;

	services
		.users
		.full_register(Register {
			user_id: Some(&user_id),
			password: Some(PASSWORD),
			..Default::default()
		})
		.await?;

	services
		.users
		.create_device(&user_id, None, (Some(token), None), None, None, None)
		.await?;

	Ok(user_id)
}

async fn create_public_room(services: &Services, base: &str) -> Result<OwnedRoomId> {
	let response: Value = services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/createRoom"))
		.bearer_auth(ALICE_TOKEN)
		.json(&json!({ "preset": "public_chat" }))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;

	let room_id = response
		.get("room_id")
		.and_then(Value::as_str)
		.ok_or_else(|| err!("createRoom response omitted room_id: {response}"))?;

	Ok(room_id.try_into()?)
}

async fn join_room(services: &Services, base: &str, room: &RoomId) -> Result {
	services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/join/{room}"))
		.bearer_auth(BOB_TOKEN)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn wait_until_ready(services: &Services, base: &str) -> Result {
	let url = format!("{base}/_matrix/client/versions");

	timeout(DEADLINE, async {
		loop {
			if services
				.client
				.clients
				.default
				.get(&url)
				.send()
				.await
				.is_ok()
			{
				break;
			}

			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.map_err(|_| err!("server listener did not become ready"))?;

	Ok(())
}
