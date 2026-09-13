#![cfg(test)]
//! A local event is never published before the room state it produced.
//!
//! Sync bounds its timeline by the retired count, while sliding sync's
//! `required_state` and `/members` read the room's current state. When the
//! count of a membership change retired before its state became current, a
//! sync in between carried the change in its timeline and the replaced
//! membership in its state. Complement Crypto run 34739197959 hit it: Alice's
//! timeline showed Bob's leave, her state still had him joined, and she shared
//! the next room key with him.
//!
//! The appservice registry lock holds the writer at a fixed point after its
//! count retired: the test holds a read guard and queues a writer behind it,
//! and tokio's fair lock parks the leave's own registry read behind that
//! writer. Whatever the leave has not made current by then stays stale until
//! the test lets go.

use std::{env::var, net::TcpListener, path::PathBuf, process::id as process_id, time::Duration};

use futures::{
	FutureExt, StreamExt,
	future::{join, join3},
	pin_mut,
};
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	matrix::{Event, pdu::PduCount},
	pdu::PduBuilder,
	ruma::{
		OwnedRoomId, OwnedUserId, RoomId, UserId,
		events::{
			StateEventType, TimelineEventType,
			room::member::{MembershipState, RoomMemberEventContent},
		},
	},
};
use tuwunel_service::{Services, users::Register};

const ALICE_TOKEN: &str = "room-state-publication-alice-token";
const BOB_TOKEN: &str = "room-state-publication-bob-token";
const PASSWORD: &str = "room-state-publication-password";
const ABSENT_APPSERVICE: &str = "room-state-publication-absent";
const DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[test]
fn a_published_leave_is_already_current_state() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path =
		PathBuf::from(root).join(format!("tuwunel-room-state-publication-{}", process_id()));

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

	register(services, "publicationalice", ALICE_TOKEN).await?;
	let bob = register(services, "publicationbob", BOB_TOKEN).await?;
	let room = create_public_room(services, base).await?;
	join_room(services, base, &room).await?;

	if membership(services, &room, &bob).await? != MembershipState::Join {
		return Err!("setup: bob is not joined to {room}");
	}

	// Hold the registry and queue a writer, so the next reader parks.
	let guard = services.appservice.read().await;
	let writer = services
		.appservice
		.unregister_appservice(ABSENT_APPSERVICE);
	pin_mut!(writer);
	if writer.as_mut().now_or_never().is_some() {
		return Err!("the registry writer did not queue behind the read guard");
	}
	if services
		.appservice
		.read()
		.now_or_never()
		.is_some()
	{
		return Err!("registry readers do not queue behind the waiting writer");
	}

	let (room, bob) = (&room, &bob);
	let observe = async move {
		let published = poll_until(|| leave_is_published(services, room, bob)).await;
		let state = membership(services, room, bob).await;
		drop(guard);

		(published, state)
	};

	let (unregistered, left, (published, state)) =
		join3(writer, leave_room(services, bob, room), observe).await;

	if unregistered.is_ok() {
		return Err!("an absent appservice was unregistered");
	}
	left?;
	if !published {
		return Err!("bob's leave never became visible to sync");
	}

	assert_eq!(
		state?,
		MembershipState::Leave,
		"sync could see bob's leave while the current state still had him joined"
	);
	assert_eq!(membership(services, room, bob).await?, MembershipState::Leave);

	Ok(())
}

/// Whether the newest event a sync may deliver now is Bob's leave.
///
/// The bound is the one sync uses: the timeline up to the retired count.
async fn leave_is_published(services: &Services, room: &RoomId, user: &UserId) -> bool {
	let Ok(retired) = services.globals.wait_pending().await else {
		return false;
	};

	let until = PduCount::Normal(retired).saturating_add(1);
	let pdus = services
		.timeline
		.pdus_rev(None, room, Some(until));
	pin_mut!(pdus);

	let Some(Ok((_, pdu))) = pdus.next().await else {
		return false;
	};

	*pdu.kind() == TimelineEventType::RoomMember
		&& pdu.state_key() == Some(user.as_str())
		&& pdu
			.get_content::<RoomMemberEventContent>()
			.is_ok_and(|content| content.membership == MembershipState::Leave)
}

/// The membership the room's current state gives `user`, as `/members` and
/// sliding sync's `required_state` read it.
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

async fn poll_until<Condition, Check>(mut condition: Condition) -> bool
where
	Condition: FnMut() -> Check + Send,
	Check: Future<Output = bool> + Send,
{
	timeout(DEADLINE, async {
		while !condition().await {
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.is_ok()
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
