#![cfg(test)]
//! A membership change whose recount is refused leaves no cached answer that
//! contradicts it.
//!
//! A membership change commits the member's indexes, then recounts the room's
//! aggregates (its member counts and servers) in a second commit. A remote
//! commit can fail without the process stopping. The recount used to panic on
//! that failure before the appservice-in-room cache was invalidated. The
//! router caught the panic, and the cache kept answering the membership that
//! the durable indexes had replaced.
//!
//! Here the recount is refused while a bridge's user joins a room the cache
//! last saw without it, and so is the join's own retry of it. The join
//! committed, so it succeeds, without panicking, and the cache answers the
//! new membership. The room's next event then recounts the room.

use std::{env::var, net::TcpListener, path::PathBuf, process::id as process_id, time::Duration};

use futures::future::join;
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	pdu::PduBuilder,
	ruma::{
		OwnedRoomId, OwnedUserId, RoomId, UserId,
		api::appservice::{Namespace, Namespaces, Registration, RegistrationInit},
		events::room::{
			member::{MembershipState, RoomMemberEventContent},
			message::RoomMessageEventContent,
		},
	},
};
use tuwunel_database::refusal;
use tuwunel_service::{Services, appservice::RegistrationInfo, users::Register};

const ALICE_TOKEN: &str = "membership-recount-refusal-alice-token";
const PASSWORD: &str = "membership-recount-refusal-password";
const APPSERVICE: &str = "membership-recount-refusal-bridge";
const DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[test]
fn a_refused_recount_leaves_no_stale_appservice_answer() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path =
		PathBuf::from(root).join(format!("tuwunel-membership-recount-refusal-{}", process_id()));

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

	let alice = register(services, "recountalice", Some(ALICE_TOKEN)).await?;
	let puppet = register(services, "recountbridge_puppet", None).await?;
	let bridge = register_appservice(services).await?;
	let room = create_public_room(services, base).await?;

	// The cache now holds the answer the join is about to replace.
	if services
		.state_cache
		.appservice_in_room(&room, &bridge)
		.await
	{
		return Err!("setup: the bridge was in {room} before its user joined");
	}
	let joined_before = services
		.state_cache
		.room_joined_count(&room)
		.await?;

	// The join's membership commits, and its recount is refused, as is the
	// retry of it that the join's later steps make.
	refusal::refuse_next("roomid_joinedcount");
	refusal::refuse_next("roomid_joinedcount");
	let join = RoomMemberEventContent::new(MembershipState::Join);
	append(services, &puppet, &room, PduBuilder::state(puppet.to_string(), &join))
		.await
		.map_err(|e| err!("a join that committed reported its refused recount: {e}"))?;
	if refusal::pending() != 0 {
		return Err!("the join never reached its recount and the retry of it");
	}
	if !services
		.state_cache
		.is_joined(&puppet, &room)
		.await
	{
		return Err!("the join's membership did not commit");
	}

	// The cache follows the durable membership, not the answer it held.
	if !services
		.state_cache
		.appservice_in_room(&room, &bridge)
		.await
	{
		return Err!("the appservice-in-room cache kept the answer the join replaced");
	}

	// The refused recount left the count behind the membership...
	let stale = services
		.state_cache
		.room_joined_count(&room)
		.await?;
	if stale != joined_before {
		return Err!("the refused recount moved the count from {joined_before} to {stale}");
	}

	// ...and the room's next event recounts it.
	let message = RoomMessageEventContent::text_plain("recount");
	append(services, &alice, &room, PduBuilder::timeline(&message)).await?;
	let repaired = services
		.state_cache
		.room_joined_count(&room)
		.await?;
	let expected = joined_before.saturating_add(1);
	if repaired != expected {
		return Err!("the next event left the count at {repaired}, not {expected}");
	}

	Ok(())
}

async fn append(
	services: &Services,
	sender: &UserId,
	room: &RoomId,
	builder: PduBuilder,
) -> Result {
	let state_lock = services.state.mutex.lock(room).await;

	services
		.timeline
		.build_and_append_pdu(builder, sender, room, &state_lock)
		.await
		.map(drop)
}

/// A bridge whose users are `@recountbridge_*`, and which no room has met.
async fn register_appservice(services: &Services) -> Result<RegistrationInfo> {
	let mut namespaces = Namespaces::new();
	namespaces.users = vec![Namespace::new(true, "@recountbridge_.*".to_owned())];

	let registration: Registration = RegistrationInit {
		id: APPSERVICE.to_owned(),
		url: None,
		as_token: format!("{APPSERVICE}-as-token"),
		hs_token: format!("{APPSERVICE}-hs-token"),
		sender_localpart: "recountbridgebot".to_owned(),
		namespaces,
		rate_limited: None,
		protocols: None,
	}
	.into();

	services
		.appservice
		.register_appservice(registration)
		.await?;

	services
		.appservice
		.get_registration_info(APPSERVICE)
		.await
		.ok_or_else(|| err!("the registered appservice is not loaded"))
}

async fn register(
	services: &Services,
	localpart: &str,
	token: Option<&str>,
) -> Result<OwnedUserId> {
	let user_id = UserId::parse_with_server_name(localpart, services.globals.server_name())?;

	services
		.users
		.full_register(Register {
			user_id: Some(&user_id),
			password: Some(PASSWORD),
			..Default::default()
		})
		.await?;

	if let Some(token) = token {
		services
			.users
			.create_device(&user_id, None, (Some(token), None), None, None, None)
			.await?;
	}

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
