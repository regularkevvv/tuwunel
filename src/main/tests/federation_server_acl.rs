#![cfg(test)]

//! Server ACL evaluation fails closed.
//!
//! The `m.room.server_acl` schema orders the evaluation: with no ACL event,
//! allow; an IP literal while `allow_ip_literals` is `false`, deny; a `deny`
//! match, deny; an `allow` match, allow; otherwise deny. `allow` defaults to an
//! empty list, "effectively disallowing every server", and `allow_ip_literals`
//! defaults to `true` "if missing or otherwise not a boolean".
//!
//! - An ACL whose `allow` is empty, missing or not a list denies every server.
//! - An ACL whose `deny` and `allow` both hold `*` denies every server.
//! - Entries that are not strings are ignored; the remaining entries apply.
//! - An `allow_ip_literals` that is not a boolean counts as `true`.

use std::{
	env::temp_dir, fs::remove_dir_all, net::TcpListener, process::id as process_id,
	time::Duration,
};

use futures::future::join;
use serde_json::{Value, json, value::to_raw_value};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	pdu::PduBuilder,
	ruma::{
		OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, ServerName, UserId,
		events::TimelineEventType,
	},
};
use tuwunel_service::{Services, users::Register};

#[test]
fn broken_server_acls_fail_closed() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = temp_dir().join(format!("tuwunel-test-server-acl-{}", process_id()));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path=\"{}\"", db_path.display()),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"log=\"warn\"".to_owned(),
	]);

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result: Result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);
		let exercise = async {
			let outcome = timeout(Duration::from_mins(2), exercise(&services, &base))
				.await
				.map_err(|error| err!("server ACL test timed out: {error}"))
				.and_then(|result| result);
			let shutdown = server.server.shutdown();
			outcome.and(shutdown)
		};
		let (run, outcome) = join(async_run(&server), exercise).await;
		drop(services);
		let stop = async_stop(&server).await;
		outcome.and(run).and(stop)
	});
	drop(server);
	drop(runtime);
	remove_dir_all(&db_path).ok();
	result
}

async fn exercise(services: &Services, base: &str) -> Result {
	while services
		.client
		.clients
		.default
		.get(format!("{base}/_matrix/client/versions"))
		.send()
		.await
		.is_err()
	{
		sleep(Duration::from_millis(20)).await;
	}

	let (creator, room_id) = public_room(services, base).await?;
	let remote = server_name("remote.test")?;
	let evil = server_name("evil.test")?;
	let literal = server_name("1.2.3.4")?;

	assert!(
		allowed(services, &remote, &room_id).await,
		"a room without an ACL event must allow every server"
	);

	let cases: [(Value, &[(&ServerName, bool)]); 9] = [
		(json!({"allow": ["*"], "deny": ["evil.test"]}), &[
			(&remote, true),
			(&evil, false),
		]),
		(json!({"allow": [], "deny": []}), &[(&remote, false), (&evil, false)]),
		(json!({"deny": ["evil.test"]}), &[(&remote, false), (&evil, false)]),
		(json!({"allow": ["*"], "deny": ["*"]}), &[(&remote, false), (&evil, false)]),
		(json!({"allow": "*"}), &[(&remote, false), (&evil, false)]),
		(json!({"allow": {"*": true}, "deny": []}), &[(&remote, false)]),
		(json!({"allow": ["*", 5, null], "deny": ["evil.test", {"x": 1}]}), &[
			(&remote, true),
			(&evil, false),
		]),
		(json!({"allow": ["*"], "allow_ip_literals": "no"}), &[
			(&remote, true),
			(&literal, true),
		]),
		(json!({"allow": ["*"], "allow_ip_literals": false}), &[
			(&remote, true),
			(&literal, false),
		]),
	];

	for (content, expectations) in cases {
		set_acl(services, &creator, &room_id, &content).await?;
		for &(server, expected) in expectations {
			assert_eq!(
				allowed(services, server, &room_id).await,
				expected,
				"ACL {content} must {} {server}",
				if expected { "allow" } else { "deny" },
			);
		}
	}

	Ok(())
}

async fn allowed(services: &Services, server: &ServerName, room_id: &RoomId) -> bool {
	services
		.event_handler
		.acl_check(server, room_id)
		.await
		.is_ok()
}

/// Sends an `m.room.server_acl` event with the given content as it is, without
/// the client API's validation, as a remote server's event would arrive.
async fn set_acl(
	services: &Services,
	sender: &UserId,
	room_id: &RoomId,
	content: &Value,
) -> Result {
	let builder = PduBuilder {
		event_type: TimelineEventType::RoomServerAcl,
		content: to_raw_value(content)?.into(),
		state_key: Some(String::new().into()),
		..PduBuilder::default()
	};
	let state_lock = services.state.mutex.lock(room_id).await;
	services
		.timeline
		.build_and_append_pdu(builder, sender, room_id, &state_lock)
		.await?;

	Ok(())
}

fn server_name(name: &str) -> Result<OwnedServerName> { Ok(name.try_into()?) }

/// A public room created by a local user, who is returned with it.
async fn public_room(services: &Services, base: &str) -> Result<(OwnedUserId, OwnedRoomId)> {
	let creator = UserId::parse_with_server_name("creator", services.globals.server_name())?;
	let token = "server-acl-creator-access-token-for-the-test";
	services
		.users
		.full_register(Register {
			user_id: Some(&creator),
			password: Some("server-acl-password"),
			..Default::default()
		})
		.await?;
	services
		.users
		.create_device(&creator, None, (Some(token), None), None, None, None)
		.await?;
	let reply: Value = services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/createRoom"))
		.bearer_auth(token)
		.json(&json!({"room_version": "11", "preset": "public_chat"}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	let room_id = reply["room_id"]
		.as_str()
		.ok_or_else(|| err!("createRoom omitted room_id: {reply}"))?;

	Ok((creator, room_id.try_into()?))
}
