#![cfg(test)]

use std::{env::var, net::TcpListener, path::PathBuf, process::id as process_id};

use futures::future::join;
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{RoomId, events::StateEventType},
};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

mod client;

const OWNER_TOKEN: &str = "space-hierarchy-completeness-owner-token";

#[test]
fn corrupt_space_child_never_yields_a_partial_hierarchy() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path = PathBuf::from(root).join(format!("tuwunel-space-hierarchy-{}", process_id()));

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

	let _owner = register(services, "spacehierarchyowner", OWNER_TOKEN).await?;
	let client = Client { services, base, token: OWNER_TOKEN };
	let child = client
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;
	let space = client
		.create_room(&json!({
			"preset": "public_chat",
			"creation_content": { "type": "m.space" },
		}))
		.await?;

	set_space_child(&client, &space, &child).await?;
	assert_hierarchy_contains(&client, &space, &child).await?;

	let child_event = services
		.state_accessor
		.room_state_get_id(&space, &StateEventType::SpaceChild, child.as_str())
		.await?;
	let pdu_id = services.timeline.get_pdu_id(&child_event).await?;
	let pdus = &services.db["pduid_pdu"];
	let saved = pdus.get(&pdu_id).await?.to_vec();
	pdus.remove(&pdu_id).await?;
	services.clear_cache().await;

	let corrupt = hierarchy(&client, &space).await?;
	assert_eq!(corrupt.0, 500, "corrupt hierarchy: {}", corrupt.1);
	let body: Value = serde_json::from_str(&corrupt.1)?;
	assert_eq!(body.get("errcode").and_then(Value::as_str), Some("M_UNKNOWN"));
	assert!(!corrupt.1.contains(child.as_str()));

	pdus.raw_put(&pdu_id, &saved).await?;
	services.clear_cache().await;
	assert_hierarchy_contains(&client, &space, &child).await
}

async fn set_space_child(client: &Client<'_>, space: &RoomId, child: &RoomId) -> Result {
	client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{space}/state/m.space.child/{child}")))
		.bearer_auth(client.token)
		.json(&json!({ "via": ["localhost"] }))
		.send()
		.await?
		.error_for_status()?;

	Ok(())
}

async fn assert_hierarchy_contains(
	client: &Client<'_>,
	space: &RoomId,
	child: &RoomId,
) -> Result {
	let response = hierarchy(client, space).await?;
	assert_eq!(response.0, 200, "healthy hierarchy: {}", response.1);
	response
		.1
		.contains(child.as_str())
		.then_some(())
		.ok_or_else(|| err!("healthy hierarchy omitted child {child}"))
}

async fn hierarchy(client: &Client<'_>, space: &RoomId) -> Result<(u16, String)> {
	let response = client
		.services
		.client
		.clients
		.default
		.get(format!("{}/_matrix/client/v1/rooms/{space}/hierarchy", client.base))
		.bearer_auth(client.token)
		.send()
		.await?;

	Ok((response.status().as_u16(), response.text().await?))
}
