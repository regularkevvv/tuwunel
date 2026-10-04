#![cfg(test)]

mod client;

use std::{
	env::temp_dir, fs::remove_dir_all, net::TcpListener, path::PathBuf,
	process::id as process_id, time::Duration,
};

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::time::timeout;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err, http,
	matrix::Event,
	ruma::{EventId, OwnedEventId, RoomId, UserId, events::StateEventType},
	utils::result::NotFound,
};
use tuwunel_service::Services;

use self::client::{Client, register, wait_until_ready};

const TOKEN: &str = "disposable-permission-read-owner-token";

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

struct Context<'a> {
	client: Client<'a>,
	room: &'a RoomId,
	owner: &'a UserId,
	target: &'a EventId,
}

struct SavedRecord {
	map: &'static str,
	key: Vec<u8>,
	value: Vec<u8>,
}

#[test]
fn failed_permission_reads_refuse_mutations_and_preserve_valid_defaults() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = DatabasePath(
		temp_dir().join(format!("tuwunel-permission-read-refusal-{}-{port}", process_id())),
	);
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={:?}", db_path.0),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		"listening=true".into(),
		"log=\"warn\"".into(),
		"allow_local_presence=false".into(),
		"allow_outgoing_presence=false".into(),
		"lockdown_public_room_directory=false".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);
		let exercise = async {
			let outcome = timeout(Duration::from_mins(2), exercise(&services, &base))
				.await
				.map_err(|_| err!("permission-read fixture exceeded its deadline"))
				.and_then(|result| result);
			let shutdown = server.server.shutdown();
			outcome.and(shutdown)
		};
		let (run, outcome) = tokio::join!(async_run(&server), exercise);
		drop(services);
		let stop = async_stop(&server).await;
		outcome.and(run).and(stop)
	});
	drop(server);
	drop(runtime);
	result
}

impl SavedRecord {
	async fn load(services: &Services, map: &'static str, key: &[u8]) -> Result<Self> {
		let value = services.db[map].get(key).await?.to_vec();
		Ok(Self { map, key: key.to_vec(), value })
	}

	async fn write(&self, services: &Services, value: &[u8]) -> Result {
		services.db[self.map]
			.raw_put(&self.key, value)
			.await?;
		services.clear_cache().await;
		Ok(())
	}

	async fn restore(&self, services: &Services) -> Result {
		self.write(services, &self.value).await
	}
}

impl Context<'_> {
	async fn put(&self, path: &str, body: &Value) -> Result<(http::StatusCode, Value)> {
		let response = self
			.client
			.services
			.client
			.clients
			.default
			.put(self.client.url(path))
			.bearer_auth(TOKEN)
			.json(body)
			.send()
			.await?;
		let status = response.status();
		Ok((status, response.json().await?))
	}

	async fn publication(&self, visibility: &str, expected: http::StatusCode) -> Result {
		let (status, body) = self
			.put(
				&format!("directory/list/room/{}", self.room),
				&json!({"visibility": visibility}),
			)
			.await?;
		assert_eq!(status, expected, "directory permission: {body}");
		assert_eq!(
			self.client
				.services
				.directory
				.is_public_room_checked(self.room)
				.await?,
			expected == http::StatusCode::OK && visibility == "public",
			"failed publication must preserve the private directory"
		);
		Ok(())
	}

	async fn frontier(&self) -> Vec<OwnedEventId> {
		let mut frontier = self
			.client
			.services
			.state
			.get_forward_extremities(self.room)
			.map(ToOwned::to_owned)
			.collect::<Vec<_>>()
			.await;
		frontier.sort_unstable();
		assert!(!frontier.is_empty(), "preservation control requires a readable frontier");
		frontier
	}

	async fn refused_redaction(&self, expected: http::StatusCode) -> Result {
		let before = self.frontier().await;
		let (status, body) = self
			.put(
				&format!("rooms/{}/redact/{}/rejected", self.room, self.target),
				&json!({"reason": "disposable refusal control"}),
			)
			.await?;
		assert_eq!(status, expected, "redaction permission: {body}");
		assert!(body.get("event_id").is_none(), "refusal cannot announce a committed redaction");
		assert_eq!(
			self.frontier().await,
			before,
			"refused redaction must preserve the room frontier"
		);
		Ok(())
	}

	async fn healthy(&self) -> Result {
		let services = self.client.services;
		let power = services
			.state_accessor
			.get_power_levels(self.room)
			.await?;
		assert!(power.user_can_send_state(self.owner, StateEventType::RoomHistoryVisibility));
		assert!(
			services
				.state_accessor
				.user_can_redact(self.target, self.owner, self.room, false)
				.await?
		);
		self.publication("public", http::StatusCode::OK)
			.await?;
		self.publication("private", http::StatusCode::OK)
			.await
	}

	async fn refused_permissions(&self) -> Result {
		let services = self.client.services;
		let error = services
			.state_accessor
			.get_power_levels(self.room)
			.await
			.expect_err("failed power read cannot become creator defaults");
		assert_eq!(error.status_code(), http::StatusCode::INTERNAL_SERVER_ERROR);
		let error = services
			.state_accessor
			.user_can_redact(self.target, self.owner, self.room, false)
			.await
			.expect_err("permission failure cannot grant creator redaction");
		assert_eq!(error.status_code(), http::StatusCode::INTERNAL_SERVER_ERROR);
		self.publication("public", http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		self.refused_redaction(http::StatusCode::INTERNAL_SERVER_ERROR)
			.await
	}
}

async fn message(client: &Client<'_>, room: &RoomId, transaction: &str) -> Result<OwnedEventId> {
	let value: Value = client
		.services
		.client
		.clients
		.default
		.put(client.url(&format!("rooms/{room}/send/m.room.message/{transaction}")))
		.bearer_auth(TOKEN)
		.json(&json!({"msgtype": "m.text", "body": "preserve me"}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	Ok(EventId::parse(
		value["event_id"]
			.as_str()
			.expect("message event ID"),
	)?)
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	let owner = register(services, "permissions-owner", TOKEN).await?;
	let client = Client { services, base, token: TOKEN };
	let room = client
		.create_room(&json!({"preset": "public_chat", "visibility": "private"}))
		.await?;
	let target = message(&client, &room, "preserved").await?;
	let context = Context {
		client,
		room: &room,
		owner: &owner,
		target: &target,
	};
	context.healthy().await?;
	let target_id = services.timeline.get_pdu_id(&target).await?;
	let saved_target = SavedRecord::load(services, "pduid_pdu", target_id.as_ref()).await?;
	power_read_failures(&context, &saved_target).await?;
	publication_alias_controls(&context).await?;
	optional_lookup_budget(&context).await?;
	canonical_read_failures(&context, &saved_target).await?;
	cross_room_refusal(&context).await?;
	genuine_absence(&context).await
}

async fn power_read_failures(context: &Context<'_>, target: &SavedRecord) -> Result {
	let services = context.client.services;
	let power = services
		.state_accessor
		.room_state_get_id(context.room, &StateEventType::RoomPowerLevels, "")
		.await?;
	let pdu_id = services.timeline.get_pdu_id(&power).await?;
	let short_event = services.short.get_shorteventid(&power).await?;
	let short_key = services
		.short
		.get_shortstatekey(&StateEventType::RoomPowerLevels, "")
		.await?;
	let hash = services
		.state
		.get_room_shortstatehash(context.room)
		.await?;
	let records = [
		SavedRecord::load(services, "pduid_pdu", pdu_id.as_ref()).await?,
		SavedRecord::load(services, "shorteventid_eventid", &short_event.to_be_bytes()).await?,
		SavedRecord::load(services, "shortstatekey_statekey", &short_key.to_be_bytes()).await?,
		SavedRecord::load(services, "shortstatehash_statediff", &hash.to_be_bytes()).await?,
		SavedRecord::load(services, "roomid_shortstatehash", context.room.as_bytes()).await?,
	];
	for record in &records {
		for missing in [true, false] {
			if missing {
				services.db[record.map]
					.remove(&record.key)
					.await?;
				services.clear_cache().await;
			} else {
				record.write(services, b"{").await?;
			}
			context.refused_permissions().await?;
			assert_eq!(
				services.db[target.map]
					.get(&target.key)
					.await?
					.as_ref(),
				target.value.as_slice(),
				"permission refusal must preserve the target bytes"
			);
			record.restore(services).await?;
			context.healthy().await?;
		}
	}
	let row = &records[0];
	for (field, replacement) in [
		("content", json!({"users_default": "invalid integer"})),
		("room_id", json!("!foreign-permission-room:localhost")),
		("event_id", json!("$different-power:localhost")),
		("state_key", json!("different")),
	] {
		let mut value: Value = serde_json::from_slice(&row.value)?;
		value[field] = replacement;
		row.write(services, &serde_json::to_vec(&value)?)
			.await?;
		context.refused_permissions().await?;
		row.restore(services).await?;
		context.healthy().await?;
	}
	let create = services
		.state_accessor
		.room_state_get_id(context.room, &StateEventType::RoomCreate, "")
		.await?;
	let create_id = services.timeline.get_pdu_id(&create).await?;
	let create_row = SavedRecord::load(services, "pduid_pdu", create_id.as_ref()).await?;
	let mut value: Value = serde_json::from_slice(&create_row.value)?;
	value["room_id"] = json!("!foreign-creator:localhost");
	create_row
		.write(services, &serde_json::to_vec(&value)?)
		.await?;
	context.refused_permissions().await?;
	create_row.restore(services).await?;
	context.healthy().await
}

async fn publication_alias_controls(context: &Context<'_>) -> Result {
	let services = context.client.services;
	let publications = &services.db["publicroomids"];
	for invalid in [b"not-an-alias".as_slice(), &[0xFF_u8]] {
		publications
			.raw_put(context.room, invalid)
			.await?;
		let error = services
			.directory
			.published_alias_checked(context.room)
			.await
			.expect_err("corrupt aliases are not proof of absence");
		assert_eq!(error.status_code(), http::StatusCode::INTERNAL_SERVER_ERROR);
		let (status, body) = context
			.put(
				&format!("directory/list/room/{}", context.room),
				&json!({"visibility": "public"}),
			)
			.await?;
		assert_eq!(status, http::StatusCode::INTERNAL_SERVER_ERROR, "{body}");
		assert_eq!(
			publications.get(context.room).await?.as_ref(),
			invalid,
			"failed alias read cannot overwrite publication metadata"
		);
		publications.remove(context.room).await?;
		context.healthy().await?;
	}
	let alias: &[u8] = b"#preserved-permission-alias:localhost";
	publications.raw_put(context.room, alias).await?;
	context
		.publication("public", http::StatusCode::OK)
		.await?;
	assert_eq!(publications.get(context.room).await?.as_ref(), alias);
	context
		.publication("private", http::StatusCode::OK)
		.await
}

async fn optional_lookup_budget(context: &Context<'_>) -> Result {
	let services = context.client.services;
	let forward_key = (StateEventType::RoomPowerLevels, "");
	let mapping = &services.db["statekey_shortstatekey"];
	let saved_forward = mapping.qry(&forward_key).await?.to_vec();
	let parent = services
		.state
		.get_room_shortstatehash(context.room)
		.await?;
	let saved_hash = services.db["roomid_shortstatehash"]
		.get(context.room)
		.await?
		.to_vec();
	let power = services
		.state_accessor
		.room_state_get_id(context.room, &StateEventType::RoomPowerLevels, "")
		.await?;
	let short_event = services.short.get_shorteventid(&power).await?;
	let hash = *services.globals.next_count().await?;
	let mut diff = parent.to_be_bytes().to_vec();
	for index in 0..7 {
		let key = format!("{index}{}", "x".repeat(80_000));
		let short_key = services
			.short
			.get_or_create_shortstatekey(&StateEventType::RoomTopic, &key)
			.await;
		diff.extend_from_slice(&short_key.to_be_bytes());
		diff.extend_from_slice(&short_event.to_be_bytes());
	}
	services.db["shortstatehash_statediff"]
		.raw_put(&hash.to_be_bytes(), &diff)
		.await?;
	services.db["roomid_shortstatehash"]
		.raw_put(context.room, hash)
		.await?;
	mapping.del(&forward_key).await?;
	services.clear_cache().await;
	let error = services
		.state_accessor
		.get_power_levels(context.room)
		.await
		.expect_err("absence fallback must charge complete mapping bytes");
	assert_eq!(error.status_code(), http::StatusCode::TOO_MANY_REQUESTS);
	context
		.publication("public", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	context
		.refused_redaction(http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	mapping
		.put_raw(&forward_key, &saved_forward)
		.await?;
	services.db["roomid_shortstatehash"]
		.raw_put(context.room, &saved_hash)
		.await?;
	services.clear_cache().await;
	context.healthy().await?;
	// A missing forward shortcut still resolves a complete normal snapshot.
	mapping.del(&forward_key).await?;
	services.clear_cache().await;
	context.healthy().await?;
	// Conflicting reverse aliases cannot select an arbitrary matching cell.
	let duplicate_key = services
		.short
		.get_or_create_shortstatekey(&StateEventType::RoomTopic, "permission-duplicate")
		.await;
	services.db["shortstatekey_statekey"]
		.put(duplicate_key, (&StateEventType::RoomPowerLevels, ""))
		.await?;
	let duplicate_hash = *services.globals.next_count().await?;
	let mut duplicate_diff = parent.to_be_bytes().to_vec();
	duplicate_diff.extend_from_slice(&duplicate_key.to_be_bytes());
	duplicate_diff.extend_from_slice(&short_event.to_be_bytes());
	services.db["shortstatehash_statediff"]
		.raw_put(&duplicate_hash.to_be_bytes(), &duplicate_diff)
		.await?;
	services.db["roomid_shortstatehash"]
		.raw_put(context.room, duplicate_hash)
		.await?;
	services.clear_cache().await;
	context.refused_permissions().await?;
	mapping
		.put_raw(&forward_key, &saved_forward)
		.await?;
	services.db["roomid_shortstatehash"]
		.raw_put(context.room, &saved_hash)
		.await?;
	services.clear_cache().await;
	context.healthy().await
}

async fn canonical_read_failures(context: &Context<'_>, target: &SavedRecord) -> Result {
	let services = context.client.services;
	let outliers = &services.db["eventid_outlierpdu"];
	let mut shadow: Value = serde_json::from_slice(&target.value)?;
	shadow["content"]["body"] = json!("stale outlier");
	outliers
		.raw_put(context.target, serde_json::to_vec(&shadow)?)
		.await?;
	services.clear_cache().await;
	assert_eq!(
		services
			.timeline
			.get_pdu(context.target)
			.await?
			.get_content::<Value>()?["body"],
		"preserve me",
		"canonical record must win over a valid outlier"
	);
	for missing in [true, false] {
		if missing {
			services.db[target.map]
				.remove(&target.key)
				.await?;
			services.clear_cache().await;
		} else {
			target.write(services, b"{").await?;
		}
		let error = services
			.timeline
			.get_pdu(context.target)
			.await
			.expect_err("outlier cannot mask failed canonical read");
		assert_eq!(error.status_code(), http::StatusCode::INTERNAL_SERVER_ERROR);
		let error = services
			.state_accessor
			.user_can_redact(context.target, context.owner, context.room, false)
			.await
			.expect_err("failed target read cannot grant redaction");
		assert_eq!(error.status_code(), http::StatusCode::INTERNAL_SERVER_ERROR);
		context
			.refused_redaction(http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		if !missing {
			assert_eq!(
				services.db[target.map]
					.get(&target.key)
					.await?
					.as_ref(),
				b"{"
			);
		} else {
			assert!(
				services.db[target.map]
					.get(&target.key)
					.await
					.is_not_found()
			);
		}
		target.restore(services).await?;
		context.healthy().await?;
	}
	let index = SavedRecord::load(services, "eventid_pduid", context.target.as_bytes()).await?;
	let mut bad_marker = vec![0_u8; 24];
	bad_marker[8] = 1;
	for invalid in [b"{".as_slice(), bad_marker.as_slice()] {
		index.write(services, invalid).await?;
		let error = services
			.timeline
			.get_pdu(context.target)
			.await
			.expect_err("invalid index cannot panic or fall back to an outlier");
		assert_eq!(error.status_code(), http::StatusCode::INTERNAL_SERVER_ERROR);
		context
			.refused_redaction(http::StatusCode::INTERNAL_SERVER_ERROR)
			.await?;
		index.restore(services).await?;
		context.healthy().await?;
	}
	outliers.remove(context.target).await?;
	// Without an accepted index, a valid outlier remains a supported source.
	let absent = EventId::parse("$permission-outlier-control:localhost")?;
	shadow["event_id"] = json!(absent);
	outliers
		.raw_put(&absent, serde_json::to_vec(&shadow)?)
		.await?;
	assert_eq!(
		services
			.timeline
			.get_pdu(&absent)
			.await?
			.get_content::<Value>()?["body"],
		"stale outlier"
	);
	outliers.remove(&absent).await?;
	assert!(
		services
			.timeline
			.get_pdu(&absent)
			.await
			.is_not_found()
	);
	assert!(
		services
			.state_accessor
			.user_can_redact(&absent, context.owner, context.room, false)
			.await?,
		"healthy high-power unknown-target authorization is retained"
	);
	context.healthy().await
}

async fn cross_room_refusal(context: &Context<'_>) -> Result {
	let services = context.client.services;
	let other_room = context
		.client
		.create_room(&json!({"preset": "public_chat"}))
		.await?;
	let other_target = message(&context.client, &other_room, "other-room").await?;
	let other_id = services
		.timeline
		.get_pdu_id(&other_target)
		.await?;
	let saved = services.db["pduid_pdu"]
		.get(&other_id)
		.await?
		.to_vec();
	assert!(
		!services
			.state_accessor
			.user_can_redact(&other_target, context.owner, context.room, false)
			.await?
	);
	let cross = Context {
		client: Client {
			services,
			base: context.client.base,
			token: TOKEN,
		},
		room: context.room,
		owner: context.owner,
		target: &other_target,
	};
	cross
		.refused_redaction(http::StatusCode::FORBIDDEN)
		.await?;
	assert_eq!(
		services.db["pduid_pdu"]
			.get(&other_id)
			.await?
			.as_ref(),
		saved.as_slice()
	);
	context.healthy().await
}

async fn genuine_absence(context: &Context<'_>) -> Result {
	let services = context.client.services;
	let power = services
		.state_accessor
		.room_state_get_id(context.room, &StateEventType::RoomPowerLevels, "")
		.await?;
	let short_key = services
		.short
		.get_shortstatekey(&StateEventType::RoomPowerLevels, "")
		.await?;
	let short_event = services.short.get_shorteventid(&power).await?;
	let parent = services
		.state
		.get_room_shortstatehash(context.room)
		.await?;
	let hash = *services.globals.next_count().await?;
	let mut diff = parent.to_be_bytes().to_vec();
	diff.extend_from_slice(&0_u64.to_be_bytes()); // removed-state delimiter
	diff.extend_from_slice(&short_key.to_be_bytes());
	diff.extend_from_slice(&short_event.to_be_bytes());
	services.db["shortstatehash_statediff"]
		.raw_put(&hash.to_be_bytes(), &diff)
		.await?;
	services.db["roomid_shortstatehash"]
		.raw_put(context.room, hash)
		.await?;
	services.clear_cache().await;
	assert!(
		services
			.state_accessor
			.state_get_optional(hash, &StateEventType::RoomPowerLevels, "")
			.await?
			.is_none(),
		"default control requires proven absence"
	);
	context.healthy().await?;
	let (status, body) = context
		.put(
			&format!("rooms/{}/redact/{}/allowed-default", context.room, context.target),
			&json!({}),
		)
		.await?;
	assert_eq!(
		status,
		http::StatusCode::OK,
		"valid defaults must allow creator redaction: {body}"
	);
	assert!(
		services
			.timeline
			.get_pdu(context.target)
			.await?
			.is_redacted()
	);
	Ok(())
}
