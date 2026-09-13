#![cfg(test)]

//! Inbound PDUs through the real federation `/send` route (Phase 2 gate C3).
//!
//! A fake remote server signs with an ed25519 key the test generates. Its
//! public half is stored as that server's verify key, where a key fetch would
//! store it, so request and event signatures verify without any network.
//!
//! - A join whose content hash does not match its content, while its signature
//!   (computed over the redacted event) is valid, is accepted and stored
//!   redacted: the content keys redaction strips are gone and the membership
//!   stays (server-server API, "Checks performed on receipt of a PDU", step 3).
//!   An honest join with the same shape keeps its content, so the redaction is
//!   the hash check's.
//! - A transaction whose body is not canonical JSON — a PDU carrying a float,
//!   or an integer outside the canonical range — is refused as `M_BAD_JSON`
//!   even when its X-Matrix signature verifies for the request without a body,
//!   and the event is never stored. The event handler refuses the same PDUs on
//!   its own.

use std::{
	env::temp_dir,
	fs::remove_dir_all,
	net::TcpListener,
	process::id as process_id,
	time::{Duration, SystemTime},
};

use futures::future::join;
use serde_json::{
	Value, json,
	value::{RawValue as RawJsonValue, to_raw_value},
};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	ruma::{
		CanonicalJsonObject, CanonicalJsonValue, MilliSecondsSinceUnixEpoch, OwnedEventId,
		OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, RoomVersionId, TransactionId, UserId,
		api::{
			OutgoingRequestExt,
			federation::{
				authentication::ServerSignaturesInput,
				discovery::{ServerSigningKeys, VerifyKey},
				transactions::send_transaction_message::v1::Request as SendRequest,
			},
		},
		events::StateEventType,
		room_version_rules::RoomVersionRules,
		serde::Base64,
		signatures::{Ed25519KeyPair, hash_and_sign_event, reference_hash, sign_json},
	},
};
use tuwunel_database::Json;
use tuwunel_service::{Services, users::Register};

const REMOTE: &str = "c3-remote.test";

const KEY_ID: &str = "ed25519:c3";

/// The fake remote server: its name, signing key and the room version rules.
struct Remote {
	name: OwnedServerName,
	keypair: Ed25519KeyPair,
	rules: RoomVersionRules,
}

#[test]
fn inbound_pdus_are_redacted_on_bad_hashes_and_refused_when_not_canonical() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = temp_dir().join(format!("tuwunel-test-pdu-integrity-{}", process_id()));
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
				.map_err(|error| err!("PDU integrity test timed out: {error}"))
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

	let remote = remote(services).await?;
	let room_id = public_room(services, base).await?;

	// An honest join keeps its content.
	let trent = remote_user("trent")?;
	let honest = signed_join(services, &room_id, &trent, &remote, "kept").await?;
	let honest_id = event_id(&honest, &remote)?;
	let (status, reply) = send_pdus(services, base, &remote, &honest).await?;
	assert_eq!(status, 200, "the honest transaction was refused: {reply}");
	assert!(
		reply["pdus"][honest_id.as_str()]
			.get("error")
			.is_none(),
		"{reply}"
	);
	assert_eq!(
		stored_content(services, &honest_id).await?,
		json!({"membership": "join", "displayname": "kept"}),
		"an honest join must keep its content"
	);

	// A join whose content no longer matches its hash is stored redacted.
	let mallory = remote_user("mallory")?;
	let mut tampered = signed_join(services, &room_id, &mallory, &remote, "signed").await?;
	match tampered.get_mut("content") {
		| Some(CanonicalJsonValue::Object(content)) => content.insert(
			"displayname".into(),
			CanonicalJsonValue::String("planted after signing".to_owned()),
		),
		| _ => return Err!("the join has no content"),
	};
	let tampered_id = event_id(&tampered, &remote)?;
	let (status, reply) = send_pdus(services, base, &remote, &tampered).await?;
	assert_eq!(status, 200, "the transaction was refused: {reply}");
	assert!(
		reply["pdus"][tampered_id.as_str()]
			.get("error")
			.is_none(),
		"{reply}"
	);
	assert_eq!(
		stored_content(services, &tampered_id).await?,
		json!({"membership": "join"}),
		"a join failing its content hash must be stored redacted"
	);
	assert!(
		services
			.state_cache
			.is_joined(&mallory, &room_id)
			.await,
		"redaction keeps the membership"
	);

	// A PDU that is not canonical JSON is refused with its transaction.
	let oscar = remote_user("oscar")?;
	let join = signed_join(services, &room_id, &oscar, &remote, "oscar").await?;
	let join_id = event_id(&join, &remote)?;
	let canonical = serde_json::to_string(&join)?;
	for (case, value) in [("a float", "1.5"), ("an out-of-range integer", "9007199254740992")] {
		let pdu = canonical.replacen(
			"\"membership\":\"join\"",
			&format!("\"membership\":\"join\",\"weight\":{value}"),
			1,
		);
		assert_ne!(pdu, canonical, "the PDU must carry {case}");

		let raw = RawJsonValue::from_string(pdu.clone())?;
		if services
			.event_handler
			.parse_incoming_pdu(&raw)
			.await
			.is_ok()
		{
			return Err!("the event handler parsed a PDU carrying {case}");
		}

		let (status, reply) = send_raw(services, base, &remote, &pdu).await?;
		assert_eq!(status, 400, "a transaction carrying {case} must be refused: {reply}");
		assert_eq!(reply["errcode"], "M_BAD_JSON", "{case}: {reply}");
	}
	assert!(
		services
			.timeline
			.get_pdu_json(&join_id)
			.await
			.is_err(),
		"a PDU that is not canonical JSON was stored"
	);
	assert!(
		!services
			.state_cache
			.is_joined(&oscar, &room_id)
			.await,
		"a PDU that is not canonical JSON took effect"
	);

	Ok(())
}

/// Generates the remote's signing key and stores its public half as the
/// remote's verify key.
async fn remote(services: &Services) -> Result<Remote> {
	let name = OwnedServerName::try_from(REMOTE)?;
	let version = KEY_ID
		.strip_prefix("ed25519:")
		.ok_or_else(|| err!("the key id names no version"))?;
	let keypair = Ed25519KeyPair::from_der(&Ed25519KeyPair::generate(), version.to_owned())
		.map_err(|error| err!("generated key does not parse: {error}"))?;
	let valid_until = SystemTime::now()
		.checked_add(Duration::from_hours(24))
		.and_then(MilliSecondsSinceUnixEpoch::from_system_time)
		.ok_or_else(|| err!("key validity overflows"))?;
	let mut keys = ServerSigningKeys::new(name.clone(), valid_until);
	keys.verify_keys
		.insert(KEY_ID.try_into()?, VerifyKey {
			key: Base64::new(keypair.public_key().to_vec()),
		});
	services.db["server_signingkeys"]
		.raw_put(name.as_str(), Json(&keys))
		.await?;
	let rules = RoomVersionId::V11
		.rules()
		.ok_or_else(|| err!("room version 11 has no rules"))?;

	Ok(Remote { name, keypair, rules })
}

/// A public room of version 11, created by a local user.
async fn public_room(services: &Services, base: &str) -> Result<OwnedRoomId> {
	let creator = UserId::parse_with_server_name("creator", services.globals.server_name())?;
	let token = "pdu-integrity-creator-access-token";
	services
		.users
		.full_register(Register {
			user_id: Some(&creator),
			password: Some("pdu-integrity-password"),
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

	Ok(room_id.try_into()?)
}

fn remote_user(localpart: &str) -> Result<OwnedUserId> {
	Ok(UserId::parse(format!("@{localpart}:{REMOTE}"))?)
}

/// A join of `user_id` as its server sends it: following the room's latest
/// event, citing the create, join rules and power levels events, hashed and
/// signed by the remote.
async fn signed_join(
	services: &Services,
	room_id: &RoomId,
	user_id: &UserId,
	remote: &Remote,
	displayname: &str,
) -> Result<CanonicalJsonObject> {
	let mut auth_events = Vec::new();
	for kind in [
		StateEventType::RoomCreate,
		StateEventType::RoomJoinRules,
		StateEventType::RoomPowerLevels,
	] {
		let id: OwnedEventId = services
			.state_accessor
			.room_state_get_id(room_id, &kind, "")
			.await?;
		auth_events.push(id);
	}
	let latest = services
		.timeline
		.latest_pdu_in_room(room_id)
		.await?;
	let mut join: CanonicalJsonObject = serde_json::from_value(json!({
		"auth_events": auth_events,
		"content": {"membership": "join", "displayname": displayname},
		"depth": u64::from(latest.depth).saturating_add(1),
		"origin_server_ts": MilliSecondsSinceUnixEpoch::now(),
		"prev_events": [latest.event_id],
		"room_id": room_id,
		"sender": user_id,
		"state_key": user_id,
		"type": "m.room.member",
	}))?;
	hash_and_sign_event(
		remote.name.as_str(),
		&remote.keypair,
		&mut join,
		&remote.rules.redaction,
	)
	.map_err(|error| err!("the join could not be signed: {error}"))?;

	Ok(join)
}

fn event_id(pdu: &CanonicalJsonObject, remote: &Remote) -> Result<OwnedEventId> {
	let hash = reference_hash(pdu, &remote.rules)
		.map_err(|error| err!("the reference hash failed: {error}"))?;

	Ok(OwnedEventId::try_from(format!("${hash}"))?)
}

async fn stored_content(services: &Services, event_id: &OwnedEventId) -> Result<Value> {
	let stored = services.timeline.get_pdu_json(event_id).await?;

	Ok(serde_json::to_value(stored.get("content"))?)
}

/// Sends one PDU in a transaction signed by the remote as ruma signs it.
async fn send_pdus(
	services: &Services,
	base: &str,
	remote: &Remote,
	pdu: &CanonicalJsonObject,
) -> Result<(u16, Value)> {
	let mut request = SendRequest::new(
		TransactionId::new(),
		remote.name.clone(),
		MilliSecondsSinceUnixEpoch::now(),
	);
	request.pdus = vec![to_raw_value(pdu)?];
	let auth = ServerSignaturesInput::new(
		remote.name.clone(),
		services.globals.server_name().to_owned(),
		&remote.keypair,
	);
	let request = request.try_into_http_request::<Vec<u8>>(base, auth, ())?;
	let response = services
		.client
		.clients
		.default
		.execute(request.try_into()?)
		.await?;
	let status = response.status().as_u16();

	Ok((status, response.json().await.unwrap_or(Value::Null)))
}

/// Sends one PDU, given as JSON text, in a transaction whose X-Matrix
/// signature covers the request without its body: a body that is not
/// canonical JSON has no canonical form to sign.
async fn send_raw(
	services: &Services,
	base: &str,
	remote: &Remote,
	pdu: &str,
) -> Result<(u16, Value)> {
	let uri = format!("/_matrix/federation/v1/send/{}", TransactionId::new());
	let destination = services.globals.server_name();
	let mut signed: CanonicalJsonObject = serde_json::from_value(json!({
		"destination": destination,
		"method": "PUT",
		"origin": remote.name,
		"uri": uri,
	}))?;
	sign_json(remote.name.as_str(), &remote.keypair, &mut signed)
		.map_err(|error| err!("the request could not be signed: {error}"))?;
	let signed = serde_json::to_value(&signed)?;
	let signature = signed["signatures"][remote.name.as_str()][KEY_ID]
		.as_str()
		.ok_or_else(|| err!("the request signature is missing"))?;
	let body = json!({
		"origin": remote.name,
		"origin_server_ts": MilliSecondsSinceUnixEpoch::now(),
		"pdus": [serde_json::from_str::<Value>(pdu)?],
		"edus": [],
	});
	let response = services
		.client
		.clients
		.default
		.put(format!("{base}{uri}"))
		.header(
			"authorization",
			format!(
				"X-Matrix origin=\"{}\",destination=\"{destination}\",key=\"{KEY_ID}\",sig=\"\
				 {signature}\"",
				remote.name
			),
		)
		.header("content-type", "application/json")
		.body(body.to_string())
		.send()
		.await?;
	let status = response.status().as_u16();

	Ok((status, response.json().await.unwrap_or(Value::Null)))
}
