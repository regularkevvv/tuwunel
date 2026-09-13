#![cfg(test)]

//! Cached server signing keys are used only within their validity.
//!
//! A fake remote server signs with ed25519 keys the test generates, stored as
//! its keys where a key fetch would store them. No key server is configured,
//! so a key that is not valid cannot be refetched and nothing leaves the test.
//!
//! - In room versions 5 and later, an event verifies only with a key whose
//!   `valid_until_ts` is at least its `origin_server_ts`; an old verify key
//!   only for events sent before its `expired_ts` (room version 5, "Signing key
//!   validity period"). Room version 4 ignores `valid_until_ts`.
//! - An X-Matrix request verifies only with one of the origin's current
//!   `verify_keys` that is still valid: `old_verify_keys` "are only valid for
//!   signing events" (server-server API, "Publishing Keys").

use std::{
	env::temp_dir,
	fs::remove_dir_all,
	net::TcpListener,
	process::id as process_id,
	time::{Duration, SystemTime},
};

use futures::future::join;
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{
		CanonicalJsonObject, MilliSecondsSinceUnixEpoch, OwnedServerName, RoomVersionId,
		TransactionId,
		api::{
			OutgoingRequestExt,
			federation::{
				authentication::ServerSignaturesInput,
				discovery::{OldVerifyKey, ServerSigningKeys, VerifyKey},
				transactions::send_transaction_message::v1::Request as SendRequest,
			},
		},
		serde::Base64,
		signatures::{Ed25519KeyPair, hash_and_sign_event},
	},
};
use tuwunel_database::Json;
use tuwunel_service::Services;

const REMOTE: &str = "c1-remote.test";

const HOUR: Duration = Duration::from_hours(1);

#[test]
fn signing_keys_are_used_only_within_their_validity() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path = temp_dir().join(format!("tuwunel-test-key-validity-{}", process_id()));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path=\"{}\"", db_path.display()),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"log=\"warn\"".to_owned(),
		"trusted_servers=[]".to_owned(),
		"only_query_trusted_key_servers=true".to_owned(),
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
				.map_err(|error| err!("key validity test timed out: {error}"))
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

	let remote = OwnedServerName::try_from(REMOTE)?;
	let current = keypair("c1")?;
	let old = keypair("c1old")?;
	let now = SystemTime::now();

	// The current key's validity lapsed an hour ago; the old key expired two
	// hours ago.
	store_keys(services, &remote, &current, at(now, -1)?, &old, at(now, -2)?).await?;

	let v11 = RoomVersionId::V11;
	let v4 = RoomVersionId::V4;
	let current_now = signed_event(&remote, &current, &v11, at(now, 0)?)?;
	assert!(
		!verifies(services, &current_now, &v11).await,
		"a v11 event sent after its key's valid_until_ts must not verify"
	);

	let current_now_v4 = signed_event(&remote, &current, &v4, at(now, 0)?)?;
	assert!(
		verifies(services, &current_now_v4, &v4).await,
		"a v4 event must verify regardless of valid_until_ts"
	);

	let old_before = signed_event(&remote, &old, &v11, at(now, -3)?)?;
	assert!(
		verifies(services, &old_before, &v11).await,
		"a v11 event sent before its old key's expired_ts must verify"
	);

	let old_after = signed_event(&remote, &old, &v11, at(now, 0)?)?;
	assert!(
		!verifies(services, &old_after, &v11).await,
		"a v11 event sent after its old key's expired_ts must not verify"
	);

	let status = send_empty_transaction(services, base, &remote, &current).await?;
	assert_eq!(status, 403, "a request signed with a lapsed key must be refused");

	let status = send_empty_transaction(services, base, &remote, &old).await?;
	assert_eq!(status, 403, "a request signed with an old verify key must be refused");

	// Once the current key is valid again, both verify.
	store_keys(services, &remote, &current, at(now, 1)?, &old, at(now, -2)?).await?;
	assert!(
		verifies(services, &current_now, &v11).await,
		"a v11 event sent within its key's validity must verify"
	);

	let status = send_empty_transaction(services, base, &remote, &current).await?;
	assert_eq!(status, 200, "a request signed with a valid current key must be accepted");

	Ok(())
}

fn keypair(version: &str) -> Result<Ed25519KeyPair> {
	Ed25519KeyPair::from_der(&Ed25519KeyPair::generate(), version.to_owned())
		.map_err(|error| err!("generated key does not parse: {error}"))
}

/// `now` shifted by whole hours.
fn at(now: SystemTime, hours: i32) -> Result<MilliSecondsSinceUnixEpoch> {
	let shift = HOUR.saturating_mul(hours.unsigned_abs());
	let time = if hours < 0 {
		now.checked_sub(shift)
	} else {
		now.checked_add(shift)
	};

	time.and_then(MilliSecondsSinceUnixEpoch::from_system_time)
		.ok_or_else(|| err!("timestamp out of range"))
}

/// Stores the remote's keys where a key fetch would store them.
async fn store_keys(
	services: &Services,
	remote: &OwnedServerName,
	current: &Ed25519KeyPair,
	valid_until: MilliSecondsSinceUnixEpoch,
	old: &Ed25519KeyPair,
	expired: MilliSecondsSinceUnixEpoch,
) -> Result {
	let mut keys = ServerSigningKeys::new(remote.clone(), valid_until);
	keys.verify_keys
		.insert(format!("ed25519:{}", current.version()).try_into()?, VerifyKey {
			key: Base64::new(current.public_key().to_vec()),
		});
	keys.old_verify_keys.insert(
		format!("ed25519:{}", old.version()).try_into()?,
		OldVerifyKey::new(expired, Base64::new(old.public_key().to_vec())),
	);
	services.db["server_signingkeys"]
		.raw_put(remote.as_str(), Json(&keys))
		.await?;

	Ok(())
}

/// A message event from the remote, hashed and signed with `key`.
fn signed_event(
	remote: &OwnedServerName,
	key: &Ed25519KeyPair,
	version: &RoomVersionId,
	origin_server_ts: MilliSecondsSinceUnixEpoch,
) -> Result<CanonicalJsonObject> {
	let rules = version
		.rules()
		.ok_or_else(|| err!("room version {version} has no rules"))?;
	let mut event: CanonicalJsonObject = serde_json::from_value(json!({
		"auth_events": [],
		"content": {"body": "hello", "msgtype": "m.text"},
		"depth": 2,
		"origin_server_ts": origin_server_ts,
		"prev_events": [],
		"room_id": format!("!room:{remote}"),
		"sender": format!("@user:{remote}"),
		"type": "m.room.message",
	}))?;
	hash_and_sign_event(remote.as_str(), key, &mut event, &rules.redaction)
		.map_err(|error| err!("the event could not be signed: {error}"))?;

	Ok(event)
}

async fn verifies(
	services: &Services,
	event: &CanonicalJsonObject,
	version: &RoomVersionId,
) -> bool {
	services
		.server_keys
		.verify_event(event, Some(version))
		.await
		.is_ok()
}

/// Sends an empty transaction signed by the remote with `key`.
async fn send_empty_transaction(
	services: &Services,
	base: &str,
	remote: &OwnedServerName,
	key: &Ed25519KeyPair,
) -> Result<u16> {
	let request =
		SendRequest::new(TransactionId::new(), remote.clone(), MilliSecondsSinceUnixEpoch::now());
	let auth = ServerSignaturesInput::new(
		remote.clone(),
		services.globals.server_name().to_owned(),
		key,
	);
	let request = request.try_into_http_request::<Vec<u8>>(base, auth, ())?;
	let response = services
		.client
		.clients
		.default
		.execute(request.try_into()?)
		.await?;
	let status = response.status().as_u16();
	let _body: Value = response.json().await.unwrap_or(Value::Null);

	Ok(status)
}
