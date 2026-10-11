#![cfg(test)]
//! Required canonical indexes commit atomically; deferred notification work
//! retains its durable plan and optional effects do not skip later work.
//! Actual child SIGKILL/cold-read controls verify accepted search, relations,
//! thread bundles and coupled redaction/original writes survive without replay.

use std::{
	collections::BTreeMap,
	env::{current_exe, temp_dir, var},
	fs::{DirBuilder, read, remove_dir_all, write},
	future::pending,
	net::TcpListener,
	path::{Path, PathBuf},
	process::{Child, Command, id as process_id},
	str::from_utf8,
	sync::{Arc, Mutex, PoisonError},
	thread,
	time::{Duration, Instant},
};

use futures::{
	StreamExt, TryStreamExt,
	future::{join, ready},
};
use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener as StubListener, TcpStream},
	spawn,
	sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
	task::JoinHandle,
	time::{sleep, timeout},
};
use tracing::Level;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	log::capture::{Capture, Data},
	matrix::{PduBuilder, event::Event},
	result::NotFound,
	ruma::{
		EventId, OwnedEventId, OwnedRoomId, OwnedServerName, RoomId, UserId,
		api::appservice::{Namespace, Namespaces, Registration, RegistrationInit},
		events::room::{
			member::{MembershipState, RoomMemberEventContent},
			message::RoomMessageEventContent,
		},
	},
};
use tuwunel_database::{Database, Interfix, refusal, serialize_key};
use tuwunel_service::{
	Services,
	rooms::state::RoomMutexGuard,
	sending::{Destination, SendingEvent},
	transaction_ids,
	users::Register,
};

const ATOMIC_PHASE: &str = "TUWUNEL_ATOMIC_EFFECT_PHASE";
const ATOMIC_DIRECTORY: &str = "TUWUNEL_ATOMIC_EFFECT_DIRECTORY";
const ATOMIC_MAPS: &[&str] = &[
	"pduid_pdu",
	"eventid_pduid",
	"roomid_shortstatehash",
	"roomid_pduleaves",
	"userdevicetxnid_response",
	"roomid_tscount_pducount",
	"tokenids",
	"tofrom_relation",
	"relatesto_typed",
	"threadid_userids",
	"threadactivityid_rootid",
	"threadrootid_latestcount",
	"eventid_originalpdu",
	"timeredacted_eventid",
];
type AtomicSnapshot = Vec<(String, Vec<u8>, Vec<u8>)>;

const ALICE_TOKEN: &str = "post-commit-effects-alice-access-token";
const BOB_TOKEN: &str = "post-commit-effects-bob-access-token-0";
const PASSWORD: &str = "post-commit-effects-password";
const APPSERVICE: &str = "post-commit-effects-bridge";
/// A member on a peer whose requests fail at once, so what is queued for it
/// stays in the sending queue.
const REMOTE: &str = "@effectsremote:127.0.0.1:9";
const DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Bodies of the transactions the stub appservice received.
type Transactions = UnboundedReceiver<Vec<u8>>;

/// The fields one logged event carried, by name.
type Fields = Vec<(&'static str, String)>;

/// The fields of every error the server logged while the capture ran.
#[derive(Clone, Default)]
struct Logged(Arc<Mutex<Vec<Fields>>>);

impl Logged {
	fn capture(&self, services: &Services) -> Arc<Capture> {
		let errors = self.clone();

		Capture::new(
			&services.server.log.capture,
			Some(|data: Data<'_>| data.level() == Level::ERROR),
			move |data: Data<'_>| {
				errors
					.0
					.lock()
					.unwrap_or_else(PoisonError::into_inner)
					.push(data.values.to_vec());
			},
		)
	}

	/// Whether a commit refusal was logged for `effect` of `event_id`.
	fn has(&self, effect: &str, event_id: &EventId) -> bool {
		let field = |fields: &[(&str, String)], name: &str| {
			fields
				.iter()
				.find(|(key, _)| *key == name)
				.map(|(_, value)| value.clone())
		};

		self.0
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.iter()
			.any(|fields| {
				field(fields, "effect").as_deref() == Some(effect)
					&& field(fields, "event_id").as_deref() == Some(event_id.as_str())
					&& field(fields, "message").is_some_and(|message| message.contains("refused"))
			})
	}
}

#[test]
fn every_effect_of_a_stored_event_runs() -> Result {
	if var(ATOMIC_PHASE).as_deref() == Ok("verify") {
		return verify_cold_effects(&PathBuf::from(
			var(ATOMIC_DIRECTORY).expect("owned directory"),
		));
	}
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let killed = var(ATOMIC_PHASE).as_deref() == Ok("accept");
	let db_path = if killed {
		PathBuf::from(var(ATOMIC_DIRECTORY).expect("owned directory")).join("database")
	} else {
		PathBuf::from(root).join(format!("tuwunel-post-commit-effects-{}", process_id()))
	};
	let modes: &[&str] = if killed { &["fresh"] } else { &["fresh", "cleanup"] };
	let mut args = Args::default_test(modes);
	args.option.extend([
		format!("database_path={db_path:?}"),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"log=\"warn\"".to_owned(),
		"grant_admin_to_first_user=false".to_owned(),
		"save_unredacted_events=true".to_owned(),
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
	let logged = Logged::default();
	let capture = logged.capture(services);
	let _capturing = capture.start();

	register(services, "effectsalice", ALICE_TOKEN).await?;
	register(services, "effectsbob", BOB_TOKEN).await?;
	let bob = UserId::parse_with_server_name("effectsbob", services.globals.server_name())?;

	// The real retry worker waits on an earlier room's canonical state lock.
	// Immediate appends in the target room still run in every build profile.
	let (retry_room, retry_barrier) =
		notification_retry_barrier(services, base, &logged, &bob).await?;
	let room = create_public_room(services, base).await?;
	assert!(
		services
			.short
			.get_shortroomid(&retry_room)
			.await? < services.short.get_shortroomid(&room).await?,
		"retry barrier precedes target-room plans in the worker's ordered inventory"
	);
	join_room(services, base, &room, BOB_TOKEN).await?;
	let remote = join_remote(services, &room).await?;
	let (mut transactions, stub) = register_appservice(services).await?;

	services
		.client
		.clients
		.default
		.put(format!("{base}/_matrix/client/v3/pushrules/global/override/effects-atomic"))
		.bearer_auth(BOB_TOKEN)
		.json(&json!({
			"conditions": [], "actions": ["notify", { "set_tweak": "highlight", "value": true }],
		}))
		.send()
		.await?
		.error_for_status()?;

	let before = notification_snapshot(services, &bob, &room).await?;

	// Bob's notification row for the thread root is refused. The row was
	// written with `expect`, so the refusal panicked the send.
	refusal::refuse_next("useridcount_notification");
	let root = send_message(
		services,
		base,
		&room,
		"root",
		&json!({
			"msgtype": "m.text",
			"body": "effectsroot",
		}),
	)
	.await?;
	refused(services, &logged, "push", &root).await?;
	if notification_snapshot(services, &bob, &room).await? != before {
		return Err!("refused notification entry left partial counts or recipient metadata");
	}

	indexed(services, base, "effectsroot", &root).await?;
	delivered(&mut transactions, &root).await?;
	federated(services, &remote, &root).await?;

	// A required derived map refusal now refuses canonical acceptance too.
	let reply_content = json!({
		"msgtype":"m.text", "body":"effectsreply",
		"m.relates_to":{"rel_type":"m.thread", "event_id":root},
	});
	refuse_canonical_send(services, base, &room, "reply", &reply_content, "tokenids").await?;

	let reply = send_message(
		services,
		base,
		&room,
		"reply",
		&json!({
			"msgtype": "m.text",
			"body": "effectsreply",
			"m.relates_to": { "rel_type": "m.thread", "event_id": root },
		}),
	)
	.await?;
	indexed(services, base, "effectsreply", &reply).await?;
	threaded(services, &root, &reply).await?;
	let accepted = atomic_snapshot(&services.db).await?;
	assert_eq!(send_message(services, base, &room, "reply", &reply_content).await?, reply);
	assert_eq!(
		atomic_snapshot(&services.db).await?,
		accepted,
		"duplicate thread txn cannot reapply bundle/participants/activity"
	);
	delivered(&mut transactions, &reply).await?;
	federated(services, &remote, &reply).await?;

	assert_count_pair(services, &bob, &room, Some(&root), (1, 1)).await?;
	atomic_receipt_resets(services, base, &bob, &room, &root, &reply).await?;
	notification_counter_errors(services, base, &bob, &room).await?;
	bounded_receipt_reset(services, base, &bob, &room, &root).await?;

	sender_read_acceptance(services, base, &room).await?;
	derived_admission_controls(services, base, &room, &root).await?;
	atomic_redaction(services, base, &room, &root).await?;
	atomic_topic_redaction(services, base, &room).await?;
	let as_content = json!({"msgtype":"m.text","body":"atomicappserviceadmission"});
	refuse_canonical_send(
		services,
		base,
		&room,
		"atomic-as-admission",
		&as_content,
		"servernameevent_data",
	)
	.await?;
	let as_event =
		send_message(services, base, &room, "atomic-as-admission", &as_content).await?;
	delivered(&mut transactions, &as_event).await?;
	if var(ATOMIC_PHASE).as_deref() == Ok("accept") {
		let directory = PathBuf::from(var(ATOMIC_DIRECTORY).expect("owned directory"));
		let queued = send_message(
			services,
			base,
			&room,
			"cold-queue",
			&json!({"msgtype":"m.text", "body":"atomiccoldqueue"}),
		)
		.await?;
		delivered(&mut transactions, &queued).await?;
		let raw = services.timeline.get_pdu_id(&queued).await?;
		let mut key = b"+post-commit-effects-bridge\xff".to_vec();
		key.extend_from_slice(raw.as_ref());
		let active = services.db["servercurrentevent_data"]
			.get(&key)
			.await?
			.to_vec();
		assert!(
			services.db["servernameevent_data"]
				.get(&key)
				.await
				.is_not_found(),
			"held appservice event has exactly one durable active owner"
		);
		write(directory.join("asqueue.snapshot"), serde_json::to_vec(&(key, active))?)?;
		let snapshot = atomic_snapshot(&services.db).await?;
		write(directory.join("accepted.snapshot"), serde_json::to_vec(&snapshot)?)?;
		write(directory.join("accept.ready"), b"canonical and required indexes flushed")?;
		pending::<()>().await;
	}

	stub.abort();
	drop(retry_barrier);

	Ok(())
}

/// Freeze only background recovery through its existing room-state exclusion.
/// The durable blocker is ordered before all later target-room plans. This
/// owned fixture uses the normal append/refusal paths in release and debug;
/// production does not acquire a test pause or an extra control surface.
async fn notification_retry_barrier(
	services: &Services,
	base: &str,
	logged: &Logged,
	bob: &UserId,
) -> Result<(OwnedRoomId, RoomMutexGuard)> {
	let room = create_public_room(services, base).await?;
	join_room(services, base, &room, BOB_TOKEN).await?;
	// This rule matches only the blocker message. The target fixture's broad
	// rule remains installed after its membership setup, as before.
	services.client.clients.default
		.put(format!("{base}/_matrix/client/v3/pushrules/global/override/effects-retry-barrier"))
		.bearer_auth(BOB_TOKEN)
		.json(&json!({
			"conditions": [{"kind":"event_match", "key":"content.body", "pattern":"notificationretrybarrier"}],
			"actions": ["notify", {"set_tweak":"highlight", "value":true}],
		}))
		.send().await?.error_for_status()?;
	let state = services.state.mutex.lock(&room).await;
	let alice = UserId::parse_with_server_name("effectsalice", services.globals.server_name())?;
	refusal::refuse_next("useridcount_notification");
	let event = services
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(
				"notificationretrybarrier",
			)),
			&alice,
			&room,
			&state,
		)
		.await?;
	refused(services, logged, "push", &event).await?;
	assert_count_pair(services, bob, &room, None, (0, 0)).await?;
	let raw = services.timeline.get_pdu_id(&event).await?;
	let plans = services.db["pduid_notificationplan"]
		.raw_rows_prefix_after(&[], None, 2)
		.await?;
	assert_eq!(
		plans.len(),
		1,
		"the real refused notification plan is the first recovery obligation"
	);
	assert_eq!(plans[0].0.as_slice(), raw.as_ref());
	Ok((room, state))
}

type Snapshot = BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>>;

/// Raw rows include notification counts, their read stamps, the notified
/// entries and both private-marker stores. Sequence reservations may leave
/// gaps, so this compares the receipt/notification state, not global `c`.
async fn notification_snapshot(
	services: &Services,
	user: &UserId,
	room: &RoomId,
) -> Result<Snapshot> {
	let mut snapshot = Snapshot::new();
	for name in [
		"userroomid_notificationcount",
		"userroomid_highlightcount",
		"useridcount_notification",
		"roomuserid_lastnotificationread",
		"roomuserid_notificationcutoff",
		"roomuserid_privateread",
		"roomuserid_lastprivatereadupdate",
		"roomuserid_privatereadsync",
	] {
		let prefix = if name == "useridcount_notification" {
			serialize_key((user, Interfix))?
		} else if name.starts_with("userroomid") {
			serialize_key((user, room))?
		} else {
			serialize_key((room, user))?
		};
		snapshot.insert(name.into(), rows(services, name, &prefix).await?);
	}
	Ok(snapshot)
}

async fn rows(services: &Services, map: &str, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	let mut result = Vec::new();
	let mut after = None;
	loop {
		let page = services.db[map]
			.raw_rows_prefix_after(prefix, after.as_deref(), 64)
			.await?;
		if page.is_empty() {
			return Ok(result);
		}
		if result.len().saturating_add(page.len()) > 4096 {
			return Err!("fixture inventory overflow");
		}
		after = page.last().map(|(key, _)| key.clone());
		result.extend(page);
	}
}

async fn assert_count_pair(
	services: &Services,
	user: &UserId,
	room: &RoomId,
	thread: Option<&EventId>,
	want: (u64, u64),
) -> Result {
	let key = match thread {
		| Some(root) => serialize_key((user, room, root))?,
		| None => serialize_key((user, room))?,
	};
	let mut counts = [0_u64; 2];
	for (at, map) in ["userroomid_notificationcount", "userroomid_highlightcount"]
		.into_iter()
		.enumerate()
	{
		match services.db[map].get(&key).await {
			| Ok(value) =>
				counts[at] = u64::from_be_bytes(
					value
						.as_ref()
						.try_into()
						.map_err(|_| err!("invalid fixture count"))?,
				),
			| Err(error) if error.is_not_found() => {},
			| Err(error) => return Err(error),
		}
	}
	if (counts[0], counts[1]) != want {
		return Err!("{thread:?} counts {counts:?}, expected {want:?}");
	}
	Ok(())
}

async fn receipt_request(
	services: &Services,
	base: &str,
	room: &RoomId,
	event: &EventId,
	thread: Option<&str>,
	want: u16,
) -> Result {
	let response = services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/rooms/{room}/receipt/m.read.private/{event}"))
		.bearer_auth(BOB_TOKEN)
		.json(&thread.map_or_else(|| json!({}), |thread| json!({ "thread_id": thread })))
		.send()
		.await?;
	let status = response.status().as_u16();
	let body: Value = response.json().await?;
	if status != want {
		return Err!("receipt answered {status}: {body}; expected {want}");
	}
	Ok(())
}

/// Refused resets preserve the marker too, so the identical API retry can
/// finish. Main/thread/unthreaded scopes also retain their separation.
async fn atomic_receipt_resets(
	services: &Services,
	base: &str,
	bob: &UserId,
	room: &RoomId,
	root: &EventId,
	reply: &EventId,
) -> Result {
	let main = send_message(
		services,
		base,
		room,
		"atomic-main",
		&json!({
			"msgtype": "m.text", "body": "atomic main",
		}),
	)
	.await?;
	assert_count_pair(services, bob, room, None, (1, 1)).await?;
	let before = notification_snapshot(services, bob, room).await?;
	refusal::refuse_next("roomuserid_lastnotificationread");
	receipt_request(services, base, room, &main, Some("main"), 500).await?;
	if refusal::pending() != 0 || notification_snapshot(services, bob, room).await? != before {
		return Err!("refused main receipt advanced marker/counts");
	}
	receipt_request(services, base, room, &main, Some("main"), 200).await?;
	assert_count_pair(services, bob, room, None, (0, 0)).await?;
	assert_count_pair(services, bob, room, Some(root), (1, 1)).await?;
	let completed = notification_snapshot(services, bob, room).await?;
	receipt_request(services, base, room, &main, Some("main"), 200).await?;
	if notification_snapshot(services, bob, room).await? != completed {
		return Err!("duplicate completed receipt rewrote notification state");
	}

	let before = notification_snapshot(services, bob, room).await?;
	refusal::refuse_next("roomuserid_privatereadsync");
	receipt_request(services, base, room, reply, Some(root.as_str()), 500).await?;
	if refusal::pending() != 0 || notification_snapshot(services, bob, room).await? != before {
		return Err!("refused threaded receipt left a partial snapshot");
	}
	receipt_request(services, base, room, reply, Some(root.as_str()), 200).await?;
	assert_count_pair(services, bob, room, Some(root), (0, 0)).await?;
	assert_count_pair(services, bob, room, None, (0, 0)).await?;

	let last = send_message(
		services,
		base,
		room,
		"atomic-unthreaded",
		&json!({
			"msgtype": "m.text", "body": "atomic unread", "m.relates_to": {
				"rel_type": "m.thread", "event_id": root,
			},
		}),
	)
	.await?;
	assert_count_pair(services, bob, room, Some(root), (1, 1)).await?;
	let before = notification_snapshot(services, bob, room).await?;
	refusal::refuse_next("userroomid_highlightcount");
	receipt_request(services, base, room, &last, None, 500).await?;
	if refusal::pending() != 0 || notification_snapshot(services, bob, room).await? != before {
		return Err!("refused unthreaded sweep left a partial snapshot");
	}
	receipt_request(services, base, room, &last, None, 200).await?;
	assert_count_pair(services, bob, room, Some(root), (0, 0)).await?;
	assert_count_pair(services, bob, room, None, (0, 0)).await?;
	Ok(())
}

/// Malformed and overflowing counters must refuse the complete derived
/// notification update, while the stored event's other effects still run.
async fn notification_counter_errors(
	services: &Services,
	base: &str,
	bob: &UserId,
	room: &RoomId,
) -> Result {
	let key = serialize_key((bob, room))?;
	for (txn_id, map, bad) in [
		("atomic-corrupt-highlight", "userroomid_highlightcount", b"broken".to_vec()),
		(
			"atomic-overflow-highlight",
			"userroomid_highlightcount",
			u64::MAX.to_be_bytes().to_vec(),
		),
		("atomic-corrupt-notify", "userroomid_notificationcount", b"broken".to_vec()),
	] {
		services.db["userroomid_notificationcount"]
			.insert(&key, 2_u64.to_be_bytes())
			.await?;
		services.db["userroomid_highlightcount"]
			.insert(&key, 3_u64.to_be_bytes())
			.await?;
		services.db[map].insert(&key, bad).await?;
		let before = notification_snapshot(services, bob, room).await?;
		let event = send_message(
			services,
			base,
			room,
			txn_id,
			&json!({
				"msgtype":"m.text", "body":txn_id,
			}),
		)
		.await?;
		if notification_snapshot(services, bob, room).await? != before {
			return Err!("counter error left partial notification state for {event}");
		}
		indexed(services, base, txn_id, &event).await?;
	}
	services.db["userroomid_notificationcount"]
		.insert(&key, 0_u64.to_be_bytes())
		.await?;
	services.db["userroomid_highlightcount"]
		.insert(&key, 0_u64.to_be_bytes())
		.await?;
	Ok(())
}

/// A complete thread sweep larger than one commit is refused rather than
/// clearing a prefix and forgetting the rest. No private marker advances.
async fn bounded_receipt_reset(
	services: &Services,
	base: &str,
	bob: &UserId,
	room: &RoomId,
	event: &EventId,
) -> Result {
	let mut seed = services.db.txn();
	let mut remove = services.db.txn();
	for index in 0..300 {
		let root: OwnedEventId = format!("$notification-limit-{index}").try_into()?;
		for name in ["userroomid_notificationcount", "userroomid_highlightcount"] {
			seed.put(&services.db[name], (bob, room, &root), 7_u64);
			remove.del(&services.db[name], (bob, room, &root));
		}
		seed.put(&services.db["roomuserid_lastnotificationread"], (room, bob, &root), 1_u64);
		remove.del(&services.db["roomuserid_lastnotificationread"], (room, bob, &root));
	}
	seed.execute().await?;
	let before = notification_snapshot(services, bob, room).await?;
	// Use a fresh accepted event so the private unthreaded marker must advance.
	let newer = send_message(
		services,
		base,
		room,
		"atomic-boundary",
		&json!({
			"msgtype":"m.text", "body":"bounded receipt", "m.relates_to": {
				"rel_type":"m.thread", "event_id":event,
			},
		}),
	)
	.await?;
	let before_request = notification_snapshot(services, bob, room).await?;
	receipt_request(services, base, room, &newer, None, 429).await?;
	if notification_snapshot(services, bob, room).await? != before_request {
		return Err!("oversized reset partially advanced receipt/count state");
	}
	remove.execute().await?;
	receipt_request(services, base, room, &newer, None, 200).await?;
	assert_count_pair(services, bob, room, None, (0, 0)).await?;
	if before.get("roomuserid_privateread") != before_request.get("roomuserid_privateread") {
		return Err!("sending to Bob unexpectedly advanced his private marker");
	}
	Ok(())
}

/// An event-acceptance refusal cannot mark the sender read or clear their
/// counts before its PDU and transaction receipt become durable.
async fn sender_read_acceptance(services: &Services, base: &str, room: &RoomId) -> Result {
	let alice = UserId::parse_with_server_name("effectsalice", services.globals.server_name())?;
	let sender_key = serialize_key((&alice, room))?;
	services.db["userroomid_notificationcount"]
		.insert(&sender_key, 4_u64.to_be_bytes())
		.await?;
	services.db["userroomid_highlightcount"]
		.insert(&sender_key, 2_u64.to_be_bytes())
		.await?;

	let before = notification_snapshot(services, &alice, room).await?;
	let pdus = rows(services, "pduid_pdu", &[]).await?;
	refusal::refuse_next("userdevicetxnid_response");
	let response = services
		.client
		.clients
		.default
		.put(format!(
			"{base}/_matrix/client/v3/rooms/{room}/send/m.room.message/atomic-sender"
		))
		.bearer_auth(ALICE_TOKEN)
		.json(&json!({"msgtype":"m.text", "body":"refused sender"}))
		.send()
		.await?;
	if response.status().as_u16() != 500 {
		return Err!("refused sender acceptance did not fail");
	}
	if refusal::pending() != 0
		|| notification_snapshot(services, &alice, room).await? != before
		|| rows(services, "pduid_pdu", &[]).await? != pdus
	{
		return Err!("refused event changed sender receipt or accepted PDUs");
	}
	send_message(
		services,
		base,
		room,
		"atomic-sender",
		&json!({"msgtype":"m.text", "body":"refused sender"}),
	)
	.await?;
	if rows(services, "pduid_pdu", &[]).await?.len() != pdus.len().saturating_add(1) {
		return Err!("sender retry failed to append exactly one PDU");
	}
	assert_count_pair(services, &alice, room, None, (0, 0)).await?;

	Ok(())
}

/// The armed refusal fired on `effect` of the stored `event_id`, and was
/// logged.
async fn refused(
	services: &Services,
	logged: &Logged,
	effect: &str,
	event_id: &EventId,
) -> Result {
	if refusal::pending() != 0 {
		return Err!("the {effect} of {event_id} was never written");
	}

	services
		.timeline
		.get_pdu_id(event_id)
		.await
		.map_err(|e| err!("{event_id} was not stored: {e}"))?;

	if !logged.has(effect, event_id) {
		return Err!("the refused {effect} of {event_id} was not logged");
	}

	Ok(())
}

/// A search for `term` finds `event_id`.
async fn indexed(services: &Services, base: &str, term: &str, event_id: &EventId) -> Result {
	let response: Value = services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/search"))
		.bearer_auth(ALICE_TOKEN)
		.json(&json!({ "search_categories": { "room_events": { "search_term": term } } }))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;

	let found = response
		.pointer("/search_categories/room_events/results")
		.and_then(Value::as_array)
		.is_some_and(|results| {
			results.iter().any(|result| {
				result
					.pointer("/result/event_id")
					.and_then(Value::as_str)
					== Some(event_id.as_str())
			})
		});

	if !found {
		return Err!("a search for {term:?} does not find {event_id}: {response}");
	}

	Ok(())
}

/// The thread root's bundle names `reply` as its latest event.
async fn threaded(services: &Services, root: &EventId, reply: &EventId) -> Result {
	let json = serde_json::to_value(services.timeline.get_pdu_json(root).await?)?;
	let latest = json
		.pointer("/unsigned/m.relations/m.thread/latest_event/event_id")
		.and_then(Value::as_str);

	if latest != Some(reply.as_str()) {
		return Err!("the thread of {root} does not end in {reply}: {json}");
	}

	Ok(())
}

/// The stub appservice receives `event_id` in a transaction.
async fn delivered(transactions: &mut Transactions, event_id: &EventId) -> Result {
	let carries = |body: &[u8]| {
		serde_json::from_slice::<Value>(body)
			.ok()
			.and_then(|txn| {
				txn.get("events")
					.and_then(Value::as_array)
					.cloned()
			})
			.is_some_and(|events| {
				events.iter().any(|event| {
					event.get("event_id").and_then(Value::as_str) == Some(event_id.as_str())
				})
			})
	};

	let received = timeout(DEADLINE, async {
		while let Some(body) = transactions.recv().await {
			if carries(&body) {
				return true;
			}
		}

		false
	})
	.await
	.unwrap_or(false);

	if !received {
		return Err!("the appservice never received {event_id}");
	}

	Ok(())
}

/// `event_id` waits in the sending queue for `server`, queued or in flight.
async fn federated(services: &Services, server: &OwnedServerName, event_id: &EventId) -> Result {
	let pdu_id = services.timeline.get_pdu_id(event_id).await?;
	let destination = Destination::Federation(server.clone());
	let is_event = |(_, event): (_, SendingEvent)| {
		ready(matches!(event, SendingEvent::Pdu(id) if id == pdu_id))
	};

	// A request leaves the queue for the in-flight set in one commit, so
	// reading the queue first cannot miss it.
	let queued = services
		.sending
		.db
		.queued_requests(&destination)
		.try_any(is_event)
		.await?;
	let in_flight = services
		.sending
		.db
		.active_requests_for(&destination)
		.try_any(is_event)
		.await?;

	if !queued && !in_flight {
		return Err!("{event_id} was never queued for {server}");
	}

	Ok(())
}

/// Alice sends `content` over the client API; the send has to succeed.
async fn send_message(
	services: &Services,
	base: &str,
	room: &RoomId,
	txn_id: &str,
	content: &Value,
) -> Result<OwnedEventId> {
	let response = services
		.client
		.clients
		.default
		.put(format!("{base}/_matrix/client/v3/rooms/{room}/send/m.room.message/{txn_id}"))
		.bearer_auth(ALICE_TOKEN)
		.json(content)
		.send()
		.await?;

	let status = response.status();
	let body: Value = response.json().await?;
	if !status.is_success() {
		return Err!("a send whose effect was refused answered {status}: {body}");
	}

	let event_id = body
		.get("event_id")
		.and_then(Value::as_str)
		.ok_or_else(|| err!("the send answered no event_id: {body}"))?;

	Ok(event_id.try_into()?)
}

/// Stores a remote join in room state, so canonical federation plans include
/// the remote server rather than relying on a cache-only membership.
async fn join_remote(services: &Services, room: &RoomId) -> Result<OwnedServerName> {
	let remote = UserId::parse(REMOTE)?;
	let builder = PduBuilder::state(
		remote.to_string(),
		&RoomMemberEventContent::new(MembershipState::Join),
	);
	let state_lock = services.state.mutex.lock(room).await;

	services
		.timeline
		.build_and_append_pdu(builder, &remote, room, &state_lock)
		.await?;
	drop(state_lock);

	let server = remote.server_name().to_owned();
	let servers: Vec<OwnedServerName> = services
		.state_cache
		.room_servers(room)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	if !servers.contains(&server) {
		return Err!("setup: {server} is not among the servers of {room}");
	}

	Ok(server)
}

/// A bridge for every room, served by a stub that answers each transaction.
async fn register_appservice(services: &Services) -> Result<(Transactions, JoinHandle<()>)> {
	let listener = StubListener::bind("127.0.0.1:0").await?;
	let url = format!("http://{}", listener.local_addr()?);
	let (tx, rx) = unbounded_channel();
	let stub = spawn(serve_transactions(listener, tx));

	let mut namespaces = Namespaces::new();
	namespaces.rooms = vec![Namespace::new(false, "!.*".to_owned())];

	let registration: Registration = RegistrationInit {
		id: APPSERVICE.to_owned(),
		url: Some(url),
		as_token: format!("{APPSERVICE}-as-token"),
		hs_token: format!("{APPSERVICE}-hs-token"),
		sender_localpart: "effectsbridgebot".to_owned(),
		namespaces,
		rate_limited: None,
		protocols: None,
	}
	.into();

	services
		.appservice
		.register_appservice(registration)
		.await?;

	Ok((rx, stub))
}

/// Answers every request an empty `200`, handing on its body.
async fn serve_transactions(listener: StubListener, tx: UnboundedSender<Vec<u8>>) {
	while let Ok((mut socket, _)) = listener.accept().await {
		if let Some(body) = read_body(&mut socket).await {
			let hold = var(ATOMIC_PHASE).as_deref() == Ok("accept")
				&& find(&body, b"atomiccoldqueue").is_some();
			tx.send(body).ok();
			if hold {
				pending::<()>().await;
			}
		}

		socket
			.write_all(
				b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
			)
			.await
			.ok();
		socket.flush().await.ok();
	}
}

async fn read_body(socket: &mut TcpStream) -> Option<Vec<u8>> {
	let mut buf = Vec::new();
	let mut chunk = [0_u8; 4096];
	loop {
		if let Some(head_end) = find(&buf, b"\r\n\r\n") {
			let content_length = content_length(&buf[..head_end])?;
			let body_start = head_end.checked_add(4)?;
			let body_end = body_start.checked_add(content_length)?;
			if buf.len() >= body_end {
				return Some(buf[body_start..body_end].to_vec());
			}
		}

		let read = socket.read(&mut chunk).await.ok()?;
		if read == 0 {
			return None;
		}

		buf.extend_from_slice(&chunk[..read]);
	}
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
	haystack
		.windows(needle.len())
		.position(|window| window == needle)
}

fn content_length(head: &[u8]) -> Option<usize> {
	from_utf8(head)
		.ok()?
		.lines()
		.find_map(|line| {
			line.split_once(':')
				.filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
		})
		.and_then(|(_, value)| value.trim().parse().ok())
}

async fn register(services: &Services, localpart: &str, token: &str) -> Result {
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

	Ok(())
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

async fn join_room(services: &Services, base: &str, room: &RoomId, token: &str) -> Result {
	services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/join/{room}"))
		.bearer_auth(token)
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

/// Refused required derived writes publish neither canonical PDU nor any of
/// the coupled indexes/transaction identity; retrying the same txn is legal.
async fn refuse_canonical_send(
	services: &Services,
	base: &str,
	room: &RoomId,
	txn: &str,
	content: &Value,
	map: &'static str,
) -> Result {
	let before = atomic_snapshot(&services.db).await?;
	refusal::refuse_next(map);
	let response = services
		.client
		.clients
		.default
		.put(format!("{base}/_matrix/client/v3/rooms/{room}/send/m.room.message/{txn}"))
		.bearer_auth(ALICE_TOKEN)
		.json(content)
		.send()
		.await?;
	assert!(!response.status().is_success(), "required index refusal must refuse acceptance");
	assert_eq!(refusal::pending(), 0, "actual canonical batch reached derived map");
	assert_eq!(
		atomic_snapshot(&services.db).await?,
		before,
		"refusal changes no coupled canonical/index rows"
	);
	Ok(())
}

async fn atomic_redaction(
	services: &Services,
	base: &str,
	room: &RoomId,
	root: &EventId,
) -> Result {
	let content = json!({"msgtype":"m.text", "body":"atomicredactiontarget",
		"m.relates_to":{"rel_type":"m.reference", "event_id":root}});
	let target = send_message(services, base, room, "atomic-redaction-target", &content).await?;
	let raw = services.timeline.get_pdu_id(&target).await?;
	indexed(services, base, "atomicredactiontarget", &target).await?;
	let (user, device, _) = services
		.users
		.find_from_token(ALICE_TOKEN)
		.await?;
	let txnid = transaction_ids::key(&user, Some(&device), "atomic-redaction".into());
	let before = atomic_snapshot(&services.db).await?;
	let url = format!("{base}/_matrix/client/v3/rooms/{room}/redact/{target}/atomic-redaction");
	for map in ["eventid_originalpdu", "userdevicetxnid_response"] {
		refusal::refuse_next(map);
		let refused = services
			.client
			.clients
			.default
			.put(&url)
			.bearer_auth(ALICE_TOKEN)
			.json(&json!({"reason":"owned atomic fixture"}))
			.send()
			.await?;
		assert!(
			!refused.status().is_success(),
			"refusal of {map} refuses the complete redaction"
		);
		assert_eq!(refusal::pending(), 0);
		assert_eq!(
			atomic_snapshot(&services.db).await?,
			before,
			"no partial deindex, original, relation delete or target rewrite"
		);
		assert!(
			services.db["userdevicetxnid_response"]
				.get(&txnid)
				.await
				.is_not_found(),
			"refused redaction cannot leave a completed transaction record"
		);
	}
	let retry = || {
		services
			.client
			.clients
			.default
			.put(&url)
			.bearer_auth(ALICE_TOKEN)
			.json(&json!({"reason":"owned atomic fixture"}))
			.send()
	};
	let (first, concurrent) = join(retry(), retry()).await;
	let response: Value = first?.error_for_status()?.json().await?;
	let concurrent: Value = concurrent?.error_for_status()?.json().await?;
	assert_eq!(
		concurrent["event_id"], response["event_id"],
		"concurrent redaction retries share one canonical event"
	);
	let redaction = response["event_id"]
		.as_str()
		.expect("accepted redaction");
	assert_eq!(
		services.db["userdevicetxnid_response"]
			.get(&txnid)
			.await?
			.as_ref(),
		redaction.as_bytes(),
		"accepted redaction and its transaction identity commit together"
	);
	assert!(
		services
			.timeline
			.get_pdu(&target)
			.await?
			.is_redacted(),
		"accepted target already redacted"
	);
	assert!(
		services.db["eventid_originalpdu"]
			.get(&target)
			.await?
			.windows(b"atomicredactiontarget".len())
			.any(|part| part == b"atomicredactiontarget"),
		"original committed with target rewrite"
	);
	for map in ["tokenids", "relatesto_typed"] {
		let rows = services.db[map].raw_stream();
		futures::pin_mut!(rows);
		while let Some((key, _)) = rows.try_next().await? {
			if map == "tokenids" {
				assert!(!key.ends_with(raw.as_ref()), "target search keys removed");
			} else {
				assert!(!key.ends_with(&raw.count()), "target typed relation removed");
			}
		}
	}
	let accepted = atomic_snapshot(&services.db).await?;
	let duplicate: Value = services
		.client
		.clients
		.default
		.put(&url)
		.bearer_auth(ALICE_TOKEN)
		.json(&json!({"reason":"owned atomic fixture"}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	assert_eq!(duplicate["event_id"], redaction, "same transaction reuses canonical redaction");
	assert_eq!(
		atomic_snapshot(&services.db).await?,
		accepted,
		"duplicate cannot reapply thread/count/original/index effects"
	);
	redaction_record_refusals(services, &url, &txnid, redaction).await?;
	Ok(())
}

/// A corrupt or incompatible completed record must never become a fresh append.
async fn redaction_record_refusals(
	services: &Services,
	url: &str,
	txnid: &[u8],
	redaction: &str,
) -> Result {
	for invalid in [Vec::new(), b"invalid-event-id".to_vec(), vec![0xFF]] {
		services.db["userdevicetxnid_response"]
			.insert(txnid, &invalid)
			.await?;
		let before = atomic_snapshot(&services.db).await?;
		let refused = services
			.client
			.clients
			.default
			.put(url)
			.bearer_auth(ALICE_TOKEN)
			.json(&json!({"reason":"owned atomic fixture"}))
			.send()
			.await?;
		assert!(!refused.status().is_success(), "invalid completed record must refuse");
		assert_eq!(
			atomic_snapshot(&services.db).await?,
			before,
			"invalid completed record cannot mutate canonical/index/original state"
		);
	}
	services.db["userdevicetxnid_response"]
		.insert(txnid, redaction.as_bytes())
		.await?;
	Ok(())
}

async fn atomic_snapshot(db: &Database) -> Result<AtomicSnapshot> {
	let mut snapshot = Vec::new();
	for name in ATOMIC_MAPS {
		let rows = db[name].raw_stream();
		futures::pin_mut!(rows);
		while let Some((key, value)) = rows.try_next().await? {
			snapshot.push(((*name).to_owned(), key.to_vec(), value.to_vec()));
		}
	}
	Ok(snapshot)
}

struct AtomicDirectory(PathBuf);
impl Drop for AtomicDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}
struct AtomicChild(Child);
impl Drop for AtomicChild {
	fn drop(&mut self) {
		self.0.kill().ok();
		self.0.wait().ok();
	}
}

#[test]
fn canonical_derived_effects_survive_actual_sigkill_without_replay() -> Result {
	let directory =
		AtomicDirectory(temp_dir().join(format!("canonical-effects-kill-{}", process_id())));
	DirBuilder::new().create(&directory.0)?;
	for phase in ["accept", "verify"] {
		let mut child = AtomicChild(
			Command::new(current_exe()?)
				.args(["--exact", "every_effect_of_a_stored_event_runs", "--nocapture"])
				.env(ATOMIC_PHASE, phase)
				.env(ATOMIC_DIRECTORY, &directory.0)
				.spawn()?,
		);
		let deadline = Instant::now()
			.checked_add(Duration::from_mins(2))
			.expect("owned deadline");
		loop {
			if phase == "accept" && directory.0.join("accept.ready").exists() {
				assert!(child.0.try_wait()?.is_none());
				child.0.kill()?;
				let status = child.0.wait()?;
				#[cfg(unix)]
				assert_eq!(
					std::os::unix::process::ExitStatusExt::signal(&status),
					Some(9),
					"actual process kill, no graceful flush/drop"
				);
				break;
			}
			if let Some(status) = child.0.try_wait()? {
				assert!(phase == "verify" && status.success(), "phase {phase} failed: {status}");
				break;
			}
			assert!(Instant::now() < deadline, "owned canonical effects phase exceeded deadline");
			thread::sleep(Duration::from_millis(20));
		}
	}
	Ok(())
}

fn verify_cold_effects(directory: &Path) -> Result {
	let mut args = Args::default_test(&[]);
	args.option.extend([
		format!("database_path={:?}", directory.join("database")),
		"listening=false".into(),
		"log=\"error\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let db = Database::open(&server.server).await?;
		let expected: AtomicSnapshot =
			serde_json::from_slice(&read(directory.join("accepted.snapshot"))?)?;
		let actual = atomic_snapshot(&db).await?;
		let (key, value): (Vec<u8>, Vec<u8>) =
			serde_json::from_slice(&read(directory.join("asqueue.snapshot"))?)?;
		assert_eq!(
			db["servercurrentevent_data"]
				.get(&key)
				.await?
				.as_ref(),
			value.as_slice(),
			"cold process retains exact held appservice queue owner"
		);
		assert!(
			db["servernameevent_data"]
				.get(&key)
				.await
				.is_not_found(),
			"no duplicate pending owner after kill"
		);
		assert_eq!(
			actual, expected,
			"cold canonical/index/original state exactly matches accepted state without effect \
			 replay"
		);
		db.close().await;
		Ok(())
	});
	drop(runtime);
	result
}

async fn derived_admission_controls(
	services: &Services,
	base: &str,
	room: &RoomId,
	root: &EventId,
) -> Result {
	let root_id = services.timeline.get_pdu_id(root).await?;
	let participants = services.db["threadid_userids"]
		.get(&root_id)
		.await?
		.to_vec();
	services.db["threadid_userids"]
		.insert(&root_id, b"invalid participant inventory")
		.await?;
	let corrupt = atomic_snapshot(&services.db).await?;
	let response = services
		.client
		.clients
		.default
		.put(format!(
			"{base}/_matrix/client/v3/rooms/{room}/send/m.room.message/corrupt-thread"
		))
		.bearer_auth(ALICE_TOKEN)
		.json(&json!({"msgtype":"m.text","body":"corruptthreadmustrefuse",
		"m.relates_to":{"rel_type":"m.thread","event_id":root}}))
		.send()
		.await?;
	assert!(
		!response.status().is_success(),
		"malformed stored participants refuse canonical reply"
	);
	assert_eq!(
		atomic_snapshot(&services.db).await?,
		corrupt,
		"corrupt inventory refusal preserves every canonical/index row"
	);
	services.db["threadid_userids"]
		.insert(&root_id, participants)
		.await?;
	let body = (0..901)
		.map(|index| format!("distincttoken{index}"))
		.collect::<Vec<_>>()
		.join(" ");
	let before = atomic_snapshot(&services.db).await?;
	let response = services
		.client
		.clients
		.default
		.put(format!(
			"{base}/_matrix/client/v3/rooms/{room}/send/m.room.message/oversized-index"
		))
		.bearer_auth(ALICE_TOKEN)
		.json(&json!({"msgtype":"m.text","body":body}))
		.send()
		.await?;
	assert!(
		!response.status().is_success(),
		"complete 901-token index cannot be partially admitted"
	);
	assert_eq!(
		atomic_snapshot(&services.db).await?,
		before,
		"batch limit changes neither canonical event nor prefix of search index"
	);
	let event = send_message(
		services,
		base,
		room,
		"oversized-index",
		&json!({"msgtype":"m.text","body":"boundedretryafterrefusal"}),
	)
	.await?;
	indexed(services, base, "boundedretryafterrefusal", &event).await?;
	Ok(())
}

async fn atomic_topic_redaction(services: &Services, base: &str, room: &RoomId) -> Result {
	let topic: Value = services
		.client
		.clients
		.default
		.put(format!("{base}/_matrix/client/v3/rooms/{room}/state/m.room.topic"))
		.bearer_auth(ALICE_TOKEN)
		.json(&json!({"topic":"atomictopicredaction"}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	let event: OwnedEventId = topic["event_id"]
		.as_str()
		.expect("accepted topic")
		.try_into()?;
	let raw = services.timeline.get_pdu_id(&event).await?;
	let rows = services.db["tokenids"].raw_stream();
	futures::pin_mut!(rows);
	let mut found = false;
	while let Some((key, _)) = rows.try_next().await? {
		found |= key.ends_with(raw.as_ref());
	}
	assert!(found, "accepted topic already indexed");
	services
		.client
		.clients
		.default
		.put(format!(
			"{base}/_matrix/client/v3/rooms/{room}/redact/{event}/atomic-topic-redaction"
		))
		.bearer_auth(ALICE_TOKEN)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;
	assert!(
		services
			.timeline
			.get_pdu(&event)
			.await?
			.is_redacted()
	);
	assert!(
		services.db["eventid_originalpdu"]
			.get(&event)
			.await?
			.windows(b"atomictopicredaction".len())
			.any(|part| part == b"atomictopicredaction")
	);
	let rows = services.db["tokenids"].raw_stream();
	futures::pin_mut!(rows);
	while let Some((key, _)) = rows.try_next().await? {
		assert!(!key.ends_with(raw.as_ref()), "accepted redaction leaves no stale topic token");
	}
	Ok(())
}
