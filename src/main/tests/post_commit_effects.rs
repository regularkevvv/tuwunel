#![cfg(test)]
//! Every effect of a stored event runs, whichever of them fails.
//!
//! A local event's effects follow the commit that stores it, each on its own:
//! push counts and notification rows, the search index, relations and
//! threads, appservice delivery, and the queue to the room's other servers. A
//! remote commit can fail without the process stopping. A refused
//! notification row panicked, and a refused search index returned early, so
//! every effect after either was skipped, federation included. The event
//! stayed on this server alone, and a retry of its transaction ran nothing
//! again.
//!
//! Here one effect's commit is refused for each of two local messages, as the
//! D1 bridge refuses a batch SQLite rejected. Each send still succeeds, the
//! refusal is logged under the effect's name, and the effects after it ran:
//! the search index or the thread, appservice delivery, and the federation
//! queue.

use std::{
	env::var,
	net::TcpListener,
	path::PathBuf,
	process::id as process_id,
	str::from_utf8,
	sync::{Arc, Mutex, PoisonError},
	time::Duration,
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
	matrix::PduCount,
	ruma::{
		EventId, OwnedEventId, OwnedRoomId, OwnedServerName, RoomId, UserId,
		api::appservice::{Namespace, Namespaces, Registration, RegistrationInit},
		events::room::member::{MembershipState, RoomMemberEventContent},
	},
};
use tuwunel_database::refusal;
use tuwunel_service::{
	Services,
	rooms::state_cache::MembershipUpdate,
	sending::{Destination, SendingEvent},
	users::Register,
};

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
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path =
		PathBuf::from(root).join(format!("tuwunel-post-commit-effects-{}", process_id()));

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={db_path:?}"),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"log=\"warn\"".to_owned(),
		"grant_admin_to_first_user=false".to_owned(),
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
	let room = create_public_room(services, base).await?;
	join_room(services, base, &room, BOB_TOKEN).await?;
	let remote = join_remote(services, &room).await?;
	let (mut transactions, stub) = register_appservice(services).await?;

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
	refused(services, &logged, "notification row", &root).await?;
	indexed(services, base, "effectsroot", &root).await?;
	delivered(&mut transactions, &root).await?;
	federated(services, &remote, &root).await?;

	// A reply's search index is refused. The refusal returned early, before
	// the reply joined its thread.
	refusal::refuse_next("tokenids");
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
	refused(services, &logged, "search index", &reply).await?;
	threaded(services, &root, &reply).await?;
	delivered(&mut transactions, &reply).await?;
	federated(services, &remote, &reply).await?;

	stub.abort();

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

/// Records a remote member, so the room's servers include theirs. No event of
/// theirs is stored: the servers an event goes to come from this cache.
async fn join_remote(services: &Services, room: &RoomId) -> Result<OwnedServerName> {
	let remote = UserId::parse(REMOTE)?;
	let count = PduCount::Normal(*services.globals.next_count().await?);

	services
		.state_cache
		.update_membership(MembershipUpdate {
			room_id: room,
			user_id: &remote,
			membership_event: RoomMemberEventContent::new(MembershipState::Join),
			sender: &remote,
			last_state: None,
			invite_via: None,
			update_joined_count: true,
			count,
		})
		.await?;

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
			tx.send(body).ok();
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
