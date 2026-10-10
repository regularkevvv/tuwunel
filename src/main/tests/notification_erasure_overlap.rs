#![cfg(test)]
#![cfg(debug_assertions)]
//! Actual recovery pauses after source validation and transaction preparation.
//! Cleanup waits through its commit; typed admission refuses a busy room.
mod client;

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	fs::{DirBuilder, remove_dir_all},
	net::TcpListener,
	path::PathBuf,
	process::id,
	sync::Arc,
	time::Duration,
};

use futures::TryStreamExt;
use serde_json::{Value, json};
use tokio::{
	task::JoinHandle,
	time::{sleep, timeout},
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	matrix::pdu::{PduCount, RawPduId},
	ruma::{OwnedEventId, OwnedRoomId, OwnedUserId},
	utils::hash::sha256,
};
use tuwunel_database::refusal;
use tuwunel_service::{Services, tasks::Status};

use self::client::{Client, field, register, wait_until_ready};

const ALICE: &str = "disposable-erasure-overlap-alice-token-001";
const BOB: &str = "disposable-erasure-overlap-bob-token-00001";
const PLAN: &str = "pduid_notificationplan";
const RECEIPT: &str = "notificationreceiptid_record";

struct Directory(PathBuf);
impl Drop for Directory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}
struct OwnedTask<T>(JoinHandle<Result<T>>);
impl<T> Drop for OwnedTask<T> {
	fn drop(&mut self) { self.0.abort(); }
}
#[derive(Clone, Copy)]
enum Case {
	Direct,
	Refused,
	Typed,
	Room,
}
impl Case {
	fn name(self) -> &'static str {
		match self {
			| Self::Direct => "direct",
			| Self::Refused => "refused",
			| Self::Typed => "typed",
			| Self::Room => "room",
		}
	}
}

#[test]
fn notification_completion_serializes_erasure_and_refuses_conflicting_history_admission() -> Result
{
	let path = std::env::temp_dir().join(format!("notification-erasure-overlap-{}", id()));
	let mut builder = DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&path)?;
	let directory = Directory(path);
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let mut args = Args::default_test(&["fresh"]);
	args.option.extend([
		format!("database_path={:?}", directory.0.join("database")),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		"listening=true".into(),
		"grant_admin_to_first_user=true".into(),
		"suppress_push_when_active=false".into(),
		"log=\"error\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	drop(listener);
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let router_server = server.clone();
		let router = tokio::spawn(async move { async_run(&router_server).await });
		let base = format!("http://127.0.0.1:{port}");
		let result = cases(&services, &base).await;
		server.server.shutdown()?;
		drop(services);
		router.await??;
		async_stop(&server).await?;
		result
	});
	drop(runtime);
	result
}

async fn cases(services: &Arc<Services>, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	services
		.pusher
		.pause_notification_retry_for_test(true);
	let alice = register(services, "erasureoverlapalice", ALICE).await?;
	services.admin.make_user_admin(&alice).await?;
	let bob = register(services, "erasureoverlapbob", BOB).await?;
	services.client.clients.default
		.put(format!("{base}/_matrix/client/v3/pushrules/global/override/overlap-notify"))
		.bearer_auth(BOB).json(&json!({"conditions":[{"kind":"event_match","key":"type","pattern":"m.room.message"}],"actions":["notify"]}))
		.send().await?.error_for_status()?;
	for case in [Case::Direct, Case::Refused, Case::Typed, Case::Room] {
		run_case(services, base, &bob, case).await?;
	}
	Ok(())
}

struct Accepted {
	room: OwnedRoomId,
	raw: RawPduId,
	event_bytes: Vec<u8>,
	plan_bytes: Vec<u8>,
}

async fn prepare_case(
	services: &Arc<Services>,
	base: &str,
	bob: &OwnedUserId,
	case: Case,
) -> Result<Accepted> {
	let owner = Client { services, base, token: ALICE };
	let room = owner
		.create_room(&json!({"preset":"private_chat"}))
		.await?;
	services
		.client
		.clients
		.default
		.post(owner.url(&format!("rooms/{room}/invite")))
		.bearer_auth(ALICE)
		.json(&json!({"user_id":bob}))
		.send()
		.await?
		.error_for_status()?;
	services
		.client
		.clients
		.default
		.post(owner.url(&format!("join/{room}")))
		.bearer_auth(BOB)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;
	services
		.pusher
		.retry_notifications_for_test()
		.await?;
	services
		.pusher
		.reset_notification_counts(bob, &room)
		.await?;
	assert!(
		services.db[PLAN]
			.raw_rows_prefix_after(&[], None, 1)
			.await?
			.is_empty(),
		"setup drains prior notification plans before the controlled refusal"
	);
	refusal::refuse_next(RECEIPT);
	let sent: Value = services
		.client
		.clients
		.default
		.put(owner.url(&format!("rooms/{room}/send/m.room.message/overlap-{}", case.name())))
		.bearer_auth(ALICE)
		.json(&json!({"msgtype":"m.text","body":"controlled notification recovery overlap"}))
		.send()
		.await?
		.error_for_status()?
		.json()
		.await?;
	assert_eq!(refusal::pending(), 0, "actual recipient transaction refused");
	let event: OwnedEventId = field(&sent, "event_id")?.try_into()?;
	let raw = services.timeline.get_pdu_id(&event).await?;
	let accepted = services.db["pduid_pdu"]
		.get(raw.as_ref())
		.await?
		.to_vec();
	let plan = services.db[PLAN]
		.get(raw.as_ref())
		.await?
		.to_vec();
	if matches!(case, Case::Room) {
		// Remove the recipient first: room shutdown must not wait on its
		// private-read guard and accidentally hide a missing plan exclusion.
		services
			.client
			.clients
			.default
			.post(owner.url(&format!("rooms/{room}/leave")))
			.bearer_auth(BOB)
			.json(&json!({}))
			.send()
			.await?
			.error_for_status()?;
	}
	Ok(Accepted {
		room,
		raw,
		event_bytes: accepted,
		plan_bytes: plan,
	})
}

async fn run_case(services: &Arc<Services>, base: &str, bob: &OwnedUserId, case: Case) -> Result {
	let Accepted {
		room,
		raw,
		event_bytes: accepted,
		plan_bytes: plan,
	} = prepare_case(services, base, bob, case).await?;
	let mut pause = services
		.pusher
		.pause_notification_commit_for_test(raw);
	let retry_services = services.clone();
	let mut recovery = OwnedTask(tokio::spawn(async move {
		retry_services
			.pusher
			.retry_notifications_for_test()
			.await
	}));
	assert_eq!(
		timeout(Duration::from_secs(10), pause.entered())
			.await
			.map_err(|_| err!("actual recovery did not reach the commit pause"))??,
		raw,
		"pause belongs to the accepted event's actual recipient commit"
	);
	let count = raw
		.pdu_count()
		.into_unsigned()
		.checked_add(1)
		.ok_or_else(|| err!("fixture boundary overflow"))?;
	let boundary = PduCount::from(count);
	if matches!(case, Case::Refused) {
		refusal::refuse_next("pduid_pdu");
	}
	let mut cleanup = if matches!(case, Case::Typed) {
		let before = snapshot(services).await?;
		let error = services
			.tasks
			.spawn_history(room.clone(), boundary, true)
			.await
			.expect_err("typed admission must refuse while recovery owns room state");
		assert_eq!(
			error.status_code(),
			reqwest::StatusCode::TOO_MANY_REQUESTS,
			"typed admission reports room contention as a retryable refusal"
		);
		assert_eq!(
			snapshot(services).await?,
			before,
			"refused admission creates no journal or notification writes"
		);
		None
	} else {
		let erasure_services = services.clone();
		let erasure_room = room.clone();
		let (started, observed) = tokio::sync::oneshot::channel();
		let mut cleanup = OwnedTask(tokio::spawn(async move {
			started.send(()).ok();
			erase(&erasure_services, erasure_room, boundary, case).await
		}));
		timeout(Duration::from_secs(10), observed)
			.await
			.map_err(|_| err!("cleanup did not start"))?
			.map_err(|_| err!("cleanup start signal was abandoned"))?;
		assert!(
			timeout(Duration::from_millis(500), &mut cleanup.0)
				.await
				.is_err(),
			"cleanup waits while actual recovery owns room state through commit"
		);
		Some(cleanup)
	};
	assert_eq!(
		services.db["pduid_pdu"]
			.get(raw.as_ref())
			.await?
			.as_ref(),
		accepted,
		"cleanup preserves canonical event bytes until recovery releases room state"
	);
	assert_eq!(
		services.db[PLAN]
			.get(raw.as_ref())
			.await?
			.as_ref(),
		plan,
		"cleanup preserves the pending plan while its actual completion is paused"
	);
	drop(pause);
	timeout(Duration::from_secs(10), &mut recovery.0)
		.await
		.map_err(|_| err!("recovery failed to leave its commit pause"))???;
	let cleanup_result = if let Some(cleanup) = &mut cleanup {
		timeout(Duration::from_secs(10), &mut cleanup.0)
			.await
			.map_err(|_| err!("cleanup did not finish after recovery"))??
	} else {
		timeout(Duration::from_secs(10), erase(services, room.clone(), boundary, Case::Typed))
			.await
			.map_err(|_| err!("typed cleanup did not finish after recovery"))?
	};
	if matches!(case, Case::Refused) {
		cleanup_result.expect_err("refused cleanup must retain its canonical event");
		assert_eq!(refusal::pending(), 0, "actual history erasure consumes the commit refusal");
		assert_eq!(
			services.db["pduid_pdu"]
				.get(raw.as_ref())
				.await?
				.as_ref(),
			accepted,
			"refused erasure leaves the accepted canonical event unchanged"
		);
		assert!(
			services.db[PLAN]
				.get(raw.as_ref())
				.await
				.is_err_and(|e| e.is_not_found()),
			"recovery committed before the subsequent history erasure was refused"
		);
		erase(services, room.clone(), boundary, Case::Direct).await?;
	} else {
		cleanup_result?;
	}
	assert_erased_and_quiet(services, raw).await
}

async fn assert_erased_and_quiet(services: &Services, raw: RawPduId) -> Result {
	assert!(
		services.db["pduid_pdu"]
			.get(raw.as_ref())
			.await
			.is_err_and(|e| e.is_not_found()),
		"successful cleanup erases the accepted canonical event"
	);
	assert!(
		services.db[PLAN]
			.get(raw.as_ref())
			.await
			.is_err_and(|e| e.is_not_found()),
		"successful cleanup leaves no plan to replay for the erased event"
	);
	let mut prefix = raw.as_ref().to_vec();
	prefix.push(tuwunel_database::SEP);
	assert!(
		services.db[RECEIPT]
			.raw_keys_prefix_after(&prefix, None, 1)
			.await?
			.is_empty(),
		"successful cleanup erases every completion receipt for the accepted event"
	);
	let before = snapshot(services).await?;
	services
		.pusher
		.retry_notifications_for_test()
		.await?;
	assert_eq!(
		snapshot(services).await?,
		before,
		"no late or repeated completion after erasure"
	);
	Ok(())
}

async fn erase(
	services: &Arc<Services>,
	room: OwnedRoomId,
	boundary: PduCount,
	case: Case,
) -> Result {
	match case {
		| Case::Direct | Case::Refused => {
			assert_eq!(
				services
					.timeline
					.purge_history(&room, boundary, true)
					.await?,
				1,
				"controlled history boundary erases exactly the accepted message"
			);
		},
		| Case::Typed => {
			let id = services
				.tasks
				.spawn_history(room, boundary, true)
				.await?;
			loop {
				let task = services
					.tasks
					.get(id.as_ref())
					.await?
					.expect("owned typed task");
				if task.status == Status::Complete {
					break;
				}
				assert!(
					matches!(task.status, Status::Scheduled | Status::Active),
					"owned typed cleanup remains scheduled or active until completion"
				);
				sleep(Duration::from_millis(20)).await;
			}
		},
		| Case::Room => {
			let guard = services.state.mutex.lock(&room).await;
			services
				.delete
				.delete_room(&room, true, guard)
				.await?;
		},
	}
	Ok(())
}

async fn snapshot(services: &Services) -> Result<Vec<(String, Vec<u8>, sha256::Digest)>> {
	let mut result = Vec::new();
	for map in [
		PLAN,
		RECEIPT,
		"notificationid_index",
		"useridcount_notification",
		"userroomid_notificationcount",
		"userroomid_highlightcount",
		"roomuserid_lastnotificationread",
		"adminjobid_record",
	] {
		let rows: Vec<_> = services.db[map]
			.raw_stream()
			.map_ok(|(key, value)| (map.to_owned(), key.to_vec(), sha256::hash(value)))
			.try_collect()
			.await?;
		result.extend(rows);
	}
	Ok(result)
}
