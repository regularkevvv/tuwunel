//! Public device presence through the real remote backend and loopback oracle.
//! Each control runs in its own process. The oracle survives two fresh client
//! processes; no federation/push workers or provider resources are started.

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{env, fs, path::Path, sync::Arc, time::Duration};

use futures::{TryStreamExt, future::poll_fn};
use ruma::{device_id, presence::PresenceState, user_id};
use serde::{Deserialize, Serialize};
use tokio::{process::Command, time::timeout};
use tuwunel_core::{Result, utils::rand};

use super::edu_tests::Fixture;
use crate::{
	Services,
	bridge_fixture::{CommitStage, Fake, Faults},
	presence::Ping,
	users::Register,
};

const CASE: &str = "TUWUNEL_REMOTE_PRESENCE_CASE";
const PHASE: &str = "TUWUNEL_REMOTE_PRESENCE_PHASE";
const ROOT: &str = "TUWUNEL_REMOTE_PRESENCE_ROOT";
const URL: &str = "TUWUNEL_REMOTE_PRESENCE_URL";
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Expected {
	pointers: Rows,
	bodies: Rows,
	status: String,
	state: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_dispatched_presence_stops_writer_and_recovers_after_apply() -> Result {
	run(
		"cancelled_dispatched_presence_stops_writer_and_recovers_after_apply",
		"before-apply",
	)
	.await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_committed_presence_stops_writer_and_recovers_without_ack() -> Result {
	run(
		"cancelled_committed_presence_stops_writer_and_recovers_without_ack",
		"before-reply",
	)
	.await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn known_remote_presence_refusal_preserves_cache_and_writer() -> Result {
	run("known_remote_presence_refusal_preserves_cache_and_writer", "refusal").await
}

#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_unsent_remote_presence_preserves_cache_and_writer() -> Result {
	run("cancelled_unsent_remote_presence_preserves_cache_and_writer", "unsent").await
}

async fn run(case: &str, kind: &str) -> Result {
	if let Ok(selected) = env::var(CASE) {
		assert_eq!(selected, case);
		let root = std::path::PathBuf::from(env::var(ROOT).expect("owned remote root"));
		let phase = env::var(PHASE).expect("owned remote phase");
		return match phase.as_str() {
			| "exercise" => Box::pin(exercise(case, kind, &root)).await,
			| "cold" | "verify" =>
				cold(&root, &env::var(URL).expect("owned loopback URL"), &phase).await,
			| _ => panic!("unknown owned remote phase"),
		};
	}
	let root = env::temp_dir().join(format!("tuwunel-remote-presence-{}", rand::string(20)));
	let mut builder = fs::DirBuilder::new();
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&root)?;
	let result = child(case, &root, "exercise", None).await;
	fs::remove_dir_all(&root)?;
	result
}

async fn child(case: &str, root: &Path, phase: &str, url: Option<&str>) -> Result {
	let mut command = Command::new(env::current_exe()?);
	command
		.arg(format!("sending::sender::remote_presence_tests::{case}"))
		.args(["--exact", "--nocapture", "--test-threads=1"])
		.env(CASE, case)
		.env(PHASE, phase)
		.env(ROOT, root)
		.kill_on_drop(true);
	if let Some(url) = url {
		command.env(URL, url);
	}
	let output = timeout(Duration::from_secs(90), command.output())
		.await
		.expect("bounded owned remote child")?;
	let stdout = String::from_utf8_lossy(&output.stdout);
	let stderr = String::from_utf8_lossy(&output.stderr);
	print!("{stdout}{stderr}");
	assert!(output.status.success(), "owned remote child {phase} failed: {}", output.status);
	assert!(stdout.contains("1 passed; 0 failed; 0 ignored;"));
	Ok(())
}

async fn exercise(case: &str, kind: &str, root: &Path) -> Result {
	let fake = Fake::start().await?;
	let fixture = Fixture::open_remote(root, &fake.url).await?;
	let services = &fixture.services;
	services
		.users
		.full_register(Register {
			user_id: Some(user_id!("@remote-presence:localhost")),
			..Default::default()
		})
		.await?;
	services
		.users
		.create_device(
			user_id!("@remote-presence:localhost"),
			Some(device_id!("remote-device")),
			(None, None),
			None,
			Some("owned remote presence"),
			None,
		)
		.await?;
	set(services, &PresenceState::Online, "accepted remote status").await?;
	assert!(next_push(services).is_none());
	let before = model(&fake);
	let map = tuwunel_bridge::catalog::map_id("userid_presenceid")
		.expect("presence pointer catalog")
		.0;
	if kind == "refusal" {
		fake.faults(Faults {
			reject_map: Some(map),
			..Faults::default()
		});
		set(services, &PresenceState::Offline, "rejected remote status")
			.await
			.expect_err("known remote refusal");
		assert!(!services.db.is_read_only());
		assert_eq!(model(&fake), before);
		assert!(next_push(services).is_none());
		fake.faults(Faults::default());
		ping(services).await?;
	} else if kind == "unsent" {
		#[cfg(debug_assertions)]
		unsent(services, &fake, &before).await?;
		#[cfg(not(debug_assertions))]
		panic!("pre-dispatch pause is a debug control");
	} else {
		let stage = if kind == "before-apply" {
			CommitStage::BeforeApply
		} else {
			assert_eq!(kind, "before-reply");
			CommitStage::BeforeReply
		};
		dispatched(services, &fake, &before, map, stage).await?;
	}
	let expected = model(&fake);
	assert_eq!(expected.pointers.len(), 1);
	assert_eq!(expected.bodies.len(), 1);
	save(root, &expected)?;
	fixture.finish().await;
	child(case, root, "cold", Some(&fake.url)).await?;
	child(case, root, "verify", Some(&fake.url)).await?;
	Ok(())
}

async fn dispatched(
	services: &Arc<Services>,
	fake: &Fake,
	before: &Expected,
	map: u16,
	stage: CommitStage,
) -> Result {
	let gate = fake.pause_commit_on(map, stage);
	let owned = services.clone();
	let task =
		tokio::spawn(
			async move { set(&owned, &PresenceState::Offline, "sent remote status").await },
		);
	timeout(Duration::from_secs(5), gate.entered.notified())
		.await
		.expect("presence commit reached selected server boundary");
	if stage == CommitStage::BeforeApply {
		assert_eq!(&model(fake), before);
	} else {
		assert_eq!(model(fake).status, "sent remote status");
	}
	let held = model(fake);
	let applied_before = fake.applied();
	let contender = queue_update(services).await;
	task.abort();
	assert!(
		task.await
			.expect_err("owned remote request aborted")
			.is_cancelled()
	);
	assert!(
		services.db.is_read_only(),
		"unknown presence outcome must stop writer before another mutation"
	);
	assert!(
		!services
			.db
			.lease_status()
			.expect("remote lease")
			.held
	);
	timeout(Duration::from_secs(5), contender)
		.await
		.expect("stopped writer fails promptly")
		.expect("owned contender joined")
		.expect_err("uncertain writer refuses next public update");
	assert_eq!(model(fake), held);
	assert_eq!(
		fake.applied(),
		applied_before,
		"queued update cannot commit any map after cancellation"
	);
	assert!(next_push(services).is_none(), "unacknowledged presence cannot wake pushes");
	gate.release.notify_one();
	timeout(Duration::from_secs(5), gate.finished.notified())
		.await
		.expect("already-dispatched request settles at oracle");
	let applied = model(fake);
	assert_eq!(applied.status, "sent remote status");
	assert_eq!(applied.state, "offline");
	let applied_after = if stage == CommitStage::BeforeApply {
		applied_before
			.checked_add(1)
			.expect("owned delayed commit count")
	} else {
		applied_before
	};
	assert_eq!(fake.applied(), applied_after, "delayed presence applies exactly once");
	Ok(())
}

async fn queue_update(services: &Arc<Services>) -> tokio::task::JoinHandle<Result> {
	let (entered, blocked) = tokio::sync::oneshot::channel();
	let owned = services.clone();
	let task = tokio::spawn(async move {
		let mut entered = Some(entered);
		let state = PresenceState::Online;
		let mut future = Box::pin(set(&owned, &state, "must not follow uncertain commit"));
		poll_fn(|context| {
			let result = future.as_mut().poll(context);
			if result.is_pending()
				&& let Some(entered) = entered.take()
			{
				entered.send(()).ok();
			}
			result
		})
		.await
	});
	timeout(Duration::from_secs(5), blocked)
		.await
		.expect("second public update contends while first holds user exclusion")
		.expect("owned contender reports pending exclusion");
	task
}

#[cfg(debug_assertions)]
async fn unsent(services: &Arc<Services>, fake: &Fake, before: &Expected) -> Result {
	let mut pause = tuwunel_database::refusal::pause_next("userid_presenceid");
	let owned = services.clone();
	let task = tokio::spawn(async move {
		set(&owned, &PresenceState::Offline, "unsent remote status").await
	});
	timeout(Duration::from_secs(5), pause.entered())
		.await
		.expect("owned transaction reaches pre-dispatch pause")?;
	task.abort();
	assert!(
		task.await
			.expect_err("owned unsent request aborted")
			.is_cancelled()
	);
	drop(pause);
	assert!(!services.db.is_read_only());
	assert_eq!(&model(fake), before);
	assert!(next_push(services).is_none());
	ping(services).await
}

async fn ping(services: &Services) -> Result {
	timeout(
		Duration::from_secs(5),
		services
			.presence
			.maybe_ping_presence(user_id!("@remote-presence:localhost"), Ping {
				device_id: Some(device_id!("remote-device")),
				new_state: Some(&PresenceState::Unavailable),
				..Default::default()
			}),
	)
	.await
	.expect("user exclusion released")?;
	let event = services
		.presence
		.get_presence(user_id!("@remote-presence:localhost"))
		.await?;
	assert_eq!(event.content.status_msg.as_deref(), Some("accepted remote status"));
	Ok(())
}

async fn cold(root: &Path, url: &str, phase: &str) -> Result {
	let fixture = Fixture::open_remote(root, url).await?;
	let services = &fixture.services;
	let expected: Expected =
		serde_json::from_slice(&fs::read(root.join("remote-expected.json"))?)?;
	assert_eq!(snapshot(services, "userid_presenceid").await?, expected.pointers);
	assert_eq!(snapshot(services, "presenceid_presence").await?, expected.bodies);
	let event = services
		.presence
		.get_presence(user_id!("@remote-presence:localhost"))
		.await?;
	assert_eq!(event.content.status_msg.as_deref(), Some(expected.status.as_str()));
	assert_eq!(event.content.presence.as_str(), expected.state);
	if phase == "cold" {
		set(services, &PresenceState::Offline, "accepted remote cold retry").await?;
		let expected = Expected {
			pointers: snapshot(services, "userid_presenceid").await?,
			bodies: snapshot(services, "presenceid_presence").await?,
			status: "accepted remote cold retry".to_owned(),
			state: "offline".to_owned(),
		};
		assert_eq!(expected.pointers.len(), 1);
		assert_eq!(expected.bodies.len(), 1);
		save(root, &expected)?;
	}
	fixture.finish().await;
	Ok(())
}

async fn snapshot(services: &Services, map: &str) -> Result<Rows> {
	services.db[map]
		.raw_stream()
		.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
		.try_collect()
		.await
}

fn model(fake: &Fake) -> Expected {
	let id = |name| {
		tuwunel_bridge::catalog::map_id(name)
			.expect("presence catalog")
			.0
	};
	let bodies = fake.rows(id("presenceid_presence"));
	assert_eq!(bodies.len(), 1);
	let body: serde_json::Value =
		serde_json::from_slice(&bodies[0].1).expect("owned presence JSON");
	Expected {
		pointers: fake.rows(id("userid_presenceid")),
		status: body["status_msg"]
			.as_str()
			.expect("owned status")
			.to_owned(),
		state: body["state"]
			.as_str()
			.expect("owned state")
			.to_owned(),
		bodies,
	}
}

fn save(root: &Path, expected: &Expected) -> Result {
	fs::write(root.join("remote-expected.json"), serde_json::to_vec(expected)?)?;
	Ok(())
}

async fn set(services: &Services, state: &PresenceState, status: &str) -> Result {
	services
		.presence
		.set_presence_for_device(
			user_id!("@remote-presence:localhost"),
			Some(device_id!("remote-device")),
			state,
			Some(status.to_owned()),
		)
		.await
}

fn next_push(services: &Services) -> Option<String> {
	services
		.sending
		.push_wakes
		.lock()
		.expect("push wakes")
		.next()
		.map(|wake| wake.user.to_string())
}
