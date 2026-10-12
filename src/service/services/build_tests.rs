//! A failed builder must finish database cleanup before returning its error.

use std::{
	path::Path,
	sync::{
		Arc, Mutex,
		atomic::{AtomicUsize, Ordering},
	},
	time::Duration,
};

use axum::{
	Router,
	body::Bytes,
	extract::State,
	http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
	response::{IntoResponse, Response as HttpResponse},
	routing::post,
};
use tokio::{sync::Notify, task::JoinHandle, time::timeout};
use tuwunel_bridge::{self as bridge, Lease, Request, Response};
use tuwunel_core::{
	Result,
	config::{Config, Figment},
};

use super::{
	Services,
	startup_tests::{isolated, server, services},
};

#[derive(Default)]
struct BridgeState {
	lease: Mutex<Option<Lease>>,
	acquired: AtomicUsize,
	released: AtomicUsize,
	renewed: AtomicUsize,
	release_entered: Notify,
	release_allowed: Notify,
}

struct Fake {
	state: Arc<BridgeState>,
	task: Option<JoinHandle<()>>,
	url: String,
}

impl Fake {
	async fn start() -> Result<Self> {
		let state = Arc::new(BridgeState::default());
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
		let url = format!("http://{}", listener.local_addr()?);
		let app = Router::new()
			.route(bridge::PATH_KV, post(kv))
			.with_state(state.clone());
		let task = tokio::spawn(async move {
			axum::serve(listener, app)
				.await
				.expect("fixture listener");
		});
		Ok(Self { state, task: Some(task), url })
	}

	async fn stop(mut self) {
		let task = self.task.take().expect("owned fixture listener");
		task.abort();
		assert!(
			task.await
				.expect_err("fixture listener aborted")
				.is_cancelled()
		);
	}
}

impl Drop for Fake {
	fn drop(&mut self) {
		if let Some(task) = &self.task {
			task.abort();
		}
	}
}

async fn kv(
	State(state): State<Arc<BridgeState>>,
	headers: HeaderMap,
	body: Bytes,
) -> HttpResponse {
	if headers
		.get("authorization")
		.and_then(|h| h.to_str().ok())
		!= Some("Bearer build-fixture-token")
	{
		return StatusCode::UNAUTHORIZED.into_response();
	}
	let response = match bridge::decode::<Request>(&body).expect("fixture CBOR") {
		| Request::Hello => Response::Hello {
			protocol: bridge::PROTOCOL_VERSION,
			schema_version: bridge::SCHEMA_VERSION,
			lease: None,
			now_ms: 1_000,
		},
		| Request::LeaseAcquire { holder, ttl_ms } => {
			assert!(state.lease.lock().expect("lease lock").is_none());
			*state.lease.lock().expect("lease lock") = Some(Lease { holder, epoch: 1 });
			state.acquired.fetch_add(1, Ordering::SeqCst);
			Response::Leased {
				epoch: 1,
				expires_at_ms: 1_000_u64.saturating_add(ttl_ms),
				now_ms: 1_000,
			}
		},
		| Request::LeaseRenew { lease, ttl_ms } => {
			assert_eq!(state.lease.lock().expect("lease lock").as_ref(), Some(&lease));
			state.renewed.fetch_add(1, Ordering::SeqCst);
			Response::Leased {
				epoch: 1,
				expires_at_ms: 1_000_u64.saturating_add(ttl_ms),
				now_ms: 1_000,
			}
		},
		| Request::LeaseRelease { lease } => {
			state.release_entered.notify_one();
			state.release_allowed.notified().await;
			assert_eq!(state.lease.lock().expect("lease lock").take(), Some(lease));
			state.released.fetch_add(1, Ordering::SeqCst);
			Response::Released
		},
		| other => panic!("builder fixture must fail before data operations: {other:?}"),
	};
	(
		[(CONTENT_TYPE, bridge::CONTENT_TYPE)],
		bridge::encode(&response).expect("fixture reply"),
	)
		.into_response()
}

async fn remote_failure(root: &Path, cancel: bool) -> Result {
	let fake = Fake::start().await?;
	let raw = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_backend", "d1"))
		.merge(("database_path", root.join("unused-database")))
		.merge(("d1_bridge_url", &fake.url))
		.merge(("d1_bridge_token", "build-fixture-token"))
		.merge(("d1_lease_ttl_ms", 60_000));
	let mut config = Config::new(&raw)?;
	// Exercise a real service builder error after database acquisition.
	config.ip_range_denylist = vec!["invalid-build-fixture-cidr".into()];
	let core = server(config);
	let mut build = tokio::spawn(Services::build(core.clone()));
	timeout(Duration::from_secs(5), fake.state.release_entered.notified())
		.await
		.expect("release dispatched");
	let returned_before_release = build.is_finished();
	if cancel {
		build.abort();
	}
	fake.state.release_allowed.notify_one();
	let result = timeout(Duration::from_secs(5), &mut build)
		.await
		.expect("builder finishes");
	if cancel && !returned_before_release {
		assert!(
			result
				.expect_err("cancelled builder")
				.is_cancelled()
		);
	} else {
		let error = result
			.expect("builder task")
			.expect_err("invalid CIDR builder fails");
		assert!(
			error.to_string().contains("ip_range_denylist"),
			"original builder failure: {error}"
		);
	}
	timeout(Duration::from_secs(5), async {
		while fake.state.released.load(Ordering::SeqCst) != 1 || Arc::strong_count(&core) != 1 {
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("release and renewal owners finish while the runtime stays live");
	assert_eq!(fake.state.acquired.load(Ordering::SeqCst), 1);
	assert_eq!(fake.state.renewed.load(Ordering::SeqCst), 0);
	assert!(
		fake.state
			.lease
			.lock()
			.expect("lease lock")
			.is_none()
	);
	assert!(!root.join("unused-database").exists());
	fake.stop().await;
	assert!(
		!returned_before_release,
		"service build returned its error before database release finished"
	);
	Ok(())
}

#[test]
fn failed_remote_build_awaits_lease_release() -> Result {
	isolated(
		"services::build_tests::failed_remote_build_awaits_lease_release",
		async |root| remote_failure(root, false).await,
	)
}

#[test]
fn cancelled_remote_build_cleanup_finishes_lease_release() -> Result {
	isolated(
		"services::build_tests::cancelled_remote_build_cleanup_finishes_lease_release",
		async |root| remote_failure(root, true).await,
	)
}

#[test]
fn failed_native_build_releases_database_and_preserves_data() -> Result {
	isolated(
		"services::build_tests::failed_native_build_releases_database_and_preserves_data",
		async |root| {
			let prepared = services(root).await?;
			prepared.db["global"]
				.insert(b"build-preservation", b"durable")
				.await?;
			let mut config = prepared.server.config.as_ref().clone();
			drop(prepared);
			config.ip_range_denylist = vec!["invalid-build-fixture-cidr".into()];
			let core = server(config);
			let error = Services::build(core.clone())
				.await
				.expect_err("invalid service configuration");
			assert!(error.to_string().contains("ip_range_denylist"), "{error}");
			assert_eq!(Arc::strong_count(&core), 1, "native build owners released");
			let reopened = services(root).await?;
			assert_eq!(
				reopened.db["global"]
					.get(b"build-preservation")
					.await?
					.as_ref(),
				b"durable"
			);
			reopened.stop().await;
			Ok(())
		},
	)
}
