use std::net::{SocketAddr, TcpListener};

use super::{bind_addrs, covers};

fn addr(addr: &str) -> SocketAddr { addr.parse().expect("test address parses") }

fn occupied_addr() -> (TcpListener, SocketAddr) {
	let listener = TcpListener::bind("127.0.0.1:0").expect("an ephemeral port binds");
	let addr = listener.local_addr().expect("bound address");

	(listener, addr)
}

#[test]
fn covers_the_same_address() {
	assert!(covers(&addr("127.0.0.1:8448"), &addr("127.0.0.1:8448")));
	assert!(covers(&addr("[::1]:8448"), &addr("[::1]:8448")));
}

#[test]
fn covers_its_family_from_the_wildcard() {
	assert!(covers(&addr("0.0.0.0:8448"), &addr("127.0.0.1:8448")));
	assert!(covers(&addr("[::]:8448"), &addr("[::1]:8448")));
}

#[test]
fn leaves_the_other_family_to_bind() {
	assert!(!covers(&addr("0.0.0.0:8448"), &addr("[::1]:8448")));
	assert!(!covers(&addr("[::]:8448"), &addr("127.0.0.1:8448")));
}

#[test]
fn leaves_another_port_to_bind() {
	assert!(!covers(&addr("0.0.0.0:8448"), &addr("127.0.0.1:8008")));
	assert!(!covers(&addr("127.0.0.1:8448"), &addr("127.0.0.1:8008")));
}

#[test]
fn a_specific_address_covers_no_wildcard() {
	assert!(!covers(&addr("127.0.0.1:8448"), &addr("0.0.0.0:8448")));
}

#[test]
fn skips_a_defaulted_address_it_cannot_bind() {
	let (_occupied, taken) = occupied_addr();
	let defaulted = true;
	let mut listening = Vec::new();
	let listeners =
		bind_addrs(&[taken], &mut listening, defaulted).expect("the address is skipped");

	assert!(listeners.is_empty(), "{listeners:?}");
	assert!(listening.is_empty(), "{listening:?}");
}

#[test]
fn fails_on_a_configured_address_it_cannot_bind() {
	let (_occupied, taken) = occupied_addr();
	let defaulted = false;
	let mut listening = Vec::new();

	bind_addrs(&[taken], &mut listening, defaulted).expect_err("the address fails loudly");
}

#[cfg(unix)]
#[test]
fn same_path_matches_one_socket() {
	use std::path::Path;

	use super::same_path;

	assert!(same_path(
		Path::new("/run/tuwunel/tuwunel.sock"),
		Path::new("/run/tuwunel/tuwunel.sock")
	));
	assert!(!same_path(Path::new("/run/tuwunel/tuwunel.sock"), Path::new("/run/other.sock")));
}

fn lifecycle_server() -> std::sync::Arc<tuwunel_core::Server> {
	use std::sync::Arc;

	use tuwunel_core::{
		config::{Config, Figment, Sources},
		log::{LogLevelReloadHandles, Logging, capture::State},
		metrics::Metrics,
	};
	let config = Config::new(&Figment::new().merge(("server_name", "test.example")))
		.expect("minimal config");
	let log = Logging {
		subscriber: Arc::new(tracing::subscriber::NoSubscriber::new()),
		reload: LogLevelReloadHandles::default(),
		capture: Arc::new(State::new()),
	};
	Arc::new(tuwunel_core::Server::new(
		config,
		Sources::default(),
		Some(&tokio::runtime::Handle::current()),
		log,
		Metrics::new(None),
	))
}

async fn failing_listener_drains_survivor(panic: bool) {
	use std::sync::Arc;

	use tokio::{
		sync::oneshot,
		time::{Duration, timeout},
	};

	use super::tasks::Listeners;
	let server = lifecycle_server();
	let mut listeners = Listeners::new(server.clone());
	let owner = Arc::new(());
	let weak = Arc::downgrade(&owner);
	let survivor = server.clone();
	let (sender, receiver) = oneshot::channel();
	listeners.tasks.spawn(async move {
		survivor.until_shutdown().await;
		receiver.await.ok();
		drop(owner);
		Ok(())
	});
	listeners.tasks.spawn(async move {
		assert!(!panic, "listener panic fixture");
		Err(std::io::Error::other("listener error fixture"))
	});
	let handle = crate::handle::ServerHandle::new();
	let mut finish = Box::pin(listeners.finish(&handle));
	timeout(Duration::from_secs(5), async {
		while !server.is_stopping() {
			assert!(futures::poll!(&mut finish).is_pending(), "survivor must still be joined");
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("listener failure requests shutdown");
	assert!(futures::poll!(&mut finish).is_pending(), "failure retains survivor completion");
	assert_eq!(weak.strong_count(), 1);
	sender.send(()).expect("survivor retained");
	let error = finish
		.await
		.expect_err("listener failure propagated");
	assert!(if panic {
		error.is_panic()
	} else {
		error
			.to_string()
			.contains("listener error fixture")
	});
	assert_eq!(weak.strong_count(), 0, "survivor joined before error returned");
}

#[tokio::test]
async fn listener_error_drains_surviving_listeners() {
	failing_listener_drains_survivor(false).await;
}

#[tokio::test]
async fn listener_panic_drains_surviving_listeners() {
	failing_listener_drains_survivor(true).await;
}

#[tokio::test]
async fn dropped_listener_set_retains_child_joins() {
	use std::sync::Arc;

	use super::tasks::Listeners;
	let server = lifecycle_server();
	let mut listeners = Listeners::new(server.clone());
	let owner = Arc::new(());
	let weak = Arc::downgrade(&owner);
	listeners.tasks.spawn(async move {
		let result = std::future::pending::<std::io::Result<()>>().await;
		drop(owner);
		result
	});
	drop(listeners);
	server
		.cleanup
		.join()
		.await
		.expect("aborted listener child joined");
	assert_eq!(weak.strong_count(), 0);
}
