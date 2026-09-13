#![cfg(test)]

//! Every client that follows a remote server's naming refuses destinations
//! inside `ip_range_denylist` (ADR-0005). A name resolving to loopback — the
//! same shape as a server name, SRV target or `.well-known` answer pointing at
//! a private or metadata address — must not be reached by the federation,
//! sender, Synapse-admin or well-known clients, while the unrestricted
//! default client reaching the same listener proves the refusals are the
//! guard's and not a missing server.
//!
//! A reachable destination that answers with a redirect into the denylist must
//! not be followed either (gate D6), whether the redirect names the denied
//! address literally or by a name resolving to it. That holds for the
//! federation clients and for the media, URL-preview and pusher clients that
//! fetch what a remote names. The first hop is an IP literal: the client does
//! not resolve it, so it stands in for a public destination (IP-literal
//! destinations are refused earlier, before a request is built). Each guarded
//! client reaches that first hop and never the redirect's target, while the
//! unrestricted client follows the same redirect to it.

use std::{
	env::temp_dir,
	fs::remove_dir_all,
	net::TcpListener,
	process::id as process_id,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering::SeqCst},
	},
};

use tokio::{
	io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
	net::TcpListener as AsyncListener,
};
use tuwunel::{Args, Runtime, Server};
use tuwunel_core::Result;

#[test]
fn federation_clients_refuse_denied_destinations() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	listener.set_nonblocking(true)?;
	let redirector = TcpListener::bind(("127.0.0.1", 0))?;
	let redirect_port = redirector.local_addr()?.port();
	redirector.set_nonblocking(true)?;
	let db_path = temp_dir().join(format!("tuwunel-test-ssrf-guard-{}", process_id()));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.extend([format!("database_path=\"{}\"", db_path.display()), "log=\"warn\"".to_owned()]);

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result: Result = runtime.block_on(async {
		let services = tuwunel::async_start(&server).await?;
		let landed = Arc::new(AtomicUsize::new(0));
		let answered = Arc::new(AtomicUsize::new(0));
		let responder = tokio::spawn(respond(AsyncListener::from_std(listener)?, landed.clone()));
		let redirecting =
			tokio::spawn(redirect(AsyncListener::from_std(redirector)?, port, answered.clone()));
		let url = format!("http://localhost:{port}/_matrix/federation/v1/version");

		assert!(
			services
				.client
				.default
				.get(&url)
				.send()
				.await
				.is_ok(),
			"the unrestricted client must reach the listener"
		);
		for (name, client) in [
			("federation", &services.client.federation),
			("sender", &services.client.sender),
			("synapse", &services.client.synapse),
			("well_known", &services.client.well_known),
		] {
			assert!(
				client.get(&url).send().await.is_err(),
				"the {name} client reached a denied address"
			);
		}

		let guarded = [
			("federation", &services.client.federation),
			("sender", &services.client.sender),
			("synapse", &services.client.synapse),
			("well_known", &services.client.well_known),
			("extern_media", &services.client.extern_media),
			("url_preview", &services.client.url_preview),
			("pusher", &services.client.pusher),
		];
		for target in ["literal", "name"] {
			let url = format!("http://127.0.0.1:{redirect_port}/{target}");

			let before = landed.load(SeqCst);
			let followed = services.client.default.get(&url).send().await?;
			assert_eq!(followed.status().as_u16(), 204, "the redirect's target must answer");
			assert_eq!(
				landed.load(SeqCst),
				before.saturating_add(1),
				"the unrestricted client must follow the {target} redirect to the listener"
			);

			for (name, client) in guarded {
				let (landed_before, answered_before) =
					(landed.load(SeqCst), answered.load(SeqCst));
				assert!(
					client.get(&url).send().await.is_err(),
					"the {name} client followed a {target} redirect to a denied address"
				);
				assert_eq!(
					answered.load(SeqCst),
					answered_before.saturating_add(1),
					"the {name} client must reach the redirecting destination, so the refusal \
					 is the redirect's"
				);
				assert_eq!(
					landed.load(SeqCst),
					landed_before,
					"the {name} client reached a denied address through a {target} redirect"
				);
			}
		}

		responder.abort();
		redirecting.abort();
		server.server.shutdown()?;
		drop(services);
		tuwunel::async_run(&server).await?;
		tuwunel::async_stop(&server).await
	});

	drop(server);
	drop(runtime);
	remove_dir_all(&db_path).ok();
	result
}

/// Answers every connection with an empty success, after reading the request
/// head: closing a socket with unread request bytes resets the connection,
/// which a client reports as a failure the guard did not cause. Counts the
/// requests it answers.
async fn respond(listener: AsyncListener, landed: Arc<AtomicUsize>) -> Result {
	loop {
		let (stream, _) = listener.accept().await?;
		let mut stream = BufReader::new(stream);
		loop {
			let mut line = String::new();
			if stream.read_line(&mut line).await? == 0 || line == "\r\n" {
				break;
			}
		}
		landed.fetch_add(1, SeqCst);
		stream
			.get_mut()
			.write_all(
				b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
			)
			.await?;
	}
}

/// The reachable destination: answers every request with a redirect to the
/// listener on `port`, by name for `/name` and by IP literal otherwise. Counts
/// the requests it answers.
async fn redirect(listener: AsyncListener, port: u16, answered: Arc<AtomicUsize>) -> Result {
	loop {
		let (stream, _) = listener.accept().await?;
		let mut stream = BufReader::new(stream);
		let mut request = String::new();
		stream.read_line(&mut request).await?;
		loop {
			let mut line = String::new();
			if stream.read_line(&mut line).await? == 0 || line == "\r\n" {
				break;
			}
		}
		let host = if request.starts_with("GET /name ") {
			"localhost"
		} else {
			"127.0.0.1"
		};
		answered.fetch_add(1, SeqCst);
		let response = format!(
			"HTTP/1.1 302 Found\r\nLocation: http://{host}:{port}/landed\r\nContent-Length: \
			 0\r\nConnection: close\r\n\r\n"
		);
		stream
			.get_mut()
			.write_all(response.as_bytes())
			.await?;
	}
}
