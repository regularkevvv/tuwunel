#![cfg(test)]

//! Every client that follows a remote server's naming refuses destinations
//! inside `ip_range_denylist` (ADR-0005). A name resolving to loopback — the
//! same shape as a server name, SRV target or `.well-known` answer pointing at
//! a private or metadata address — must not be reached by the federation,
//! sender, Synapse-admin or well-known clients, while the unrestricted
//! default client reaching the same listener proves the refusals are the
//! guard's and not a missing server.

use std::{env::temp_dir, fs::remove_dir_all, net::TcpListener, process::id as process_id};

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
	let db_path = temp_dir().join(format!("tuwunel-test-ssrf-guard-{}", process_id()));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.extend([format!("database_path=\"{}\"", db_path.display()), "log=\"warn\"".to_owned()]);

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result: Result = runtime.block_on(async {
		let services = tuwunel::async_start(&server).await?;
		let responder = tokio::spawn(respond(AsyncListener::from_std(listener)?));
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

		responder.abort();
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
/// which a client reports as a failure the guard did not cause.
async fn respond(listener: AsyncListener) -> Result {
	loop {
		let (stream, _) = listener.accept().await?;
		let mut stream = BufReader::new(stream);
		loop {
			let mut line = String::new();
			if stream.read_line(&mut line).await? == 0 || line == "\r\n" {
				break;
			}
		}
		stream
			.get_mut()
			.write_all(
				b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
			)
			.await?;
	}
}
