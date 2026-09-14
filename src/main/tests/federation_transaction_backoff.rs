#![cfg(all(test, feature = "direct_tls"))]

//! A transaction a peer refuses waits for the backoff.
//!
//! A peer answers a transaction it processed with 200 and per-PDU results
//! (server-server API, `PUT /_matrix/federation/v1/send/{txnId}`), so a JSON
//! 4xx refused the whole transaction. The sender records the refusal against
//! the destination: an event queued for the peer afterwards does not send the
//! refused transaction again at once, and the retry armed at the backoff's
//! earliest retry does.
//!
//! The server is its own refusing peer. It sends to its TLS listener by
//! address, which is not its server name, so no loopback guard applies, and its
//! inbound authentication answers 403 `M_FORBIDDEN` because the request's
//! X-Matrix destination is not its server name.

#[cfg(test)]
mod tests {
	use std::{
		env::temp_dir,
		fs::remove_dir_all,
		net::TcpListener,
		path::PathBuf,
		process::id as process_id,
		time::{Duration, SystemTime, UNIX_EPOCH},
	};

	use futures::future::join;
	use serde_json::json;
	use tokio::time::{sleep, timeout};
	use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
	use tuwunel_core::{
		Result, err,
		ruma::{OwnedServerName, ServerName},
		utils::time::now_secs,
	};
	use tuwunel_service::{
		Services,
		federation::{PeerBackoff, ShouldAttempt},
		sending::EduBuf,
	};

	const CERTIFICATE: &str = "../../nix/pkgs/complement/certificate.crt";
	const PRIVATE_KEY: &str = "../../nix/pkgs/complement/private_key.key";

	/// `sender_retry_grace`: the backoff after a lone failure.
	const GRACE_SECS: u64 = 5;

	/// Allowance past the latest wake for the retry to be refused and recorded.
	const SLACK_SECS: u64 = 3;

	#[test]
	fn a_refused_transaction_waits_for_the_backoff() -> Result {
		let listener = TcpListener::bind(("127.0.0.1", 0))?;
		let port = listener.local_addr()?.port();
		let db_path =
			temp_dir().join(format!("tuwunel-test-transaction-backoff-{}", process_id()));
		let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
		let mut args = Args::default_test(&["fresh", "cleanup"]);
		args.option.extend([
			format!("database_path={db_path:?}"),
			"server_name=\"sender.test\"".to_owned(),
			"address=[\"127.0.0.1\"]".to_owned(),
			format!("port={port}"),
			"listening=true".to_owned(),
			"log=\"warn\"".to_owned(),
			"ip_range_denylist=[]".to_owned(),
			"allow_invalid_tls_certificates=true".to_owned(),
			format!("sender_retry_grace={GRACE_SECS}"),
			format!("tls.certs={:?}", source.join(CERTIFICATE)),
			format!("tls.key={:?}", source.join(PRIVATE_KEY)),
		]);

		let runtime = Runtime::new(Some(&args))?;
		let server = Server::new(Some(&args), Some(&runtime))?;
		let result: Result = runtime.block_on(async {
			let services = async_start(&server).await?;
			drop(listener);
			let exercise = async {
				let outcome = timeout(Duration::from_mins(2), exercise(&services, port))
					.await
					.map_err(|error| err!("transaction backoff test timed out: {error}"))
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

	async fn exercise(services: &Services, port: u16) -> Result {
		wait_until_ready(services, &format!("https://127.0.0.1:{port}")).await?;

		let peer = OwnedServerName::try_from(format!("127.0.0.1:{port}"))?;
		assert_eq!(
			services.federation.should_attempt(&peer).await,
			ShouldAttempt::Yes,
			"a peer never attempted must be attemptable"
		);

		queue_edu(services, &peer, 1).await?;
		let refused = next_failure(services, &peer, None).await?;

		// The refusal holds the peer back for the grace tier past it.
		let ShouldAttempt::No { earliest_retry } =
			services.federation.should_attempt(&peer).await
		else {
			panic!("a refused transaction left {peer} attemptable at once");
		};

		let earliest_secs = epoch_secs(earliest_retry)?;
		assert_eq!(
			earliest_secs,
			refused.anchor_secs.saturating_add(GRACE_SECS),
			"the earliest retry must be the grace tier past the refusal"
		);

		// Past the refusal's second, a re-sent transaction's refusal would carry
		// a later instant.
		while now_secs() <= refused.anchor_secs {
			sleep(Duration::from_millis(50)).await;
		}

		queue_edu(services, &peer, 2).await?;
		sleep(Duration::from_secs(1)).await;

		assert!(
			now_secs() < earliest_secs,
			"the test reached the earliest retry too late to judge"
		);
		assert_eq!(
			failure(services, &peer).await?.anchor_secs,
			refused.anchor_secs,
			"the next queued event re-sent the refused transaction before its backoff"
		);

		// With nothing further queued, the retry armed at the earliest retry
		// re-sends the transaction. The wake spreads over at most another
		// delay-width, the grace here.
		let retried = next_failure(services, &peer, Some(refused.anchor_secs)).await?;
		assert!(
			retried.anchor_secs >= earliest_secs,
			"retried at {} before the earliest retry {earliest_secs}",
			retried.anchor_secs
		);

		let latest_secs = earliest_secs
			.saturating_add(GRACE_SECS)
			.saturating_add(SLACK_SECS);

		assert!(
			retried.anchor_secs <= latest_secs,
			"retried at {}, past the armed wake's latest {latest_secs}",
			retried.anchor_secs
		);

		assert!(
			!services.federation.sender_gave_up(&peer).await,
			"a peer refusing for seconds must not be given up"
		);

		Ok(())
	}

	async fn wait_until_ready(services: &Services, base: &str) -> Result {
		let url = format!("{base}/_matrix/client/versions");
		while services
			.client
			.clients
			.default
			.get(&url)
			.send()
			.await
			.is_err()
		{
			sleep(Duration::from_millis(20)).await;
		}

		Ok(())
	}

	/// Queues one typing EDU for `peer`, the `n`th of the test.
	async fn queue_edu(services: &Services, peer: &ServerName, n: u64) -> Result {
		let edu = json!({
			"edu_type": "m.typing",
			"content": {
				"room_id": "!room:sender.test",
				"user_id": format!("@user{n}:sender.test"),
				"typing": true,
			},
		});

		services
			.sending
			.send_edu_server(peer, EduBuf::from_slice(&serde_json::to_vec(&edu)?))
			.await
	}

	/// The peer's current failure record.
	async fn failure(services: &Services, peer: &ServerName) -> Result<PeerBackoff> {
		services
			.federation
			.peer_backoff(peer)
			.await
			.ok_or_else(|| err!("{peer} has no recorded failure"))
	}

	/// Waits for a failure of `peer` recorded after `after`, or for its first.
	async fn next_failure(
		services: &Services,
		peer: &ServerName,
		after: Option<u64>,
	) -> Result<PeerBackoff> {
		timeout(Duration::from_secs(30), async {
			loop {
				if let Some(backoff) = services.federation.peer_backoff(peer).await
					&& after.is_none_or(|after| backoff.anchor_secs > after)
				{
					return backoff;
				}

				sleep(Duration::from_millis(50)).await;
			}
		})
		.await
		.map_err(|_| err!("no transaction to {peer} was refused and recorded after {after:?}"))
	}

	fn epoch_secs(time: SystemTime) -> Result<u64> {
		time.duration_since(UNIX_EPOCH)
			.map(|elapsed| elapsed.as_secs())
			.map_err(|error| err!("{error}"))
	}
}
