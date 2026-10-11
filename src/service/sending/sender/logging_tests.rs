//! Push failure diagnostics retain recovery information without the push key.
use std::{
	io::{self, Write},
	sync::{Arc, Mutex},
	time::Duration,
};

use tracing::instrument::WithSubscriber;
use tracing_subscriber::fmt::format::FmtSpan;
use tuwunel_core::{
	Error, Result,
	ruma::{server_name, user_id},
};

use super::{
	CurTransactionStatus, Delivery, Destination, PUSH_FAILURE_STREAK, QueueRecovery,
	QueueRetries, SendingFutures, Service, TransactionStatus, WakeQueue, edu_tests::Fixture,
	incarnation_tests::isolated,
};

const PUSHKEY: &str = "disposable-pushkey-must-never-appear-in-diagnostics";

struct Writer(Arc<Mutex<Vec<u8>>>);

impl Write for Writer {
	fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
		self.0
			.lock()
			.expect("owned log buffer")
			.extend_from_slice(bytes);
		Ok(bytes.len())
	}

	fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

#[test]
fn destination_debug_redacts_pushkey_and_retains_variant_identity() {
	let destination =
		Destination::Push(user_id!("@diagnostic:localhost").to_owned(), PUSHKEY.into());
	for rendered in [format!("{destination:?}"), format!("{destination:#?}")] {
		assert!(rendered.contains("Push"));
		assert!(rendered.contains("@diagnostic:localhost"));
		assert!(rendered.contains("[redacted]"));
		assert!(!rendered.contains(PUSHKEY));
	}
	assert_eq!(
		format!("{:?}", Destination::Appservice("diagnostic-appservice".into())),
		"Appservice(\"diagnostic-appservice\")"
	);
	assert_eq!(
		format!("{:?}", Destination::Federation(server_name!("remote.example").to_owned())),
		"Federation(\"remote.example\")"
	);
}

#[test]
fn real_push_failure_and_local_failure_logs_redact_pushkey() -> Result {
	isolated(
		"sending::sender::logging_tests::real_push_failure_and_local_failure_logs_redact_pushkey",
		async {
			let fixture = Fixture::new().await?;
			let output = Arc::new(Mutex::new(Vec::new()));
			let buffer = output.clone();
			let subscriber = tracing_subscriber::fmt()
				.with_ansi(false)
				.without_time()
				.with_max_level(tracing::Level::INFO)
				.with_span_events(FmtSpan::NEW)
				.with_writer(move || Writer(buffer.clone()))
				.finish();
			let destination =
				Destination::Push(user_id!("@diagnostic:localhost").to_owned(), PUSHKEY.into());
			let mut statuses = CurTransactionStatus::new();
			let mut retries = QueueRetries::new();
			statuses.insert(destination.clone(), TransactionStatus::Running);
			async {
				let error = Error::bad_database("disposable_gateway_status_503");
				Service::record_push_failure(&destination, &error, 1, Duration::from_secs(7));
				Service::record_push_failure(
					&destination,
					&error,
					PUSH_FAILURE_STREAK,
					Duration::from_secs(11),
				);
				fixture
					.services
					.sending
					.handle_response(
						Ok(Delivery::LocalFailure(
							destination.clone(),
							Box::new(Error::bad_database("disposable_local_failure_marker")),
						)),
						&mut SendingFutures::new(),
						&mut statuses,
						&mut WakeQueue::new(),
						&mut QueueRecovery::ResumePending,
						&mut retries,
					)
					.await?;
				// Exercise the actual INFO instrument span and disappeared-pusher
				// ERROR event too; no pusher exists and no HTTP request is made.
				let outcome = fixture
					.services
					.sending
					.send_events_dest_push(
						user_id!("@diagnostic:localhost").to_owned(),
						PUSHKEY.into(),
						Vec::new(),
					)
					.await
					.expect("missing pusher is locally acknowledged");
				assert!(
					matches!(outcome, Delivery::Acknowledged(owner, _) if owner == destination)
				);
				Ok::<_, Error>(())
			}
			.with_subscriber(subscriber)
			.await?;
			assert!(!statuses.contains_key(&destination));
			assert!(
				retries.contains_key(&destination),
				"local failure retains scheduled recovery"
			);
			fixture.finish().await;
			let captured = String::from_utf8(output.lock().expect("owned log buffer").clone())
				.expect("UTF-8 tracing output");
			for diagnostic in [
				"WARN",
				"ERROR",
				"@diagnostic:localhost",
				"streak=1",
				"streak=4",
				"retry_in_seconds=7",
				"retry_in_seconds=11",
				"disposable_gateway_status_503",
				"Push transaction failed",
				"Push notifications for this pusher are not being delivered",
				"Local delivery failed; accepted work retained",
				"disposable_local_failure_marker",
				"Push(",
				"[redacted]",
				"Pusher disappeared before delivery",
				"push{",
			] {
				assert!(
					captured.contains(diagnostic),
					"missing retained diagnostic: {diagnostic}"
				);
			}
			assert!(
				!captured.contains(PUSHKEY),
				"push key leaked into actual sender diagnostics"
			);
			Ok(())
		},
	)
}
