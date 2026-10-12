//! Native failure controls for immutable attempt persistence and retirement.

use tuwunel_core::Result;

use super::{
	Destination,
	ack_tests::{enqueue, rows},
	edu_tests::Fixture,
	incarnation_tests::isolated,
};

async fn prepare(
	fixture: &Fixture,
	destination: &Destination,
	value: usize,
) -> Result<super::super::data::PreparedAttempt> {
	let item = enqueue(&fixture.services, destination, value).await?;
	let data = &fixture.services.sending.db;
	data.mark_as_active(std::iter::once(&item))
		.await?;
	let selected = data
		.selected_acknowledgement(destination, &[item.1])
		.await?;
	let recipient = match destination {
		| Destination::Appservice(id) => Some(
			match fixture
				.services
				.appservice
				.get_registration(id)
				.await
			{
				| Some(registration) => super::super::data::appservice_owner(&registration)?,
				| None => [1; 32],
			},
		),
		| _ => None,
	};
	data.persist_attempt(
		destination,
		selected,
		br#"{"events":[],"ephemeral":[{"type":"m.typing"}]}"#.to_vec(),
		recipient,
	)
	.await
}

#[test]
fn losing_the_entire_journal_cannot_recompose_attempted_deliveries() -> Result {
	isolated(
		"sending::sender::attempt_tests::losing_the_entire_journal_cannot_recompose_attempted_deliveries",
		async {
			let fixture = Fixture::new().await?;
			let destination = Destination::Appservice("lost-journal".into());
			let attempt = prepare(&fixture, &destination, 1).await?;
			let data = &fixture.services.sending.db;
			let journal = rows(&fixture.services, "sendingtransaction_record").await?;
			let active = rows(&fixture.services, "servercurrentevent_data").await?;
			let global = rows(&fixture.services, "global").await?;
			for key in journal.keys() {
				fixture.services.db["sendingtransaction_record"]
					.remove(key)
					.await?;
			}
			data.load_attempt(&destination)
				.await
				.expect_err("whole journal loss is not an unattempted active batch");
			data.persist_attempt(
				&destination,
				attempt.acknowledgement.clone(),
				br#"{"events":[]}"#.to_vec(),
				Some([1; 32]),
			)
			.await
			.expect_err("no new body or ID may replace a lost attempt");
			data.acknowledge_active(&destination, &attempt.acknowledgement)
				.await
				.expect_err("missing journal cannot retire active obligations");
			assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, active);
			assert_eq!(rows(&fixture.services, "global").await?, global);
			for (key, value) in &journal {
				fixture.services.db["sendingtransaction_record"]
					.insert(key, value)
					.await?;
			}
			let replay = data
				.load_attempt(&destination)
				.await?
				.expect("restored exact attempt");
			assert_eq!(replay.transaction_id(), attempt.transaction_id());
			assert_eq!(replay.body, attempt.body);
			data.acknowledge_active(&destination, &replay.acknowledgement)
				.await?;
			fixture.finish().await;
			Ok(())
		},
	)
}

#[test]
fn ownership_witness_corruption_and_atomic_commit_refusal_preserve_rows() -> Result {
	isolated(
		"sending::sender::attempt_tests::ownership_witness_corruption_and_atomic_commit_refusal_preserve_rows",
		async {
			let fixture = Fixture::new().await?;
			let destination = Destination::Appservice("witness-refusal".into());
			let data = &fixture.services.sending.db;
			let item = enqueue(&fixture.services, &destination, 1).await?;
			data.mark_as_active(std::iter::once(&item))
				.await?;
			let selected = data
				.selected_acknowledgement(&destination, &[item.1])
				.await?;
			let active = rows(&fixture.services, "servercurrentevent_data").await?;
			// Allow the counter reservation, then refuse the actual journal plus
			// witness transaction before either map is mutated.
			tuwunel_database::refusal::refuse_after("global", 1);
			data.persist_attempt(
				&destination,
				selected.clone(),
				br#"{"events":[]}"#.to_vec(),
				Some([1; 32]),
			)
			.await
			.expect_err("witness and journal must commit together");
			assert!(
				rows(&fixture.services, "sendingtransaction_record")
					.await?
					.is_empty()
			);
			assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, active);
			let attempt = data
				.persist_attempt(
					&destination,
					selected,
					br#"{"events":[]}"#.to_vec(),
					Some([1; 32]),
				)
				.await?;
			let global = rows(&fixture.services, "global").await?;
			let (key, original) = global
				.iter()
				.find(|(key, _)| key.starts_with(&[0x06]))
				.expect("persisted ownership witness");
			for replacement in [None, Some(b"corrupt ownership".as_slice())] {
				match replacement {
					| None => fixture.services.db["global"].remove(key).await?,
					| Some(value) => {
						fixture.services.db["global"]
							.insert(key, value)
							.await?;
					},
				}
				data.load_attempt(&destination)
					.await
					.expect_err("missing or corrupt witness refuses replay");
				data.acknowledge_active(&destination, &attempt.acknowledgement)
					.await
					.expect_err("missing or corrupt witness refuses retirement");
				assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, active);
				fixture.services.db["global"]
					.insert(key, original)
					.await?;
			}
			let journal = rows(&fixture.services, "sendingtransaction_record").await?;
			tuwunel_database::refusal::refuse_next("global");
			data.acknowledge_active(&destination, &attempt.acknowledgement)
				.await
				.expect_err("witness retirement is atomic with all selected rows");
			assert_eq!(rows(&fixture.services, "global").await?, global);
			assert_eq!(rows(&fixture.services, "sendingtransaction_record").await?, journal);
			assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, active);
			data.acknowledge_active(&destination, &attempt.acknowledgement)
				.await?;
			assert!(
				!rows(&fixture.services, "global")
					.await?
					.contains_key(key)
			);
			fixture.finish().await;
			Ok(())
		},
	)
}

#[test]
fn failed_persistence_and_ack_preserve_all_obligations() -> Result {
	isolated(
		"sending::sender::attempt_tests::failed_persistence_and_ack_preserve_all_obligations",
		async {
			let fixture = Fixture::new().await?;
			let destination = Destination::Appservice("persist-refusal".into());
			let item = enqueue(&fixture.services, &destination, 1).await?;
			let data = &fixture.services.sending.db;
			data.mark_as_active(std::iter::once(&item))
				.await?;
			let selected = data
				.selected_acknowledgement(&destination, &[item.1])
				.await?;
			let active = rows(&fixture.services, "servercurrentevent_data").await?;
			tuwunel_database::refusal::refuse_next("sendingtransaction_record");
			data.persist_attempt(
				&destination,
				selected.clone(),
				br#"{"events":[]}"#.to_vec(),
				Some([1; 32]),
			)
			.await
			.expect_err("refused persistence must not send");
			assert!(
				rows(&fixture.services, "sendingtransaction_record")
					.await?
					.is_empty()
			);
			assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, active);
			let attempt = data
				.persist_attempt(
					&destination,
					selected.clone(),
					br#"{"events":[]}"#.to_vec(),
					Some([1; 32]),
				)
				.await?;
			let journal = rows(&fixture.services, "sendingtransaction_record").await?;
			data.acknowledge_active(&destination, &selected)
				.await
				.expect_err("row-only ACK cannot bypass persisted ownership");
			tuwunel_database::refusal::refuse_next("servercurrentevent_data");
			data.acknowledge_active(&destination, &attempt.acknowledgement)
				.await
				.expect_err("retirement is one atomic commit");
			assert_eq!(rows(&fixture.services, "sendingtransaction_record").await?, journal);
			assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, active);
			data.acknowledge_active(&destination, &attempt.acknowledgement)
				.await?;
			assert!(
				rows(&fixture.services, "sendingtransaction_record")
					.await?
					.is_empty()
			);
			assert!(
				rows(&fixture.services, "servercurrentevent_data")
					.await?
					.is_empty()
			);
			data.acknowledge_active(&destination, &attempt.acknowledgement)
				.await?;
			fixture.finish().await;
			Ok(())
		},
	)
}

#[test]
fn corrupt_or_missing_journal_bytes_refuse_replay_and_retirement() -> Result {
	isolated(
		"sending::sender::attempt_tests::corrupt_or_missing_journal_bytes_refuse_replay_and_retirement",
		async {
			let fixture = Fixture::new().await?;
			let destination = Destination::Appservice("persist-corrupt".into());
			let attempt = prepare(&fixture, &destination, 1).await?;
			let data = &fixture.services.sending.db;
			let journal = rows(&fixture.services, "sendingtransaction_record").await?;
			let active = rows(&fixture.services, "servercurrentevent_data").await?;
			let map = &fixture.services.db["sendingtransaction_record"];
			for (key, value) in &journal {
				let mut corrupt = value.clone();
				corrupt[0] ^= 0x40;
				map.insert(key, &corrupt).await?;
				data.load_attempt(&destination)
					.await
					.expect_err("corrupt header or body refuses replay");
				data.acknowledge_active(&destination, &attempt.acknowledgement)
					.await
					.expect_err("corrupt header or body refuses retirement");
				assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, active);
				map.insert(key, value).await?;
				map.remove(key).await?;
				data.load_attempt(&destination)
					.await
					.expect_err("missing header or chunk refuses replay");
				map.insert(key, value).await?;
			}
			let replay = data
				.load_attempt(&destination)
				.await?
				.expect("repaired attempt");
			assert_eq!(replay.transaction_id(), attempt.transaction_id());
			assert_eq!(replay.body, attempt.body);
			assert_eq!(rows(&fixture.services, "sendingtransaction_record").await?, journal);
			data.acknowledge_active(&destination, &replay.acknowledgement)
				.await?;
			fixture.finish().await;
			Ok(())
		},
	)
}

#[test]
fn incomplete_cancellation_resumes_before_admission_and_old_ack_preserves_new_attempt() -> Result
{
	isolated(
		"sending::sender::attempt_tests::incomplete_cancellation_resumes_before_admission_and_old_ack_preserves_new_attempt",
		async {
			let fixture = Fixture::new().await?;
			let destination = Destination::Appservice("persist-cancel".into());
			let old = prepare(&fixture, &destination, 1).await?;
			let data = &fixture.services.sending.db;
			tuwunel_database::refusal::refuse_next("servercurrentevent_data");
			data.delete_all_requests_for(&destination)
				.await
				.expect_err("cancellation marker remains after refused row cleanup");
			assert_eq!(
				rows(&fixture.services, "sendingtransaction_record")
					.await?
					.len(),
				1
			);
			let pending = enqueue(&fixture.services, &destination, 2).await?;
			assert!(
				rows(&fixture.services, "servernameevent_data")
					.await?
					.contains_key(&pending.0)
			);
			assert!(
				rows(&fixture.services, "servercurrentevent_data")
					.await?
					.is_empty()
			);
			assert!(
				rows(&fixture.services, "sendingtransaction_record")
					.await?
					.is_empty()
			);
			let stale = enqueue(&fixture.services, &destination, 3).await?;
			data.delete_all_requests_for(&destination).await?;
			data.mark_as_active(std::iter::once(&stale))
				.await?;
			assert!(
				rows(&fixture.services, "servercurrentevent_data")
					.await?
					.is_empty(),
				"cancelled dispatch hint cannot recreate an admission"
			);
			let pending = enqueue(&fixture.services, &destination, 2).await?;
			data.mark_as_active(std::iter::once(&pending))
				.await?;
			let selected = data
				.selected_acknowledgement(&destination, &[pending.1])
				.await?;
			let new = data
				.persist_attempt(
					&destination,
					selected,
					br#"{"events":[]}"#.to_vec(),
					Some([1; 32]),
				)
				.await?;
			assert_ne!(new.transaction_id(), old.transaction_id());
			let active = rows(&fixture.services, "servercurrentevent_data").await?;
			let journal = rows(&fixture.services, "sendingtransaction_record").await?;
			data.acknowledge_active(&destination, &old.acknowledgement)
				.await?;
			assert_eq!(rows(&fixture.services, "servercurrentevent_data").await?, active);
			assert_eq!(rows(&fixture.services, "sendingtransaction_record").await?, journal);
			data.acknowledge_active(&destination, &new.acknowledgement)
				.await?;
			fixture.finish().await;
			Ok(())
		},
	)
}

#[test]
fn a_prepared_attempt_cannot_reach_a_reused_appservice_id() -> Result {
	isolated(
		"sending::sender::attempt_tests::a_prepared_attempt_cannot_reach_a_reused_appservice_id",
		async {
			use ruma::api::appservice::{Namespaces, Registration, RegistrationInit};
			use tokio::{
				io::{AsyncReadExt, AsyncWriteExt},
				net::TcpListener,
			};
			let fixture = Fixture::new().await?;
			let services = &fixture.services;
			<crate::appservice::Service as crate::Service>::worker(services.appservice.clone())
				.await?;
			let listener = TcpListener::bind("127.0.0.1:0").await?;
			let make_registration = |url: String, suffix: &str| -> Registration {
				RegistrationInit {
					id: "reused-owner".into(),
					url: Some(url),
					as_token: format!("disposable-owner-as-{suffix}"),
					hs_token: format!("disposable-owner-hs-{suffix}"),
					sender_localpart: "owner-bot".into(),
					namespaces: Namespaces::new(),
					rate_limited: None,
					protocols: None,
				}
				.into()
			};
			services
				.appservice
				.register_appservice(make_registration("http://127.0.0.1:1".into(), "a"))
				.await?;
			let destination = Destination::Appservice("reused-owner".into());
			let attempt = prepare(&fixture, &destination, 1).await?;
			services
				.appservice
				.unregister_appservice("reused-owner")
				.await?;
			services
				.appservice
				.register_appservice(make_registration(
					format!("http://{}", listener.local_addr()?),
					"b",
				))
				.await?;
			let peer = tokio::spawn(async move {
				let Ok(Ok((mut socket, _))) =
					tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
						.await
				else {
					return false;
				};
				let mut request = [0_u8; 4096];
				let size = socket
					.read(&mut request)
					.await
					.expect("owned peer read");
				assert!(size > 0, "actual HTTP reached replacement registration");
				socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.expect("owned acknowledgement");
				true
			});
			let outcome = services
				.sending
				.deliver_appservice_attempt("reused-owner".into(), attempt)
				.await;
			let reached_replacement = peer.await.expect("owned peer completed");
			assert!(
				!reached_replacement,
				"stale prepared body was delivered to replacement appservice URL/token"
			);
			assert!(
				matches!(outcome, Ok(super::Delivery::Unprepared(..))),
				"stale attempt must refuse HTTP"
			);
			fixture.finish().await;
			Ok(())
		},
	)
}

#[test]
fn unregister_waits_for_the_owned_http_delivery() -> Result {
	isolated(
		"sending::sender::attempt_tests::unregister_waits_for_the_owned_http_delivery",
		async {
			use ruma::api::appservice::{Namespaces, Registration, RegistrationInit};
			use tokio::{
				io::{AsyncReadExt, AsyncWriteExt},
				net::TcpListener,
			};
			let fixture = Fixture::new().await?;
			let services = &fixture.services;
			<crate::appservice::Service as crate::Service>::worker(services.appservice.clone())
				.await?;
			let listener = TcpListener::bind("127.0.0.1:0").await?;
			let registration: Registration = RegistrationInit {
				id: "in-flight-owner".into(),
				url: Some(format!("http://{}", listener.local_addr()?)),
				as_token: "disposable-in-flight-as".into(),
				hs_token: "disposable-in-flight-hs".into(),
				sender_localpart: "in-flight-bot".into(),
				namespaces: Namespaces::new(),
				rate_limited: None,
				protocols: None,
			}
			.into();
			services
				.appservice
				.register_appservice(registration)
				.await?;
			let destination = Destination::Appservice("in-flight-owner".into());
			prepare(&fixture, &destination, 1).await?;
			let sender = services.sending.clone();
			let client = tokio::spawn(async move {
				sender
					.send_events(destination, vec![super::SendingEvent::Flush])
					.await
			});
			let (mut peer, _) =
				tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
					.await
					.expect("bounded actual request")?;
			let mut request = [0_u8; 4096];
			assert!(peer.read(&mut request).await? > 0);
			assert!(
				tokio::time::timeout(
					std::time::Duration::from_millis(100),
					services
						.appservice
						.unregister_appservice("in-flight-owner")
				)
				.await
				.is_err(),
				"unregister may not replace a registration while HTTP owns its body"
			);
			assert!(
				services
					.appservice
					.get_registration("in-flight-owner")
					.await
					.is_some()
			);
			peer.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await?;
			drop(peer);
			assert!(matches!(
				client.await.expect("owned sender completed"),
				Ok(super::Delivery::Acknowledged(..))
			));
			services
				.appservice
				.unregister_appservice("in-flight-owner")
				.await?;
			assert!(
				rows(services, "sendingtransaction_record")
					.await?
					.is_empty()
			);
			assert!(
				rows(services, "servercurrentevent_data")
					.await?
					.is_empty()
			);
			fixture.finish().await;
			Ok(())
		},
	)
}
