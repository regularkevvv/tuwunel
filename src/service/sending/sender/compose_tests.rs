//! Exercise the real sender and ACK cleanup against corrupt durable rows.
//! Every case owns its reference database; no remote transaction is needed.

use std::{collections::BTreeMap, time::Duration};

use futures::TryStreamExt;
use tokio::time::timeout;
use tuwunel_core::{
	Result,
	ruma::{
		api::appservice::{Namespaces, Registration, RegistrationInit},
		server_name,
	},
};

use super::{
	CurTransactionStatus, Delivery, Destination, EduBuf, QueueRecovery, QueueRetries,
	SendingEvent, SendingFutures, WakeQueue, edu_tests::Fixture,
};
use crate::{
	Services,
	rooms::timeline::RawPduId,
	sending::{TAG_DEVICE_LIST_CHANGED, TAG_TO_DEVICE},
};

type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Clone, Copy)]
enum Fault {
	Pdu,
	Edu,
	ToDevice,
	DeviceList,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appservice_corrupt_pdu_is_not_acknowledged() -> Result {
	verify(false, Fault::Pdu).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn federation_corrupt_pdu_is_not_acknowledged() -> Result { verify(true, Fault::Pdu).await }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appservice_corrupt_edu_is_not_acknowledged() -> Result {
	verify(false, Fault::Edu).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn federation_corrupt_edu_is_not_acknowledged() -> Result { verify(true, Fault::Edu).await }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appservice_truncated_to_device_is_not_acknowledged() -> Result {
	verify(false, Fault::ToDevice).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appservice_invalid_device_list_is_not_acknowledged() -> Result {
	verify(false, Fault::DeviceList).await
}

async fn physical(services: &Services, map: &str) -> Result<Rows> {
	services.db[map]
		.raw_stream()
		.map_ok(|(key, value)| (key.to_vec(), value.to_vec()))
		.try_collect()
		.await
}

async fn destination(services: &Services, federation: bool) -> Result<Destination> {
	Ok(if federation {
		Destination::Federation(server_name!("remote.example").to_owned())
	} else {
		let mut registration: Registration = RegistrationInit {
			id: "compose-refusal".into(),
			url: Some("http://127.0.0.1:9".into()),
			as_token: "disposable-compose-as-token".into(),
			hs_token: "disposable-compose-hs-token".into(),
			sender_localpart: "compose-bot".into(),
			namespaces: Namespaces::new(),
			rate_limited: None,
			protocols: None,
		}
		.into();
		registration.receive_ephemeral = true;
		registration.msc3202_transaction_extensions = true;
		services
			.appservice
			.load_appservice(registration)
			.await?;
		Destination::Appservice("compose-refusal".into())
	})
}

async fn corrupt_active(services: &Services, key: &[u8], raw: RawPduId, fault: Fault) -> Result {
	match fault {
		| Fault::Pdu =>
			services.db["pduid_pdu"]
				.insert(&raw, b"{".as_slice())
				.await?,
		| Fault::Edu =>
			services.db["servercurrentevent_data"]
				.insert(key, b"{".as_slice())
				.await?,
		| Fault::ToDevice =>
			services.db["servercurrentevent_data"]
				.insert(key, &[TAG_TO_DEVICE])
				.await?,
		| Fault::DeviceList => {
			let mut value = vec![TAG_DEVICE_LIST_CHANGED];
			value.extend_from_slice(&3_u64.to_be_bytes());
			value.extend_from_slice(b"invalid-user-id");
			services.db["servercurrentevent_data"]
				.insert(key, &value)
				.await?;
		},
	}
	Ok(())
}

async fn verify(federation: bool, fault: Fault) -> Result {
	let fixture = Fixture::new().await?;
	let services = &fixture.services;
	let destination = destination(services, federation).await?;

	let mut bytes = 1_u64.to_be_bytes().to_vec();
	bytes.extend_from_slice(&2_u64.to_be_bytes());
	let raw = RawPduId::from_bytes(&bytes)?;
	let event = match fault {
		| Fault::Pdu => SendingEvent::Pdu(raw),
		| Fault::Edu => SendingEvent::Edu(EduBuf::from_slice(
			br#"{"type":"m.typing","content":{"user_ids":[]}}"#,
		)),
		| Fault::ToDevice => {
			let mut value = EduBuf::from_slice(&[TAG_TO_DEVICE]);
			value.extend_from_slice(&3_u64.to_be_bytes());
			value.extend_from_slice(br#"{"type":"m.room.encrypted","sender":"@sender:localhost","to_user_id":"@recipient:localhost","to_device_id":"DEVICE","content":{}}"#);
			SendingEvent::ToDevice(value)
		},
		| Fault::DeviceList => {
			let mut value = EduBuf::from_slice(&[TAG_DEVICE_LIST_CHANGED]);
			value.extend_from_slice(&3_u64.to_be_bytes());
			value.extend_from_slice(b"@recipient:localhost");
			SendingEvent::DeviceListChanged(value)
		},
	};
	services
		.sending
		.queue_and_dispatch(destination.clone(), event)
		.await?;
	let queued = services
		.sending
		.db
		.queued_requests(&destination)
		.try_collect::<Vec<_>>()
		.await?;
	assert_eq!(queued.len(), 1);
	services
		.sending
		.db
		.mark_as_active(queued.iter())
		.await?;
	let key = &queued[0].0;
	corrupt_active(services, key, raw, fault).await?;
	let before = physical(services, "servercurrentevent_data").await?;
	let peers_before = physical(services, "servername_status").await?;
	assert_eq!(before.len(), 1);
	let mut futures = SendingFutures::new();
	let mut statuses = CurTransactionStatus::new();
	let mut wakes = WakeQueue::new();
	let mut retries = QueueRetries::new();
	services
		.sending
		.startup_netburst(services.sending.shard_id(&destination), &mut futures, &mut statuses)
		.await?;
	let response = timeout(Duration::from_secs(2), futures.next())
		.await
		.expect("compose before network")
		.expect("one active transaction");
	let acknowledged = matches!(response, Ok(Delivery::Acknowledged(_, _)));
	let local_refusal = matches!(&response, Ok(Delivery::Unprepared(_, error)) if error.status_code().is_server_error());
	let mut stage = QueueRecovery::ResumePending;
	services
		.sending
		.handle_response(
			response,
			&mut futures,
			&mut statuses,
			&mut wakes,
			&mut stage,
			&mut retries,
		)
		.await?;
	let after = physical(services, "servercurrentevent_data").await?;
	eprintln!(
		"compose refused={} active before={} after={}",
		!acknowledged,
		before.len(),
		after.len()
	);
	assert!(!acknowledged, "corrupt composition is not an acknowledged transaction");
	assert!(local_refusal, "local preparation has a distinct typed outcome");
	assert_eq!(after, before, "failed composition preserves every active row byte");
	assert_eq!(
		physical(services, "servername_status").await?,
		peers_before,
		"local corruption is not recorded against the peer"
	);
	assert!(wakes.is_empty(), "no remote retry timer is armed");
	assert_eq!(retries.len(), 1, "one local retry owns the destination");
	let retry = retries
		.get_mut(&destination)
		.expect("owned local retry");
	assert_eq!(retry.1, QueueRecovery::ResumePending, "retry cannot run ACK cleanup");
	retry.0 = tokio::time::Instant::now();
	services
		.sending
		.retry_queue(&mut futures, &mut statuses, &mut retries)
		.await?;
	let response = timeout(Duration::from_secs(2), futures.next())
		.await
		.expect("repeat local preparation")
		.expect("owned retry transaction");
	assert!(matches!(&response, Ok(Delivery::Unprepared(_, _))));
	services
		.sending
		.handle_response(
			response,
			&mut futures,
			&mut statuses,
			&mut wakes,
			&mut stage,
			&mut retries,
		)
		.await?;
	assert_eq!(retries.len(), 1, "repeated preparation failure coalesces");
	assert_eq!(physical(services, "servercurrentevent_data").await?, before);
	assert_eq!(physical(services, "servername_status").await?, peers_before);
	assert!(
		physical(services, "servernameevent_data")
			.await?
			.is_empty()
	);
	drop(futures);
	fixture.finish().await;
	Ok(())
}
