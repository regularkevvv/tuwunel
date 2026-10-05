use futures::{StreamExt, TryStreamExt, stream::iter};
use tuwunel_core::{
	Error, Result, err,
	http::StatusCode,
	ruma::api::error::{ErrorKind, LimitExceededErrorData},
	utils::ReadyExt,
};

use super::{
	SendingEvent, decode_outgoing, decode_sending, missing_count_is_zero, retain_existing,
	within_prefix,
};

fn unavailable() -> Error { err!(Database("queue read unavailable")) }

fn busy() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"queue admission busy".into(),
		StatusCode::TOO_MANY_REQUESTS,
	)
}

fn missing() -> Error { Error::BadRequest(ErrorKind::NotFound, "row absent") }

#[tokio::test]
async fn a_failed_scan_is_not_an_empty_queue_or_a_partial_success() {
	let faults: [fn() -> Error; 2] = [unavailable, busy];
	for fault in faults {
		for fail_first in [true, false] {
			let mut rows: Vec<Result<(&[u8], &[u8])>> = Vec::new();
			if !fail_first {
				rows.push(Ok((b"+as1\xffevent", b"edu")));
			}
			rows.push(Err(fault()));
			rows.push(Ok((b"+as1\xfflater", b"later")));

			let result = iter(rows)
				.ready_take_while(|row| within_prefix(row, b"+as1\xff"))
				.map(decode_sending)
				.try_collect::<Vec<_>>()
				.await;

			let error =
				result.expect_err("storage errors must survive prefix filtering and decoding");
			assert_eq!(error.status_code(), fault().status_code());
			assert_eq!(error.to_string(), fault().to_string());
		}
	}
}

#[test]
fn prefix_end_and_malformed_rows_remain_distinct_from_scan_failure() {
	assert!(!within_prefix(&Ok((b"+other\xffevent", b"")), b"+as1\xff"));
	assert!(within_prefix(&Err(unavailable()), b"+as1\xff"));
	decode_outgoing(Ok((b"malformed", b""))).expect_err("invalid row is not a panic or omission");
	decode_sending(Err(unavailable())).expect_err("active and queued errors propagate");
}

#[test]
fn malformed_pending_pdu_ids_are_errors_for_every_destination() {
	for prefix in
		[b"+as1\xff".as_slice(), b"$@u:remote.example\xffkey\xff", b"remote.example\xff"]
	{
		for suffix in [b"".as_slice(), b"short", &[0x80; 16], &[1; 17]] {
			let mut key = prefix.to_vec();
			key.extend_from_slice(suffix);
			decode_outgoing(Ok((&key, b"")))
				.expect_err("corrupt pending PDU IDs cannot panic or disappear");
		}
	}
}

#[test]
fn queue_existence_only_drops_confirmed_missing_rows() {
	let item = (b"key".to_vec(), SendingEvent::BadgeRefresh);
	assert!(retain_existing(item.clone(), Err(missing())).is_none());
	assert_eq!(
		retain_existing(item.clone(), Ok(()))
			.expect("existing row")
			.expect("lookup passed"),
		item
	);
	retain_existing(item, Err(unavailable()))
		.expect("storage failure is retained")
		.expect_err("storage failure must not mean delivered");
}

#[test]
fn edu_watermark_defaults_only_when_missing() {
	assert_eq!(missing_count_is_zero(Err(missing())).expect("new destination"), 0);
	assert_eq!(missing_count_is_zero(Ok(37)).expect("existing watermark"), 37);
	missing_count_is_zero(Err(unavailable()))
		.expect_err("failed lookup is not a new destination");
	missing_count_is_zero(Err(Error::bad_database("malformed watermark")))
		.expect_err("corrupt counters never rewind to zero");
}

#[test]
fn frozen_push_tags_require_a_push_destination_and_a_valid_pdu_id() {
	let mut raw = 1_u64.to_be_bytes().to_vec();
	raw.extend_from_slice(&2_u64.to_be_bytes());
	let mut key = b"$@u:remote.example\xffkey\xff".to_vec();
	key.extend_from_slice(&raw);
	let (_, event, _) =
		decode_outgoing(Ok((&key, &[super::TAG_FROZEN_PUSH]))).expect("frozen queue row");
	assert!(matches!(event, SendingEvent::FrozenPush(_)));
	decode_outgoing(Ok((&key, &[super::TAG_FROZEN_PUSH, 0])))
		.expect_err("exact frozen tag shape required");
	for prefix in [b"+as\xff".as_slice(), b"remote.example\xff"] {
		let mut key = prefix.to_vec();
		key.extend_from_slice(&raw);
		decode_outgoing(Ok((&key, &[super::TAG_FROZEN_PUSH])))
			.expect_err("frozen decisions are push-only");
	}
	let mut key = b"$@u:remote.example\xffkey\xff".to_vec();
	key.extend_from_slice(b"invalid");
	decode_outgoing(Ok((&key, &[super::TAG_FROZEN_PUSH])))
		.expect_err("corrupt ID cannot disappear as an EDU");
}
