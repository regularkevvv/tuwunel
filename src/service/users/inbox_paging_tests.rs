//! Native store controls for bounded inbox cursors and atomic ACK garbage
//! collection.
use super::*;
use crate::users::device_removal_tests::fixture;

async fn plant(
	services: &crate::Services,
	user: &UserId,
	device: &DeviceId,
	value: &[u8],
) -> Result<u64> {
	let count = services.globals.next_count().await?;
	services.db["todeviceid_events"]
		.put_raw((user, device, *count), value)
		.await?;
	Ok(*count)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_pages_drain_every_row_and_replay_without_ack() -> Result {
	let services = fixture().await?;
	let user = ruma::user_id!("@pages:localhost");
	let device: &DeviceId = "PAGES".into();
	let mut expected = Vec::new();
	for index in 0..300 {
		expected.push(
			plant(&services, user, device, &serde_json::to_vec(&json!({"n":index}))?).await?,
		);
	}
	let until = services.globals.wait_pending().await?;
	let first = services
		.users
		.to_device_page(user, device, 0, until, MAX_TO_DEVICE_PAGE_ROWS)
		.await?;
	assert_eq!(first.events.len(), MAX_TO_DEVICE_PAGE_ROWS, "bounded first page");
	assert!(!first.complete, "backlog is not claimed complete");
	assert_eq!(first.next_batch, expected[127], "cursor ends at retained event");
	let retry = services
		.users
		.to_device_page(user, device, 0, until, MAX_TO_DEVICE_PAGE_ROWS)
		.await?;
	assert_eq!(retry.next_batch, first.next_batch, "unacknowledged retry replays cursor");
	assert_eq!(
		serde_json::to_vec(&retry.events)?,
		serde_json::to_vec(&first.events)?,
		"retry replays identical events"
	);
	let mut received = first
		.events
		.into_iter()
		.map(|(count, _)| count)
		.collect::<Vec<_>>();
	let mut since = first.next_batch;
	loop {
		let page = services
			.users
			.to_device_page(user, device, since, until, MAX_TO_DEVICE_PAGE_ROWS)
			.await?;
		received.extend(page.events.into_iter().map(|(count, _)| count));
		since = page.next_batch;
		if page.complete {
			break;
		}
	}
	assert_eq!(received, expected, "every row appears exactly once while advancing");
	assert_eq!(since, until, "completed page reaches captured frontier");
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn page_bytes_admit_legal_maximum_value_and_leave_peek_for_next_page() -> Result {
	let services = fixture().await?;
	let user = ruma::user_id!("@bytes:localhost");
	let device: &DeviceId = "BYTES".into();
	let empty =
		serde_json::to_vec(&json!({"type":"m.test", "sender":user, "content":{"pad":""}}))?;
	let value = serde_json::to_vec(&json!({
		"type":"m.test", "sender":user,
		"content":{"pad":"x".repeat(tuwunel_bridge::MAX_VALUE_BYTES.saturating_sub(empty.len()))},
	}))?;
	assert_eq!(
		value.len(),
		tuwunel_bridge::MAX_VALUE_BYTES,
		"valid event uses legal maximum row width"
	);
	let mut counts = Vec::new();
	for _ in 0..3 {
		counts.push(plant(&services, user, device, &value).await?);
	}
	let until = services.globals.wait_pending().await?;
	let page = services
		.users
		.to_device_page(user, device, 0, until, MAX_TO_DEVICE_PAGE_ROWS)
		.await?;
	assert!(!page.events.is_empty(), "legal MAX_VALUE singleton is admitted");
	assert!(page.events.len() < counts.len(), "aggregate byte ceiling truncates page");
	assert!(!page.complete, "peek is not omitted as a completed range");
	assert_eq!(
		page.next_batch,
		counts[page.events.len().saturating_sub(1)],
		"cursor stays before peek"
	);
	let next = services
		.users
		.to_device_page(user, device, page.next_batch, until, MAX_TO_DEVICE_PAGE_ROWS)
		.await?;
	assert_eq!(
		page.events
			.len()
			.saturating_add(next.events.len()),
		3,
		"byte-limited pages retain every legal row"
	);
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_page_refuses_before_any_ack_and_repaired_retry_delivers() -> Result {
	let services = fixture().await?;
	let user = ruma::user_id!("@corrupt:localhost");
	let device: &DeviceId = "CORRUPT".into();
	let claimed = plant(&services, user, device, b"{}").await?;
	let damaged = plant(&services, user, device, b"{invalid").await?;
	let prefix = serialize_key((user, device, Interfix))?;
	let before = services.db["todeviceid_events"]
		.raw_rows_prefix_after(&prefix, None, 10)
		.await?;
	services
		.users
		.to_device_page(user, device, claimed, damaged, MAX_TO_DEVICE_PAGE_ROWS)
		.await
		.expect_err("corrupt JSON refuses cursor response");
	assert_eq!(
		services.db["todeviceid_events"]
			.raw_rows_prefix_after(&prefix, None, 10)
			.await?,
		before,
		"read refusal leaves previous claim untouched"
	);
	services.db["todeviceid_events"]
		.put_raw((user, device, damaged), b"{}")
		.await?;
	let repaired = services
		.users
		.to_device_page(user, device, claimed, damaged, MAX_TO_DEVICE_PAGE_ROWS)
		.await?;
	assert_eq!(repaired.events.len(), 1, "same cursor retries repaired pending row");
	let mut bad = prefix.to_vec();
	bad.push(0xFE);
	services.db["todeviceid_events"]
		.insert(&bad, b"{}")
		.await?;
	services
		.users
		.to_device_page(user, device, claimed, u64::MAX, MAX_TO_DEVICE_PAGE_ROWS)
		.await
		.expect_err("malformed key cannot be dropped from page");
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ack_gc_is_bounded_atomic_and_never_deletes_unclaimed_events() -> Result {
	let services = fixture().await?;
	let user = ruma::user_id!("@ack:localhost");
	let device: &DeviceId = "ACK".into();
	let mut counts = Vec::new();
	for _ in 0..300 {
		counts.push(plant(&services, user, device, b"{}").await?);
	}
	services
		.users
		.remove_to_device_events(user, device, counts[255])
		.await?;
	let prefix = serialize_key((user, device, Interfix))?;
	let rows = services.db["todeviceid_events"]
		.raw_rows_prefix_after(&prefix, None, 400)
		.await?;
	assert_eq!(rows.len(), 172, "one ACK GC commit contains at most 128 rows");
	let pending = services
		.users
		.to_device_page(user, device, counts[255], counts[299], MAX_TO_DEVICE_PAGE_ROWS)
		.await?;
	assert_eq!(pending.events.len(), 44, "all unclaimed events survive bounded GC");
	services
		.users
		.remove_to_device_events(user, device, counts[255])
		.await?;
	assert_eq!(
		services.db["todeviceid_events"]
			.raw_rows_prefix_after(&prefix, None, 400)
			.await?
			.len(),
		44,
		"later GC pass drains acknowledged backlog"
	);
	let mut corrupt = prefix.to_vec();
	corrupt.push(0);
	services.db["todeviceid_events"]
		.insert(&corrupt, b"{}")
		.await?;
	let before = services.db["todeviceid_events"]
		.raw_rows_prefix_after(&prefix, None, 400)
		.await?;
	services
		.users
		.remove_to_device_events(user, device, counts[299])
		.await
		.expect_err("every selected ACK key is checked before the first deletion");
	assert_eq!(
		services.db["todeviceid_events"]
			.raw_rows_prefix_after(&prefix, None, 400)
			.await?,
		before,
		"corrupt ACK inventory changes no row"
	);
	services.stop().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_borrowed_peek_refuses_before_issuing_page_cursor() -> Result {
	let services = fixture().await?;
	let user = ruma::user_id!("@peek:localhost");
	let device: &DeviceId = "PEEK".into();
	for _ in 0..MAX_TO_DEVICE_PAGE_ROWS {
		plant(&services, user, device, b"{}").await?;
	}
	let damaged = plant(&services, user, device, b"{invalid").await?;
	services
		.users
		.to_device_page(user, device, 0, damaged, MAX_TO_DEVICE_PAGE_ROWS)
		.await
		.expect_err("corrupt lookahead is propagated without cloning or advancing past it");
	services.db["todeviceid_events"]
		.put_raw((user, device, damaged), b"{}")
		.await?;
	let retry = services
		.users
		.to_device_page(user, device, 0, damaged, MAX_TO_DEVICE_PAGE_ROWS)
		.await?;
	assert_eq!(
		retry.events.len(),
		MAX_TO_DEVICE_PAGE_ROWS,
		"repaired same cursor returns first page"
	);
	assert!(!retry.complete, "valid lookahead remains for next page");
	services.stop().await;
	Ok(())
}
