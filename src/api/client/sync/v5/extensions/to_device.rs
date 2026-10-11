use ruma::api::client::sync::sync_events::v5::response;
use tuwunel_core::{Result, at};

use super::{Connection, SyncInfo};

#[tracing::instrument(name = "to_device", level = "trace", skip_all, ret)]
pub(super) async fn collect(
	SyncInfo { services, sender_user, sender_device, .. }: SyncInfo<'_>,
	conn: &Connection,
) -> Result<Option<response::ToDevice>> {
	let Some(sender_device) = sender_device else {
		return Ok(None);
	};

	// Per MSC3885, the to-device extension carries its own opaque `since` token
	// (independent of the global `pos`). matrix-rust-sdk persists it separately
	// from `pos`, so trust the request's claim when present and fall back to
	// `globalsince` only when the client did not provide one.
	let since = since(conn);
	let page = services
		.users
		.to_device_page(
			sender_user,
			sender_device,
			since,
			conn.next_batch,
			tuwunel_service::users::device::MAX_TO_DEVICE_PAGE_ROWS,
		)
		.await?;
	let to_device =
		(!page.events.is_empty() || page.next_batch > since).then(|| response::ToDevice {
			next_batch: page.next_batch.to_string().into(),
			events: page.events.into_iter().map(at!(1)).collect(),
		});

	Ok(to_device)
}

/// Runs after every response source read succeeded, never during collection.
pub(crate) async fn acknowledge(
	services: &tuwunel_service::Services,
	sender_user: &ruma::UserId,
	sender_device: Option<&ruma::DeviceId>,
	conn: &Connection,
) -> Result {
	if conn.extensions.to_device.enabled.unwrap_or(false)
		&& let Some(device) = sender_device
	{
		services
			.users
			.remove_to_device_events(sender_user, device, since(conn))
			.await?;
	}
	Ok(())
}

fn since(conn: &Connection) -> u64 {
	conn.extensions
		.to_device
		.since
		.as_deref()
		.and_then(|s| s.parse::<u64>().ok())
		.unwrap_or(conn.globalsince)
		.min(conn.next_batch)
}

/// An independent inbox cursor can trail even when the global pos caught up.
#[must_use]
pub(crate) fn needs_collection(conn: &Connection) -> bool {
	conn.extensions.to_device.enabled.unwrap_or(false) && since(conn) < conn.next_batch
}
