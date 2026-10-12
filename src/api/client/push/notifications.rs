use axum::extract::State;
use ruma::{MilliSecondsSinceUnixEpoch, api::client::push::get_notifications, push::Action};
use tuwunel_core::{
	Error, Result, err,
	matrix::{Event, PduId},
	utils::string::to_small_string,
};

use crate::Ruma;

/// Paginate through notification events. Storage failures refuse the page;
/// erased/redacted and filtered rows still advance its exclusive cursor.
pub(crate) async fn get_notifications_route(
	State(services): State<crate::State>,
	body: Ruma<get_notifications::v3::Request>,
) -> Result<get_notifications::v3::Response> {
	use get_notifications::v3::Notification;
	let sender_user = body.sender_user();
	let mut from = body
		.body
		.from
		.as_deref()
		.map(str::parse)
		.transpose()
		.map_err(|e| err!(Request(InvalidParam("Invalid `from' parameter: {e}"))))?;
	let limit: usize = body
		.body
		.limit
		.map(TryInto::try_into)
		.transpose()?
		.unwrap_or(50)
		.clamp(1, 100);
	let only_highlight = body
		.body
		.only
		.as_deref()
		.is_some_and(|only| only.contains("highlight"));
	let mut notifications = Vec::with_capacity(limit);
	let mut examined = 0_usize;
	let mut bytes = 0_usize;
	let mut next_token = None;
	// Bound work even when every row is filtered. Return progress so the
	// client can continue rather than repeatedly scanning the same rows.
	while examined < 512 && notifications.len() < limit {
		let page_limit = 64
			.min(limit.saturating_sub(notifications.len()))
			.min(512_usize.saturating_sub(examined));
		let page = services
			.pusher
			.get_notifications(sender_user, from, page_limit)
			.await?;
		if page.is_empty() {
			next_token = None;
			break;
		}
		let short_page = page.len() < page_limit;
		for (count, notify, row_bytes) in page {
			bytes = bytes.saturating_add(row_bytes);
			if bytes > 4 * 1024 * 1024 {
				use ruma::api::error::{ErrorKind, LimitExceededErrorData};
				return Err(Error::Request(
					ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
					"Notification page byte limit exceeded".into(),
					http::StatusCode::TOO_MANY_REQUESTS,
				));
			}
			examined = examined.saturating_add(1);
			from = Some(count);
			next_token = Some(count);
			if !only_highlight || notify.actions.iter().any(Action::is_highlight) {
				let id = PduId {
					shortroomid: notify.sroomid,
					count: count.into(),
				};
				let event = match services
					.timeline
					.get_pdu_from_id(&id.into())
					.await
				{
					| Ok(event) => Some(event),
					| Err(error) if error.is_not_found() => None,
					| Err(error) => return Err(error),
				};
				if let Some(event) = event.filter(|event| !event.is_redacted()) {
					if services
						.short
						.get_shortroomid(event.room_id())
						.await? != notify.sroomid
					{
						return Err(Error::bad_database("Notification source room mismatch"));
					}
					let read = services
						.pusher
						.notification_is_read(sender_user, &event, count)
						.await?;
					notifications.push(Notification {
						room_id: event.room_id().into(),
						event: event.into_format(),
						ts: MilliSecondsSinceUnixEpoch(notify.ts.try_into().map_err(|_| {
							Error::bad_database("Invalid notification timestamp")
						})?),
						read,
						profile_tag: notify.tag,
						actions: notify.actions,
					});
				}
			}
			if notifications.len() == limit {
				break;
			}
		}
		if short_page {
			break;
		}
	}
	Ok(get_notifications::v3::Response {
		next_token: next_token.map(to_small_string),
		notifications,
	})
}
