use axum::extract::State;
use ruma::{MilliSecondsSinceUnixEpoch, UInt, UserId, api::Direction};
use synapse_admin_api::users::list_users::{
	v2::{self, UserMinorDetails},
	v3,
};
use tuwunel_core::{
	Error, Result,
	utils::math::{ruma_from_usize, usize_from_ruma},
};

use crate::{Ruma, client::admin::require_admin};

const MAX_DEVICE_ROWS: usize = 4096;
const MAX_DEVICE_BYTES: usize = 256 * 1024;
const MAX_DETAILS_BYTES: usize = 256 * 1024;

struct InventoryBudget {
	device_rows: usize,
	device_bytes: usize,
	detail_bytes: usize,
}

impl Default for InventoryBudget {
	fn default() -> Self {
		Self {
			device_rows: MAX_DEVICE_ROWS,
			device_bytes: MAX_DEVICE_BYTES,
			detail_bytes: MAX_DETAILS_BYTES,
		}
	}
}

/// The `deactivated` query filter, whose semantics differ between v2 and v3.
#[derive(Clone, Copy)]
enum DeactivatedFilter {
	/// Include both active and deactivated users (v3 absent).
	Any,

	/// Only deactivated users (v3 `true`).
	Only,

	/// Exclude deactivated users (v2 absent/false, v3 `false`).
	Exclude,

	/// Include deactivated users (v2 `true`).
	Include,
}

/// Filter and pagination parameters shared by the v2 and v3 list endpoints.
struct ListParams<'a> {
	from: usize,
	limit: usize,
	name: Option<&'a str>,
	user_id: Option<&'a str>,
	admins: Option<bool>,
	locked: bool,
	deactivated: DeactivatedFilter,
	dir: Direction,
}

/// # `GET /_synapse/admin/v2/users`
pub(crate) async fn admin_list_users_v2_route(
	State(services): State<crate::State>,
	body: Ruma<v2::Request>,
) -> Result<v2::Response> {
	require_admin(&services, body.sender_user()).await?;

	let deactivated = match body.deactivated {
		| true => DeactivatedFilter::Include,
		| false => DeactivatedFilter::Exclude,
	};

	let params = ListParams {
		from: usize_from_ruma(body.from),
		limit: body.limit.map_or(100, usize_from_ruma),
		name: body.name.as_deref(),
		user_id: body.user_id.as_deref(),
		admins: body.admins,
		locked: body.locked,
		deactivated,
		dir: body.dir.unwrap_or(Direction::Forward),
	};

	let (users, next_token, total) = list_users(services, &params).await?;

	Ok(v2::Response { users, next_token, total })
}

/// # `GET /_synapse/admin/v3/users`
pub(crate) async fn admin_list_users_v3_route(
	State(services): State<crate::State>,
	body: Ruma<v3::Request>,
) -> Result<v3::Response> {
	require_admin(&services, body.sender_user()).await?;

	let deactivated = match body.deactivated {
		| None => DeactivatedFilter::Any,
		| Some(true) => DeactivatedFilter::Only,
		| Some(false) => DeactivatedFilter::Exclude,
	};

	let params = ListParams {
		from: usize_from_ruma(body.from),
		limit: body.limit.map_or(100, usize_from_ruma),
		name: body.name.as_deref(),
		user_id: body.user_id.as_deref(),
		admins: body.admins,
		locked: body.locked,
		deactivated,
		dir: body.dir.unwrap_or(Direction::Forward),
	};

	let (users, next_token, total) = list_users(services, &params).await?;

	Ok(v3::Response { users, next_token, total })
}

/// Returns the filtered, name-ordered and paginated user page, the `next_token`
/// (present only while the page does not reach the end of the filtered set) and
/// the filtered total. `order_by` beyond `name` is not backed by stored fields,
/// so the name ordering (reversed for `dir=b`) is the only one applied.
async fn list_users(
	services: crate::State,
	params: &ListParams<'_>,
) -> Result<(Vec<UserMinorDetails>, Option<String>, UInt)> {
	// An exact filtered total requires a complete source inventory. Refuse
	// larger sources rather than paging a silently shortened list.
	let mut names = services.users.bounded_registered_users().await?;

	names.sort_unstable();

	if matches!(params.dir, Direction::Backward) {
		names.reverse();
	}

	let mut matched = Vec::new();
	let mut budget = InventoryBudget::default();
	for user_id in &names {
		if let Some(details) = user_minor_details(services, user_id, params, &mut budget).await? {
			let encoded_bytes = serde_json::to_vec(&details)?.len();
			budget.detail_bytes = budget
				.detail_bytes
				.checked_sub(encoded_bytes)
				.ok_or_else(inventory_limit)?;
			matched.push(details);
		}
	}

	let matched_count = matched.len();
	let total = ruma_from_usize(matched_count);

	let page: Vec<UserMinorDetails> = matched
		.into_iter()
		.skip(params.from)
		.take(params.limit)
		.collect();

	let end = params.from.saturating_add(page.len());
	let next_token = (end < matched_count).then(|| end.to_string());

	Ok((page, next_token, total))
}

/// Applies the substring, admin, locked and deactivated filters to one user and
/// builds its `UserMinorDetails`, or returns `None` when the user is filtered
/// out.
async fn user_minor_details(
	services: crate::State,
	user_id: &UserId,
	params: &ListParams<'_>,
	budget: &mut InventoryBudget,
) -> Result<Option<UserMinorDetails>> {
	let name = user_id.as_str();

	let displayname = optional_field(services.profile.displayname(user_id).await)?;

	if let Some(needle) = params.user_id.filter(|_| params.name.is_none())
		&& !name.contains(needle)
	{
		return Ok(None);
	}

	if let Some(needle) = params.name {
		let in_localpart = user_id.localpart().contains(needle);
		let in_displayname = displayname
			.as_deref()
			.is_some_and(|display| display.contains(needle));

		if !in_localpart && !in_displayname {
			return Ok(None);
		}
	}

	let admin = services.admin.user_is_admin(user_id).await;
	if let Some(want_admin) = params.admins
		&& want_admin != admin
	{
		return Ok(None);
	}

	let locked = services.users.is_locked(user_id).await;
	if locked && !params.locked {
		return Ok(None);
	}

	let deactivated = services.users.is_deactivated(user_id).await?;

	let keep = match params.deactivated {
		| DeactivatedFilter::Any | DeactivatedFilter::Include => true,
		| DeactivatedFilter::Only => deactivated,
		| DeactivatedFilter::Exclude => !deactivated,
	};

	if !keep {
		return Ok(None);
	}

	let avatar_url =
		optional_field(services.profile.avatar_url(user_id).await)?.map(|url| url.to_string());

	let erased = services.users.is_erased(user_id).await;

	let devices = services
		.users
		.bounded_devices_metadata(user_id, budget.device_rows, budget.device_bytes)
		.await?;
	budget.device_rows = budget
		.device_rows
		.saturating_sub(devices.examined);
	budget.device_bytes = budget
		.device_bytes
		.saturating_sub(devices.encoded_bytes);
	let last_seen_ts = devices
		.devices
		.into_iter()
		.filter_map(|device| device.last_seen_ts)
		.max();

	Ok(Some(UserMinorDetails {
		displayname,
		avatar_url,
		admin,
		deactivated,
		locked,
		erased,
		last_seen_ts,
		// tuwunel has no creation timestamp; emit a 0 sentinel (strict clients reject null).
		creation_ts: Some(MilliSecondsSinceUnixEpoch(UInt::from(0_u32))),
		..UserMinorDetails::new(name.to_owned())
	}))
}

fn optional_field<T>(field: Result<T>) -> Result<Option<T>> {
	match field {
		| Ok(value) => Ok(Some(value)),
		| Err(error) if error.is_not_found() => Ok(None),
		| Err(error) => Err(error),
	}
}

fn inventory_limit() -> Error {
	Error::Request(
		ruma::api::error::ErrorKind::LimitExceeded(ruma::api::error::LimitExceededErrorData {
			retry_after: None,
		}),
		"User-list inventory limit reached".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}
