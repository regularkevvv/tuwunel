use std::sync::atomic::{AtomicUsize, Ordering};

use axum::extract::State;
use futures::{FutureExt, Stream, StreamExt, pin_mut};
use ruma::{
	UserId,
	api::client::user_directory::search_users::{self},
	events::room::join_rules::JoinRule,
};
use tuwunel_core::{
	Result,
	utils::{
		BoolExt, FutureBoolExt,
		stream::{BroadbandExt, ReadyExt},
	},
};
use tuwunel_service::Services;

use crate::Ruma;

// Tuwunel can handle a lot more results than synapse
const LIMIT_MAX: usize = 500;
const LIMIT_DEFAULT: usize = 10;

/// Most accounts one search examines.
///
/// Accounts are read in id order, and a search stops after this many whether
/// or not it has found `limit` matches, so a request reads a bounded number
/// of rows however many accounts exist. `limited` then tells the client its
/// results may be incomplete, which is what the field is for.
pub(crate) const EXAMINED_MAX: usize = 1_000;

/// # `POST /_matrix/client/r0/user_directory/search`
///
/// Searches all known users for a match.
///
/// - Hides any local users that aren't in any public rooms (i.e. those that
///   have the join rule set to public) and don't share a room with the sender
/// - Hides appservice senders and users in exclusive appservice user namespaces
pub(crate) async fn search_users_route(
	State(services): State<crate::State>,
	body: Ruma<search_users::v3::Request>,
) -> Result<search_users::v3::Response> {
	let sender_user = body.sender_user();
	let limit = usize::try_from(body.limit)
		.unwrap_or(LIMIT_DEFAULT)
		.min(LIMIT_MAX);

	let search_term = body.search_term.to_lowercase();
	let examined = AtomicUsize::new(0);
	let users = examine_at_most(services.users.stream(), EXAMINED_MAX, &examined)
		.ready_filter(|&user_id| user_id != sender_user)
		.map(ToOwned::to_owned)
		.broad_filter_map(async |user_id| {
			let display_name = services.profile.displayname(&user_id).await.ok();

			should_show_user(
				&services,
				sender_user,
				&user_id,
				display_name.as_deref(),
				&search_term,
			)
			.await
			.then_async(async || search_users::v3::User {
				user_id: user_id.clone(),
				display_name,
				avatar_url: services.profile.avatar_url(&user_id).await.ok(),
			})
			.await
		});

	pin_mut!(users);
	let results = users.by_ref().take(limit).collect().await;
	let limited =
		users.next().await.is_some() || examined.load(Ordering::Relaxed) >= EXAMINED_MAX;

	Ok(search_users::v3::Response { results, limited })
}

/// `candidates`, cut off after `max`, counting into `examined` every one
/// read: the bound on the rows one directory request reads.
pub(crate) fn examine_at_most<'a, S>(
	candidates: S,
	max: usize,
	examined: &'a AtomicUsize,
) -> impl Stream<Item = S::Item> + Send + 'a
where
	S: Stream + Send + 'a,
	S::Item: Send,
{
	candidates.take(max).inspect(move |_| {
		examined.fetch_add(1, Ordering::Relaxed);
	})
}

async fn should_show_user(
	services: &Services,
	sender_user: &UserId,
	target_user: &UserId,
	target_display_name: Option<&str>,
	search_term: &str,
) -> bool {
	let user_id_matches = target_user
		.as_str()
		.to_lowercase()
		.contains(search_term);

	let display_name_matches = target_display_name
		.map(str::to_lowercase)
		.is_some_and(|display_name| display_name.contains(search_term));

	if !user_id_matches && !display_name_matches {
		return false;
	}

	if services
		.appservice
		.is_exclusive_user_id(target_user)
		.await
	{
		return false;
	}

	if services
		.server
		.config
		.show_all_local_users_in_user_directory
	{
		return true;
	}

	let user_in_public_room = services
		.state_cache
		.rooms_joined(target_user)
		.map(ToOwned::to_owned)
		.broad_any(async |room_id| {
			services
				.state_accessor
				.get_join_rules(&room_id)
				.map(|rule| matches!(rule, JoinRule::Public))
				.await
		});

	let user_sees_user = services
		.state_cache
		.user_sees_user(sender_user, target_user);

	pin_mut!(user_in_public_room, user_sees_user);
	user_in_public_room.or(user_sees_user).await
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicUsize, Ordering};

	use futures::{FutureExt, StreamExt, future::ready, stream};

	use super::examine_at_most;

	#[test]
	fn a_search_reads_no_more_candidates_than_its_cap() {
		let pulled = AtomicUsize::new(0);
		let examined = AtomicUsize::new(0);
		let source = stream::iter(0_u32..10_000).inspect(|_| {
			pulled.fetch_add(1, Ordering::Relaxed);
		});

		// A search matching almost nothing still stops at the cap. The source
		// is always ready, so the search completes on its first poll.
		let hits: Vec<u32> = examine_at_most(source, 1_000, &examined)
			.filter(|n| ready(n % 997 == 5))
			.collect()
			.now_or_never()
			.expect("a ready stream completes at once");

		assert_eq!(pulled.load(Ordering::Relaxed), 1_000, "read past the cap");
		assert_eq!(examined.load(Ordering::Relaxed), 1_000);
		assert_eq!(hits, vec![5], "a match past the cap was examined");
	}

	#[test]
	fn a_search_over_fewer_candidates_than_its_cap_reads_them_all() {
		let examined = AtomicUsize::new(0);
		let all: Vec<u32> = examine_at_most(stream::iter(0_u32..10), 1_000, &examined)
			.collect()
			.now_or_never()
			.expect("a ready stream completes at once");

		assert_eq!(all.len(), 10);
		assert_eq!(examined.load(Ordering::Relaxed), 10);
	}
}
