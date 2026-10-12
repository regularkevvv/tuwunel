#![cfg(test)]

use std::{env::var, fs::remove_dir_all, path::PathBuf, process::id as process_id};

use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_admin::{fini, init};
use tuwunel_core::{
	Result, http,
	ruma::{OwnedUserId, UserId},
};
use tuwunel_service::Services;

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn local_user_pages_preserve_rows_cursors_errors_and_output_budgets() -> Result {
	let root = var("TMPDIR").unwrap_or_else(|_| "/tmp".into());
	let db_path = DatabasePath(
		PathBuf::from(root).join(format!("tuwunel-local-user-pages-{}", process_id())),
	);
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.push(format!("database_path={:?}", db_path.0));
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		init(&services.admin);
		let outcome = exercise(&services).await;
		fini(&services.admin);
		let shutdown = server.server.shutdown();
		drop(services);
		let run = async_run(&server).await;
		let stop = async_stop(&server).await;
		outcome.and(shutdown).and(run).and(stop)
	});
	drop(runtime);
	result
}

async fn exercise(services: &Services) -> Result {
	let map = &services.db["userid_password"];
	map.clear().await?;
	for index in 0..70 {
		let password = if index % 2 == 0 { "*" } else { "" };
		map.insert(&format!("@page-{index:03}:localhost"), password)
			.await?;
	}
	let mut after: Option<OwnedUserId> = None;
	let mut seen = Vec::new();
	let mut pages = 0_usize;
	loop {
		let page = services
			.users
			.local_user_page(after.as_deref(), 16)
			.await?;
		assert!(page.examined <= 17);
		seen.extend(
			page.users
				.into_iter()
				.map(|user| user.to_string()),
		);
		pages = pages.saturating_add(1);
		let Some(next) = page.next else {
			break;
		};
		if let Some(previous) = after {
			assert!(next.as_str() > previous.as_str());
		}
		after = Some(next);
	}
	assert_eq!(pages, 5);
	assert_eq!(
		seen,
		(0..70)
			.filter(|index| index % 2 == 0)
			.map(|index| format!("@page-{index:03}:localhost"))
			.collect::<Vec<_>>()
	);
	for limit in [0, 33, usize::MAX] {
		assert_eq!(
			services
				.users
				.local_user_page(None, limit)
				.await
				.expect_err("invalid page limit")
				.status_code(),
			http::StatusCode::BAD_REQUEST
		);
	}
	map.insert(b"\x01invalid", "*").await?;
	services
		.users
		.local_user_page(None, 16)
		.await
		.expect_err("invalid row cannot disappear from a page");
	map.remove(b"\x01invalid").await?;
	map.insert(&[0xFF], "*").await?;
	let after = UserId::parse("@page-068:localhost").expect("valid cursor");
	services
		.users
		.local_user_page(Some(&after), 1)
		.await
		.expect_err("a malformed lookahead cannot hide the next page");
	map.remove(&[0xFF]).await?;
	let after = UserId::parse("@page-069:localhost").expect("final cursor");
	let exhausted = services
		.users
		.local_user_page(Some(&after), 16)
		.await?;
	assert!(exhausted.users.is_empty());
	assert_eq!(exhausted.examined, 0);
	assert!(exhausted.next.is_none());
	disabled_and_reply_budgets(services).await?;
	query_inventory_pages(services).await
}

async fn disabled_and_reply_budgets(services: &Services) -> Result {
	let map = &services.db["userid_password"];
	map.clear().await?;
	for index in 0..40 {
		map.insert(&format!("@disabled-{index:03}:localhost"), "")
			.await?;
	}
	let first = services.users.local_user_page(None, 16).await?;
	assert!(first.users.is_empty());
	assert_eq!(first.examined, 17);
	assert_eq!(
		first
			.next
			.as_ref()
			.expect("disabled row cursor")
			.as_str(),
		"@disabled-015:localhost"
	);
	let second = services
		.users
		.local_user_page(first.next.as_deref(), 16)
		.await?;
	assert!(second.users.is_empty());
	assert_eq!(
		second
			.next
			.as_ref()
			.expect("second disabled cursor")
			.as_str(),
		"@disabled-031:localhost"
	);
	let third = services
		.users
		.local_user_page(second.next.as_deref(), 16)
		.await?;
	assert!(third.users.is_empty());
	assert_eq!(third.examined, 8);
	assert!(third.next.is_none());
	let Ok(Some(output)) = services
		.admin
		.command_in_place("users list-users --limit 16".into(), None)
		.await
	else {
		panic!("admin page must succeed and produce output");
	};
	assert!(
		output
			.as_str()
			.contains("Page contains 0 local user account(s); examined 17 inventory rows")
	);
	assert!(
		output
			.as_str()
			.contains("!admin users list-users --after @disabled-015:localhost --limit 16")
	);
	assert!(
		!output
			.as_str()
			.contains("@disabled-016:localhost"),
		"lookahead cannot leak into output"
	);

	map.clear().await?;
	for index in 0..32 {
		let user = UserId::parse(format!("@long-{index:02}{}:localhost", "x".repeat(210)))
			.expect("valid long user id");
		map.insert(&user, "*").await?;
	}
	assert_eq!(
		services
			.users
			.local_user_page(None, 32)
			.await
			.expect_err("reply budget must refuse an oversized page")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	assert_eq!(
		services
			.users
			.user_inventory_page(None, 32, false)
			.await
			.expect_err("all-user pages share the reply budget")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	Ok(())
}

async fn query_inventory_pages(services: &Services) -> Result {
	let map = &services.db["userid_password"];
	map.clear().await?;
	for index in 0..70 {
		map.insert(&format!("@page-{index:03}:localhost"), if index % 2 == 0 { "*" } else { "" })
			.await?;
	}
	let historical = UserId::parse("@zz%legacy:localhost").expect("valid historical user");
	assert!(historical.is_historical());
	map.insert(&historical, "").await?;
	let all = services
		.users
		.user_inventory_page(None, 16, false)
		.await?;
	assert_eq!(all.users.len(), 16);
	assert!(
		all.users
			.iter()
			.any(|user| user.as_str() == "@page-001:localhost"),
		"inactive users belong to the registered-user inventory"
	);
	let first = services
		.users
		.user_inventory_page(None, 16, true)
		.await?;
	assert!(first.users.is_empty());
	assert_eq!(first.examined, 17);
	assert_eq!(
		first
			.next
			.as_ref()
			.expect("filtered page cursor")
			.as_str(),
		"@page-015:localhost"
	);
	let mut after: Option<OwnedUserId> = None;
	let mut found = Vec::new();
	let mut pages = 0_usize;
	loop {
		let page = services
			.users
			.user_inventory_page(after.as_deref(), 16, true)
			.await?;
		assert!(page.examined <= 17);
		found.extend(page.users);
		pages = pages.saturating_add(1);
		let Some(next) = page.next else {
			break;
		};
		if let Some(previous) = after {
			assert!(next.as_str() > previous.as_str());
		}
		after = Some(next);
	}
	assert_eq!(pages, 5);
	assert_eq!(found, vec![historical]);
	let Ok(Some(output)) = services
		.admin
		.command_in_place("query users iter-users --historical --limit 16".into(), None)
		.await
	else {
		panic!("historical inventory command must succeed");
	};
	assert!(
		output
			.as_str()
			.contains("Page contains 0 user inventory row(s); examined 17 rows")
	);
	assert!(output.as_str().contains(
		"!admin query users iter-users --after @page-015:localhost --limit 16 --historical"
	));
	assert!(
		!output.as_str().contains("zz%legacy"),
		"the first filtered page cannot scan ahead to a match"
	);
	map.insert(b"\x01invalid", "*").await?;
	services
		.users
		.user_inventory_page(None, 16, true)
		.await
		.expect_err("filtered pages cannot discard malformed inventory keys");
	let Err(output) = services
		.admin
		.command_in_place("query users iter-users --historical".into(), None)
		.await
	else {
		panic!("inventory command must fail on malformed storage");
	};
	assert!(!output.as_str().contains("Page contains"));
	Ok(())
}
