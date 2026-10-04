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
	Ok(())
}
