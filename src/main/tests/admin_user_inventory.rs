#![cfg(test)]

use std::{
	env::temp_dir, fs::remove_dir_all, net::TcpListener, path::PathBuf,
	process::id as process_id, time::Duration,
};

use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err, http,
	ruma::{OwnedUserId, UserId},
};
use tuwunel_database::Json;
use tuwunel_service::{Services, profile::Propagation, users::MAX_LOCAL_USER_COUNT_ROWS};

const TOKEN: &str = "disposable-admin-user-inventory-token";

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

struct Endpoint<'a> {
	services: &'a Services,
	base: &'a str,
}

#[test]
fn admin_user_pages_preserve_totals_or_refuse_incomplete_inventories() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let db_path =
		DatabasePath(temp_dir().join(format!("tuwunel-admin-user-inventory-{}", process_id())));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={:?}", db_path.0),
		"address=[\"127.0.0.1\"]".into(),
		format!("port={port}"),
		"listening=true".into(),
		"log=\"warn\"".into(),
	]);
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);
		let exercise = async {
			let outcome = exercise(&Endpoint { services: &services, base: &base }).await;
			let shutdown = server.server.shutdown();
			outcome.and(shutdown)
		};
		let (run, outcome) = tokio::join!(async_run(&server), exercise);
		drop(services);
		let stop = async_stop(&server).await;
		outcome.and(run).and(stop)
	});
	drop(runtime);
	result
}

impl Endpoint<'_> {
	async fn request(&self, version: u8, query: &str) -> Result<(http::StatusCode, Value)> {
		let response = self
			.services
			.client
			.clients
			.default
			.get(format!("{}/_synapse/admin/v{version}/users?{query}", self.base))
			.bearer_auth(TOKEN)
			.send()
			.await?;
		let status = response.status();
		Ok((status, response.json().await?))
	}

	async fn page(&self, version: u8, query: &str, total: usize) -> Result<Value> {
		let (status, body) = self.request(version, query).await?;
		assert_eq!(status, http::StatusCode::OK, "{body}");
		assert_eq!(body["total"], total);
		assert!(body["users"].is_array());
		Ok(body)
	}

	async fn refused(&self, version: u8, query: &str, status: http::StatusCode) -> Result {
		let (actual, body) = self.request(version, query).await?;
		assert_eq!(actual, status, "{body}");
		assert!(body.get("users").is_none(), "failure must not return a partial page");
		assert!(body.get("total").is_none(), "failure must not return a short total");
		if status == http::StatusCode::TOO_MANY_REQUESTS {
			assert_eq!(body["errcode"], "M_LIMIT_EXCEEDED");
		}
		Ok(())
	}
}

async fn exercise(endpoint: &Endpoint<'_>) -> Result {
	let services = endpoint.services;
	timeout(Duration::from_secs(10), async {
		loop {
			let ready = services
				.client
				.clients
				.default
				.get(format!("{}/_matrix/client/versions", endpoint.base))
				.send()
				.await
				.is_ok_and(|response| response.status().is_success());
			if ready {
				break;
			}
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| err!("disposable admin API listener did not become ready"))?;
	let admin = &services.globals.server_user;
	let users = &services.db["userid_password"];
	users.clear().await?;
	users.insert(admin, "*").await?;
	services
		.users
		.create_device(admin, None, (Some(TOKEN), None), None, None, None)
		.await?;
	let alice = UserId::parse("@test-alice:localhost").expect("valid user");
	let bob = UserId::parse("@test-bob:localhost").expect("valid user");
	let carol = UserId::parse("@test-carol:localhost").expect("valid user");
	let locked = UserId::parse("@test-locked:localhost").expect("valid user");
	for user in [&alice, &carol, &locked] {
		users.insert(user, "*").await?;
	}
	users.insert(&bob, "").await?;
	services.db["userid_locked"]
		.insert(&locked, "")
		.await?;
	services.db["userid_erased"]
		.insert(&alice, "")
		.await?;
	services
		.profile
		.set_displayname(&carol, Some("Visible Carol"), Some(Propagation::None))
		.await?;
	services.db["userdeviceid_metadata"]
		.put((&alice, "D"), Json(json!({"device_id":"D", "last_seen_ts":123})))
		.await?;
	let page = endpoint
		.page(3, "user_id=test&limit=2", 3)
		.await?;
	assert_eq!(page["users"][0]["name"], alice.as_str());
	assert_eq!(page["users"][0]["last_seen_ts"], 123);
	assert_eq!(page["users"][0]["erased"], true);
	assert_eq!(page["users"][1]["name"], bob.as_str());
	assert_eq!(page["users"][1]["deactivated"], true);
	assert_eq!(page["next_token"], "2");
	let last = endpoint
		.page(3, "user_id=test&from=2&limit=2", 3)
		.await?;
	assert_eq!(last["users"].as_array().expect("array").len(), 1);
	assert_eq!(last["users"][0]["name"], carol.as_str());
	assert!(last["next_token"].is_null());
	let backward = endpoint
		.page(3, "user_id=test&dir=b&from=1&limit=1", 3)
		.await?;
	assert_eq!(backward["users"][0]["name"], bob.as_str());
	assert_eq!(backward["next_token"], "2");
	endpoint.page(2, "user_id=test", 2).await?;
	endpoint
		.page(2, "user_id=test&deactivated=true", 3)
		.await?;
	let only = endpoint
		.page(3, "user_id=test&deactivated=true", 1)
		.await?;
	assert_eq!(only["users"][0]["name"], bob.as_str());
	endpoint
		.page(3, "user_id=test&deactivated=false", 2)
		.await?;
	endpoint
		.page(3, "user_id=test&locked=true", 4)
		.await?;
	endpoint
		.page(3, "user_id=test&admins=false", 3)
		.await?;
	let admins = endpoint.page(3, "admins=true", 1).await?;
	assert_eq!(admins["users"][0]["name"], admin.as_str());
	let name = endpoint
		.page(3, "name=Visible&user_id=nonexistent", 1)
		.await?;
	assert_eq!(name["users"][0]["name"], carol.as_str());
	endpoint
		.page(3, "user_id=test&from=9999", 3)
		.await?;
	ancillary_errors(endpoint, &alice).await?;
	inventory_errors(endpoint, &alice).await?;
	aggregate_budgets(endpoint).await
}

async fn ancillary_errors(endpoint: &Endpoint<'_>, alice: &UserId) -> Result {
	let services = endpoint.services;
	let alias = services.admin.admin_alias.alias();
	let aliases = &services.db["alias_roomid"];
	let saved = match aliases.get(alias).await {
		| Ok(value) => Some(value.to_vec()),
		| Err(error) if error.is_not_found() => None,
		| Err(error) => return Err(error),
	};
	aliases.insert(alias, "not-a-room-id").await?;
	for version in [2, 3] {
		endpoint
			.refused(
				version,
				"user_id=test&admins=false",
				http::StatusCode::INTERNAL_SERVER_ERROR,
			)
			.await?;
	}
	if let Some(value) = saved {
		aliases.raw_put(alias, &value).await?;
	} else {
		aliases.remove(alias).await?;
	}
	assert!(
		!services
			.admin
			.user_is_admin_checked(alice)
			.await?
	);
	Ok(())
}

async fn inventory_errors(endpoint: &Endpoint<'_>, alice: &UserId) -> Result {
	let services = endpoint.services;
	let users = &services.db["userid_password"];
	users.insert(&[0xFF], "*").await?;
	for version in [2, 3] {
		endpoint
			.refused(
				version,
				"user_id=nonexistent&limit=1",
				http::StatusCode::INTERNAL_SERVER_ERROR,
			)
			.await?;
	}
	users.remove(&[0xFF]).await?;
	let profiles = &services.db["useridprofilekey_value"];
	profiles
		.put((alice, "displayname"), b"not-json")
		.await?;
	endpoint
		.refused(3, "user_id=test", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	profiles.del((alice, "displayname")).await?;
	let metadata = &services.db["userdeviceid_metadata"];
	metadata
		.put((alice, "CORRUPT"), b"not-json")
		.await?;
	endpoint
		.refused(3, "user_id=test&limit=1", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	metadata.clear().await?;
	metadata
		.put((alice, "KEY"), Json(json!({"device_id":"OTHER"})))
		.await?;
	endpoint
		.refused(3, "user_id=test", http::StatusCode::INTERNAL_SERVER_ERROR)
		.await?;
	metadata.clear().await?;
	for index in 0..128 {
		let device = format!("D{index:03}");
		metadata
			.put((alice, &device), Json(json!({"device_id":device})))
			.await?;
	}
	endpoint.page(3, "user_id=test-alice", 1).await?;
	metadata
		.put((alice, "OVERFLOW"), Json(json!({"device_id":"OVERFLOW"})))
		.await?;
	for version in [2, 3] {
		endpoint
			.refused(version, "user_id=test&limit=1", http::StatusCode::TOO_MANY_REQUESTS)
			.await?;
	}
	metadata.clear().await?;
	users.clear().await?;
	users
		.insert(&services.globals.server_user, "*")
		.await?;
	for index in 1..=MAX_LOCAL_USER_COUNT_ROWS {
		users
			.insert(&format!("@disabled-{index:04}:localhost"), "")
			.await?;
	}
	for version in [2, 3] {
		endpoint
			.refused(version, "user_id=nonexistent&limit=1", http::StatusCode::TOO_MANY_REQUESTS)
			.await?;
	}
	let unauthenticated = services
		.client
		.clients
		.default
		.get(format!("{}/_synapse/admin/v3/users", endpoint.base))
		.send()
		.await?;
	assert_eq!(unauthenticated.status(), http::StatusCode::UNAUTHORIZED);
	users.clear().await?;
	users
		.insert(&services.globals.server_user, "*")
		.await?;
	for index in 0..600 {
		let user = UserId::parse(format!("@long-{index:03}{}:localhost", "x".repeat(210)))
			.expect("valid long user ID");
		users.insert(&user, "").await?;
	}
	assert_eq!(services.users.bounded_local_users().await?.len(), 1);
	endpoint
		.refused(3, "user_id=nonexistent&limit=1", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	Ok(())
}

async fn aggregate_budgets(endpoint: &Endpoint<'_>) -> Result {
	let services = endpoint.services;
	let users = &services.db["userid_password"];
	let metadata = &services.db["userdeviceid_metadata"];
	users.clear().await?;
	users
		.insert(&services.globals.server_user, "*")
		.await?;
	for index in 0..33 {
		let user = UserId::parse(format!("@many-{index:03}:localhost")).expect("valid user");
		users.insert(&user, "*").await?;
		for index in 0..128 {
			let device = format!("D{index:03}");
			metadata
				.put((&user, &device), Json(json!({"device_id":device})))
				.await?;
		}
	}
	endpoint
		.refused(3, "user_id=many&limit=1", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	users.clear().await?;
	metadata.clear().await?;
	users
		.insert(&services.globals.server_user, "*")
		.await?;
	let large = "x".repeat(60 * 1024);
	let mut ids: Vec<OwnedUserId> = Vec::new();
	for index in 0..5 {
		let user = UserId::parse(format!("@bytes-{index:03}:localhost")).expect("valid user");
		users.insert(&user, "*").await?;
		metadata
			.put((&user, "D"), Json(json!({"device_id":"D", "display_name":large})))
			.await?;
		ids.push(user);
	}
	endpoint
		.refused(3, "user_id=bytes&limit=1", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	metadata.clear().await?;
	for user in &ids {
		services
			.profile
			.set_displayname(user, Some(&large), Some(Propagation::None))
			.await?;
	}
	endpoint
		.refused(3, "user_id=bytes&limit=1", http::StatusCode::TOO_MANY_REQUESTS)
		.await?;
	Ok(())
}
