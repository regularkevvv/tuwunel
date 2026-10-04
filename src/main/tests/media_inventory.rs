#![cfg(test)]

use std::{env::temp_dir, fs::remove_dir_all, path::PathBuf, process::id as process_id};

use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_admin::{fini, init};
use tuwunel_core::{
	Result, http,
	ruma::{Mxc, api::error::ErrorKind},
};
use tuwunel_service::Services;

struct DatabasePath(PathBuf);
impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn media_inventory_is_complete_or_refused_before_bulk_deletion() -> Result {
	let db_path =
		DatabasePath(temp_dir().join(format!("tuwunel-media-inventory-{}", process_id())));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.extend([format!("database_path={:?}", db_path.0), "log=\"warn\"".into()]);
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
	drop(server);
	drop(runtime);
	result
}

async fn exercise(services: &Services) -> Result {
	let map = &services.db["mediaid_file"];
	map.clear().await?;
	assert!(services.media.get_all_mxcs().await?.is_empty());
	let anchor = Mxc {
		server_name: services.globals.server_name(),
		media_id: "anchor",
	};
	let bytes: &[u8] = b"disposable media inventory anchor";
	services
		.media
		.create(&anchor, None, None, None, bytes)
		.await?;
	assert_eq!(services.media.get_all_mxcs().await?.len(), 1);
	for index in 0..4095 {
		map.insert(&format!("mxc://localhost/record-{index:04}"), 0_u64.to_be_bytes())
			.await?;
	}
	assert_eq!(services.media.get_all_mxcs().await?.len(), 4096);
	map.insert("mxc://localhost/row-overflow", 0_u64.to_be_bytes())
		.await?;
	let error = services
		.media
		.get_all_mxcs()
		.await
		.expect_err("an extra row cannot produce a short inventory");
	assert_eq!(error.status_code(), http::StatusCode::TOO_MANY_REQUESTS);
	assert!(matches!(error.kind(), ErrorKind::LimitExceeded(_)));
	assert_eq!(
		services
			.media
			.delete_by_date_size(u64::MAX, 0, false)
			.await
			.expect_err("inventory refusal must precede deletions")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	let command = format!(
		"media delete-all-from-server {} --yes-i-want-to-delete-local-media",
		services.globals.server_name()
	);
	assert!(
		services
			.admin
			.command_in_place(command, None)
			.await
			.is_err()
	);
	assert_eq!(services.media.get(&anchor, None).await?.content, bytes);
	map.remove("mxc://localhost/row-overflow").await?;
	assert_eq!(services.media.get_all_mxcs().await?.len(), 4096);
	corruption_and_byte_budget(services).await
}

async fn corruption_and_byte_budget(services: &Services) -> Result {
	let map = &services.db["mediaid_file"];
	map.clear().await?;
	map.insert("not-an-mxc", 0_u64.to_be_bytes())
		.await?;
	services
		.media
		.get_all_mxcs()
		.await
		.expect_err("malformed stored URI cannot disappear from the inventory");
	services
		.media
		.delete_by_date_size(u64::MAX, 0, false)
		.await
		.expect_err("corruption must refuse before deletion");
	assert!(map.get("not-an-mxc").await.is_ok());
	map.clear().await?;
	map.insert(&[0xFE], 0_u64.to_be_bytes()).await?;
	services
		.media
		.get_all_mxcs()
		.await
		.expect_err("invalid UTF-8 must propagate");
	map.clear().await?;
	for index in 0..180 {
		let mut key = format!("mxc://localhost/bytes-{index:04}").into_bytes();
		key.push(0xFF);
		key.extend_from_slice(&vec![b'x'; 6000]);
		map.insert(&key, 0_u64.to_be_bytes()).await?;
	}
	assert_eq!(
		services
			.media
			.get_all_mxcs()
			.await
			.expect_err("key bytes have an independent budget below the row limit")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	assert_eq!(
		services
			.media
			.delete_by_date_size(u64::MAX, 0, false)
			.await
			.expect_err("byte overflow cannot produce partial deletions")
			.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS
	);
	Ok(())
}
