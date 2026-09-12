#![cfg(test)]

//! Media byte quotas and remote retention (phase 2 deliverable 6). A local
//! user's uploads are refused past `media_user_quota` and counted down again
//! when deleted; media stored for a remote server, thumbnails included, is
//! refused past `media_remote_server_quota` and removed after
//! `media_remote_retention` without touching local media; a counter that does
//! not exist is measured from the stored objects.

use std::{env::temp_dir, fs::remove_dir_all, process::id as process_id, time::Duration};

use tokio::time::sleep;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	ruma::{Mxc, ServerName, UserId, api::error::ErrorKind},
};
use tuwunel_service::{
	Services,
	media::{Dim, Owner},
};

const BYTES: &[u8] = &[7; 600];

#[test]
fn media_quotas_and_remote_retention() -> Result {
	let db_path = temp_dir().join(format!("tuwunel-test-media-quota-{}", process_id()));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option.extend([
		format!("database_path=\"{}\"", db_path.display()),
		"media_user_quota=1000".to_owned(),
		"media_remote_server_quota=\"1 KB\"".to_owned(),
		"media_remote_retention=1".to_owned(),
		"log=\"warn\"".to_owned(),
	]);

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result: Result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let outcome = exercise(&services).await;

		server.server.shutdown()?;
		drop(services);
		async_run(&server).await?;
		async_stop(&server).await?;

		outcome
	});

	drop(server);
	drop(runtime);
	remove_dir_all(&db_path).ok();
	result
}

async fn exercise(services: &Services) -> Result {
	let media = &services.media;
	let ours = services.globals.server_name();
	let user = UserId::parse(format!("@quota:{ours}"))?;
	let remote = <&ServerName>::try_from("remote.test")?;
	let first = Mxc {
		server_name: ours,
		media_id: "quotafirst",
	};
	let second = Mxc {
		server_name: ours,
		media_id: "quotasecond",
	};
	let cached = Mxc {
		server_name: remote,
		media_id: "cachedfirst",
	};
	let refused = Mxc {
		server_name: remote,
		media_id: "cachedsecond",
	};

	media
		.create(&first, Some(&user), None, None, BYTES)
		.await?;
	too_large(
		media
			.create(&second, Some(&user), None, None, BYTES)
			.await,
		"an upload past the user quota",
	)?;
	if media.get_metadata(&second).await.is_some() {
		return Err!("a refused upload left a record");
	}
	usage(services, Owner::User(&user), 600, "after one upload").await?;

	media.delete(&first).await?;
	usage(services, Owner::User(&user), 0, "after deleting it").await?;
	media
		.create(&second, Some(&user), None, None, BYTES)
		.await?;

	services.db["userid_mediabytes"]
		.remove(user.as_str())
		.await?;
	usage(services, Owner::User(&user), 600, "measured from storage").await?;

	media
		.create(&cached, None, None, None, BYTES)
		.await?;
	media
		.upload_thumbnail(&cached, None, None, &Dim::new(32, 32, None), &BYTES[..300])
		.await?;
	usage(
		services,
		Owner::Server(remote),
		900,
		"after storing remote media and a thumbnail",
	)
	.await?;
	too_large(
		media
			.create(&refused, None, None, None, BYTES)
			.await,
		"remote media past the server quota",
	)?;

	sleep(Duration::from_millis(1500)).await;
	let expired = media.expire_remote_media().await?;
	if expired != 1 {
		return Err!("retention removed {expired} media, expected 1");
	}
	usage(services, Owner::Server(remote), 0, "after retention").await?;
	if media.get_metadata(&cached).await.is_some() {
		return Err!("expired remote media kept its record");
	}
	if media.get_metadata(&second).await.is_none() {
		return Err!("retention removed local media");
	}

	Ok(())
}

async fn usage(services: &Services, owner: Owner<'_>, expected: u64, when: &str) -> Result {
	let used = services.media.quota_usage(owner).await;
	if used != expected {
		return Err!("{owner:?} used {used} bytes {when}, expected {expected}");
	}

	Ok(())
}

fn too_large(result: Result, what: &str) -> Result {
	match result {
		| Err(e) if e.kind() == ErrorKind::TooLarge => Ok(()),
		| Err(e) => Err(err!("{what} failed with {e}, not M_TOO_LARGE")),
		| Ok(()) => Err!("{what} was accepted"),
	}
}
