#![cfg(test)]

//! Media byte quotas and remote retention (phase 2 deliverable 6). A local
//! user's uploads are refused past `media_user_quota` and counted down again
//! when deleted; media stored for a remote server, thumbnails included, is
//! refused past `media_remote_server_quota` and removed after
//! `media_remote_retention` without touching local media; a counter that does
//! not exist is measured from the stored objects.

use std::{
	env::temp_dir,
	fs::{read_dir, remove_dir_all, remove_file},
	path::Path,
	process::id as process_id,
	time::Duration,
};

use futures::future::join;
use tokio::time::sleep;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	ruma::{Mxc, ServerName, UserId, api::error::ErrorKind},
};
use tuwunel_database::refusal;
use tuwunel_service::{
	Services,
	media::{Dim, Owner},
};

const BYTES: &[u8] = &[7; 600];
const SMALL: &[u8] = &[9; 100];

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
		let outcome = exercise(&services, &db_path.join("media")).await;

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

async fn exercise(services: &Services, media_dir: &Path) -> Result {
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

	// A delete whose objects are already gone, as a retry after a kill between
	// removing the objects and removing their records would find them, still
	// releases exactly the bytes the records carry.
	let lost = Mxc {
		server_name: remote,
		media_id: "cachedlost",
	};
	media
		.create(&lost, None, None, None, &BYTES[..500])
		.await?;
	usage(services, Owner::Server(remote), 500, "after storing media about to be lost").await?;
	for entry in read_dir(media_dir)? {
		remove_file(entry?.path())?;
	}
	media.delete(&lost).await?;
	usage(
		services,
		Owner::Server(remote),
		0,
		"after deleting media whose objects were gone",
	)
	.await?;
	if media.get_metadata(&lost).await.is_some() {
		return Err!("a delete whose objects were gone kept the record");
	}

	retried_async_upload(services, &user).await
}

/// An async upload is charged once, however it is retried. The first attempt
/// stores the content and its charge, and is refused where it removes the
/// pending entry, as an interrupted attempt leaves it. Its retry, and an
/// upload racing another to one media ID, find content there and are refused
/// before any charge.
async fn retried_async_upload(services: &Services, user: &UserId) -> Result {
	let media = &services.media;
	let ours = services.globals.server_name();
	let used = media.quota_usage(Owner::User(user)).await;
	let charged = u64::try_from(SMALL.len())?;
	let interrupted = Mxc {
		server_name: ours,
		media_id: "quotainterrupted",
	};

	media
		.create_pending(&interrupted, user, u64::MAX)
		.await?;
	refusal::refuse_next("mediaid_pending");
	if media
		.upload_pending(&interrupted, user, None, None, SMALL)
		.await
		.is_ok()
	{
		return Err!("an upload whose pending removal was refused reported success");
	}
	if refusal::pending() != 0 {
		return Err!("the upload never reached its pending removal");
	}
	usage(
		services,
		Owner::User(user),
		used.saturating_add(charged),
		"after the interrupted upload",
	)
	.await?;

	cannot_overwrite(
		media
			.upload_pending(&interrupted, user, None, None, SMALL)
			.await,
		"the retry of an interrupted upload",
	)?;
	usage(services, Owner::User(user), used.saturating_add(charged), "after its retry").await?;
	if services.db["mediaid_pending"]
		.get(&interrupted.to_string())
		.await
		.is_ok()
	{
		return Err!("the retry left the interrupted upload's pending entry");
	}

	let raced = Mxc {
		server_name: ours,
		media_id: "quotaraced",
	};
	media
		.create_pending(&raced, user, u64::MAX)
		.await?;
	let (first, second) = join(
		media.upload_pending(&raced, user, None, None, SMALL),
		media.upload_pending(&raced, user, None, None, SMALL),
	)
	.await;
	match (first, second) {
		| (Ok(()), refused) | (refused, Ok(())) =>
			cannot_overwrite(refused, "the losing upload of a race")?,
		| (Err(first), Err(second)) =>
			return Err!("both racing uploads failed: {first}; {second}"),
	}
	usage(
		services,
		Owner::User(user),
		used.saturating_add(charged.saturating_mul(2)),
		"after two uploads raced",
	)
	.await
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

fn cannot_overwrite(result: Result, what: &str) -> Result {
	match result {
		| Err(e) if e.kind() == ErrorKind::CannotOverwriteMedia => Ok(()),
		| Err(e) => Err(err!("{what} failed with {e}, not M_CANNOT_OVERWRITE_MEDIA")),
		| Ok(()) => Err!("{what} was accepted"),
	}
}
