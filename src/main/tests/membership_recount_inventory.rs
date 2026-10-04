#![cfg(test)]

use std::{
	env::temp_dir, fs::remove_dir_all, path::PathBuf, process::id as process_id, time::Duration,
};

use tokio::time::timeout;
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Error, Result, err, http,
	ruma::{RoomId, UserId},
};
use tuwunel_database::{Interfix, serialize_key};
use tuwunel_service::Services;

const PENDING: &str = "membership_recount_pending";

struct DatabasePath(PathBuf);

impl Drop for DatabasePath {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn incomplete_recount_inventories_preserve_aggregates_and_repair_obligations() -> Result {
	let path = DatabasePath(temp_dir().join(format!("recount-inventory-{}", process_id())));
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option
		.push(format!("database_path={:?}", path.0));
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let outcome = timeout(Duration::from_mins(2), exercise(&services))
			.await
			.map_err(|_| err!("recount inventory fixture exceeded its deadline"))
			.and_then(|outcome| outcome);
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

async fn assert_counts(
	services: &Services,
	room: &RoomId,
	counts: [u64; 3],
	pending: bool,
) -> Result {
	for (name, expected) in ["roomid_joinedcount", "roomid_invitedcount", "roomid_knockedcount"]
		.into_iter()
		.zip(counts)
	{
		assert_eq!(
			services.db[name]
				.get(room.as_bytes())
				.await?
				.as_ref(),
			expected.to_be_bytes(),
			"complete aggregate {name}"
		);
	}
	let marker = services.db["global"].qry(&(PENDING, room)).await;
	if pending {
		assert!(marker?.is_empty(), "refused inventory preserves the durable repair marker");
	} else {
		assert!(
			marker
				.expect_err("successful repair clears marker")
				.is_not_found(),
			"only genuine marker absence means repaired"
		);
	}
	Ok(())
}

async fn key(services: &Services, map: &str, room: &RoomId, user: &str) -> Result {
	services.db[map]
		.put_raw((room, user), 1_u64.to_be_bytes())
		.await?;
	Ok(())
}

async fn corruption(services: &Services, room: &RoomId) -> Result {
	for map in [
		"roomuserid_joined",
		"roomuserid_invitecount",
		"roomuserid_knockedcount",
		"roomserverids",
	] {
		for suffix in [b"broken identifier".as_slice(), b"\xff".as_slice()] {
			let mut key = serialize_key((room, Interfix))?.to_vec();
			key.extend_from_slice(suffix);
			services.db[map]
				.raw_put(key.as_slice(), 1_u64.to_be_bytes())
				.await?;
			let error = services
				.state_cache
				.update_joined_count(room)
				.await
				.expect_err("corrupt inventory refuses recount");
			assert!(
				matches!(error, Error::Database(_)),
				"corrupt stored identifiers have database classification"
			);
			assert_eq!(
				services.db[map]
					.get(key.as_slice())
					.await?
					.as_ref(),
				1_u64.to_be_bytes(),
				"refusal preserves corrupt inventory record"
			);
			assert_counts(services, room, [2, 1, 1], true).await?;
			assert!(
				services.db["roomserverids"]
					.qry(&(room, "one.test"))
					.await?
					.is_empty(),
				"refusal preserves current server index"
			);
			assert!(
				services.db["serverroomids"]
					.qry(&("one.test", room))
					.await?
					.is_empty(),
				"refusal preserves reverse server index"
			);
			services.db[map].remove(key.as_slice()).await?;
			services
				.state_cache
				.repair_joined_count(room)
				.await?;
			assert_counts(services, room, [2, 1, 1], false).await?;
		}
	}
	Ok(())
}

async fn row_boundaries(services: &Services) -> Result {
	let room = RoomId::parse("!recount-rows:localhost")?;
	for chunk in 0..16_usize {
		let mut txn = services.db.txn();
		for offset in 0..256_usize {
			let index = chunk
				.checked_mul(256)
				.and_then(|base| base.checked_add(offset))
				.expect("owned row index fits usize");
			if index >= 4095 {
				break;
			}
			let user = UserId::parse(format!("@member-{index}:one.test"))?;
			txn.put_raw(&services.db["roomuserid_joined"], (&*room, &*user), 1_u64.to_be_bytes());
			txn.put_raw(&services.db["userroomid_joined"], (&*user, &*room), 1_u64.to_be_bytes());
		}
		txn.execute().await?;
	}
	services
		.state_cache
		.update_joined_count(&room)
		.await?;
	services
		.state_cache
		.update_joined_count(&room)
		.await?;
	assert_counts(services, &room, [4095, 0, 0], false).await?;
	key(services, "roomuserid_invitecount", &room, "@overflow:two.test").await?;
	let error = services
		.state_cache
		.update_joined_count(&room)
		.await
		.expect_err("shared rows include invitation and server inventories");
	assert_eq!(
		error.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS,
		"row overflow classification"
	);
	assert_counts(services, &room, [4095, 0, 0], true).await?;
	services.db["roomuserid_invitecount"]
		.remove(serialize_key((&*room, "@overflow:two.test"))?.as_ref())
		.await?;
	services
		.state_cache
		.repair_joined_count(&room)
		.await?;
	assert_counts(services, &room, [4095, 0, 0], false).await
}

async fn exercise(services: &Services) -> Result {
	let room = RoomId::parse("!recount-inventory:localhost")?;
	key(services, "roomuserid_joined", &room, "@one:one.test").await?;
	key(services, "roomuserid_joined", &room, "@two:two.test").await?;
	key(services, "roomuserid_invitecount", &room, "@invite:invited.test").await?;
	key(services, "roomuserid_knockedcount", &room, "@knock:knocked.test").await?;
	services.db["roomserverids"]
		.put((&*room, "stale.test"), &[0_u8; 0][..])
		.await?;
	services.db["serverroomids"]
		.put(("stale.test", &*room), &[0_u8; 0][..])
		.await?;
	services
		.state_cache
		.update_joined_count(&room)
		.await?;
	assert_counts(services, &room, [2, 1, 1], false).await?;
	assert!(
		services.db["roomserverids"]
			.qry(&(&*room, "stale.test"))
			.await
			.expect_err("stale forward server removed")
			.is_not_found(),
		"successful repair removes stale server"
	);
	assert!(
		services.db["serverroomids"]
			.qry(&("stale.test", &*room))
			.await
			.expect_err("stale reverse server removed")
			.is_not_found(),
		"successful repair removes stale reverse server"
	);
	corruption(services, &room).await?;
	row_boundaries(services).await?;
	byte_boundaries(services).await?;
	Ok(())
}

fn byte_member(
	index: usize,
	width: usize,
	prefix: usize,
) -> Result<tuwunel_core::ruma::OwnedUserId> {
	let stem = format!("@b{index:04}");
	let suffix = ":one.test";
	let padding = width
		.checked_sub(prefix)
		.and_then(|remaining| remaining.checked_sub(stem.len()))
		.and_then(|remaining| remaining.checked_sub(suffix.len()))
		.expect("owned byte boundary leaves room for a valid user ID");
	Ok(UserId::parse(format!("{stem}{}{suffix}", "x".repeat(padding)))?)
}

async fn byte_boundaries(services: &Services) -> Result {
	let room = RoomId::parse("!recount-bytes:localhost")?;
	let prefix = room
		.as_str()
		.len()
		.checked_add(1)
		.expect("owned room prefix fits usize");
	let server_key = serialize_key((&*room, "one.test"))?;
	let last_width = 256_usize
		.checked_sub(server_key.len())
		.expect("owned server key is below a member key width");
	services.db["roomserverids"]
		.put((&*room, "one.test"), &[0_u8; 0][..])
		.await?;
	services.db["serverroomids"]
		.put(("one.test", &*room), &[0_u8; 0][..])
		.await?;
	for chunk in 0..8_usize {
		let mut txn = services.db.txn();
		for offset in 0..256_usize {
			let index = chunk
				.checked_mul(256)
				.and_then(|base| base.checked_add(offset))
				.expect("owned byte row index fits usize");
			let width = if index == 2047 { last_width } else { 256 };
			let user = byte_member(index, width, prefix)?;
			let key = serialize_key((&*room, &*user))?;
			assert_eq!(key.len(), width, "byte boundary uses actual encoded keys");
			txn.put_raw(&services.db["roomuserid_joined"], (&*room, &*user), 1_u64.to_be_bytes());
			txn.put_raw(&services.db["userroomid_joined"], (&*user, &*room), 1_u64.to_be_bytes());
		}
		txn.execute().await?;
	}
	services
		.state_cache
		.update_joined_count(&room)
		.await?;
	assert_counts(services, &room, [2048, 0, 0], false).await?;
	let exact = byte_member(2047, last_width, prefix)?;
	let overflow = byte_member(
		2047,
		last_width
			.checked_add(1)
			.expect("owned one-byte overflow fits usize"),
		prefix,
	)?;
	let mut txn = services.db.txn();
	txn.del(&services.db["roomuserid_joined"], (&*room, &*exact));
	txn.del(&services.db["userroomid_joined"], (&*exact, &*room));
	txn.put_raw(&services.db["roomuserid_joined"], (&*room, &*overflow), 1_u64.to_be_bytes());
	txn.put_raw(&services.db["userroomid_joined"], (&*overflow, &*room), 1_u64.to_be_bytes());
	txn.execute().await?;
	let error = services
		.state_cache
		.update_joined_count(&room)
		.await
		.expect_err("shared byte inventory is one byte over budget");
	assert_eq!(
		error.status_code(),
		http::StatusCode::TOO_MANY_REQUESTS,
		"byte overflow classification"
	);
	assert_counts(services, &room, [2048, 0, 0], true).await?;
	assert_eq!(
		services.db["roomuserid_joined"]
			.qry(&(&*room, &*overflow))
			.await?
			.as_ref(),
		1_u64.to_be_bytes(),
		"byte refusal preserves the complete membership record"
	);
	let mut txn = services.db.txn();
	txn.del(&services.db["roomuserid_joined"], (&*room, &*overflow));
	txn.del(&services.db["userroomid_joined"], (&*overflow, &*room));
	txn.put_raw(&services.db["roomuserid_joined"], (&*room, &*exact), 1_u64.to_be_bytes());
	txn.put_raw(&services.db["userroomid_joined"], (&*exact, &*room), 1_u64.to_be_bytes());
	txn.execute().await?;
	services
		.state_cache
		.repair_joined_count(&room)
		.await?;
	assert_counts(services, &room, [2048, 0, 0], false).await
}
