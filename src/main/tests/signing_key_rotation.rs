#![cfg(test)]

//! Signing-key rotation (phase 2 deliverable 7). `server rotate-signing-key`
//! stages a key that the next start makes active, keeping the retired key as
//! an old verify key with the expiry recorded at that start. Separate
//! processes seed, resume and start again over one database: before the
//! restart the current key stays active; after it the staged key signs, the
//! retired key is retained and still verifies what it signed, and a start with
//! nothing staged changes neither.

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
	env::{current_exe, var},
	fs::{DirBuilder, read_to_string, remove_dir_all, write},
	path::{Path, PathBuf},
	process::{Command, id as process_id},
};

use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	ruma::{
		CanonicalJsonObject, CanonicalJsonValue, MilliSecondsSinceUnixEpoch,
		signatures::{PublicKeyMap, PublicKeySet, verify_json},
	},
};
use tuwunel_service::Services;

const PHASE_ENV: &str = "TUWUNEL_SIGNING_KEY_ROTATION_PHASE";
const DIRECTORY_ENV: &str = "TUWUNEL_SIGNING_KEY_ROTATION_DIRECTORY";

struct OwnedDirectory(PathBuf);

impl Drop for OwnedDirectory {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

#[test]
fn signing_key_rotation_survives_restart() -> Result {
	if let Ok(phase) = var(PHASE_ENV) {
		let directory = PathBuf::from(var(DIRECTORY_ENV).expect("child directory"));
		return run_server(&directory, &phase);
	}
	let root = PathBuf::from(var("TMPDIR").unwrap_or_else(|_| "/tmp".into()));
	let directory = root.join(format!("signing-key-rotation-{}", process_id()));
	let mut builder = DirBuilder::new();
	builder.recursive(false);
	#[cfg(unix)]
	builder.mode(0o700);
	builder.create(&directory)?; // Never adopt a pre-existing directory.
	let directory = OwnedDirectory(directory);
	for phase in ["seed", "resume", "again"] {
		let status = Command::new(current_exe()?)
			.env(PHASE_ENV, phase)
			.env(DIRECTORY_ENV, &directory.0)
			.status()?;
		assert!(status.success(), "signing-key rotation child {phase} failed");
	}
	Ok(())
}

fn run_server(directory: &Path, phase: &str) -> Result {
	let modes: &[&str] = if phase == "seed" { &["fresh"] } else { &[] };
	let mut args = Args::default_test(modes);
	args.maintenance = true;
	args.option
		.push(format!("database_path={:?}", directory.join("database")));
	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let outcome = match phase {
			| "seed" => seed(&services, directory).await,
			| "resume" => resume(&services, directory).await,
			| "again" => again(&services, directory).await,
			| _ => panic!("unexpected child phase"),
		};
		let shutdown = server.server.shutdown();
		drop(services);
		let run = async_run(&server).await;
		let stop = async_stop(&server).await;
		outcome.and(shutdown).and(run).and(stop)
	});
	drop(runtime);
	result
}

async fn seed(services: &Services, directory: &Path) -> Result {
	let keys = &services.server_keys;
	let retiring = keys.active_key_id().to_owned();
	let signed = signed_object(services, "before")?;
	let staged = keys.stage_signing_key().await?;

	if staged == retiring {
		return Err!("the staged key reuses the active key id");
	}
	if keys.active_key_id().as_str() != retiring.as_str() {
		return Err!("staging changed the active key before a restart");
	}

	write(directory.join("retiring"), retiring.as_str())?;
	write(directory.join("staged"), staged.as_str())?;
	write(directory.join("signed"), serde_json::to_string(&signed)?)?;

	Ok(())
}

async fn resume(services: &Services, directory: &Path) -> Result {
	let keys = &services.server_keys;
	let ours = services.globals.server_name().as_str();
	let retired = read_to_string(directory.join("retiring"))?;
	let staged = read_to_string(directory.join("staged"))?;

	if keys.active_key_id().as_str() != staged {
		return Err!("the staged key is not active after a restart");
	}
	let expired = retained(services, &retired, &staged).await?;

	let before: CanonicalJsonObject =
		serde_json::from_str(&read_to_string(directory.join("signed"))?)?;
	if signed_with(&before, ours) != [retired.clone()] {
		return Err!("the seeded object was not signed by the retired key");
	}
	verify(services, &before).await?;

	let after = signed_object(services, "after")?;
	if signed_with(&after, ours) != [staged] {
		return Err!("a new signature does not use the staged key");
	}
	verify(services, &after).await?;

	write(directory.join("expired"), u64::from(expired.get()).to_string())?;

	Ok(())
}

async fn again(services: &Services, directory: &Path) -> Result {
	let retired = read_to_string(directory.join("retiring"))?;
	let staged = read_to_string(directory.join("staged"))?;
	let expired = read_to_string(directory.join("expired"))?;

	if services.server_keys.active_key_id().as_str() != staged {
		return Err!("a start with nothing staged changed the active key");
	}
	let expiry = retained(services, &retired, &staged).await?;
	if u64::from(expiry.get()).to_string() != expired {
		return Err!("a start with nothing staged changed the retired key's expiry");
	}

	Ok(())
}

/// The retired key is this server's only old verify key, expired no later
/// than now, and still among the keys its signatures are verified with; the
/// active key is not also recorded as retired.
async fn retained(
	services: &Services,
	retired: &str,
	active: &str,
) -> Result<MilliSecondsSinceUnixEpoch> {
	let ours = services.globals.server_name();
	let stored = services
		.server_keys
		.signing_keys_for(ours)
		.await?;
	let old: Vec<_> = stored.old_verify_keys.iter().collect();
	let [(id, key)] = old.as_slice() else {
		return Err!("expected exactly one old verify key, found {}", old.len());
	};
	if id.as_str() != retired || id.as_str() == active {
		return Err!("the old verify key is not the retired key");
	}
	if key.expired_ts > MilliSecondsSinceUnixEpoch::now() {
		return Err!("the retired key expires in the future");
	}
	if !services
		.server_keys
		.verify_keys_for(ours)
		.await
		.keys()
		.any(|id| id.as_str() == retired)
	{
		return Err!("the retired key is not among this server's verify keys");
	}

	Ok(key.expired_ts)
}

/// Verifies `object`'s signatures against the keys this server publishes for
/// itself, the retired key included.
async fn verify(services: &Services, object: &CanonicalJsonObject) -> Result {
	let ours = services.globals.server_name();
	let keys: PublicKeySet = services
		.server_keys
		.verify_keys_for(ours)
		.await
		.into_iter()
		.map(|(id, key)| (id.to_string(), key.key))
		.collect();

	let map: PublicKeyMap = [(ours.to_string(), keys)].into();
	verify_json(&map, object).map_err(|e| err!("signature does not verify: {e}"))
}

fn signed_object(services: &Services, label: &str) -> Result<CanonicalJsonObject> {
	let mut object = CanonicalJsonObject::new();
	object.insert("label".into(), CanonicalJsonValue::String(label.into()));
	services.server_keys.sign_json(&mut object)?;

	Ok(object)
}

/// The key ids that signed `object` for `server`.
fn signed_with(object: &CanonicalJsonObject, server: &str) -> Vec<String> {
	let Some(CanonicalJsonValue::Object(servers)) = object.get("signatures") else {
		return Vec::new();
	};
	let Some(CanonicalJsonValue::Object(keys)) = servers.get(server) else {
		return Vec::new();
	};

	keys.keys().map(ToString::to_string).collect()
}
