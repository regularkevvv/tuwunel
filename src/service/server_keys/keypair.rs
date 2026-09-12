use std::sync::Arc;

use ruma::{
	MilliSecondsSinceUnixEpoch, OwnedServerSigningKeyId, ServerName,
	api::federation::discovery::{OldVerifyKey, ServerSigningKeys, VerifyKey},
	serde::Base64,
	signatures::Ed25519KeyPair,
};
use tuwunel_core::{Result, debug, debug_info, err, error, utils, utils::string_from_bytes};
use tuwunel_database::{Database, Deserialized, Json};

use super::VerifyKeys;

/// The current keypair record in `global`.
const CURRENT: &[u8] = b"keypair";

/// A keypair staged by `server rotate-signing-key`, made current at the next
/// start.
const NEXT: &[u8] = b"keypair_next";

pub(super) async fn init(
	db: &Arc<Database>,
	server_name: &ServerName,
) -> Result<(Box<Ed25519KeyPair>, VerifyKeys)> {
	promote_next(db, server_name).await?;

	let keypair = match load(db).await {
		| Ok(keypair) => keypair,
		| Err(e) => {
			error!("Keypair invalid. Deleting...");
			remove(db).await?;
			return Err(e);
		},
	};

	let verify_key = VerifyKey {
		key: Base64::new(keypair.public_key().to_vec()),
	};

	let verify_keys: VerifyKeys = [(key_id(keypair.version())?, verify_key)].into();

	Ok((keypair, verify_keys))
}

/// Stages a new keypair to replace the current one at the next start and
/// returns its key id. Staging again replaces the staged keypair.
pub(super) async fn stage_next(db: &Arc<Database>) -> Result<OwnedServerSigningKeyId> {
	let (version, _) = generate(db, NEXT).await?;

	key_id(&version)
}

/// Makes a staged keypair current. The retired key's public half is first
/// recorded as one of this server's old verify keys, expiring now, so it stays
/// published for verifying what it signed (server-server API, "Publishing
/// Keys"). A start interrupted between the steps repeats the promotion, and a
/// staged key that is already current is not retired.
async fn promote_next(db: &Arc<Database>, server_name: &ServerName) -> Result {
	let global = &db["global"];
	let Ok(next) = global.get(NEXT).await.map(|next| next.to_vec()) else {
		return Ok(());
	};

	if let Ok(current) = global.get(CURRENT).await
		&& current[..] != next[..]
	{
		let (version, der) = parse(&current);
		let retired = Ed25519KeyPair::from_der(&der, version.clone())
			.map_err(|e| err!("Failed to load ed25519 keypair from der: {e:?}"))?;

		let old = OldVerifyKey::new(
			MilliSecondsSinceUnixEpoch::now(),
			Base64::new(retired.public_key().to_vec()),
		);

		let signingkeys = &db["server_signingkeys"];
		let mut keys: ServerSigningKeys = signingkeys
			.get(server_name)
			.await
			.deserialized()
			.unwrap_or_else(|_| {
				ServerSigningKeys::new(server_name.to_owned(), MilliSecondsSinceUnixEpoch::now())
			});

		keys.old_verify_keys
			.insert(key_id(&version)?, old);
		signingkeys
			.raw_put(server_name, Json(&keys))
			.await?;

		debug_info!("Retired Ed25519 keypair: {version:?}");
	}

	global.insert(CURRENT, &next).await?;
	global.remove(NEXT).await
}

async fn load(db: &Arc<Database>) -> Result<Box<Ed25519KeyPair>> {
	let stored = db["global"].get_blocking(CURRENT).map(|ref val| {
		let (ver, der) = parse(val);
		debug!("Found existing Ed25519 keypair: {ver:?}");
		(ver, der)
	});

	let (version, key) = match stored {
		| Ok(entry) => entry,
		| Err(e) => {
			assert!(e.is_not_found(), "unexpected error fetching keypair");
			generate(db, CURRENT).await?
		},
	};

	let key = Ed25519KeyPair::from_der(&key, version)
		.map_err(|e| err!("Failed to load ed25519 keypair from der: {e:?}"))?;

	Ok(Box::new(key))
}

/// Splits a stored `(version, der)` keypair record; the database deserializer
/// is having trouble with this record, so it is split by hand.
fn parse(val: &[u8]) -> (String, Vec<u8>) {
	let mut elems = val.split(|&b| b == b'\xFF');
	let vlen = elems.next().expect("invalid keypair entry").len();
	let ver = string_from_bytes(&val[..vlen]).expect("invalid keypair version");
	let der = val[vlen.saturating_add(1)..].to_vec();

	(ver, der)
}

async fn generate(db: &Arc<Database>, slot: &[u8]) -> Result<(String, Vec<u8>)> {
	let keypair = Ed25519KeyPair::generate();

	let id = utils::rand::string(8);
	debug_info!("Generated new Ed25519 keypair: {id:?}");

	let value: (String, Vec<u8>) = (id, keypair.to_vec());
	db["global"].raw_put(slot, &value).await?;

	Ok(value)
}

fn key_id(version: &str) -> Result<OwnedServerSigningKeyId> {
	Ok(format!("ed25519:{version}").try_into()?)
}

#[inline]
async fn remove(db: &Arc<Database>) -> Result {
	let global = &db["global"];
	global.remove(CURRENT).await
}
