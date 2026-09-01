use std::sync::Arc;

use ruma::{api::federation::discovery::VerifyKey, serde::Base64, signatures::Ed25519KeyPair};
use tuwunel_core::{Result, debug, debug_info, err, error, utils, utils::string_from_bytes};
use tuwunel_database::Database;

use super::VerifyKeys;

pub(super) async fn init(db: &Arc<Database>) -> Result<(Box<Ed25519KeyPair>, VerifyKeys)> {
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

	let id = format!("ed25519:{}", keypair.version());
	let verify_keys: VerifyKeys = [(id.try_into()?, verify_key)].into();

	Ok((keypair, verify_keys))
}

async fn load(db: &Arc<Database>) -> Result<Box<Ed25519KeyPair>> {
	let stored = db["global"]
		.get_blocking(b"keypair")
		.map(|ref val| {
			// database deserializer is having trouble with this so it's manual for now
			let mut elems = val.split(|&b| b == b'\xFF');
			let vlen = elems.next().expect("invalid keypair entry").len();
			let ver = string_from_bytes(&val[..vlen]).expect("invalid keypair version");
			let der = val[vlen.saturating_add(1)..].to_vec();
			debug!("Found existing Ed25519 keypair: {ver:?}");
			(ver, der)
		});

	let (version, key) = match stored {
		| Ok(entry) => entry,
		| Err(e) => {
			assert!(e.is_not_found(), "unexpected error fetching keypair");
			create(db).await?
		},
	};

	let key = Ed25519KeyPair::from_der(&key, version)
		.map_err(|e| err!("Failed to load ed25519 keypair from der: {e:?}"))?;

	Ok(Box::new(key))
}

async fn create(db: &Arc<Database>) -> Result<(String, Vec<u8>)> {
	let keypair = Ed25519KeyPair::generate();

	let id = utils::rand::string(8);
	debug_info!("Generated new Ed25519 keypair: {id:?}");

	let value: (String, Vec<u8>) = (id, keypair.to_vec());
	db["global"].raw_put(b"keypair", &value).await?;

	Ok(value)
}

#[inline]
async fn remove(db: &Arc<Database>) -> Result {
	let global = &db["global"];
	global.remove(b"keypair").await
}
