//! Application-layer sealing of stored upstream OAuth grants.
//!
//! ADR-0004 stores the upstream OIDC grant encrypted. The database encrypts at
//! rest, but a database export, a backup, or anything that reads rows through
//! the storage bridge sees what was written; sealing the token material itself
//! keeps a stored access, refresh or ID token out of all of those.
//!
//! AES-256-GCM, with a fresh random 96-bit nonce per seal and the record's
//! session id as associated data, so sealed material moved onto another record
//! does not open. A key's identifier is derived from the key itself (the first
//! eight bytes of its SHA-256), so rotation needs no second setting: the
//! current key seals, and it or any listed previous key opens.

use std::fmt;

use aws_lc_rs::{
	aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey},
	rand::{SecureRandom, SystemRandom},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as b64};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Err, Result, err, utils::hash::sha256};

/// Length in bytes of a grant key.
pub const KEY_LEN: usize = 32;

/// Grant token material as it is persisted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Sealed {
	/// Identifier of the key that sealed this material.
	pub kid: String,

	/// The nonce, URL-safe base64 without padding.
	pub nonce: String,

	/// Ciphertext followed by its authentication tag, URL-safe base64 without
	/// padding.
	pub data: String,
}

/// The token material of one grant: the only part of a session that is sealed.
#[derive(Default, Deserialize, Serialize)]
pub(super) struct Material {
	pub(super) access_token: Option<String>,
	pub(super) refresh_token: Option<String>,
	pub(super) id_token: Option<String>,
}

impl Material {
	pub(super) fn is_empty(&self) -> bool {
		self.access_token.is_none() && self.refresh_token.is_none() && self.id_token.is_none()
	}
}

/// The configured grant keys: one that seals, any number that still open.
pub struct Keys {
	current: Option<Key>,
	previous: Vec<Key>,
}

struct Key {
	kid: String,
	key: LessSafeKey,
}

/// Only key identifiers are ever formatted, never key material.
impl fmt::Debug for Keys {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let previous: Vec<&str> = self
			.previous
			.iter()
			.map(|key| key.kid.as_str())
			.collect();

		f.debug_struct("Keys")
			.field("current", &self.current_kid())
			.field("previous", &previous)
			.finish()
	}
}

impl Keys {
	/// Load the current key and the keys it replaced.
	///
	/// # Errors
	///
	/// A key that is not 32 bytes of URL-safe base64 without padding is a
	/// configuration error, reported without echoing the value.
	pub fn new(current: Option<&str>, previous: &[String]) -> Result<Self> {
		Ok(Self {
			current: current.map(load).transpose()?,
			previous: previous
				.iter()
				.map(|key| load(key))
				.collect::<Result<_>>()?,
		})
	}

	/// Identifier of the key new material is sealed with, if one is set.
	#[must_use]
	pub fn current_kid(&self) -> Option<&str> {
		self.current.as_ref().map(|key| key.kid.as_str())
	}

	/// Seal `material` for the record `sess_id`. Returns `None` when no current
	/// key is configured, in which case the caller stores the material as is.
	pub(super) fn seal(&self, sess_id: &str, material: &Material) -> Result<Option<Sealed>> {
		let Some(key) = &self.current else {
			return Ok(None);
		};

		let mut nonce = [0_u8; NONCE_LEN];
		SystemRandom::new()
			.fill(&mut nonce)
			.map_err(|_| err!("Grant nonce generation failed."))?;

		let mut data = serde_json::to_vec(material)?;
		key.key
			.seal_in_place_append_tag(
				Nonce::assume_unique_for_key(nonce),
				Aad::from(sess_id.as_bytes()),
				&mut data,
			)
			.map_err(|_| err!("Grant sealing failed."))?;

		Ok(Some(Sealed {
			kid: key.kid.clone(),
			nonce: b64.encode(nonce),
			data: b64.encode(data),
		}))
	}

	/// Open material sealed for the record `sess_id`.
	///
	/// Fails when no configured key carries the sealing key's identifier, when
	/// the stored encoding is malformed, or when authentication fails — which
	/// includes material copied from another record.
	pub(super) fn open(&self, sess_id: &str, sealed: &Sealed) -> Result<Material> {
		let Some(key) = self
			.current
			.iter()
			.chain(&self.previous)
			.find(|key| key.kid == sealed.kid)
		else {
			return Err!("No configured grant key opens this grant.");
		};

		let nonce: [u8; NONCE_LEN] = b64
			.decode(&sealed.nonce)
			.ok()
			.and_then(|nonce| nonce.try_into().ok())
			.ok_or_else(|| err!("Sealed grant nonce is malformed."))?;

		let mut data = b64
			.decode(&sealed.data)
			.map_err(|_| err!("Sealed grant data is malformed."))?;

		let plain = key
			.key
			.open_in_place(
				Nonce::assume_unique_for_key(nonce),
				Aad::from(sess_id.as_bytes()),
				&mut data,
			)
			.map_err(|_| err!("Sealed grant failed authentication."))?;

		serde_json::from_slice(plain).map_err(|_| err!("Sealed grant material is malformed."))
	}
}

fn load(encoded: &str) -> Result<Key> {
	let raw = b64
		.decode(encoded.trim())
		.ok()
		.filter(|raw| raw.len() == KEY_LEN)
		.ok_or_else(|| {
			err!(Config(
				"oauth_grant_key",
				"A grant key must be 32 bytes as URL-safe base64 without padding."
			))
		})?;

	let kid = b64.encode(&sha256::hash(&raw)[..8]);
	let key = UnboundKey::new(&AES_256_GCM, &raw)
		.map_err(|_| err!(Config("oauth_grant_key", "A grant key could not be loaded.")))?;

	Ok(Key { kid, key: LessSafeKey::new(key) })
}
