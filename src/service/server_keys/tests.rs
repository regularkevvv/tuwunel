use ruma::{
	MilliSecondsSinceUnixEpoch, OwnedServerSigningKeyId, UInt,
	api::federation::discovery::{OldVerifyKey, ServerSigningKeys, VerifyKey},
	owned_server_name,
	serde::Base64,
};

use super::{KeyUse, merge_signing_keys, usable_key};

fn ts(ms: u64) -> MilliSecondsSinceUnixEpoch {
	MilliSecondsSinceUnixEpoch(UInt::new_saturating(ms))
}

fn id(key_id: &str) -> OwnedServerSigningKeyId { key_id.try_into().expect("valid key id") }

fn key(byte: u8) -> VerifyKey { VerifyKey::new(Base64::new(vec![byte; 32])) }

/// A key response listing `current` as verify keys valid until `valid_until`.
fn keys(valid_until: u64, current: &[&str]) -> ServerSigningKeys {
	let mut keys = ServerSigningKeys::new(owned_server_name!("remote.test"), ts(valid_until));
	for (byte, key_id) in (1..).zip(current) {
		keys.verify_keys.insert(id(key_id), key(byte));
	}

	keys
}

#[test]
fn merge_caps_validity_at_the_latest() {
	let merged = merge_signing_keys(None, keys(10_000, &["ed25519:a"]), ts(5_000));

	assert_eq!(merged.valid_until_ts, ts(5_000));
	assert!(merged.verify_keys.contains_key(&id("ed25519:a")));
}

#[test]
fn a_key_dropped_from_the_newer_list_expires_with_the_older() {
	let stored = keys(1_000, &["ed25519:a"]);
	let merged = merge_signing_keys(Some(stored), keys(2_000, &["ed25519:b"]), ts(u64::MAX));

	assert_eq!(merged.valid_until_ts, ts(2_000));
	assert!(merged.verify_keys.contains_key(&id("ed25519:b")));
	assert!(!merged.verify_keys.contains_key(&id("ed25519:a")));
	assert_eq!(merged.old_verify_keys[&id("ed25519:a")].expired_ts, ts(1_000));
}

#[test]
fn an_older_response_never_extends_validity() {
	let stored = keys(2_000, &["ed25519:a"]);
	let merged =
		merge_signing_keys(Some(stored), keys(1_000, &["ed25519:a", "ed25519:c"]), ts(u64::MAX));

	assert_eq!(merged.valid_until_ts, ts(2_000));
	assert!(!merged.verify_keys.contains_key(&id("ed25519:c")));
	assert_eq!(merged.old_verify_keys[&id("ed25519:c")].expired_ts, ts(1_000));
}

#[test]
fn a_published_expiry_is_kept() {
	let stored = keys(1_000, &["ed25519:a"]);
	let mut new = keys(2_000, &["ed25519:b"]);
	new.old_verify_keys
		.insert(id("ed25519:a"), OldVerifyKey::new(ts(1_500), key(9).key));

	let merged = merge_signing_keys(Some(stored), new, ts(u64::MAX));

	assert_eq!(merged.old_verify_keys[&id("ed25519:a")].expired_ts, ts(1_500));
}

#[test]
fn usable_key_respects_use_and_validity() {
	let mut keys = keys(1_000, &["ed25519:a"]);
	keys.old_verify_keys
		.insert(id("ed25519:o"), OldVerifyKey::new(ts(500), key(9).key));

	let usable = |key_id: &str, usage| usable_key(&keys, &id(key_id), usage).is_some();

	assert!(usable("ed25519:a", KeyUse::Event(Some(ts(1_000)))));
	assert!(!usable("ed25519:a", KeyUse::Event(Some(ts(1_001)))));
	assert!(usable("ed25519:a", KeyUse::Event(None)));

	assert!(usable("ed25519:o", KeyUse::Event(Some(ts(500)))));
	assert!(!usable("ed25519:o", KeyUse::Event(Some(ts(501)))));
	assert!(usable("ed25519:o", KeyUse::Event(None)));

	assert!(usable("ed25519:a", KeyUse::Request(ts(1_000))));
	assert!(!usable("ed25519:a", KeyUse::Request(ts(1_001))));
	assert!(!usable("ed25519:o", KeyUse::Request(ts(0))), "an old key signed a request");
	assert!(!usable("ed25519:z", KeyUse::Event(None)), "an unknown key was usable");
}
