use ruma::{
	CanonicalJsonObject, MilliSecondsSinceUnixEpoch, OwnedServerSigningKeyId, UInt,
	api::federation::discovery::{OldVerifyKey, ServerSigningKeys, VerifyKey},
	owned_server_name,
	serde::{Base64, Raw},
	server_name,
	signatures::{Ed25519KeyPair, sign_json},
};
use serde_json::{json, value::to_raw_value};

use super::{KeyUse, PubKeys, merge_signing_keys, request::verify_server_keys, usable_key};
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

fn keypair(version: &str) -> Ed25519KeyPair {
	Ed25519KeyPair::from_der(&Ed25519KeyPair::generate(), version.to_owned())
		.expect("generated key parses")
}

fn pubkey(key: &Ed25519KeyPair) -> Base64 { Base64::new(key.public_key().to_vec()) }

/// An unsigned key response for `origin`, listing `listed` as its verify key.
fn key_response(origin: &str, listed: &Ed25519KeyPair) -> CanonicalJsonObject {
	serde_json::from_value(json!({
		"server_name": origin,
		"valid_until_ts": 1_000,
		"verify_keys": {
			format!("ed25519:{}", listed.version()): {"key": pubkey(listed)},
		},
		"old_verify_keys": {},
		"signatures": {},
	}))
	.expect("key response object")
}

fn signed(
	mut object: CanonicalJsonObject,
	signers: &[(&str, &Ed25519KeyPair)],
) -> CanonicalJsonObject {
	for (entity, key) in signers {
		sign_json(entity, *key, &mut object).expect("object signs");
	}

	object
}

fn raw(object: &CanonicalJsonObject) -> Raw<ServerSigningKeys> {
	Raw::from_json(to_raw_value(object).expect("raw key response"))
}

fn notary_keys(key: &Ed25519KeyPair) -> PubKeys {
	[(format!("ed25519:{}", key.version()), pubkey(key))].into()
}

#[test]
fn a_self_signed_key_response_verifies() {
	let origin = keypair("o");
	let response = signed(key_response("origin.test", &origin), &[("origin.test", &origin)]);

	verify_server_keys(&raw(&response), None).expect("a self-signed response verifies");
}

#[test]
fn an_unsigned_or_forged_key_response_is_refused() {
	let origin = keypair("o");
	let forger = keypair("o");

	let unsigned = raw(&key_response("origin.test", &origin));
	assert!(unsigned.deserialize().is_ok(), "the response parses");
	assert!(verify_server_keys(&unsigned, None).is_err(), "an unsigned response verified");

	// Signed under the listed key id, but not by the listed key.
	let forged = signed(key_response("origin.test", &origin), &[("origin.test", &forger)]);
	assert!(verify_server_keys(&raw(&forged), None).is_err(), "a forged response verified");

	// Signed by a key it does not list.
	let unlisted = keypair("x");
	let unlisted = signed(key_response("origin.test", &origin), &[("origin.test", &unlisted)]);
	assert!(verify_server_keys(&raw(&unlisted), None).is_err(), "an unlisted key verified");
}

#[test]
fn a_notary_response_needs_both_signatures() {
	let origin = keypair("o");
	let notary = keypair("n");
	let other = keypair("n");
	let notary_name = server_name!("notary.test");
	let known = notary_keys(&notary);
	let verify = |signers: &[(&str, &Ed25519KeyPair)]| {
		let response = signed(key_response("origin.test", &origin), signers);
		verify_server_keys(&raw(&response), Some((notary_name, &known)))
	};

	assert!(verify(&[("origin.test", &origin)]).is_err(), "no notary signature verified");
	assert!(verify(&[("notary.test", &notary)]).is_err(), "no origin signature verified");
	assert!(
		verify(&[("origin.test", &origin), ("notary.test", &other)]).is_err(),
		"a notary signature by another key verified"
	);

	verify(&[("origin.test", &origin), ("notary.test", &notary)])
		.expect("a response signed by both verifies");

	verify(&[("origin.test", &origin), ("notary.test", &notary), ("third.test", &other)])
		.expect("a third party's signature is not considered");
}

#[test]
fn a_notary_relaying_its_own_keys_signs_once() {
	let notary = keypair("n");
	let response = signed(key_response("notary.test", &notary), &[("notary.test", &notary)]);

	verify_server_keys(
		&raw(&response),
		Some((server_name!("notary.test"), &notary_keys(&notary))),
	)
	.expect("the notary's own key response verifies");
}
