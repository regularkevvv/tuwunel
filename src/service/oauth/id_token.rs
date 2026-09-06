//! OpenID Connect ID Token verification.
//!
//! In the authorization code flow the `id_token` is the only assertion that is
//! cryptographically bound to the identity provider. A userinfo response is
//! merely whatever the bearer access token happens to dereference to, so an
//! attacker able to substitute a token — or a compromised userinfo host —
//! substitutes the identity with it. This module implements the relying-party
//! checks OpenID Connect Core 1.0 §3.1.3.7 requires before the claims are
//! trusted: the signature over a key published in the provider's JWKS, an
//! exact `iss`, an `aud` containing our `client_id` (plus `azp` when several
//! audiences are present), `exp`/`iat` inside a small clock-skew leeway, and
//! the `nonce` bound to the authorization request.
//!
//! The verifying algorithm is taken from the JWK, never from the token header.
//! A token that relabels an `RS256`/`ES256` provider as `HS256` and MACs
//! itself with the provider's public key — the algorithm-confusion attack of
//! RFC 8725 §2.1 and §3.1 — therefore cannot select a symmetric verifier, and
//! `alg: none` is rejected outright because `jsonwebtoken`'s `Algorithm` has
//! no such variant for its header parser to accept.

use serde::Deserialize;
use subtle::ConstantTimeEq;
use tuwunel_core::{
	Err, Result, err,
	jwt::{
		Algorithm, AlgorithmFamily, DecodingKey, Validation, decode, decode_header,
		get_current_timestamp,
		jwk::{AlgorithmParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm},
	},
};

/// Clock-skew tolerance applied to `exp`, `nbf` and `iat`, in seconds.
///
/// OpenID Connect Core 1.0 §3.1.3.7 permits a small leeway for the difference
/// between the provider's clock and ours. Kept deliberately tight: the whole
/// point of a 15-minute Matrix session (ADR-0004) is defeated by a generous
/// window on the assertion that opens it.
pub const CLOCK_SKEW_LEEWAY: u64 = 60;

/// Claims read out of a verified `id_token`.
///
/// Only the claims that participate in a security decision are modelled. The
/// remaining profile claims stay in the userinfo response, which is the
/// provider's own answer for mutable attributes.
#[derive(Clone, Debug, Deserialize)]
pub struct IdTokenClaims {
	/// Issuer identifier of the provider that signed this token.
	pub iss: String,

	/// Stable subject identifier at that issuer. Together with `iss` this is
	/// the only identity key (ADR-0004); every other claim is mutable.
	pub sub: String,

	/// Audience: a single string or an array that must contain our
	/// `client_id`.
	pub aud: Audience,

	/// Expiration time, in seconds since the Unix epoch.
	pub exp: u64,

	/// Issued-at time, in seconds since the Unix epoch.
	pub iat: Option<u64>,

	/// Authorized party. Required by §3.1.3.7 item 4 when `aud` holds more
	/// than one value, and then it must be our `client_id`.
	pub azp: Option<String>,

	/// Echo of the `nonce` sent in the authorization request, binding this
	/// token to that one browser session.
	pub nonce: Option<String>,

	/// Time the provider authenticated the end user, when it reports one.
	pub auth_time: Option<u64>,
}

/// The `aud` claim is a case-sensitive string or an array of them
/// (RFC 7519 §4.1.3).
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum Audience {
	/// A single audience value.
	One(String),

	/// Several audience values.
	Many(Vec<String>),
}

impl Audience {
	/// Number of audience values carried by the claim.
	#[must_use]
	pub fn len(&self) -> usize {
		match self {
			| Self::One(..) => 1,
			| Self::Many(values) => values.len(),
		}
	}

	/// Whether the claim carries no audience at all.
	#[must_use]
	pub fn is_empty(&self) -> bool { self.len() == 0 }

	/// Whether `value` appears among the audience values.
	#[must_use]
	pub fn contains(&self, value: &str) -> bool {
		match self {
			| Self::One(one) => one == value,
			| Self::Many(many) => many.iter().any(|aud| aud == value),
		}
	}
}

/// Verify an `id_token` against a provider's published JWKS.
///
/// `issuer` is the provider's issuer identifier and is compared exactly; the
/// only tolerance is the trailing slash that URL normalization appends to an
/// otherwise empty path (`https://accounts.google.com` parses back as
/// `https://accounts.google.com/`), which is the same tolerance the discovery
/// check applies. `nonce` is the value sent in the authorization request: when
/// one was sent the token must echo it, per §3.1.3.7 item 11.
///
/// Returns the verified claims. Every failure is an `Unauthorized` request
/// error carrying only the reason, never the token.
pub fn verify(
	id_token: &str,
	jwks: &JwkSet,
	issuer: &str,
	client_id: &str,
	nonce: Option<&str>,
) -> Result<IdTokenClaims> {
	let header = decode_header(id_token)
		.map_err(|e| err!(Request(Unauthorized("id_token has no acceptable JWS header: {e}"))))?;

	// Redundant with the algorithm pinned from the JWK below, but stated here so
	// a symmetric header is refused before any key selection happens at all.
	if header.alg.family() == AlgorithmFamily::Hmac {
		return Err!(Request(Unauthorized(
			"id_token is signed with a symmetric algorithm; refusing an alg-confusion token."
		)));
	}

	let jwk = select_jwk(jwks, header.kid.as_deref())?;
	let algorithm = jwk_algorithm(jwk)?;

	let key = DecodingKey::from_jwk(jwk)
		.map_err(|e| err!(Request(Unauthorized("id_token signing key is unusable: {e}"))))?;

	// `decode` rejects a header whose `alg` is not in this list, so pinning the
	// single algorithm the JWK is published for is what closes RFC 8725 §3.1.
	let mut validation = Validation::new(algorithm);
	validation.leeway = CLOCK_SKEW_LEEWAY;
	validation.validate_exp = true;
	validation.validate_nbf = true;
	validation.validate_aud = true;
	validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
	validation.set_audience(&[client_id]);
	validation.set_issuer(&issuer_forms(issuer));

	let claims = decode::<IdTokenClaims>(id_token, &key, &validation)
		.map_err(|e| err!(Request(Unauthorized("id_token failed validation: {e}"))))?
		.claims;

	check_audience(&claims, client_id)?;
	check_issued_at(&claims)?;
	check_nonce(&claims, nonce)?;

	Ok(claims)
}

/// The `kid` a token names in its header, when it parses and names one.
///
/// Used to decide whether the cached JWKS can possibly verify the token before
/// a fetch is attempted; a token this cannot parse fails verification anyway.
#[must_use]
pub fn key_id(id_token: &str) -> Option<String> { decode_header(id_token).ok()?.kid }

/// The issuer strings accepted as an exact match.
///
/// `Url::as_str()` renders an empty path as `/`, so a configured issuer of
/// `https://accounts.google.com` round-trips as `https://accounts.google.com/`
/// while the provider signs the unslashed form. Both spellings of the same
/// identifier are accepted and nothing else; this mirrors the trailing-slash
/// trim the discovery issuer check already performs.
fn issuer_forms(issuer: &str) -> Vec<String> {
	let trimmed = issuer.trim_end_matches('/');

	if trimmed == issuer {
		vec![issuer.to_owned()]
	} else {
		vec![issuer.to_owned(), trimmed.to_owned()]
	}
}

/// Pick the JWK that signed this token.
///
/// A `kid` selects one key exactly. Without a `kid` the set must hold exactly
/// one key, because guessing among several is how a relying party ends up
/// trusting a key the provider retired.
fn select_jwk<'a>(jwks: &'a JwkSet, kid: Option<&str>) -> Result<&'a Jwk> {
	if let Some(kid) = kid {
		return jwks.find(kid).ok_or_else(|| {
			err!(Request(Unauthorized("id_token names a key id absent from the provider JWKS.")))
		});
	}

	match jwks.keys.as_slice() {
		| [jwk] => Ok(jwk),
		| [] => Err!(Request(Unauthorized("Provider JWKS is empty."))),
		| _ => Err!(Request(Unauthorized(
			"id_token carries no key id and the provider JWKS publishes several keys."
		))),
	}
}

/// Resolve the signature algorithm a JWK may be used with.
///
/// The published `alg` wins when present; otherwise it is derived from the key
/// material, which is what a provider that omits `alg` leaves us. Symmetric
/// keys and the RSA key-encryption algorithms are refused: neither can verify
/// an ID token signature, and accepting an `oct` key here is exactly the
/// confusion this module exists to prevent.
fn jwk_algorithm(jwk: &Jwk) -> Result<Algorithm> {
	if let Some(key_algorithm) = jwk.common.key_algorithm {
		return match key_algorithm {
			| KeyAlgorithm::ES256 => Ok(Algorithm::ES256),
			| KeyAlgorithm::ES384 => Ok(Algorithm::ES384),
			| KeyAlgorithm::RS256 => Ok(Algorithm::RS256),
			| KeyAlgorithm::RS384 => Ok(Algorithm::RS384),
			| KeyAlgorithm::RS512 => Ok(Algorithm::RS512),
			| KeyAlgorithm::PS256 => Ok(Algorithm::PS256),
			| KeyAlgorithm::PS384 => Ok(Algorithm::PS384),
			| KeyAlgorithm::PS512 => Ok(Algorithm::PS512),
			| KeyAlgorithm::EdDSA => Ok(Algorithm::EdDSA),
			| other => Err!(Request(Unauthorized(
				"Provider JWKS key is published for {other:?}, which cannot verify an id_token."
			))),
		};
	}

	match &jwk.algorithm {
		| AlgorithmParameters::EllipticCurve(ec) => match ec.curve {
			| EllipticCurve::P256 => Ok(Algorithm::ES256),
			| EllipticCurve::P384 => Ok(Algorithm::ES384),
			| ref other => {
				Err!(Request(Unauthorized("Provider JWKS key uses unsupported curve {other:?}.")))
			},
		},
		| AlgorithmParameters::OctetKeyPair(okp) => match okp.curve {
			| EllipticCurve::Ed25519 => Ok(Algorithm::EdDSA),
			| ref other => {
				Err!(Request(Unauthorized("Provider JWKS key uses unsupported curve {other:?}.")))
			},
		},
		| AlgorithmParameters::RSA(..) => Ok(Algorithm::RS256),
		| AlgorithmParameters::OctetKey(..) => Err!(Request(Unauthorized(
			"Provider JWKS publishes a symmetric key; refusing to verify an id_token with it."
		))),
		| other => {
			Err!(Request(Unauthorized("Provider JWKS key type is unsupported: {other:?}")))
		},
	}
}

/// Enforce the multi-audience rule of §3.1.3.7 items 3 and 4.
///
/// `jsonwebtoken` already rejects an `aud` that omits us. What it does not do
/// is demand `azp` once the token is addressed to more than one party, which
/// is the check that stops a token minted for another relying party from being
/// replayed here.
fn check_audience(claims: &IdTokenClaims, client_id: &str) -> Result {
	if !claims.aud.contains(client_id) {
		return Err!(Request(Unauthorized("id_token audience does not contain this client.")));
	}

	if claims.aud.len() > 1 && claims.azp.as_deref() != Some(client_id) {
		return Err!(Request(Unauthorized(
			"id_token has several audiences without an azp naming this client."
		)));
	}

	Ok(())
}

/// Reject a token issued further in the future than the clock-skew leeway.
///
/// `exp` is validated by `jsonwebtoken`; `iat` is not, and a provider clock
/// that runs far ahead would otherwise hand out an assertion that stays valid
/// long past its intended window.
fn check_issued_at(claims: &IdTokenClaims) -> Result {
	let Some(iat) = claims.iat else {
		return Ok(());
	};

	let now = get_current_timestamp();

	if iat > now.saturating_add(CLOCK_SKEW_LEEWAY) {
		return Err!(Request(Unauthorized("id_token is issued too far in the future.")));
	}

	Ok(())
}

/// Bind the token to the authorization request that asked for it.
///
/// §3.1.3.7 item 11: when a `nonce` was sent it must be present and equal.
/// Compared in constant time; the value is a server-generated secret and an
/// oracle on it would let a callback be replayed against a session it does not
/// belong to.
fn check_nonce(claims: &IdTokenClaims, nonce: Option<&str>) -> Result {
	let Some(nonce) = nonce else {
		return Ok(());
	};

	let Some(claimed) = claims.nonce.as_deref() else {
		return Err!(Request(Unauthorized(
			"id_token omits the nonce sent in the authorization request."
		)));
	};

	if !bool::from(claimed.as_bytes().ct_eq(nonce.as_bytes())) {
		return Err!(Request(Unauthorized("id_token nonce does not match the session nonce.")));
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use aws_lc_rs::signature::{self, EcdsaKeyPair};
	use serde_json::json;
	use tuwunel_core::jwt::{EncodingKey, Header, encode};

	use super::*;

	const ISSUER: &str = "https://team.cloudflareaccess.com/cdn-cgi/access/sso/oidc/abc123";
	const CLIENT_ID: &str = "abc123";
	const NONCE: &str = "session-nonce-value";

	/// An ES256 key pair plus the JWK a provider would publish for it.
	struct Signer {
		key: EncodingKey,
		jwk: Jwk,
	}

	fn signer(kid: &str) -> Signer {
		let alg = &signature::ECDSA_P256_SHA256_FIXED_SIGNING;
		let pkcs8 = EcdsaKeyPair::generate(alg)
			.expect("generate P-256 key")
			.to_pkcs8v1()
			.expect("serialize P-256 key");

		let key = EncodingKey::from_ec_der(pkcs8.as_ref());
		let mut jwk = Jwk::from_encoding_key(&key, Algorithm::ES256).expect("derive JWK");
		jwk.common.key_id = Some(kid.to_owned());

		Signer { key, jwk }
	}

	fn now() -> u64 { get_current_timestamp() }

	fn claims() -> serde_json::Value {
		json!({
			"iss": ISSUER,
			"sub": "user-subject-1",
			"aud": CLIENT_ID,
			"exp": now().saturating_add(300),
			"iat": now(),
			"nonce": NONCE,
		})
	}

	fn sign(signer: &Signer, claims: &serde_json::Value) -> String {
		let mut header = Header::new(Algorithm::ES256);
		header.kid = signer.jwk.common.key_id.clone();

		encode(&header, claims, &signer.key).expect("sign id_token")
	}

	fn jwks(signer: &Signer) -> JwkSet { JwkSet { keys: vec![signer.jwk.clone()] } }

	fn verify_with(signer: &Signer, claims: &serde_json::Value) -> Result<IdTokenClaims> {
		verify(&sign(signer, claims), &jwks(signer), ISSUER, CLIENT_ID, Some(NONCE))
	}

	#[test]
	fn accepts_a_well_formed_token() {
		let signer = signer("kid-1");
		let verified = verify_with(&signer, &claims()).expect("valid id_token verifies");

		assert_eq!(verified.sub, "user-subject-1");
		assert_eq!(verified.iss, ISSUER);
	}

	#[test]
	fn rejects_a_forged_signature() {
		let provider = signer("kid-1");
		let attacker = signer("kid-1");

		let token = sign(&attacker, &claims());
		let error = verify(&token, &jwks(&provider), ISSUER, CLIENT_ID, Some(NONCE))
			.expect_err("a token signed by another key must not verify");

		assert!(format!("{error}").contains("failed validation"), "unexpected: {error}");
	}

	#[test]
	fn rejects_a_truncated_signature() {
		let signer = signer("kid-1");
		let token = sign(&signer, &claims());
		let tampered = token
			.rsplit_once('.')
			.map(|(body, _)| format!("{body}.AAAA"))
			.expect("token has three parts");

		verify(&tampered, &jwks(&signer), ISSUER, CLIENT_ID, Some(NONCE))
			.expect_err("a tampered signature must not verify");
	}

	#[test]
	fn rejects_a_wrong_audience() {
		let signer = signer("kid-1");
		let mut claims = claims();
		claims["aud"] = json!("some-other-client");

		verify_with(&signer, &claims).expect_err("a token for another client must not verify");
	}

	#[test]
	fn rejects_several_audiences_without_azp() {
		let signer = signer("kid-1");
		let mut claims = claims();
		claims["aud"] = json!([CLIENT_ID, "some-other-client"]);

		let error = verify_with(&signer, &claims)
			.expect_err("a multi-audience token needs an azp naming this client");

		assert!(format!("{error}").contains("azp"), "unexpected: {error}");
	}

	#[test]
	fn accepts_several_audiences_with_matching_azp() {
		let signer = signer("kid-1");
		let mut claims = claims();
		claims["aud"] = json!([CLIENT_ID, "some-other-client"]);
		claims["azp"] = json!(CLIENT_ID);

		verify_with(&signer, &claims).expect("azp naming this client is accepted");
	}

	#[test]
	fn rejects_a_wrong_issuer() {
		let signer = signer("kid-1");
		let mut claims = claims();
		claims["iss"] = json!("https://evil.example/cdn-cgi/access/sso/oidc/abc123");

		verify_with(&signer, &claims).expect_err("a token from another issuer must not verify");
	}

	#[test]
	fn rejects_an_expired_token() {
		let signer = signer("kid-1");
		let mut claims = claims();
		claims["exp"] = json!(
			now()
				.saturating_sub(CLOCK_SKEW_LEEWAY)
				.saturating_sub(60)
		);

		let error = verify_with(&signer, &claims).expect_err("an expired token must not verify");

		assert!(format!("{error}").contains("failed validation"), "unexpected: {error}");
	}

	#[test]
	fn rejects_a_token_issued_in_the_future() {
		let signer = signer("kid-1");
		let mut claims = claims();
		claims["iat"] = json!(
			now()
				.saturating_add(CLOCK_SKEW_LEEWAY)
				.saturating_add(600)
		);

		let error = verify_with(&signer, &claims)
			.expect_err("a token issued far in the future must not verify");

		assert!(format!("{error}").contains("future"), "unexpected: {error}");
	}

	#[test]
	fn rejects_a_mismatched_nonce() {
		let signer = signer("kid-1");
		let mut claims = claims();
		claims["nonce"] = json!("a-different-nonce");

		let error = verify_with(&signer, &claims).expect_err("a replayed nonce must not verify");

		assert!(format!("{error}").contains("nonce"), "unexpected: {error}");
	}

	#[test]
	fn rejects_a_missing_nonce() {
		let signer = signer("kid-1");
		let mut claims = claims();
		claims
			.as_object_mut()
			.expect("claims object")
			.remove("nonce");

		let error = verify_with(&signer, &claims)
			.expect_err("a token omitting the requested nonce must not verify");

		assert!(format!("{error}").contains("nonce"), "unexpected: {error}");
	}

	#[test]
	fn rejects_alg_none() {
		let signer = signer("kid-1");
		let header = b64_json(&json!({ "alg": "none", "kid": "kid-1" }));
		let payload = b64_json(&claims());
		let token = format!("{header}.{payload}.");

		verify(&token, &jwks(&signer), ISSUER, CLIENT_ID, Some(NONCE))
			.expect_err("an unsigned token must not verify");
	}

	#[test]
	fn rejects_hmac_alg_confusion_against_a_public_key() {
		// The classic RFC 8725 §3.1 attack: relabel the token HS256 and MAC it
		// with material the attacker can derive from the published JWK.
		let signer = signer("kid-1");
		let public = serde_json::to_vec(&signer.jwk).expect("serialize published JWK");
		let mut header = Header::new(Algorithm::HS256);
		header.kid = Some("kid-1".to_owned());

		let token = encode(&header, &claims(), &EncodingKey::from_secret(&public))
			.expect("sign an HS256 token");

		let error = verify(&token, &jwks(&signer), ISSUER, CLIENT_ID, Some(NONCE))
			.expect_err("an HS256 token must not verify against an EC JWKS");

		assert!(format!("{error}").contains("symmetric"), "unexpected: {error}");
	}

	#[test]
	fn rejects_a_symmetric_jwks_key() {
		let key = EncodingKey::from_secret(b"a shared secret an IdP should never publish");
		let mut jwk = Jwk::from_encoding_key(&key, Algorithm::HS256).expect("derive oct JWK");
		jwk.common.key_id = Some("kid-1".to_owned());
		jwk.common.key_algorithm = None;

		let header = b64_json(&json!({ "alg": "ES256", "kid": "kid-1" }));
		let token = format!("{header}.{}.AAAA", b64_json(&claims()));

		let error = verify(&token, &JwkSet { keys: vec![jwk] }, ISSUER, CLIENT_ID, Some(NONCE))
			.expect_err("a symmetric JWKS key must not verify an id_token");

		assert!(format!("{error}").contains("symmetric"), "unexpected: {error}");
	}

	#[test]
	fn rejects_an_unknown_key_id() {
		let provider = signer("kid-1");
		let other = signer("kid-2");

		let error =
			verify(&sign(&provider, &claims()), &jwks(&other), ISSUER, CLIENT_ID, Some(NONCE))
				.expect_err("a key id absent from the JWKS must not verify");

		assert!(format!("{error}").contains("key id"), "unexpected: {error}");
	}

	#[test]
	fn accepts_the_trailing_slash_form_of_an_issuer() {
		let signer = signer("kid-1");
		let mut claims = claims();
		claims["iss"] = json!("https://accounts.example.com");

		let token = sign(&signer, &claims);
		verify(&token, &jwks(&signer), "https://accounts.example.com/", CLIENT_ID, Some(NONCE))
			.expect("URL normalization's trailing slash is tolerated");
	}

	fn b64_json(value: &serde_json::Value) -> String {
		use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as b64};

		b64.encode(serde_json::to_vec(value).expect("serialize"))
	}
}
