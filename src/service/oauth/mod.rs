pub mod id_token;
pub mod providers;
pub mod server;
pub mod sessions;
#[cfg(test)]
mod tests;
pub mod token_response;
pub mod user_info;

use std::{
	collections::HashMap,
	net::IpAddr,
	sync::{Arc, Mutex},
	time::Instant,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as b64encode};
use futures::{Stream, StreamExt, TryStreamExt};
use http::StatusCode;
use reqwest::{
	Method,
	header::{ACCEPT, CONTENT_TYPE},
};
use ruma::{
	UserId,
	api::error::{ErrorKind, LimitExceededErrorData},
};
use serde::Serialize;
use serde_json::Value as JsonValue;
use tuwunel_core::{
	Err, Error, Result, debug_warn, err, implement, info,
	itertools::Itertools,
	utils::{hash::sha256, result::LogErr, stream::ReadyExt},
	warn,
};
use url::Url;

pub use self::{
	id_token::{IdTokenClaims, verify as verify_id_token_claims},
	providers::{Provider, ProviderId},
	server::Server,
	sessions::{CODE_VERIFIER_LENGTH, SESSION_ID_LENGTH, Session, SessionId},
	token_response::TokenResponse,
	user_info::UserInfo,
};
use self::{providers::Providers, sessions::Sessions};
use crate::{SelfServices, client::read_response_capped};

/// Per-client-IP token-bucket table: last-refill instant and remaining tokens.
type Ratelimiter = Mutex<HashMap<IpAddr, (Instant, f64)>>;

pub struct Service {
	services: SelfServices,
	pub providers: Arc<Providers>,
	pub sessions: Arc<Sessions>,
	pub server: Option<Arc<Server>>,
	ratelimiter: Ratelimiter,
	device_ratelimiter: Ratelimiter,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		let providers = Arc::new(Providers::build(args));
		let sessions = Arc::new(Sessions::build(args, providers.clone()));
		let server = Server::build(args)?.map(Arc::new);

		Ok(Arc::new(Self {
			services: args.services.clone(),
			sessions,
			providers,
			server,
			ratelimiter: Mutex::new(HashMap::new()),
			device_ratelimiter: Mutex::new(HashMap::new()),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
#[inline]
pub fn get_server(&self) -> Result<&Server> {
	self.server
		.as_deref()
		.ok_or_else(|| err!(Request(Unrecognized("OIDC server not configured"))))
}

/// Cap on the rate-limit table; fully refilled buckets are pruned past it.
const RATELIMIT_MAP_CAP: usize = 1 << 16;

/// Always-on throttle for the RFC 8628 device user-code endpoints. The
/// `user_code` is low-entropy by design (§6.1), so §5.1 requires bounding
/// guesses regardless of the optional `oidc_rc_*` knobs; the burst stays
/// generous for the one code a real user enters.
const DEVICE_RC_PER_SECOND: f64 = 1.0;
const DEVICE_RC_BURST: f64 = 60.0;

/// Shared per-client-IP token-bucket throttle for the OIDC endpoints. A no-op
/// unless both `oidc_rc_per_second` and `oidc_rc_burst_count` are configured.
#[implement(Service)]
pub fn check_rate_limit(&self, client: IpAddr) -> Result {
	let config = &self.services.config;
	let rate = f64::from(config.oidc_rc_per_second);
	let burst = f64::from(config.oidc_rc_burst_count);

	if rate <= 0.0 || burst <= 0.0 {
		return Ok(());
	}

	check_bucket(&self.ratelimiter, client, rate, burst)
}

/// Always-on anti-brute-force throttle for the device user-code endpoints
/// (RFC 8628 §5.1), independent of the optional `oidc_rc_*` knobs.
#[implement(Service)]
pub fn check_device_rate_limit(&self, client: IpAddr) -> Result {
	check_bucket(&self.device_ratelimiter, client, DEVICE_RC_PER_SECOND, DEVICE_RC_BURST)
}

fn check_bucket(table: &Ratelimiter, client: IpAddr, rate: f64, burst: f64) -> Result {
	let now = Instant::now();
	let mut buckets = table.lock()?;

	// A fully refilled bucket equals an absent one; prune those past the cap so
	// a source-address spray cannot grow the table without bound.
	if buckets.len() >= RATELIMIT_MAP_CAP {
		buckets.retain(|_, bucket| {
			let (last, toks) = *bucket;
			now.duration_since(last)
				.as_secs_f64()
				.mul_add(rate, toks)
				< burst
		});
	}

	let (last_time, tokens) = buckets
		.entry(client)
		.or_insert_with(|| (now, burst));

	let new_tokens = now
		.duration_since(*last_time)
		.as_secs_f64()
		.mul_add(rate, *tokens)
		.min(burst);

	if new_tokens < 1.0 {
		return Err(Error::Request(
			ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
			"Too many OIDC requests.".into(),
			StatusCode::TOO_MANY_REQUESTS,
		));
	}

	*last_time = now;
	*tokens = new_tokens - 1.0;

	Ok(())
}

/// Remove all session state for a user. For debug and developer use only;
/// deleting state can cause registration conflicts and unintended
/// re-registrations.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn delete_user_sessions(&self, user_id: &UserId) {
	self.user_sessions(user_id)
		.ready_filter_map(Result::ok)
		.ready_filter_map(|(_, session)| session.sess_id)
		.for_each(|sess_id| async move {
			self.sessions.delete(&sess_id).await;
		})
		.await;
}

/// Revoke all session tokens for a user.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn revoke_user_tokens(&self, user_id: &UserId) {
	self.user_sessions(user_id)
		.ready_filter_map(Result::ok)
		.for_each(|(provider, session)| async move {
			self.revoke_token((&provider, &session))
				.await
				.log_err()
				.ok();
		})
		.await;
}

/// Get user's authorizations. Lists pairs of `(Provider, Session)` for a user.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub fn user_sessions(
	&self,
	user_id: &UserId,
) -> impl Stream<Item = Result<(Provider, Session)>> + Send {
	self.sessions
		.get_by_user(user_id)
		.and_then(async |session| Ok((self.sessions.provider(&session).await?, session)))
}

/// Network request to a Provider returning userinfo for a Session. The session
/// must have a valid access token.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all, ret)]
pub async fn request_userinfo(
	&self,
	(provider, session): (&Provider, &Session),
) -> Result<UserInfo> {
	#[derive(Debug, Serialize)]
	struct Query;

	let url = provider
		.userinfo_url
		.clone()
		.ok_or_else(|| err!(Config("userinfo_url", "Missing userinfo URL in config")))?;

	self.request((Some(provider), Some(session)), Method::GET, url, Option::<Query>::None)
		.await
		.and_then(|value| serde_json::from_value(value).map_err(Into::into))
		.log_err()
}

/// Network request to a Provider returning information for a Session based on
/// its access token.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all, ret)]
pub async fn request_tokeninfo(
	&self,
	(provider, session): (&Provider, &Session),
) -> Result<UserInfo> {
	#[derive(Debug, Serialize)]
	struct Query;

	let url = provider
		.introspection_url
		.clone()
		.ok_or_else(|| {
			err!(Config("introspection_url", "Missing introspection URL in config"))
		})?;

	self.request((Some(provider), Some(session)), Method::GET, url, Option::<Query>::None)
		.await
		.and_then(|value| serde_json::from_value(value).map_err(Into::into))
		.log_err()
}

/// Network request to a Provider revoking a Session's token.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all, ret)]
pub async fn revoke_token(&self, (provider, session): (&Provider, &Session)) -> Result {
	#[derive(Debug, Serialize)]
	struct RevokeQuery<'a> {
		client_id: &'a str,
		client_secret: &'a str,
	}

	let client_secret = provider.get_client_secret().await?;

	let query = RevokeQuery {
		client_id: &provider.client_id,
		client_secret: &client_secret,
	};

	let url = provider
		.revocation_url
		.clone()
		.ok_or_else(|| err!(Config("revocation_url", "Missing revocation URL in config")))?;

	self.request((Some(provider), Some(session)), Method::POST, url, Some(query))
		.await
		.log_err()
		.map(|_| ())
}

/// Network request to a Provider to obtain an access token for a Session using
/// a provided code.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all, ret)]
pub async fn request_token(
	&self,
	(provider, session): (&Provider, &Session),
	code: &str,
) -> Result<TokenResponse> {
	#[derive(Debug, Serialize)]
	struct TokenQuery<'a> {
		client_id: &'a str,
		client_secret: &'a str,
		grant_type: &'a str,
		code: &'a str,
		code_verifier: Option<&'a str>,
		redirect_uri: Option<&'a str>,
	}

	let client_secret = provider.get_client_secret().await?;

	let query = TokenQuery {
		client_id: &provider.client_id,
		client_secret: &client_secret,
		grant_type: "authorization_code",
		code,
		code_verifier: session.code_verifier.as_deref(),
		redirect_uri: provider.callback_url.as_ref().map(Url::as_str),
	};

	let url = provider
		.token_url
		.clone()
		.ok_or_else(|| err!(Config("token_url", "Missing token URL in config")))?;

	self.request((Some(provider), Some(session)), Method::POST, url, Some(query))
		.await
		.and_then(|value| serde_json::from_value(value).map_err(Into::into))
		.log_err()
}

/// Send a request to a provider; this is somewhat abstract since URL's are
/// formed prior to this call and could point at anything, however this function
/// uses the oauth-specific http client and is configured for JSON with special
/// casing for an `error` property in the response.
///
/// The response is deliberately not recorded: a token-endpoint reply is raw
/// JSON containing the access, refresh and ID tokens, and no such value may
/// reach a log at any level (plan.md, non-negotiable invariant 7). Callers that
/// need a decoded view record their own redacted type instead.
#[implement(Service)]
#[tracing::instrument(name = "request", level = "debug", skip(self, body))]
pub async fn request<Body>(
	&self,
	(provider, session): (Option<&Provider>, Option<&Session>),
	method: Method,
	url: Url,
	body: Option<Body>,
) -> Result<JsonValue>
where
	Body: Serialize,
{
	let mut request = self
		.services
		.client
		.oauth
		.request(method, url)
		.header(ACCEPT, "application/json");

	if let Some(body) = body.map(serde_html_form::to_string).transpose()? {
		request = request
			.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
			.body(body);
	}

	if let Some(session) = session
		&& let Some(access_token) = session.access_token.clone()
	{
		request = request.bearer_auth(access_token);
	}

	let limit = self.services.config.max_response_size;
	let http_response = request.send().await?.error_for_status()?;

	let body = read_response_capped(http_response, limit).await?;
	let response: JsonValue = serde_json::from_slice(&body)?;

	if let Some(response) = response.as_object().as_ref()
		&& let Some(error) = response.get("error").and_then(JsonValue::as_str)
	{
		let description = response
			.get("error_description")
			.and_then(JsonValue::as_str)
			.unwrap_or("(no description)");

		return Err!(Request(Forbidden("Error from provider: {error}: {description}",)));
	}

	Ok(response)
}

/// Verify the `id_token` a provider returned for this session.
///
/// Returns the verified claims, or `None` when the provider issued no
/// `id_token` and does not require one. The provider's JWKS is served from the
/// runtime cache; a token naming a key id the cached set does not hold forces
/// one bounded re-fetch, which is how a key rotation between our last fetch
/// and this login is picked up without turning forged key ids into traffic.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all, fields(provider = provider.id()))]
pub async fn verify_id_token(
	&self,
	(provider, session): (&Provider, &Session),
) -> Result<Option<IdTokenClaims>> {
	// `require_id_token` implies verification: demanding the assertion and then
	// not checking it would be worse than not demanding it.
	if !provider.verify_id_token && !provider.require_id_token {
		return Ok(None);
	}

	let Some(token) = session.id_token.as_deref() else {
		if provider.require_id_token {
			return Err!(Request(Unauthorized(
				"Provider returned no id_token but require_id_token is set."
			)));
		}

		return Ok(None);
	};

	let issuer = provider
		.issuer_url
		.as_ref()
		.map(Url::as_str)
		.ok_or_else(|| err!(Config("issuer_url", "Missing issuer URL in config")))?;

	let kid = id_token::key_id(token);
	let mut jwks = self.providers.jwks(provider, false).await?;

	if kid
		.as_deref()
		.is_some_and(|kid| jwks.find(kid).is_none())
	{
		debug_warn!(
			provider = provider.id(),
			"id_token names a key id absent from the cached JWKS; re-fetching.",
		);

		jwks = self.providers.jwks(provider, true).await?;
	}

	id_token::verify(token, &jwks, issuer, &provider.client_id, session.query_nonce.as_deref())
		.map(Some)
}

/// Outcome of re-checking an upstream authorization.
///
/// The three cases are deliberately distinct: only an explicit refusal from
/// the provider may end a Matrix session, because a provider outage that
/// looked like a refusal would log out every user at once.
#[derive(Clone, Debug)]
pub enum Recheck {
	/// The provider re-authorized the grant under its current policy.
	Allowed,

	/// The provider refused: the grant is revoked, or its policy no longer
	/// admits this user.
	Denied(String),

	/// The provider could not be reached, or failed transiently. The session
	/// is left intact and the caller answers a retryable error.
	Unavailable(String),
}

/// Re-check every upstream grant a user holds with a provider that demands it.
///
/// Returns `None` when no provider configured with `require_upstream_refresh`
/// applies to this user, which is the case for accounts that never came
/// through SSO — the sealed recovery account and appservice users included.
///
/// A refusal from any applicable provider denies; otherwise a single provider
/// that could not be reached makes the whole answer unavailable. Denial wins
/// over unavailability so a partial outage cannot mask a revocation.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn recheck_user(&self, user_id: &UserId) -> Option<Recheck> {
	let sessions: Vec<(Provider, Session)> = self
		.user_sessions(user_id)
		.ready_filter_map(Result::ok)
		.ready_filter(|(provider, _)| provider.require_upstream_refresh)
		.collect()
		.await;

	if sessions.is_empty() {
		return None;
	}

	let mut unavailable = None;

	for (provider, session) in &sessions {
		match self.recheck_session((provider, session)).await {
			| Recheck::Allowed => (),
			| Recheck::Denied(reason) => return Some(Recheck::Denied(reason)),
			| Recheck::Unavailable(reason) => unavailable = Some(Recheck::Unavailable(reason)),
		}
	}

	Some(unavailable.unwrap_or(Recheck::Allowed))
}

/// Re-check one upstream grant and persist the refreshed grant on success.
///
/// The `refresh_token` grant is preferred: Cloudflare Access re-evaluates its
/// Access policies while serving it ("Cloudflare will use the refresh token to
/// obtain a new access token after checking the user's identity against your
/// Access policies" — Cloudflare One docs, Generic OIDC application), so it is
/// a policy check and not merely a liveness check. A provider or configuration
/// that issued no refresh token falls back to a userinfo request with the
/// stored access token, which proves only that the token is still accepted;
/// that weaker guarantee is recorded here so a deployment relying on it is not
/// misled.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all, fields(provider = provider.id()))]
pub async fn recheck_session(&self, (provider, session): (&Provider, &Session)) -> Recheck {
	let Some(sess_id) = session.sess_id.as_deref() else {
		return Recheck::Denied("Session has no identifier to re-check.".to_owned());
	};

	if let Some(refresh_token) = session.refresh_token.clone() {
		return match self
			.request_refresh((provider, session), &refresh_token)
			.await
		{
			| Err(error) => classify_upstream(&error),
			| Ok(token) => {
				let refreshed = match session.clone().apply_token_response(token) {
					| Ok(refreshed) => refreshed,
					| Err(error) => {
						return Recheck::Unavailable(format!(
							"Provider token response could not be applied: {error}"
						));
					},
				};

				self.sessions.put(&refreshed).await;
				info!(%sess_id, provider = provider.id(), "Upstream grant re-authorized.");

				Recheck::Allowed
			},
		};
	}

	if session.access_token.is_none() {
		return Recheck::Denied("No upstream grant is stored for this session.".to_owned());
	}

	match self.request_userinfo((provider, session)).await {
		| Ok(..) => Recheck::Allowed,
		| Err(error) => classify_upstream(&error),
	}
}

/// Classify a provider failure as a policy denial or a transient failure.
///
/// Only an explicit `4xx` from the provider denies, and `408`/`429` are
/// excluded because they are the provider asking us to come back later. A
/// network failure, a `5xx`, a malformed response, and any local error are all
/// transient by construction: the alternative is a provider outage revoking
/// every session on the server.
#[must_use]
pub fn classify_upstream(error: &Error) -> Recheck {
	let status = match error {
		| Error::Reqwest(error) => error.status(),
		| Error::Request(.., status) => Some(*status),
		| _ => None,
	};

	match status {
		| Some(status)
			if status.is_client_error()
				&& status != StatusCode::REQUEST_TIMEOUT
				&& status != StatusCode::TOO_MANY_REQUESTS =>
			Recheck::Denied(format!("Provider refused the grant: {error}")),

		| _ => Recheck::Unavailable(format!("Provider could not be reached: {error}")),
	}
}

/// Network request exchanging a stored refresh token for a fresh grant.
///
/// No bearer is attached: RFC 6749 §6 authenticates the client with its
/// credentials in the request body, and sending the expiring access token as a
/// bearer here is what makes a re-check fail once it lapses.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn request_refresh(
	&self,
	(provider, _session): (&Provider, &Session),
	refresh_token: &str,
) -> Result<TokenResponse> {
	#[derive(Debug, Serialize)]
	struct RefreshQuery<'a> {
		client_id: &'a str,
		client_secret: &'a str,
		grant_type: &'a str,
		refresh_token: &'a str,
		#[serde(skip_serializing_if = "Option::is_none")]
		scope: Option<&'a str>,
	}

	let client_secret = provider.get_client_secret().await?;
	let scope = provider.scope.iter().join(" ");
	let scope = (!scope.is_empty()).then_some(scope);

	let query = RefreshQuery {
		client_id: &provider.client_id,
		client_secret: &client_secret,
		grant_type: "refresh_token",
		refresh_token,
		scope: scope.as_deref(),
	};

	let url = provider
		.token_url
		.clone()
		.ok_or_else(|| err!(Config("token_url", "Missing token URL in config")))?;

	self.request((Some(provider), None), Method::POST, url, Some(query))
		.await
		.and_then(|value| serde_json::from_value(value).map_err(Into::into))
}

/// Clear the stored upstream grant for every session of a user.
///
/// The provider is asked to revoke the token first when it publishes a
/// revocation endpoint, then the token material is dropped from the record.
/// The identity association is deliberately kept: deleting it would unbind
/// `(iss, sub)` from the account and the next login would register a new one.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn clear_user_grants(&self, user_id: &UserId) {
	let sessions: Vec<(Provider, Session)> = self
		.user_sessions(user_id)
		.ready_filter_map(Result::ok)
		.collect()
		.await;

	for (provider, session) in sessions {
		self.revoke_token((&provider, &session))
			.await
			.log_err()
			.ok();

		let sess_id = session.sess_id.clone();
		let cleared = Session {
			access_token: None,
			refresh_token: None,
			id_token: None,
			expires_at: None,
			expires_in: None,
			refresh_token_expires_at: None,
			refresh_token_expires_in: None,
			token_type: None,
			code_verifier: None,
			query_nonce: None,
			cookie_nonce: None,
			authorize_expires_at: None,
			..session
		};

		self.sessions.put(&cleared).await;

		info!(?sess_id, provider = provider.id(), "Cleared stored upstream grant.");
	}
}

/// Generate a unique-id string determined by the combination of `Provider` and
/// `Session` instances.
#[inline]
pub fn unique_id((provider, session): (&Provider, &Session)) -> Result<String> {
	unique_id_parts((provider, session)).and_then(unique_id_iss_sub)
}

/// Generate a unique-id string determined by the combination of `Provider`
/// instance and `sub` string.
#[inline]
pub fn unique_id_sub((provider, sub): (&Provider, &str)) -> Result<String> {
	unique_id_sub_parts((provider, sub)).and_then(unique_id_iss_sub)
}

/// Generate a unique-id string determined by the combination of `issuer_url`
/// and `Session` instance.
#[inline]
pub fn unique_id_iss((iss, session): (&str, &Session)) -> Result<String> {
	unique_id_iss_parts((iss, session)).and_then(unique_id_iss_sub)
}

/// Generate a unique-id string determined by the `issuer_url` and the `sub`
/// strings directly.
pub fn unique_id_iss_sub((iss, sub): (&str, &str)) -> Result<String> {
	let hash = sha256::delimited([iss, sub].iter());
	let b64 = b64encode.encode(hash);

	Ok(b64)
}

fn unique_id_parts<'a>(
	(provider, session): (&'a Provider, &'a Session),
) -> Result<(&'a str, &'a str)> {
	identity_issuer(provider)
		.ok_or_else(|| err!(Config("issuer_url", "issuer_url not found for this provider.")))
		.and_then(|iss| unique_id_iss_parts((iss, session)))
}

fn unique_id_sub_parts<'a>(
	(provider, sub): (&'a Provider, &'a str),
) -> Result<(&'a str, &'a str)> {
	identity_issuer(provider)
		.ok_or_else(|| err!(Config("issuer_url", "issuer_url not found for this provider.")))
		.map(|iss| (iss, sub))
}

/// Issuer string used as input to the identity hash. Pinned per-brand for
/// providers whose published issuer has changed under us, so existing account
/// associations survive the change.
fn identity_issuer(provider: &Provider) -> Option<&str> {
	match provider.brand.as_str() {
		| "github" => Some("https://github.com/"),
		| _ => provider.issuer_url.as_ref().map(Url::as_str),
	}
}

fn unique_id_iss_parts<'a>((iss, session): (&'a str, &'a Session)) -> Result<(&'a str, &'a str)> {
	session
		.user_info
		.as_ref()
		.map(|user_info| user_info.sub.as_str())
		.ok_or_else(|| err!(Request(NotFound("user_info not found for this session."))))
		.map(|sub| (iss, sub))
}
