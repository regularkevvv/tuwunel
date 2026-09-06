use axum::extract::State;
use ruma::{
	DeviceId, UserId,
	api::{
		client::session::refresh_token::v3::{Request, Response},
		error::{ErrorKind, UnknownTokenErrorData},
	},
};
use tuwunel_core::{
	Err, Error, Result, debug_info, info,
	utils::{BoolExt, future::OptionFutureExt, time::timepoint_has_passed},
	warn,
};
use tuwunel_service::{
	Services,
	oauth::Recheck,
	users::device::{RefreshToken, generate_refresh_token},
};

use crate::{ClientIp, Ruma};

/// # `POST /_matrix/client/v3/refresh`
///
/// Refresh an access token.
///
/// <https://spec.matrix.org/v1.15/client-server-api/#post_matrixclientv3refresh>
#[tracing::instrument(skip_all, fields(%client), name = "refresh_token")]
pub(crate) async fn refresh_token_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<Request>,
) -> Result<Response> {
	let refresh_token_claim = body.body.refresh_token;

	if !refresh_token_claim.starts_with("refresh_") {
		return Err!(Request(Forbidden("Refresh token is malformed.")));
	}

	match services
		.users
		.classify_refresh_token(&refresh_token_claim)
		.await
	{
		| RefreshToken::Current { user_id, device_id, expires_at } => {
			upstream_gate(&services, &user_id, &device_id).await?;

			if expires_at.is_some_and(timepoint_has_passed) {
				let hard = services.server.config.refresh_token_hard_logout;
				hard.then_async(|| services.users.remove_device(&user_id, &device_id))
					.unwrap_or_else_async(async || {
						services
							.users
							.remove_refresh_token(&user_id, &device_id)
							.await
							.ok();
					})
					.await;

				return Err(Error::BadRequest(
					ErrorKind::UnknownToken(UnknownTokenErrorData { soft_logout: !hard }),
					"Refresh token has expired.",
				));
			}

			let refresh_token = Some(generate_refresh_token());
			let (access_token, expires_in_ms) = services.users.generate_access_token(true);

			services
				.users
				.set_access_token(
					&user_id,
					&device_id,
					&access_token,
					expires_in_ms,
					refresh_token.as_deref(),
				)
				.await?;

			debug_info!(?user_id, ?device_id, ?expires_in_ms, "refreshed their access_token",);

			Ok(Response {
				access_token,
				refresh_token,
				expires_in_ms,
			})
		},

		| RefreshToken::Replayed { user_id, device_id, current, grace } if grace => {
			upstream_gate(&services, &user_id, &device_id).await?;

			// Benign double-submit: re-issue an access token for the unchanged
			// refresh token rather than rotating it.
			let (access_token, expires_in_ms) = services.users.generate_access_token(true);

			services
				.users
				.set_access_token(&user_id, &device_id, &access_token, expires_in_ms, None)
				.await?;

			Ok(Response {
				access_token,
				refresh_token: Some(current),
				expires_in_ms,
			})
		},

		| RefreshToken::Replayed { user_id, device_id, .. } => {
			let revoke = services.server.config.refresh_token_reuse_revoke;
			debug_info!(?user_id, ?device_id, revoke, "refresh token reused after rotation");

			if revoke {
				services
					.users
					.remove_device(&user_id, &device_id)
					.await;
			}

			Err(Error::BadRequest(
				ErrorKind::UnknownToken(UnknownTokenErrorData { soft_logout: !revoke }),
				"Refresh token has already been used.",
			))
		},

		| RefreshToken::Unknown => Err!(Request(Forbidden("Refresh token is unrecognized."))),
	}
}

/// Re-check the upstream authorization before a Matrix access token is issued.
///
/// ADR-0004 makes a successful upstream policy re-check a precondition for a
/// new Matrix access token, which is what turns a short `access_token_ttl`
/// into a bound on how long a revoked or newly-denied identity keeps a Matrix
/// session. Providers that do not opt in through `require_upstream_refresh`,
/// and accounts that never came through SSO, pass straight through.
///
/// A refusal removes the device and answers `M_UNKNOWN_TOKEN` with
/// `soft_logout: false`: the identity is gone, not merely stale, so the client
/// must not keep the device and retry. An unreachable provider answers
/// `M_CONNECTION_FAILED` (HTTP 502) and revokes nothing, so an Access outage
/// costs refreshes rather than every session on the server; the session then
/// outlives the policy for as long as the outage lasts, the bounded tolerance
/// this design accepts.
///
/// Audit lines carry the user, device and provider only. No access token,
/// refresh token, authorization code or upstream grant is logged (plan.md,
/// non-negotiable invariant 7).
async fn upstream_gate(services: &Services, user_id: &UserId, device_id: &DeviceId) -> Result {
	match services.oauth.recheck_user(user_id).await {
		| None | Some(Recheck::Allowed) => Ok(()),

		| Some(Recheck::Denied(reason)) => {
			warn!(
				%user_id,
				%device_id,
				%reason,
				"Upstream identity provider refused the session; revoking the device.",
			);

			services
				.users
				.remove_device(user_id, device_id)
				.await;

			Err(Error::BadRequest(
				ErrorKind::UnknownToken(UnknownTokenErrorData { soft_logout: false }),
				"The identity provider no longer authorizes this session.",
			))
		},

		| Some(Recheck::Unavailable(reason)) => {
			info!(
				%user_id,
				%device_id,
				%reason,
				"Upstream identity provider unreachable; refusing to refresh without revoking.",
			);

			Err!(Request(ConnectionFailed(
				"The identity provider could not be reached to re-authorize this session."
			)))
		},
	}
}
