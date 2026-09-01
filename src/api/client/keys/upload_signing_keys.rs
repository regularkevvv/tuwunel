use axum::extract::State;
use ruma::{
	UserId,
	api::client::{
		keys::upload_signing_keys,
		uiaa::{AuthFlow, AuthType, UiaaInfo},
	},
	encryption::CrossSigningKey,
	serde::Raw,
};
use serde_json::{json, value::to_raw_value};
use tuwunel_core::{
	Err, Error, Result, debug, debug_error, err,
	result::NotFound,
	utils,
	utils::{BoolExt, OptionExt},
};
use tuwunel_service::{Services, uiaa::SESSION_ID_LENGTH, users::parse_master_key};

use crate::{Ruma, router::auth_uiaa};

/// # `POST /_matrix/client/r0/keys/device_signing/upload`
///
/// Uploads end-to-end key information for the sender user.
///
/// - Requires UIAA to verify password
/// - For OIDC devices, requires OAuth re-authentication via SSO (MSC4312)
/// - For appservices with `device_management` enabled, UIAA is skipped even
///   when cross-signing keys already exist (MSC4190)
pub(crate) async fn upload_signing_keys_route(
	State(services): State<crate::State>,
	body: Ruma<upload_signing_keys::v3::Request>,
) -> Result<upload_signing_keys::v3::Response> {
	let sender_user = body.sender_user();

	// Access token is required for this endpoint regardless of conditional UIAA so
	// we'll always have a sender_user.
	if let Ok(exists) = check_for_new_keys(
		&services,
		sender_user,
		body.self_signing_key.as_ref(),
		body.user_signing_key.as_ref(),
		body.master_key.as_ref(),
	)
	.await
	.inspect_err(|e| debug_error!(?e))
	{
		if let Some(result) = exists {
			// No-op, they tried to reupload the same set of keys
			// (lost connection for example)
			return Ok(result);
		}

		// Some of the keys weren't found, so we let them upload
		debug!("Skipping UIA as per MSC3967: user had no existing keys");
		return persist_signing_keys(&services, &body).await;
	}

	// MSC4190: appservices with device_management may replace existing
	// cross-signing keys without UIAA.
	if body
		.appservice_info
		.as_ref()
		.is_some_and(|appservice| appservice.registration.device_management)
	{
		debug!(
			"Skipping UIAA for {sender_user} as this is from an appservice and MSC4190 is \
			 enabled"
		);

		return persist_signing_keys(&services, &body).await;
	}

	let is_oidc = body
		.sender_device()
		.ok()
		.map_async(|sender_device| {
			services
				.users
				.is_oidc_device(sender_user, sender_device)
		})
		.await
		.unwrap_or(false);

	// MSC4312: OIDC devices require OAuth re-authentication for cross-signing
	// reset. If a bypass was granted via SSO re-auth, skip UIAA entirely.
	if is_oidc
		&& services
			.users
			.can_replace_cross_signing_keys(sender_user)
			.await
	{
		return persist_signing_keys(&services, &body).await;
	}

	// First attempt from OIDC device: issue m.oauth flow.
	if is_oidc && body.auth.is_none() {
		return Err(Error::Uiaa(create_oauth_uiaa(&services, sender_user, &body).await?));
	}

	let authed_user = auth_uiaa(&services, &body).await?;

	assert_eq!(sender_user, authed_user, "Expected UIAA of {sender_user} and not {authed_user}");
	persist_signing_keys(&services, &body).await
}

async fn persist_signing_keys(
	services: &Services,
	body: &Ruma<upload_signing_keys::v3::Request>,
) -> Result<upload_signing_keys::v3::Response> {
	services
		.users
		.add_cross_signing_keys(
			body.sender_user(),
			&body.master_key,
			&body.self_signing_key,
			&body.user_signing_key,
			true, // notify so that other users see the new keys
		)
		.await?;

	Ok(upload_signing_keys::v3::Response {})
}

async fn create_oauth_uiaa(
	services: &Services,
	sender_user: &UserId,
	body: &Ruma<upload_signing_keys::v3::Request>,
) -> Result<UiaaInfo> {
	let session = utils::random_string(SESSION_ID_LENGTH);
	let issuer = services.oauth.get_server()?.issuer_url()?;
	let base = issuer.trim_end_matches('/');
	let url = format!("{base}/_tuwunel/oidc/account?action=org.matrix.cross_signing_reset");

	let uiaainfo = UiaaInfo {
		flows: vec![AuthFlow { stages: vec![AuthType::OAuth] }],
		params: Some(to_raw_value(&json!({"m.oauth": { "url": url }}))?),
		session: Some(session),
		..Default::default()
	};

	services
		.uiaa
		.create(
			sender_user,
			body.sender_device()?,
			&uiaainfo,
			body.json_body
				.as_ref()
				.ok_or_else(|| err!(Request(NotJson("JSON body is not valid"))))?,
		)
		.await;

	Ok(uiaainfo)
}

async fn check_for_new_keys(
	services: &Services,
	user_id: &UserId,
	self_signing_key: Option<&Raw<CrossSigningKey>>,
	user_signing_key: Option<&Raw<CrossSigningKey>>,
	master_signing_key: Option<&Raw<CrossSigningKey>>,
) -> Result<Option<upload_signing_keys::v3::Response>> {
	debug!("checking for existing keys");

	let empty = match master_signing_key {
		| Some(new_master) => !master_key_matches(services, user_id, new_master).await?,
		| None => false,
	};

	if let Some(new_user_signing) = user_signing_key {
		let fetched = services.users.get_user_signing_key(user_id).await;

		if fetched.is_not_found() {
			if !empty {
				return Err!(Request(Forbidden(
					"Tried to update an existing user signing key, UIA required"
				)));
			}
		} else if fetched?.deserialize()? != new_user_signing.deserialize()? {
			return Err!(Request(Forbidden(
				"Tried to change an existing user signing key, UIA required"
			)));
		}
	}

	if let Some(new_self_signing) = self_signing_key {
		let fetched = services
			.users
			.get_self_signing_key(None, user_id, &|_| true)
			.await;

		if fetched.is_not_found() {
			if !empty {
				return Err!(Request(Forbidden(
					"Tried to add a new signing key independently from the master key"
				)));
			}
		} else if fetched?.deserialize()? != new_self_signing.deserialize()? {
			return Err!(Request(Forbidden(
				"Tried to update an existing self signing key, UIA required"
			)));
		}
	}

	Ok(empty
		.is_false()
		.into_option()
		.map(|()| upload_signing_keys::v3::Response {}))
}

/// Returns `true` if the user already has a master key matching `new_master`,
/// `false` if they have no master key. Returns `Err` on mismatch or any other
/// error.
async fn master_key_matches(
	services: &Services,
	user_id: &UserId,
	new_master: &Raw<CrossSigningKey>,
) -> Result<bool> {
	let (new_id, new_value) = parse_master_key(user_id, new_master)?;
	let existing = services
		.users
		.get_master_key(None, user_id, &|_| true)
		.await;

	if existing.is_not_found() {
		return Ok(false);
	}

	let (existing_id, existing_value) = parse_master_key(user_id, &existing?)?;
	if existing_id != new_id || existing_value != new_value {
		return Err!(Request(Forbidden("Tried to change an existing master key, UIA required")));
	}

	Ok(true)
}
