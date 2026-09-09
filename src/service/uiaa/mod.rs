mod lifecycle;
mod requests;
mod sweep;
mod transitions;

use std::{
	ops::ControlFlow,
	sync::{Arc, RwLock},
	time::Instant,
};

use ruma::{
	CanonicalJsonValue, DeviceId, OwnedDeviceId, OwnedUserId, UserId,
	api::{
		client::uiaa::{
			AuthData, AuthType, EmailIdentity, Password, ThirdpartyIdCredentials, UiaaInfo,
			UserIdentifier,
		},
		error::{ErrorKind, LimitExceededErrorData, StandardErrorBody},
	},
};
use tuwunel_core::{
	Err, Result, err, error, extract, implement,
	utils::{self, BoolExt, MutexMap, hash::verify_password, string::EMPTY},
};
use tuwunel_database::{Database, Map};

pub struct Service {
	userdevicesessionid_uiaarequest: RwLock<requests::Requests>,
	transitions: MutexMap<transitions::SessionKey, ()>,
	admission: tokio::sync::Mutex<()>,
	sweep_cursor: tokio::sync::Mutex<sweep::Cursors>,
	db: Data,
	services: Arc<crate::services::OnceServices>,
}

struct Data {
	database: Arc<Database>,
	uiaasessionid_metadata: Arc<Map>,
	userdevicesessionid_uiaainfo: Arc<Map>,
}

type RequestKey = (OwnedUserId, OwnedDeviceId, String);

pub const SESSION_ID_LENGTH: usize = 32;

#[derive(Clone, Copy)]
enum EmailIdentityMode {
	Validate,
	Claim,
}

#[async_trait::async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			userdevicesessionid_uiaarequest: RwLock::new(requests::Requests::default()),
			transitions: MutexMap::new(),
			admission: tokio::sync::Mutex::new(()),
			sweep_cursor: tokio::sync::Mutex::new(sweep::Cursors::default()),
			db: Data {
				database: args.db.clone(),
				uiaasessionid_metadata: args.db["uiaasessionid_metadata"].clone(),
				userdevicesessionid_uiaainfo: args.db["userdevicesessionid_uiaainfo"].clone(),
			},
			services: args.services.clone(),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		loop {
			tokio::select! {
				result = self.sweep_sessions() => result?,
				() = self.services.server.until_shutdown() => return Ok(()),
			}
			tokio::select! {
				() = tokio::time::sleep(sweep::INTERVAL) => {},
				() = self.services.server.until_shutdown() => return Ok(()),
			}
		}
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Creates a new Uiaa session. Make sure the session token is unique.
#[implement(Service)]
pub async fn create(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	uiaainfo: &UiaaInfo,
	json_body: &CanonicalJsonValue,
) -> Result {
	let session = uiaainfo
		.session
		.as_ref()
		.ok_or_else(|| err!(Request(InvalidParam("Missing UIAA session identifier."))))?;

	let _transition = self
		.transitions
		.lock(&transitions::SessionKey::new(session))
		.await;

	self.set_uiaa_request(user_id, device_id, session, json_body)?;

	let result = self
		.save_progress(user_id, device_id, session, uiaainfo, true)
		.await;
	if result.is_err() {
		self.remove_uiaa_request(user_id, device_id, session);
	}
	result
}

/// Authenticate one stage without taking ownership of an email proof.
///
/// Generic UIAA consumers may validate email identity, but only registration
/// assigns a durable owner to that proof.
#[implement(Service)]
pub async fn try_auth(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	auth: &AuthData,
	uiaainfo: &UiaaInfo,
) -> Result<(bool, UiaaInfo)> {
	self.try_auth_inner(user_id, device_id, auth, uiaainfo, EmailIdentityMode::Validate)
		.await
}

/// Authenticate one registration stage and claim an email proof when present.
///
/// The claim is tied to the exact user, device, and UIAA session tuple before
/// the email stage is recorded as complete.
#[implement(Service)]
pub async fn try_auth_registration(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	auth: &AuthData,
	uiaainfo: &UiaaInfo,
) -> Result<(bool, UiaaInfo)> {
	self.try_auth_inner(user_id, device_id, auth, uiaainfo, EmailIdentityMode::Claim)
		.await
}

#[implement(Service)]
async fn try_auth_inner(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	auth: &AuthData,
	uiaainfo: &UiaaInfo,
	email_identity_mode: EmailIdentityMode,
) -> Result<(bool, UiaaInfo)> {
	// Serialize read, stage side effects, and consumption with SSO completion.
	// The database writer lease supplies cross-process exclusion; this lock
	// prevents two requests in that writer from spending the same proof.
	let session = auth
		.session()
		.map(ToOwned::to_owned)
		.unwrap_or_else(|| utils::random_string(SESSION_ID_LENGTH));
	let _transition = self
		.transitions
		.lock(&transitions::SessionKey::new(&session))
		.await;

	let mut uiaainfo = self
		.load_session(user_id, device_id, auth, uiaainfo, session)
		.await?;

	match auth {
		// Completed stages are polled, not executed again. In particular a
		// registration-token retry must not spend a second token use.
		| _ if auth
			.auth_type()
			.is_some_and(|stage| uiaainfo.completed.contains(&stage)) => {},
		// Find out what the user completed
		| AuthData::Password(password) => {
			if let ControlFlow::Break(authed) = self
				.verify_password(user_id, &mut uiaainfo, password)
				.await?
			{
				return Ok((authed, uiaainfo));
			}
		},
		| AuthData::RegistrationToken(t) => {
			let token = t.token.trim();
			match self
				.services
				.registration_tokens
				.try_consume(token)
				.await
			{
				| Ok(()) => {
					uiaainfo
						.completed
						.push(AuthType::RegistrationToken);
				},
				| Err(error) if error.kind() == ErrorKind::forbidden() => {
					uiaainfo.auth_error = Some(Box::new(StandardErrorBody {
						kind: ErrorKind::forbidden(),
						message: "Invalid registration token.".to_owned(),
					}));

					return Ok((false, uiaainfo));
				},
				| Err(error) => return Err(error),
			}
		},
		| AuthData::FallbackAcknowledgement(_session) => {
			// A fallback acknowledgement is a session re-poll. The fallback
			// web handler (e.g. the SSO callback) is what records completion.
		},
		| AuthData::OAuth(_) => {
			// MSC4312: OAuth cross-signing reset uses SSO re-authentication.
			// If a bypass was granted via SSO re-auth, mark OAuth as completed.
			if !uiaainfo.completed.contains(&AuthType::OAuth) {
				if self
					.services
					.users
					.can_replace_cross_signing_keys(user_id)
					.await
				{
					uiaainfo.completed.push(AuthType::OAuth);
				} else {
					uiaainfo.auth_error = Some(Box::new(StandardErrorBody {
						kind: ErrorKind::forbidden(),
						message: "OAuth cross-signing reset not approved for this session."
							.to_owned(),
					}));

					return Ok((false, uiaainfo));
				}
			}
		},
		| AuthData::Dummy(_) => {
			uiaainfo.completed.push(AuthType::Dummy);
		},
		| AuthData::Terms(_) => {
			// MSC1692: an empty auth dict accepts every presented policy.
			uiaainfo.completed.push(AuthType::Terms);
		},
		| AuthData::EmailIdentity(EmailIdentity { thirdparty_id_creds, .. }) => {
			// A stray id_server is tolerated and id_access_token is never required.
			let validated = self
				.authenticate_email_identity(
					user_id,
					device_id,
					&uiaainfo,
					thirdparty_id_creds,
					email_identity_mode,
				)
				.await?;

			if !validated {
				uiaainfo.auth_error = Some(Box::new(StandardErrorBody {
					kind: ErrorKind::forbidden(),
					message: "Email address has not been validated.".to_owned(),
				}));

				return Ok((false, uiaainfo));
			}

			uiaainfo.completed.push(AuthType::EmailIdentity);
		},
		| _ => error!("UIAA authentication type not supported"),
	}

	// Check if a flow now succeeds
	let mut completed = false;
	'flows: for flow in &mut uiaainfo.flows {
		for stage in &flow.stages {
			if !uiaainfo.completed.contains(stage) {
				continue 'flows;
			}
		}
		// We didn't break, so this flow succeeded!
		completed = true;
	}

	let session = uiaainfo
		.session
		.as_ref()
		.expect("session is always set");

	if matches!(email_identity_mode, EmailIdentityMode::Claim)
		&& uiaainfo
			.completed
			.contains(&AuthType::EmailIdentity)
	{
		let claim = (user_id.to_owned(), device_id.to_owned(), session.as_str().into());

		if !self
			.services
			.threepid
			.refresh_claim(&claim)
			.await?
		{
			uiaainfo
				.completed
				.retain(|stage| stage != &AuthType::EmailIdentity);

			uiaainfo.auth_error = Some(Box::new(StandardErrorBody {
				kind: ErrorKind::forbidden(),
				message: "Email address has not been validated.".to_owned(),
			}));

			self.save_progress(user_id, device_id, session, &uiaainfo, false)
				.await?;

			return Ok((false, uiaainfo));
		}
	}

	if !completed {
		self.save_progress(user_id, device_id, session, &uiaainfo, false)
			.await?;

		return Ok((false, uiaainfo));
	}

	// Retain the session until registration spends its email claim.
	let retain_session = matches!(email_identity_mode, EmailIdentityMode::Claim)
		&& uiaainfo
			.completed
			.contains(&AuthType::EmailIdentity);

	self.finish_session(user_id, device_id, session, &uiaainfo, retain_session)
		.await?;

	Ok((true, uiaainfo))
}

#[implement(Service)]
async fn authenticate_email_identity(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	uiaainfo: &UiaaInfo,
	creds: &ThirdpartyIdCredentials,
	mode: EmailIdentityMode,
) -> Result<bool> {
	match mode {
		| EmailIdentityMode::Validate => Ok(self
			.services
			.threepid
			.session_validated(creds.sid.as_str(), creds.client_secret.as_str())
			.await),
		| EmailIdentityMode::Claim => {
			let session = uiaainfo
				.session
				.as_ref()
				.expect("session is always set");

			let claim = (user_id.to_owned(), device_id.to_owned(), session.as_str().into());

			self.services
				.threepid
				.claim_validated(creds.sid.as_str(), creds.client_secret.as_str(), claim)
				.await
		},
	}
}

#[implement(Service)]
async fn verify_password(
	&self,
	user_id: &UserId,
	uiaainfo: &mut UiaaInfo,
	password: &Password,
) -> Result<ControlFlow<bool>> {
	let Password { identifier, password, user, .. } = password;

	let username = extract!(identifier, x in Some(UserIdentifier::Matrix(ruma::api::client::uiaa::MatrixUserIdentifier { user: x, .. })))
		.or_else(|| cfg!(feature = "element_hacks").and(user.as_ref()))
		.ok_or(err!(Request(Unrecognized("Identifier type not recognized."))))?;

	let user_id_from_username =
		UserId::parse_with_server_name(username.clone(), self.services.globals.server_name())
			.map_err(|_| err!(Request(InvalidParam("User ID is invalid."))))?;

	// Check if the access token being used matches the credentials used for UIAA
	if user_id != user_id_from_username {
		return Err!(Request(Forbidden("User ID and access token mismatch.")));
	}

	let user_id = user_id_from_username;
	// First try local password hash verification
	let password_verified = self
		.services
		.users
		.password_hash(&user_id)
		.await
		.is_ok_and(|hash| verify_password(password, &hash).is_ok());

	// Only LDAP-origin accounts fall back to LDAP; others would trigger a
	// directory-wide search.
	#[cfg(feature = "ldap")]
	let password_verified = if !password_verified
		&& self.services.server.config.ldap.enable
		&& self
			.services
			.users
			.origin(&user_id)
			.await
			.is_ok_and(|origin| origin == "ldap")
		&& let Ok(dns) = self.services.users.search_ldap(&user_id).await
		&& let Some((user_dn, _is_admin)) = dns.first()
	{
		self.services
			.users
			.auth_ldap(user_dn, password)
			.await
			.is_ok()
	} else {
		password_verified
	};

	if !password_verified {
		uiaainfo.auth_error = Some(Box::new(StandardErrorBody {
			kind: ErrorKind::forbidden(),
			message: "Invalid username or password.".to_owned(),
		}));

		return Ok(ControlFlow::Break(false));
	}

	uiaainfo.completed.push(AuthType::Password);

	Ok(ControlFlow::Continue(()))
}

#[implement(Service)]
fn set_uiaa_request(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	session: &str,
	request: &CanonicalJsonValue,
) -> Result {
	let key = (user_id.to_owned(), device_id.to_owned(), session.to_owned());

	self.userdevicesessionid_uiaarequest
		.write()
		.expect("locked for writing")
		.insert(key, request, Instant::now())
		.map_err(|error| match error {
			| requests::Refusal::BodyTooLarge =>
				err!(Request(TooLarge("UIAA request body exceeds the cache limit."))),
			| requests::Refusal::Duplicate =>
				err!(Request(InvalidParam("UIAA session already exists."))),
			| requests::Refusal::Capacity => tuwunel_core::Error::Request(
				ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
				"Too many pending UIAA requests.".into(),
				http::StatusCode::TOO_MANY_REQUESTS,
			),
		})
}

#[implement(Service)]
fn remove_uiaa_request(&self, user_id: &UserId, device_id: &DeviceId, session: &str) {
	let key = (user_id.to_owned(), device_id.to_owned(), session.to_owned());
	self.userdevicesessionid_uiaarequest
		.write()
		.expect("locked for writing")
		.remove(&key);
}

#[implement(Service)]
pub fn get_uiaa_request(
	&self,
	user_id: &UserId,
	device_id: Option<&DeviceId>,
	session: &str,
) -> Option<CanonicalJsonValue> {
	let device_id = device_id.unwrap_or_else(|| EMPTY.into());
	let key = (user_id.to_owned(), device_id.to_owned(), session.to_owned());

	self.userdevicesessionid_uiaarequest
		.write()
		.expect("locked for writing")
		.get(&key, Instant::now())
}
