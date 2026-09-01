use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as b64};
use serde::{Deserialize, Serialize};
use tuwunel_core::{
	Err, Result, err, implement,
	utils::{hash::sha256, time::now_secs},
};
use tuwunel_database::{Cbor, Deserialized};

// Bounds the per-row footprint so an unauthenticated DCR endpoint cannot
// evict every other client from the FIFO cache with one huge record.
const MAX_REGISTRATION_BYTES: usize = 4096;

// Grant and response types the server understands; MSC2966 requires dropping
// any others from a registration before it is stored and echoed.
const KNOWN_GRANT_TYPES: [&str; 2] = ["authorization_code", "refresh_token"];
const KNOWN_RESPONSE_TYPES: [&str; 1] = ["code"];

#[derive(Debug, Deserialize, Serialize)]
pub struct DcrRequest {
	pub redirect_uris: Vec<String>,
	pub client_name: Option<String>,
	pub client_uri: Option<String>,
	pub logo_uri: Option<String>,
	#[serde(default)]
	pub contacts: Vec<String>,
	pub token_endpoint_auth_method: Option<String>,
	pub grant_types: Option<Vec<String>>,
	pub response_types: Option<Vec<String>>,
	pub application_type: Option<String>,
	pub policy_uri: Option<String>,
	pub tos_uri: Option<String>,
	pub software_id: Option<String>,
	pub software_version: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClientRegistration {
	pub client_id: String,
	pub redirect_uris: Vec<String>,
	pub client_name: Option<String>,
	pub client_uri: Option<String>,
	pub logo_uri: Option<String>,
	pub contacts: Vec<String>,
	pub token_endpoint_auth_method: String,
	pub grant_types: Vec<String>,
	pub response_types: Vec<String>,
	pub application_type: Option<String>,
	pub policy_uri: Option<String>,
	pub tos_uri: Option<String>,
	pub software_id: Option<String>,
	pub software_version: Option<String>,
	pub registered_at: u64,
}

#[implement(super::Server)]
pub async fn register_client(&self, request: DcrRequest) -> Result<ClientRegistration> {
	let request = normalize(request);
	let serialized = serde_json::to_vec(&request).expect("DcrRequest is always serializable");

	if serialized.len() > MAX_REGISTRATION_BYTES {
		return Err!(Request(TooLarge(
			"Client registration exceeds {MAX_REGISTRATION_BYTES} byte limit"
		)));
	}

	let client_id = b64.encode(sha256::hash(&serialized));

	if let Ok(existing) = self.get_client(&client_id).await {
		return Ok(existing);
	}

	let auth_method = request
		.token_endpoint_auth_method
		.unwrap_or_else(|| "none".to_owned());

	let response_types = request
		.response_types
		.unwrap_or_else(|| vec!["code".to_owned()]);

	let grant_types = request
		.grant_types
		.unwrap_or_else(|| vec!["authorization_code".to_owned(), "refresh_token".to_owned()]);

	let registration = ClientRegistration {
		client_id,
		redirect_uris: request.redirect_uris,
		client_name: request.client_name,
		client_uri: request.client_uri,
		logo_uri: request.logo_uri,
		contacts: request.contacts,
		token_endpoint_auth_method: auth_method,
		grant_types,
		response_types,
		application_type: request.application_type,
		policy_uri: request.policy_uri,
		tos_uri: request.tos_uri,
		software_id: request.software_id,
		software_version: request.software_version,
		registered_at: now_secs(),
	};

	self.db
		.oidcclientid_registration
		.raw_put(&*registration.client_id, Cbor(&registration))
		.await?;

	Ok(registration)
}

#[implement(super::Server)]
pub async fn get_client(&self, client_id: &str) -> Result<ClientRegistration> {
	self.db
		.oidcclientid_registration
		.get(client_id)
		.await
		.deserialized::<Cbor<_>>()
		.map(|cbor: Cbor<ClientRegistration>| cbor.0)
		.map_err(|_| err!(Request(NotFound("Unknown client_id"))))
}

fn normalize(mut request: DcrRequest) -> DcrRequest {
	request.redirect_uris.sort();
	request.contacts.sort();
	prune(&mut request.grant_types, &KNOWN_GRANT_TYPES);
	prune(&mut request.response_types, &KNOWN_RESPONSE_TYPES);

	request
}

fn prune(types: &mut Option<Vec<String>>, known: &[&str]) {
	let Some(types) = types else {
		return;
	};

	types.retain(|ty| known.contains(&ty.as_str()));
	types.sort();
}
