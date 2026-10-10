use tuwunel_core::{Result, debug, debug_error, debug_info, debug_warn, implement, trace};

use super::DestString;
use crate::client::read_response_capped;

#[implement(super::Service)]
#[tracing::instrument(
	name = "well-known",
	level = "debug",
	ret(level = "debug"),
	skip(self)
)]
pub(super) async fn request_well_known(&self, dest: &str) -> Result<Option<DestString>> {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	trace!("Requesting well known for {dest}");
	let response = services_root
		.client
		.well_known
		.get(format!("https://{dest}/.well-known/matrix/server"))
		.send()
		.await;

	trace!("response: {response:?}");
	if let Err(e) = &response {
		debug!("error: {e:?}");
		return Ok(None);
	}

	let response = response?;
	if !response.status().is_success() {
		debug!("response not 2XX");
		return Ok(None);
	}

	let Ok(body) = read_response_capped(response, 12288).await else {
		debug_warn!("response unreadable or exceeds size limit");
		return Ok(None);
	};

	let text = String::from_utf8_lossy(&body);
	trace!("response text: {text:?}");

	let body: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();

	let m_server = body
		.get("m.server")
		.unwrap_or(&serde_json::Value::Null)
		.as_str()
		.unwrap_or_default();

	if ruma::identifiers_validation::server_name::validate(m_server).is_err() {
		debug_error!("response content missing or invalid");
		return Ok(None);
	}

	debug_info!("{dest:?} found at {m_server:?}");
	Ok(Some(m_server.into()))
}
