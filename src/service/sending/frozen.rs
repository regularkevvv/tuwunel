//! Substitute persisted transaction bytes before Ruma adds authentication.

use std::sync::Arc;

use ruma::api::{
	BytesBody, Metadata, OutgoingBody, OutgoingRequest,
	error::IntoHttpError,
	path_builder::{PathBuilder, SinglePath},
};
use tuwunel_core::{Error, Result};

#[derive(Clone, Debug)]
pub(crate) struct FrozenRequest<T> {
	request: T,
	body: Arc<[u8]>,
}

impl<T> FrozenRequest<T> {
	pub(crate) fn new(request: T, body: Arc<[u8]>) -> Self { Self { request, body } }
}

impl<T: OutgoingRequest> Metadata for FrozenRequest<T> {
	type Authentication = T::Authentication;
	type PathBuilder = T::PathBuilder;

	const METHOD: http::Method = T::METHOD;
	const PATH_BUILDER: Self::PathBuilder = T::PATH_BUILDER;
	const RATE_LIMITED: bool = T::RATE_LIMITED;
}

impl<T: OutgoingRequest> OutgoingRequest for FrozenRequest<T> {
	type Body = BytesBody;
	type EndpointError = T::EndpointError;
	type IncomingResponse = T::IncomingResponse;

	fn try_into_http_request_inner(
		self,
		base_url: &str,
		path_input: <Self::PathBuilder as PathBuilder>::Input<'_>,
	) -> std::result::Result<http::Request<Self::Body>, IntoHttpError> {
		let (mut parts, _) = self
			.request
			.try_into_http_request_inner(base_url, path_input)?
			.into_parts();
		parts.headers.remove(http::header::CONTENT_LENGTH);
		Ok(http::Request::from_parts(parts, BytesBody(self.body.to_vec())))
	}
}

pub(super) fn body<T: OutgoingRequest<PathBuilder = SinglePath>>(request: T) -> Result<Vec<u8>> {
	request
		.try_into_http_request_inner("http://localhost", ())
		.map_err(|_| Error::bad_database("Cannot serialize outgoing transaction"))?
		.into_body()
		.try_into_buf::<Vec<u8>>()
		.map_err(|_| Error::bad_database("Cannot serialize outgoing transaction body"))
}

#[cfg(test)]
mod tests {
	use ruma::{
		MilliSecondsSinceUnixEpoch,
		api::{
			OutgoingRequestExt,
			federation::{
				authentication::ServerSignaturesInput, transactions::send_transaction_message,
			},
		},
		server_name,
		signatures::Ed25519KeyPair,
		uint,
	};

	use super::{FrozenRequest, body};

	#[test]
	fn federation_authentication_signs_the_saved_body() {
		let key = Ed25519KeyPair::from_der(&Ed25519KeyPair::generate(), "fixture".into())
			.expect("owned generated signing key");
		let origin = server_name!("localhost").to_owned();
		let destination = server_name!("remote.example").to_owned();
		let auth = ServerSignaturesInput::new(origin.clone(), destination, &key);
		let mut request = send_transaction_message::v1::Request {
			transaction_id: "matrix-0000000000000001".into(),
			origin,
			origin_server_ts: MilliSecondsSinceUnixEpoch(uint!(1234)),
			pdus: Vec::new(),
			edus: Vec::new(),
		};
		let saved = body(request.clone()).expect("serialized transaction");
		let original = request
			.clone()
			.try_into_http_request::<Vec<u8>>("https://remote.example", auth.clone(), ())
			.expect("signed original");
		request.origin_server_ts = MilliSecondsSinceUnixEpoch(uint!(9876));
		let changed = request
			.clone()
			.try_into_http_request::<Vec<u8>>("https://remote.example", auth.clone(), ())
			.expect("signed changed stub");
		let frozen = FrozenRequest::new(request, saved.into())
			.try_into_http_request::<Vec<u8>>("https://remote.example", auth, ())
			.expect("signed frozen replay");
		assert_ne!(
			changed.headers()[http::header::AUTHORIZATION],
			original.headers()[http::header::AUTHORIZATION],
			"changed timestamp changes authenticated content"
		);
		assert_eq!(frozen.uri(), original.uri());
		assert_eq!(frozen.body(), original.body());
		assert_eq!(
			frozen.headers()[http::header::AUTHORIZATION],
			original.headers()[http::header::AUTHORIZATION],
			"Ruma signs saved bytes, not the reconstruction stub"
		);
	}
}
