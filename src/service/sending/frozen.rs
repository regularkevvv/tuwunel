//! Substitute persisted transaction bytes before Ruma adds authentication.

use std::{
	io::{self, Write},
	sync::Arc,
};

#[cfg(test)]
use ruma::api::OutgoingBody;
use ruma::api::{
	BytesBody, Metadata, OutgoingRequest,
	error::IntoHttpError,
	path_builder::{PathBuilder, SinglePath},
};
use serde::Serialize;
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

#[derive(Debug)]
pub(super) enum Body {
	Empty,
	Ready(Vec<u8>),
	TooLarge,
}

pub(super) fn bounded_body<T>(request: T) -> Result<Body>
where
	T: OutgoingRequest<PathBuilder = SinglePath>,
	T::Body: Serialize,
{
	let body = request
		.try_into_http_request_inner("http://localhost", ())
		.map_err(|_| Error::bad_database("Cannot prepare outgoing transaction body"))?
		.into_body();
	bounded_json(&body, super::data::BODY_LIMIT)
}

pub(super) fn bounded_json<T: Serialize>(value: &T, limit: usize) -> Result<Body> {
	let mut writer = BudgetWriter {
		bytes: Vec::new(),
		limit,
		exceeded: false,
	};
	match serde_json::to_writer(&mut writer, value) {
		| Ok(()) => Ok(Body::Ready(writer.bytes)),
		| Err(_) if writer.exceeded => Ok(Body::TooLarge),
		| Err(_) => Err(Error::bad_database("Cannot serialize outgoing transaction body")),
	}
}

/// Bound caller-supplied appservice serializers before retaining their bytes.
pub(super) fn bounded_custom<F>(serialize: F, limit: usize) -> Result<Body>
where
	F: FnOnce(&mut dyn Write) -> Result,
{
	let mut writer = BudgetWriter {
		bytes: Vec::new(),
		limit,
		exceeded: false,
	};
	match serialize(&mut writer) {
		| Ok(()) | Err(_) if writer.exceeded => Ok(Body::TooLarge),
		| Ok(()) => Ok(Body::Ready(writer.bytes)),
		| Err(error) => Err(error),
	}
}

struct BudgetWriter {
	bytes: Vec<u8>,
	limit: usize,
	exceeded: bool,
}

impl Write for BudgetWriter {
	fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
		let length = self.bytes.len().saturating_add(bytes.len());
		if length > self.limit {
			self.exceeded = true;
			return Err(io::Error::from(io::ErrorKind::FileTooLarge));
		}
		if length > self.bytes.capacity() {
			let capacity = length
				.max(self.bytes.capacity().saturating_mul(2))
				.min(self.limit);
			self.bytes
				.try_reserve_exact(capacity.saturating_sub(self.bytes.len()))
				.map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
		}
		self.bytes.extend_from_slice(bytes);
		Ok(bytes.len())
	}

	fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

#[cfg(test)]
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

	use super::{Body, FrozenRequest, body, bounded_body, bounded_json};

	#[test]
	fn bounded_json_accepts_exact_size_and_refuses_the_next_byte() {
		let value = "escaped \u{0} and é";
		let expected = serde_json::to_vec(value).expect("fixture JSON");
		let Body::Ready(actual) = bounded_json(&value, expected.len()).expect("exact budget")
		else {
			panic!("exact serialized boundary must fit");
		};
		assert_eq!(actual, expected);
		assert!(matches!(
			bounded_json(
				&value,
				expected
					.len()
					.checked_sub(1)
					.expect("nonempty JSON")
			)
			.expect("size classification"),
			Body::TooLarge
		));
	}

	#[test]
	fn custom_serializer_cannot_hide_a_size_refusal() {
		assert!(matches!(
			super::bounded_custom(
				|writer| {
					let _ignored = writer.write_all(b"too large");
					Ok(())
				},
				2
			)
			.unwrap(),
			Body::TooLarge
		));
		let Body::Ready(bytes) = super::bounded_custom(
			|writer| {
				writer.write_all(b"ok")?;
				Ok(())
			},
			2,
		)
		.unwrap() else {
			panic!("exact serializer boundary");
		};
		assert_eq!(bytes, b"ok");
	}

	#[test]
	fn serialization_failure_is_not_a_batch_size_signal() {
		struct Invalid;
		impl serde::Serialize for Invalid {
			fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
				Err(serde::ser::Error::custom("owned serializer failure"))
			}
		}
		bounded_json(&Invalid, 8).expect_err("malformed serialization must remain an error");
	}

	#[test]
	fn bounded_appservice_body_matches_pinned_ruma_encoding() {
		let mut request = ruma::api::appservice::event::push_events::v1::Request::new(
			"owned-encoding".into(),
			Vec::new(),
		);
		request
			.ephemeral
			.push(ruma::serde::Raw::from_json(
				serde_json::value::to_raw_value(
					&serde_json::json!({"type":"example.encoding", "content":{"value":"escaped \u{0} and é"}}),
				)
				.expect("fixture raw JSON"),
			));
		let expected = body(request.clone()).expect("Ruma body");
		let Body::Ready(actual) = bounded_body(request).expect("bounded Ruma body") else {
			panic!("small request must fit");
		};
		assert_eq!(actual, expected);
	}

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
		let expected = body(request.clone()).expect("Ruma serialized transaction");
		let Body::Ready(saved) = bounded_body(request.clone()).expect("bounded transaction")
		else {
			panic!("small federation body must fit");
		};
		assert_eq!(saved, expected, "bounded serialization preserves Ruma bytes before signing");
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
