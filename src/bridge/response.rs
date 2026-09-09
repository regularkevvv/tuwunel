//! Bounded KV replies, including the SQL result and transport envelopes.
//!
//! The structural CBOR parser is shared with request ingress. Both directions
//! use the same byte, nesting and item caps; error labels identify replies.
//! A SQL result budget is deliberately smaller than the encoded reply budget
//! to reserve space for framing. This does not bound concurrent invocations
//! or the Container's accumulated multi-page scan snapshots.

use serde::Deserialize;

use super::{Error, Response};

/// Maximum encoded KV reply, including framing: 4 MiB.
pub const MAX_BYTES: usize = super::request::MAX_BYTES;
/// Aggregate SQL key/value bytes plus conservative per-row overhead.
pub const DATA_BYTES: usize = MAX_BYTES - 4 * 1024;
/// Conservative allowance for a row's CBOR framing and scalar SQL columns.
pub const ROW_OVERHEAD: usize = 32;

/// A read exceeded its response budget, not a write/request limit.
#[must_use]
pub fn too_large() -> Error {
	Error::TooLarge {
		what: "response bytes".into(),
		limit: u64::try_from(MAX_BYTES).unwrap_or(u64::MAX),
	}
}

fn malformed() -> Error { Error::Invalid("malformed response CBOR".into()) }

fn frame_error(error: Error) -> Error {
	match error {
		| Error::TooLarge { what, limit } => Error::TooLarge {
			what: what.replacen("request", "response", 1),
			limit,
		},
		| _ => malformed(),
	}
}

/// Retains a transport chunk only if the complete reply remains bounded.
/// Callers must stop reading on refusal.
pub fn append(body: &mut Vec<u8>, chunk: &[u8]) -> Result<(), Error> {
	super::request::append(body, chunk).map_err(frame_error)
}

/// Checks response shape and counts encoded bytes without allocating a body.
pub fn check(response: &Response) -> Result<(), Error> {
	match response {
		| Response::Got { vals } => {
			if vals.len() > super::MAX_GET_KEYS
				|| vals
					.iter()
					.flatten()
					.any(|v| v.len() > super::MAX_VALUE_BYTES)
			{
				return Err(malformed());
			}
		},
		| Response::Scanned { items, more } => {
			if items.len() > usize::try_from(super::MAX_SCAN_PAGE).unwrap_or(0)
				|| (*more && items.is_empty())
				|| items.iter().any(|(k, v)| {
					k.is_empty()
						|| k.len() > super::MAX_KEY_BYTES
						|| v.len() > super::MAX_VALUE_BYTES
				}) {
				return Err(malformed());
			}
		},
		| _ => {},
	}
	super::request::check_serialized_size(response).map_err(frame_error)
}

/// Encodes a validated response in the unchanged protocol-v1 wire format.
pub fn encode(response: &Response) -> Result<Vec<u8>, Error> {
	check(response)?;
	super::encode(response)
}

/// Decodes exactly one structurally bounded reply, without reflecting input.
pub fn decode(bytes: &[u8]) -> Result<Response, Error> {
	super::request::preflight(bytes).map_err(frame_error)?;
	let mut decoder = minicbor_serde::Deserializer::new(bytes);
	let response = Response::deserialize(&mut decoder).map_err(|_| malformed())?;
	if decoder.decoder().position() != bytes.len() {
		return Err(malformed());
	}
	check(&response)?;
	Ok(response)
}

/// Validates a reply's variant, cardinality and scan cursor against its call.
/// A valid CBOR body must not silently truncate a Get or move a scan backwards.
pub fn check_for(response: &Response, request: &super::Request) -> Result<(), Error> {
	use super::Request;
	let valid = match (request, response) {
		| (_, Response::Error(_))
		| (Request::Hello, Response::Hello { .. })
		| (Request::Commit { .. }, Response::Committed { .. })
		| (
			Request::LeaseAcquire { .. } | Request::LeaseRenew { .. },
			Response::Leased { .. },
		)
		| (Request::LeaseRelease { .. }, Response::Released) => true,
		| (Request::Get { keys, .. }, Response::Got { vals }) => keys.len() == vals.len(),
		| (
			Request::Scan { reverse, from, inclusive, limit, .. },
			Response::Scanned { items, .. },
		) => {
			let ordered = |a: &[u8], b: &[u8], inclusive: bool| {
				let order = if *reverse { b.cmp(a) } else { a.cmp(b) };
				order.is_lt() || (inclusive && order.is_eq())
			};
			items.len() <= usize::try_from(*limit).unwrap_or(0)
				&& items
					.windows(2)
					.all(|pair| ordered(&pair[0].0, &pair[1].0, false))
				&& from.as_ref().is_none_or(|from| {
					items
						.first()
						.is_none_or(|(key, _)| ordered(from, key, *inclusive))
				})
		},
		| _ => false,
	};
	if valid {
		Ok(())
	} else {
		Err(Error::Invalid("reply does not match request".into()))
	}
}

#[cfg(test)]
mod tests {
	use serde_bytes::ByteBuf;

	use super::*;

	#[test]
	fn large_values_fit_but_aggregate_replies_do_not() {
		let val = ByteBuf::from(vec![1; super::super::MAX_VALUE_BYTES]);
		let fits = Response::Got { vals: vec![Some(val.clone()); 2] };
		let bytes = encode(&fits).expect("two largest values fit");
		assert_eq!(bytes, super::super::encode(&fits).expect("legacy encoding"));
		assert_eq!(decode(&bytes).expect("bounded decode"), fits);
		assert_eq!(encode(&Response::Got { vals: vec![Some(val); 3] }), Err(too_large()));
	}

	#[test]
	fn response_transport_and_decode_are_bounded_and_redacted() {
		let mut body = vec![0; MAX_BYTES];
		assert_eq!(append(&mut body, &[1]), Err(too_large()));
		assert_eq!(body.len(), MAX_BYTES);
		for bytes in [
			vec![0x9B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
			vec![0x81; 100],
			// A wrapped unit variant must not leave its value unread.
			b"\xa1\x68Released\x00".to_vec(),
			b"\x67private".to_vec(),
		] {
			let error = decode(&bytes).expect_err("invalid response");
			assert!(!error.to_string().contains("private"));
		}
		let mut bytes = encode(&Response::Released).expect("encode");
		bytes.push(0);
		assert_eq!(decode(&bytes), Err(malformed()));
		assert_eq!(decode(&vec![0; MAX_BYTES + 1]), Err(too_large()));
	}

	#[test]
	fn invalid_rows_and_nonprogressing_pages_are_refused() {
		for response in [
			Response::Scanned { items: vec![], more: true },
			Response::Scanned {
				items: vec![(ByteBuf::new(), ByteBuf::new())],
				more: false,
			},
			Response::Got {
				vals: vec![None; super::super::MAX_GET_KEYS + 1],
			},
		] {
			assert_eq!(check(&response), Err(malformed()));
		}
	}

	#[test]
	fn replies_must_match_the_request_and_advance_its_cursor() {
		use super::super::Request;
		let key = |n| ByteBuf::from(vec![n]);
		let get = Request::Get { map: 0, keys: vec![key(1)] };
		assert!(check_for(&Response::Got { vals: vec![] }, &get).is_err());
		assert!(check_for(&Response::Released, &get).is_err());
		for reverse in [false, true] {
			let request = Request::Scan {
				map: 0,
				reverse,
				from: Some(key(2)),
				inclusive: false,
				limit: 2,
				lease: None,
			};
			for (keys, valid) in if reverse {
				[
					(vec![1, 0], true),
					(vec![2, 1], false),
					(vec![0, 1], false),
					(vec![1, 1], false),
				]
			} else {
				[
					(vec![3, 4], true),
					(vec![2, 3], false),
					(vec![4, 3], false),
					(vec![3, 3], false),
				]
			} {
				let response = Response::Scanned {
					items: keys
						.into_iter()
						.map(|n| (key(n), ByteBuf::new()))
						.collect(),
					more: true,
				};
				assert_eq!(check_for(&response, &request).is_ok(), valid);
			}
		}
	}
}
