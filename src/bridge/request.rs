//! Bounded KV request framing and decoding, independent of the transport.
//!
//! The byte limit applies to the entire encoded request, not just its values.
//! A rejected commit is never split. Media upload bodies use their separate
//! streaming path. Response/result budgets are separate from this ingress
//! bound.

use minicbor::{Decoder, data::Type, encode::Write};
use serde::{Deserialize, Serialize};

use super::{Error, Request};

/// Maximum encoded KV request: 4 MiB, including CBOR fields and framing.
pub const MAX_BYTES: usize = 4 * 1024 * 1024;
/// Maximum CBOR nesting, checked before serde can descend into unknown fields.
pub const MAX_DEPTH: usize = 32;
/// Maximum CBOR items/chunks, checked before container-length allocation hints.
pub const MAX_ITEMS: usize = 65_536;

fn too_large(what: &str, limit: usize) -> Error {
	Error::TooLarge {
		what: what.into(),
		limit: u64::try_from(limit).unwrap_or(u64::MAX),
	}
}

fn malformed() -> Error { Error::Invalid("malformed request CBOR".into()) }

/// Appends a transport chunk only when the entire retained body stays bounded.
/// On refusal, the existing body is unchanged; callers must stop reading.
pub fn append(body: &mut Vec<u8>, chunk: &[u8]) -> Result<(), Error> {
	if body.len().saturating_add(chunk.len()) > MAX_BYTES {
		return Err(too_large("request bytes", MAX_BYTES));
	}
	body.extend_from_slice(chunk);
	Ok(())
}

/// Encodes a shape- and byte-validated request without changing its wire
/// format.
pub fn encode(request: &Request) -> Result<Vec<u8>, Error> {
	super::check(request)?;
	super::encode(request)
}

/// Largest prefix of read keys fitting both the item and encoded-byte limits.
///
/// Reserves 128 bytes for the enum/map/array envelope and nine bytes per byte
/// string header (the maximum CBOR length header). Normal short-key batches
/// still contain up to 900 keys. Zero means even the first key cannot fit.
pub fn get_key_count<I, K>(keys: I) -> usize
where
	I: IntoIterator<Item = K>,
	K: AsRef<[u8]>,
{
	let mut size = 128_usize;
	keys.into_iter()
		.take(super::MAX_GET_KEYS)
		.take_while(|key| {
			size = size
				.saturating_add(key.as_ref().len())
				.saturating_add(9);
			size <= MAX_BYTES
		})
		.count()
}

/// Decodes exactly one bounded, structurally checked and validated KV request.
/// Decoder errors never include the input, unknown field names or enum values.
pub fn decode(bytes: &[u8]) -> Result<Request, Error> {
	preflight(bytes)?;
	let mut decoder = minicbor_serde::Deserializer::new(bytes);
	let request = Request::deserialize(&mut decoder).map_err(|_| malformed())?;
	if decoder.decoder().position() != bytes.len() {
		return Err(malformed());
	}
	super::check(&request)?;
	Ok(request)
}

/// Counts the exact existing CBOR encoding without allocating an encoded body.
pub(super) fn check_size(request: &Request) -> Result<(), Error> {
	check_serialized_size(request)
}

pub(crate) fn check_serialized_size<T: Serialize>(value: &T) -> Result<(), Error> {
	let mut length = Length(0);
	value
		.serialize(&mut minicbor_serde::Serializer::new(&mut length))
		.map_err(|_| too_large("request bytes", MAX_BYTES))
}

struct Length(usize);

impl Write for Length {
	type Error = Error;

	fn write_all(&mut self, bytes: &[u8]) -> Result<(), Error> {
		let next = self.0.saturating_add(bytes.len());
		if next > MAX_BYTES {
			return Err(too_large("request bytes", MAX_BYTES));
		}
		self.0 = next;
		Ok(())
	}
}

pub(crate) fn preflight(bytes: &[u8]) -> Result<(), Error> {
	if bytes.len() > MAX_BYTES {
		return Err(too_large("request bytes", MAX_BYTES));
	}
	let mut decoder = Decoder::new(bytes);
	let mut remaining = MAX_ITEMS;
	item(&mut decoder, &mut remaining, 0)?;
	if decoder.position() != bytes.len() {
		return Err(malformed());
	}
	Ok(())
}

fn spend(remaining: &mut usize) -> Result<(), Error> {
	*remaining = remaining
		.checked_sub(1)
		.ok_or_else(|| too_large("request items", MAX_ITEMS))?;
	Ok(())
}

// Recursion is bounded before descent, including tags and unknown fields.
// Only scalar values reach Decoder::skip, never its unbounded container walk.
fn item(decoder: &mut Decoder<'_>, remaining: &mut usize, depth: usize) -> Result<(), Error> {
	if depth >= MAX_DEPTH {
		return Err(too_large("request nesting", MAX_DEPTH));
	}
	spend(remaining)?;
	match decoder.datatype().map_err(|_| malformed())? {
		| Type::Array | Type::ArrayIndef => {
			let length = decoder.array().map_err(|_| malformed())?;
			container(decoder, remaining, depth, length, false)
		},
		| Type::Map | Type::MapIndef => {
			let length = decoder.map().map_err(|_| malformed())?;
			container(decoder, remaining, depth, length, true)
		},
		| Type::Tag => {
			decoder.tag().map_err(|_| malformed())?;
			item(decoder, remaining, depth.saturating_add(1))
		},
		| Type::BytesIndef => {
			for chunk in decoder.bytes_iter().map_err(|_| malformed())? {
				spend(remaining)?;
				chunk.map_err(|_| malformed())?;
			}
			Ok(())
		},
		| Type::StringIndef => {
			for chunk in decoder.str_iter().map_err(|_| malformed())? {
				spend(remaining)?;
				chunk.map_err(|_| malformed())?;
			}
			Ok(())
		},
		| Type::Break | Type::Unknown(_) => Err(malformed()),
		| _ => decoder.skip().map_err(|_| malformed()),
	}
}

fn container(
	decoder: &mut Decoder<'_>,
	remaining: &mut usize,
	depth: usize,
	length: Option<u64>,
	map: bool,
) -> Result<(), Error> {
	if let Some(length) = length {
		let items = length.saturating_mul(if map { 2 } else { 1 });
		if items > u64::try_from(*remaining).unwrap_or(u64::MAX) {
			return Err(too_large("request items", MAX_ITEMS));
		}
		for _ in 0..items {
			item(decoder, remaining, depth.saturating_add(1))?;
		}
	} else {
		loop {
			if decoder.datatype().map_err(|_| malformed())? == Type::Break {
				decoder.skip().map_err(|_| malformed())?;
				break;
			}
			item(decoder, remaining, depth.saturating_add(1))?;
			if map {
				item(decoder, remaining, depth.saturating_add(1))?;
			}
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn read_chunking_preserves_normal_batches_and_bounds_large_keys() {
		for key_size in [1, super::super::MAX_KEY_BYTES] {
			let key = vec![0; key_size];
			let count = get_key_count(std::iter::repeat_n(key.as_slice(), 1_000));
			assert!(count > 0 && count <= super::super::MAX_GET_KEYS);
			if key_size == 1 {
				assert_eq!(count, super::super::MAX_GET_KEYS);
			}
			let request = Request::Get {
				map: u16::MAX,
				keys: vec![serde_bytes::ByteBuf::from(key); count],
			};
			assert!(
				super::super::encode(&request)
					.expect("wire")
					.len() <= MAX_BYTES
			);
		}
		assert_eq!(get_key_count([vec![0; MAX_BYTES].as_slice()]), 0);
	}

	#[test]
	fn chunk_and_count_boundaries_are_exact() {
		let full = vec![0; MAX_BYTES];
		let mut length = Length(0);
		length.write_all(&full).expect("exact limit");
		length.write_all(&[0]).expect_err("one byte over");
		assert_eq!(length.0, MAX_BYTES);
		let mut body = Vec::new();
		for chunk in full.chunks(16_384) {
			append(&mut body, chunk).expect("bounded chunk");
		}
		append(&mut body, &[1]).expect_err("one byte over");
		assert_eq!(body, full);
	}

	#[test]
	fn whole_commit_fits_exactly_then_refuses_one_extra_byte() {
		use serde_bytes::ByteBuf;

		use super::super::{Lease, Mutation, digest};
		let mut ops = vec![
			Mutation::Put {
				map: 0,
				key: ByteBuf::from(b"k".to_vec()),
				val: ByteBuf::from(vec![0; super::super::MAX_VALUE_BYTES]),
			};
			2
		];
		ops.push(Mutation::Put {
			map: 0,
			key: ByteBuf::from(b"last".to_vec()),
			val: ByteBuf::from(vec![0; 65_536]),
		});
		let request = |ops: Vec<Mutation>| Request::Commit {
			request_id: ByteBuf::from(vec![1; super::super::REQUEST_ID_LEN]),
			lease: Lease { holder: "byte-boundary".into(), epoch: 1 },
			digest: ByteBuf::from(digest(&ops).to_vec()),
			ops,
		};
		let initial = encode(&request(ops.clone()))
			.expect("below limit")
			.len();
		let tail_len = MAX_BYTES
			.checked_sub(initial)
			.expect("headroom")
			.saturating_add(65_536);
		let Mutation::Put { val, .. } = &mut ops[2] else { unreachable!() };
		*val = ByteBuf::from(vec![0; tail_len]);
		let exact = request(ops.clone());
		let wire = encode(&exact).expect("exact limit");
		assert_eq!(wire.len(), MAX_BYTES);
		assert_eq!(decode(&wire).expect("exact-limit decode"), exact);
		let Mutation::Put { val, .. } = &mut ops[2] else { unreachable!() };
		*val = ByteBuf::from(vec![0; tail_len.saturating_add(1)]);
		assert_eq!(
			encode(&request(ops)).expect_err("one byte over"),
			too_large("request bytes", MAX_BYTES)
		);
	}

	#[test]
	fn malformed_or_excessive_structure_is_rejected_before_typed_decode() {
		for bytes in [
			vec![],
			vec![0xFF],
			vec![0x9F],
			vec![0xBF, 0x01, 0xFF],
			vec![0x9B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
			vec![0xBB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
			vec![0x81; MAX_DEPTH + 1],
			vec![0xC0; MAX_DEPTH + 1],
			vec![0; MAX_BYTES + 1],
		] {
			preflight(&bytes).expect_err("invalid framing or resource bound");
		}
		let mut trailing = encode(&Request::Hello).expect("hello");
		trailing.push(0);
		decode(&trailing).expect_err("trailing data");
		// Serde's unit-variant visitor can leave the map's value unread.
		// A structurally complete outer value must also be fully consumed by
		// the typed request decoder, not merely by the structural preflight.
		let wrapped = minicbor_serde::to_vec(std::collections::BTreeMap::from([("Hello", 0)]))
			.expect("wrapped unit variant");
		preflight(&wrapped).expect("one complete CBOR value");
		assert_eq!(
			minicbor_serde::from_slice::<Request>(&wrapped).expect("baseline decoder"),
			Request::Hello
		);
		decode(&wrapped).expect_err("unconsumed variant payload");
	}

	#[test]
	fn indefinite_containers_and_chunks_remain_well_formed_and_bounded() {
		for bytes in [
			vec![0x9F, 0x01, 0xFF],
			vec![0xBF, 0x01, 0x02, 0xFF],
			vec![0x5F, 0x41, 0x00, 0xFF],
			vec![0x7F, 0x61, b'a', 0xFF],
		] {
			preflight(&bytes).expect("bounded indefinite value");
		}
		let mut many = vec![0x9F];
		many.extend(std::iter::repeat_n(0, MAX_ITEMS));
		many.push(0xFF);
		preflight(&many).expect_err("aggregate item count");
	}

	#[test]
	fn malformed_request_errors_never_echo_values() {
		let secret = super::super::encode(&"private-unknown-variant").expect("string");
		assert_eq!(decode(&secret).expect_err("not a request"), malformed());
		let bytes = encode(&Request::Hello).expect("hello");
		assert_eq!(bytes, super::super::encode(&Request::Hello).expect("old encoding"));
		assert_eq!(decode(&bytes).expect("decode"), Request::Hello);
	}

	#[test]
	fn bounded_unknown_fields_remain_forward_compatible() {
		let mut wire = minicbor::Encoder::new(Vec::new());
		wire.map(1)
			.expect("enum")
			.str("Get")
			.expect("variant")
			.map(3)
			.expect("fields")
			.str("map")
			.expect("map field")
			.u16(0)
			.expect("map")
			.str("keys")
			.expect("keys field")
			.array(1)
			.expect("keys")
			.bytes(b"k")
			.expect("key")
			.str("future")
			.expect("unknown field")
			.begin_array()
			.expect("future container")
			.map(1)
			.expect("nested map")
			.str("optional")
			.expect("nested field")
			.bool(true)
			.expect("value")
			.end()
			.expect("break");
		assert_eq!(decode(&wire.into_writer()).expect("bounded future field"), Request::Get {
			map: 0,
			keys: vec![serde_bytes::ByteBuf::from(b"k".to_vec())]
		});
	}
}
