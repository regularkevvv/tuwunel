//! Each active key owns a counter identity until acknowledgement or
//! cancellation. Queue keys and event payloads may be reused; this identity
//! must not be.

use tuwunel_core::{Error, Result};

use super::SendingEvent;

const TAG: u8 = 0x05;
const VERSION: u8 = 1;
const HEADER: usize = 2 + size_of::<u64>();

pub(super) fn encode(event: &SendingEvent, identity: u64) -> Result<Vec<u8>> {
	if identity == 0 || event.value_bytes().first() == Some(&TAG) {
		return Err(Error::bad_database("Invalid active delivery identity payload"));
	}
	let mut value = vec![TAG, VERSION];
	value.extend_from_slice(&identity.to_be_bytes());
	value.extend_from_slice(event.value_bytes());
	Ok(value)
}

pub(super) fn identity(value: &[u8]) -> Result<Option<u64>> {
	if value.first() != Some(&TAG) {
		return Ok(None);
	}
	if value.get(1) != Some(&VERSION) || value.len() < HEADER {
		return Err(Error::bad_database("Invalid active delivery identity envelope"));
	}
	let bytes = value[2..HEADER]
		.try_into()
		.expect("checked active identity width");
	let identity = u64::from_be_bytes(bytes);
	if identity == 0 || value.get(HEADER) == Some(&TAG) {
		return Err(Error::bad_database("Invalid active delivery identity"));
	}
	Ok(Some(identity))
}

pub(super) fn payload(value: &[u8]) -> Result<&[u8]> {
	Ok(if identity(value)?.is_some() {
		&value[HEADER..]
	} else {
		value
	})
}
