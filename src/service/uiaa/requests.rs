//! Bounded, process-local copies of the original UIAA request body.
//! These are not authentication grants. Expiring a cached body does not
//! authorize a session or replace durable session-age/replay checks.
use std::{
	collections::BTreeMap,
	io::{self, Write},
	time::{Duration, Instant},
};

use ruma::CanonicalJsonValue;

use super::RequestKey;

const MAX_ENTRIES: usize = 1024;
const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_AGE: Duration = Duration::from_mins(15);

#[derive(Default)]
pub(super) struct Requests {
	entries: BTreeMap<RequestKey, Entry>,
	bytes: usize,
}

struct Entry {
	body: Box<[u8]>,
	charge: usize,
	created: Instant,
}

#[derive(Debug, PartialEq)]
pub(super) enum Refusal {
	BodyTooLarge,
	Capacity,
	Duplicate,
}

impl Requests {
	pub(super) fn insert(
		&mut self,
		key: RequestKey,
		body: &CanonicalJsonValue,
		now: Instant,
	) -> Result<(), Refusal> {
		self.expire(now);
		if self.entries.contains_key(&key) {
			return Err(Refusal::Duplicate);
		}
		if self.entries.len() >= MAX_ENTRIES {
			return Err(Refusal::Capacity);
		}
		let mut encoded = BoundedBody::default();
		serde_json::to_writer(&mut encoded, body).map_err(|_| Refusal::BodyTooLarge)?;
		let charge = [encoded.0.len(), key.0.as_str().len(), key.1.as_str().len(), key.2.len()]
			.into_iter()
			.try_fold(0_usize, usize::checked_add)
			.ok_or(Refusal::Capacity)?;
		let total = self
			.bytes
			.checked_add(charge)
			.filter(|total| *total <= MAX_BYTES)
			.ok_or(Refusal::Capacity)?;
		// Store compact serialized bytes rather than a cloned JSON tree. The
		// byte budget includes owned key strings; entry/node overhead is bounded
		// separately by MAX_ENTRIES. No live entry is evicted to admit a caller.
		self.entries.insert(key, Entry {
			body: encoded.0.into_boxed_slice(),
			charge,
			created: now,
		});
		self.bytes = total;
		Ok(())
	}

	pub(super) fn get(&mut self, key: &RequestKey, now: Instant) -> Option<CanonicalJsonValue> {
		self.expire(now);
		self.entries
			.get(key)
			.and_then(|entry| serde_json::from_slice(&entry.body).ok())
	}

	pub(super) fn remove(&mut self, key: &RequestKey) {
		if let Some(entry) = self.entries.remove(key) {
			self.bytes = self
				.bytes
				.checked_sub(entry.charge)
				.expect("UIAA cache byte accounting");
		}
	}

	fn expire(&mut self, now: Instant) {
		// This bounded scan never visits more than MAX_ENTRIES, and accesses
		// cannot refresh the absolute creation deadline.
		self.entries.retain(|_, entry| {
			let retain = now.saturating_duration_since(entry.created) < MAX_AGE;
			if !retain {
				self.bytes = self
					.bytes
					.checked_sub(entry.charge)
					.expect("UIAA cache byte accounting");
			}
			retain
		});
	}
}

#[derive(Default)]
struct BoundedBody(Vec<u8>);

impl Write for BoundedBody {
	fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
		if bytes.len() > MAX_BODY_BYTES.saturating_sub(self.0.len()) {
			return Err(io::Error::other("UIAA request body limit"));
		}
		self.0.extend_from_slice(bytes);
		Ok(bytes.len())
	}

	fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

#[cfg(test)]
mod tests {
	use super::*;

	fn key(session: usize) -> RequestKey {
		(
			ruma::user_id!("@alice:example.test").to_owned(),
			"DEVICE".into(),
			session.to_string(),
		)
	}

	fn body() -> CanonicalJsonValue {
		serde_json::from_str(r#"{"devices":["one","two"],"unicode":"\u00e9"}"#).unwrap()
	}

	#[test]
	fn round_trip_is_bound_to_user_device_and_session() {
		let now = Instant::now();
		let mut requests = Requests::default();
		requests.insert(key(1), &body(), now).unwrap();
		assert_eq!(requests.get(&key(1), now), Some(body()));
		let mut other = key(1);
		other.0 = ruma::user_id!("@bob:example.test").to_owned();
		assert_eq!(requests.get(&other, now), None);
		other = key(1);
		other.1 = "OTHER".into();
		assert_eq!(requests.get(&other, now), None);
		assert_eq!(requests.get(&key(2), now), None);
	}

	#[test]
	fn access_never_refreshes_expiry_and_completion_reclaims_bytes() {
		let now = Instant::now();
		let mut requests = Requests::default();
		requests.insert(key(1), &body(), now).unwrap();
		assert!(
			requests
				.get(
					&key(1),
					(now + MAX_AGE)
						.checked_sub(Duration::from_millis(1))
						.unwrap()
				)
				.is_some()
		);
		assert!(requests.get(&key(1), now + MAX_AGE).is_none());
		assert_eq!(requests.bytes, 0);
		requests.insert(key(2), &body(), now).unwrap();
		requests.remove(&key(2));
		requests.remove(&key(2));
		assert_eq!(requests.bytes, 0);
		assert!(requests.entries.is_empty());
	}

	#[test]
	fn duplicate_does_not_replace_body_or_extend_lifetime() {
		let now = Instant::now();
		let mut requests = Requests::default();
		requests.insert(key(1), &body(), now).unwrap();
		let bytes = requests.bytes;
		assert_eq!(
			requests.insert(key(1), &CanonicalJsonValue::Null, now + Duration::from_secs(1)),
			Err(Refusal::Duplicate)
		);
		assert_eq!(requests.bytes, bytes);
		assert_eq!(requests.get(&key(1), now), Some(body()));
		assert!(requests.get(&key(1), now + MAX_AGE).is_none());
	}

	#[test]
	fn entry_limit_refuses_without_evicting_live_requests() {
		let now = Instant::now();
		let mut requests = Requests::default();
		for i in 0..MAX_ENTRIES {
			requests
				.insert(key(i), &CanonicalJsonValue::Null, now)
				.unwrap();
		}
		assert_eq!(requests.insert(key(MAX_ENTRIES), &body(), now), Err(Refusal::Capacity));
		assert_eq!(requests.entries.len(), MAX_ENTRIES);
		assert_eq!(requests.get(&key(0), now), Some(CanonicalJsonValue::Null));
		requests
			.insert(key(MAX_ENTRIES), &body(), now + MAX_AGE)
			.unwrap();
		assert_eq!(requests.entries.len(), 1);
	}

	#[test]
	fn aggregate_bytes_and_single_body_are_bounded() {
		let now = Instant::now();
		let mut requests = Requests::default();
		let too_large = CanonicalJsonValue::String("a".repeat(MAX_BODY_BYTES));
		assert_eq!(requests.insert(key(1), &too_large, now), Err(Refusal::BodyTooLarge));
		assert_eq!(requests.bytes, 0);
		let large = CanonicalJsonValue::String("a".repeat(MAX_BODY_BYTES - 2));
		for i in 0..7 {
			requests.insert(key(i), &large, now).unwrap();
		}
		assert_eq!(requests.insert(key(8), &large, now), Err(Refusal::Capacity));
		assert!(requests.bytes <= MAX_BYTES);
		assert_eq!(requests.entries.len(), 7);
		requests.remove(&key(0));
		requests.insert(key(8), &large, now).unwrap();
	}
}
