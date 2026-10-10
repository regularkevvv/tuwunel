//! Fixed-size push retry state. The failed physical admissions own the write;
//! a cancelled or replaced batch cannot recreate a destination's backoff.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tuwunel_core::{Error, Result, utils::exponential_backoff_remaining_secs};
use tuwunel_database::Txn;

use super::{ActiveAcknowledgement, Data, Destination, parse_servercurrentevent};

const PREFIX: u8 = 0x07;
const VERSION: u8 = 1;
const WIDTH: usize = 13;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::sending) struct PushBackoff {
	pub(in crate::sending) tries: u32,
	failed_at_ms: u64,
}

fn now_ms() -> Result<u64> {
	let elapsed = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_err(|_| Error::bad_database("Push retry clock precedes the Unix epoch"))?;
	u64::try_from(elapsed.as_millis())
		.map_err(|_| Error::bad_database("Push retry clock exceeds timestamp limit"))
}

impl PushBackoff {
	pub(in crate::sending) fn failed(tries: u32) -> Result<Self> {
		if tries == 0 {
			return Err(Error::bad_database("Push retry streak must be nonzero"));
		}
		Ok(Self { tries, failed_at_ms: now_ms()? })
	}

	pub(in crate::sending) fn remaining(&self, min: u64, max: u64) -> Result<Option<Duration>> {
		Ok(self.remaining_at(now_ms()?, min, max))
	}

	fn remaining_at(&self, now: u64, min: u64, max: u64) -> Option<Duration> {
		let elapsed = Duration::from_millis(now.saturating_sub(self.failed_at_ms));
		exponential_backoff_remaining_secs(min, max, elapsed, self.tries)
	}

	fn encode(self) -> [u8; WIDTH] {
		let mut value = [0_u8; WIDTH];
		value[0] = VERSION;
		value[1..5].copy_from_slice(&self.tries.to_be_bytes());
		value[5..].copy_from_slice(&self.failed_at_ms.to_be_bytes());
		value
	}

	fn decode(value: &[u8]) -> Result<Self> {
		if value.len() != WIDTH || value[0] != VERSION {
			return Err(Error::bad_database("Invalid push retry record"));
		}
		let tries = u32::from_be_bytes(
			value[1..5]
				.try_into()
				.expect("validated retry width"),
		);
		if tries == 0 {
			return Err(Error::bad_database("Invalid push retry streak"));
		}
		let failed_at_ms = u64::from_be_bytes(
			value[5..]
				.try_into()
				.expect("validated timestamp width"),
		);
		Ok(Self { tries, failed_at_ms })
	}
}

fn key(destination: &Destination) -> Result<Vec<u8>> {
	if !matches!(destination, Destination::Push(..)) {
		return Err(Error::bad_database("Push retry destination mismatch"));
	}
	let prefix = destination.get_prefix();
	if prefix.len().saturating_add(1) > tuwunel_bridge::MAX_KEY_BYTES {
		return Err(Error::bad_database("Push retry owner exceeds key limit"));
	}
	let mut key = Vec::with_capacity(prefix.len().saturating_add(1));
	key.push(PREFIX);
	key.extend_from_slice(&prefix);
	Ok(key)
}

impl Data {
	pub(in crate::sending) async fn push_backoff(
		&self,
		destination: &Destination,
	) -> Result<Option<PushBackoff>> {
		let _guard = self.active_write.lock().await;
		let key = key(destination)?;
		let mut backoff = match self.db["global"].get(&key).await {
			| Ok(value) => PushBackoff::decode(&value)?,
			| Err(error) if error.is_not_found() => return Ok(None),
			| Err(error) => return Err(error),
		};
		// A backward wall-clock step starts one full bounded retry window from
		// the corrected clock. Persist the rebase so a future timestamp cannot
		// repeatedly renew the hold after every wake or process restart. Do not
		// expire retry metadata by age: it belongs to the still-owed active rows
		// and retires only with their matching ACK/cancellation/erasure.
		let now = now_ms()?;
		if backoff.failed_at_ms > now {
			backoff.failed_at_ms = now;
			let mut txn = self.db.txn();
			txn.insert_raw(&self.db["global"], key, backoff.encode());
			txn.execute().await?;
		}
		Ok(Some(backoff))
	}

	pub(in crate::sending) async fn persist_push_backoff(
		&self,
		destination: &Destination,
		rows: &ActiveAcknowledgement,
		backoff: PushBackoff,
	) -> Result<bool> {
		let _guard = self.active_write.lock().await;
		self.require_active_schema().await?;
		let key = key(destination)?;
		if rows.rows.is_empty() {
			return Ok(false);
		}
		for (key, expected) in &rows.rows {
			self.validate_active_identity(expected)?;
			let (owner, _) = parse_servercurrentevent(key, expected)?;
			if &owner != destination {
				return Err(Error::bad_database("Push failure membership mismatch"));
			}
			match self.servercurrentevent_data.get(key).await {
				| Ok(value) if value.as_ref() == expected.as_slice() => {},
				| Ok(_) => return Ok(false),
				| Err(error) if error.is_not_found() => return Ok(false),
				| Err(error) => return Err(error),
			}
		}
		let mut txn = self.db.txn();
		txn.insert_raw(&self.db["global"], key, backoff.encode());
		txn.execute().await?;
		Ok(true)
	}

	// Caller holds active_write; acknowledgement stages this in its row-removal
	// transaction, while cancellation calls it after retiring all owned rows.
	pub(super) fn stage_clear_push_backoff(
		&self,
		txn: &mut Txn,
		destination: &Destination,
	) -> Result {
		if matches!(destination, Destination::Push(..)) {
			txn.del_raw(&self.db["global"], key(destination)?);
		}
		Ok(())
	}

	pub(super) async fn clear_push_backoff(&self, destination: &Destination) -> Result {
		let mut txn = self.db.txn();
		self.stage_clear_push_backoff(&mut txn, destination)?;
		txn.execute().await
	}
}

#[cfg(test)]
mod tests {
	use super::{PushBackoff, WIDTH};

	#[test]
	fn retry_record_is_fixed_width_and_rejects_corruption() {
		let backoff = PushBackoff { tries: u32::MAX, failed_at_ms: 123_456 };
		assert_eq!(PushBackoff::decode(&backoff.encode()).unwrap(), backoff);
		for length in 0..WIDTH {
			PushBackoff::decode(&backoff.encode()[..length]).expect_err("truncated retry record");
		}
		PushBackoff::decode(&[0_u8; WIDTH]).expect_err("unknown retry version");
		let mut value = backoff.encode();
		value[1..5].fill(0);
		PushBackoff::decode(&value).expect_err("zero retry streak");
	}
	#[test]
	fn retry_clock_policy_honors_exact_expiry_and_bounded_backward_steps() {
		use std::time::Duration;
		let backoff = PushBackoff { tries: 2, failed_at_ms: 10_000 };
		assert_eq!(backoff.remaining_at(9_000, 3, 60), Some(Duration::from_secs(12)));
		assert_eq!(backoff.remaining_at(21_999, 3, 60), Some(Duration::from_millis(1)));
		assert_eq!(backoff.remaining_at(22_000, 3, 60), None);
		assert_eq!(backoff.remaining_at(u64::MAX, 3, 60), None);
		let maximum = PushBackoff { tries: u32::MAX, failed_at_ms: 10_000 };
		assert_eq!(maximum.remaining_at(0, u64::MAX, 60), Some(Duration::from_mins(1)));
		assert_eq!(maximum.remaining_at(10_000, 0, 0), None);
	}
}
