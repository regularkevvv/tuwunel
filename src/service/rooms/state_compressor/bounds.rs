use ruma::api::error::{ErrorKind, LimitExceededErrorData};
use tuwunel_core::{Error, Result};

use super::{CompressedState, CompressedStateEvent, ShortStateInfo};

/// Complete states and each added/removed run contain at most 4,096 cells.
pub(super) const MAX_STATE_EVENTS: usize = 4096;
/// Refuse pathological historical chains instead of recursing without a bound.
pub(super) const MAX_LAYERS: usize = 16;
/// Parent, two complete runs, and the optional removed-run separator.
pub(super) const MAX_DIFF_BYTES: usize =
	2 * size_of::<u64>() + 2 * MAX_STATE_EVENTS * size_of::<CompressedStateEvent>();
const MAX_RETAINED_EVENTS: usize = 32 * 1024;

pub(super) fn check_stack(stack: &[ShortStateInfo]) -> Result {
	if stack.len() > MAX_LAYERS {
		return Err(state_limit());
	}
	let mut retained = 0_usize;
	for info in stack {
		for state in [&info.full_state, &info.added, &info.removed] {
			if state.len() > MAX_STATE_EVENTS {
				return Err(state_limit());
			}
			retained = retained.saturating_add(state.len());
		}
		if retained > MAX_RETAINED_EVENTS {
			return Err(state_limit());
		}
	}
	Ok(())
}

pub(super) fn candidate_state(
	parent: Option<&CompressedState>,
	added: &CompressedState,
	removed: &CompressedState,
) -> Result<CompressedState> {
	if parent.is_some_and(|state| state.len() > MAX_STATE_EVENTS)
		|| added.len() > MAX_STATE_EVENTS
		|| removed.len() > MAX_STATE_EVENTS
	{
		return Err(state_limit());
	}
	let mut state = parent.cloned().unwrap_or_default();
	state.extend(added.iter().copied());
	if parent.is_some() {
		for event in removed {
			state.remove(event);
		}
	}
	if state.len() > MAX_STATE_EVENTS {
		return Err(state_limit());
	}
	Ok(state)
}

pub(super) fn state_limit() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Room state reconstruction limit reached".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}
