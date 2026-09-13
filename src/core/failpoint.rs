//! Crash points for the fault-injection harness (feature `failpoints`).
//!
//! The feature is enabled only for the `matrix-homeserver:faults` test image
//! (the template's `just container-build-faults`); a release build compiles
//! none of this, and the template's `container-verify` checks the release
//! binary for [`ENV`].
//!
//! [`ENV`] arms points as a comma-separated list of `name=N`: the N-th time
//! this process passes `name`, it aborts. A bare `name` is `name=1`. Abort
//! runs no destructor, flushes nothing, and releases no lease, which is the
//! closest a process gets to being killed at that instruction.
//!
//! A point fires once per container filesystem. Before it aborts it leaves a
//! mark in the temporary directory, and a process that finds the mark starts
//! with that point disarmed. The local Container runtime restarts a failed
//! container in place, with the same environment (workerd sets Docker's
//! `on-failure` restart policy), and without the mark the restarted process
//! would crash at the same point again. A fresh container has no mark.

use std::{
	path::PathBuf,
	sync::{
		LazyLock,
		atomic::{AtomicU64, Ordering::SeqCst},
	},
};

/// The environment variable that arms failpoints.
pub const ENV: &str = "TUWUNEL_FAILPOINTS";

/// Every point the code base passes. Arming any other name is a harness
/// mistake, refused rather than left to never fire.
pub const POINTS: [&str; 4] = [
	// The database's commit of a batch that records a client transaction id,
	// before the bridge call is sent, and after its acknowledgement.
	"commit.before_dispatch",
	"commit.after_reply",
	// A client send, before its event is appended, and after its Txn
	// committed but before the HTTP response.
	"send.before_append",
	"send.after_append",
];

struct Armed {
	name: &'static str,
	at: u64,
	hits: AtomicU64,
}

static ARMED: LazyLock<Vec<Armed>> = LazyLock::new(|| {
	let Ok(spec) = std::env::var(ENV) else {
		return Vec::new();
	};
	match parse(&spec) {
		| Ok(points) => points
			.into_iter()
			.filter(|(name, _)| !mark(name).exists())
			.map(|(name, at)| Armed { name, at, hits: AtomicU64::new(0) })
			.collect(),
		| Err(error) => {
			eprintln!("{ENV}: {error}; aborting");
			std::process::abort();
		},
	}
});

/// Where a fired point leaves its mark.
#[must_use]
pub fn mark(name: &str) -> PathBuf {
	std::env::temp_dir().join(format!("tuwunel-failpoint-{name}"))
}

/// Parses an arming list into `(point, hit)` pairs.
///
/// # Errors
///
/// An unknown point, a hit that is not a positive integer, or a point armed
/// twice.
pub fn parse(spec: &str) -> Result<Vec<(&'static str, u64)>, String> {
	let mut armed: Vec<(&'static str, u64)> = Vec::new();
	for entry in spec
		.split(',')
		.map(str::trim)
		.filter(|entry| !entry.is_empty())
	{
		let (name, at) = entry.split_once('=').unwrap_or((entry, "1"));
		let name = POINTS
			.iter()
			.copied()
			.find(|point| *point == name.trim())
			.ok_or_else(|| format!("unknown failpoint {:?}", name.trim()))?;
		let at = at
			.trim()
			.parse::<u64>()
			.ok()
			.filter(|at| *at > 0)
			.ok_or_else(|| format!("failpoint {name} needs a positive hit count"))?;
		if armed.iter().any(|(armed, _)| *armed == name) {
			return Err(format!("failpoint {name} is armed twice"));
		}
		armed.push((name, at));
	}

	Ok(armed)
}

/// Passes the point `name`, aborting the process when this is its armed hit.
pub fn hit(name: &'static str) {
	debug_assert!(POINTS.contains(&name), "{name} is not a declared failpoint");
	let Some(point) = ARMED.iter().find(|point| point.name == name) else {
		return;
	};

	let hits = point.hits.fetch_add(1, SeqCst).saturating_add(1);
	if hits == point.at {
		if let Err(error) = std::fs::write(mark(name), b"") {
			eprintln!(
				"failpoint {name}: no mark left ({error}); a restart in place meets it again"
			);
		}
		eprintln!("failpoint {name} fired at hit {hits}; aborting");
		std::process::abort();
	}
}

#[cfg(test)]
mod tests {
	use super::{mark, parse};

	#[test]
	fn arming_lists_name_declared_points_and_positive_hits() {
		assert_eq!(parse(""), Ok(Vec::new()));
		assert_eq!(parse("send.after_append"), Ok(vec![("send.after_append", 1)]));
		assert_eq!(
			parse(" commit.after_reply=3 , send.before_append=2 "),
			Ok(vec![("commit.after_reply", 3), ("send.before_append", 2)])
		);
		for refused in [
			"send.after",
			"commit.after_reply=0",
			"commit.after_reply=-1",
			"commit.after_reply=x",
			"send.after_append,send.after_append=2",
		] {
			assert!(parse(refused).is_err(), "{refused}");
		}
	}

	#[test]
	fn a_mark_is_one_file_per_point_in_the_temporary_directory() {
		let path = mark("send.after_append");
		assert_eq!(path.parent(), Some(std::env::temp_dir().as_path()));
		assert_ne!(path, mark("send.before_append"));
	}
}
