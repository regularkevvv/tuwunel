use rocksdb::{Direction, ErrorKind, IteratorMode};
use tokio::runtime::{Handle, RuntimeFlavor};
use tuwunel_core::{Err, Result};

/// Drives one asynchronous backend operation from a synchronous caller.
///
/// The facade keeps a handful of blocking readers (startup signing keys, the
/// global counter) that predate the asynchronous backend contract. On the
/// RocksDB and model backends those read inline; on the remote backend the
/// read is a network round trip, so the worker thread is handed back to tokio
/// with `block_in_place` while the future is driven on the current runtime.
/// That requires the multi-threaded runtime the server always uses; anywhere
/// else this is a returned error, never a panic or a nested runtime.
pub(crate) fn blocking<F>(future: F) -> Result<F::Output>
where
	F: Future,
{
	match Handle::try_current() {
		| Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread =>
			Ok(tokio::task::block_in_place(|| handle.block_on(future))),
		| Ok(_) => Err!(Database(
			"a blocking read reached the remote backend outside a multi-threaded runtime"
		)),
		| Err(_) => Err!(Database("a blocking read reached the remote backend with no runtime")),
	}
}

#[inline]
pub(crate) fn _into_direction(mode: &IteratorMode<'_>) -> Direction {
	use Direction::{Forward, Reverse};
	use IteratorMode::{End, From, Start};

	match mode {
		| Start | From(_, Forward) => Forward,
		| End | From(_, Reverse) => Reverse,
	}
}

/// Converts a RocksDB result into the crate's error type.
///
/// Successful values pass through unchanged. RocksDB errors are normalized by
/// [`map_err`] before entering the crate-wide error representation.
#[inline]
pub(crate) fn result<T>(
	r: std::result::Result<T, rocksdb::Error>,
) -> Result<T, tuwunel_core::Error> {
	r.map_or_else(or_else, and_then)
}

#[inline(always)]
pub(crate) fn and_then<T>(t: T) -> Result<T, tuwunel_core::Error> { Ok(t) }

pub(crate) fn or_else<T>(e: rocksdb::Error) -> Result<T, tuwunel_core::Error> { Err(map_err(e)) }

/// Reports whether RocksDB marked an operation as incomplete.
///
/// Incomplete operations are retryable cursor conditions. Error conversion
/// maps them to the standard I/O `WouldBlock` category.
#[inline]
pub(crate) fn is_incomplete(e: &rocksdb::Error) -> bool { e.kind() == ErrorKind::Incomplete }

/// Converts a RocksDB error into the crate's error representation.
///
/// The RocksDB category is translated to the closest standard I/O error kind,
/// while the original engine message becomes the error payload.
pub(crate) fn map_err(e: rocksdb::Error) -> tuwunel_core::Error {
	let kind = io_error_kind(&e.kind());
	let string = e.into_string();

	std::io::Error::new(kind, string).into()
}

fn io_error_kind(e: &ErrorKind) -> std::io::ErrorKind {
	use std::io;

	match e {
		| ErrorKind::NotFound => io::ErrorKind::NotFound,
		| ErrorKind::Corruption => io::ErrorKind::InvalidData,
		| ErrorKind::InvalidArgument => io::ErrorKind::InvalidInput,
		| ErrorKind::Aborted => io::ErrorKind::Interrupted,
		| ErrorKind::NotSupported => io::ErrorKind::Unsupported,
		| ErrorKind::CompactionTooLarge => io::ErrorKind::FileTooLarge,
		| ErrorKind::MergeInProgress | ErrorKind::Busy => io::ErrorKind::ResourceBusy,
		| ErrorKind::Expired | ErrorKind::TimedOut => io::ErrorKind::TimedOut,
		| ErrorKind::Incomplete | ErrorKind::TryAgain => io::ErrorKind::WouldBlock,
		| ErrorKind::ColumnFamilyDropped
		| ErrorKind::ShutdownInProgress
		| ErrorKind::IOError
		| ErrorKind::Unknown => io::ErrorKind::Other,
	}
}
