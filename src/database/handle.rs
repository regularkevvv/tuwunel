//! Database values returned by point queries.
//!
//! A handle exposes stored bytes through standard reference traits without
//! committing the facade to one backend's value representation: the RocksDB
//! backend returns zero-copy pinned slices, while other backends return
//! owned bytes. Callers can deserialize directly from the handle or copy the
//! bytes into owned storage.

use std::{fmt, fmt::Debug, ops::Deref};

use rocksdb::DBPinnableSlice;
use serde::{Deserialize, Serialize, Serializer};
use tuwunel_core::Result;

use crate::{Deserialized, Slice, keyval::deserialize_val};

/// Backend-neutral view of a value returned by a point query.
///
/// For RocksDB results the handle keeps its underlying [`DBPinnableSlice`]
/// alive and dereferences to [`Slice`] without an additional copy; for other
/// backends it owns the bytes outright. Convert it into `Vec<u8>` when the
/// bytes must outlive the handle.
pub struct Handle<'a> {
	val: Inner<'a>,
}

/// The backend-specific value storage behind a handle.
enum Inner<'a> {
	/// Zero-copy pin into RocksDB block storage.
	Pinned(DBPinnableSlice<'a>),
	/// Bytes owned by the handle itself.
	Owned(Box<[u8]>),
}

impl<'a> From<DBPinnableSlice<'a>> for Handle<'a> {
	fn from(val: DBPinnableSlice<'a>) -> Self { Self { val: Inner::Pinned(val) } }
}

impl From<Box<[u8]>> for Handle<'_> {
	fn from(val: Box<[u8]>) -> Self { Self { val: Inner::Owned(val) } }
}

impl From<Vec<u8>> for Handle<'_> {
	fn from(val: Vec<u8>) -> Self { Self { val: Inner::Owned(val.into()) } }
}

impl Debug for Handle<'_> {
	// The slice's address is the informative content here.
	#[expect(clippy::pointer_format)]
	fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
		let val: &Slice = self;
		let ptr = val.as_ptr();
		let len = val.len();
		let kind = match self.val {
			| Inner::Pinned(_) => "pinned",
			| Inner::Owned(_) => "owned",
		};
		write!(out, "Handle {{{kind}: {{ptr: {ptr:?}, len: {len}}}}}")
	}
}

impl Serialize for Handle<'_> {
	#[inline]
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let bytes: &Slice = self;
		serializer.serialize_bytes(bytes)
	}
}

impl Deserialized for Result<Handle<'_>> {
	#[inline]
	fn map_de<T, U, F>(self, f: F) -> Result<U>
	where
		F: FnOnce(T) -> U,
		T: for<'de> Deserialize<'de>,
	{
		self?.map_de(f)
	}
}

impl<'a> Deserialized for Result<&'a Handle<'a>> {
	#[inline]
	fn map_de<T, U, F>(self, f: F) -> Result<U>
	where
		F: FnOnce(T) -> U,
		T: for<'de> Deserialize<'de>,
	{
		self.and_then(|handle| handle.map_de(f))
	}
}

impl<'a> Deserialized for &'a Handle<'a> {
	#[inline]
	fn map_de<T, U, F>(self, f: F) -> Result<U>
	where
		F: FnOnce(T) -> U,
		T: for<'de> Deserialize<'de>,
	{
		deserialize_val(self.as_ref()).map(f)
	}
}

impl From<Handle<'_>> for Vec<u8> {
	fn from(handle: Handle<'_>) -> Self {
		match handle.val {
			| Inner::Pinned(val) => val.to_vec(),
			| Inner::Owned(val) => val.into_vec(),
		}
	}
}

impl Deref for Handle<'_> {
	type Target = Slice;

	#[inline]
	fn deref(&self) -> &Self::Target {
		match &self.val {
			| Inner::Pinned(val) => val,
			| Inner::Owned(val) => val,
		}
	}
}

impl AsRef<Slice> for Handle<'_> {
	#[inline]
	fn as_ref(&self) -> &Slice { self }
}
