//! Stable map identifiers are owned by the shared wire contract.
//!
//! Append new maps in `tuwunel_bridge::catalog`. The database's catalog
//! bijection test checks every descriptor, including tombstones, against it.

pub use tuwunel_bridge::catalog::{MAP_IDS, SCHEMA_VERSION, map_id};
