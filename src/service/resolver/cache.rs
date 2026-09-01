use std::{net::IpAddr, sync::Arc, time::SystemTime};

use futures::{Stream, StreamExt, future::join};
use ruma::ServerName;
use serde::{Deserialize, Serialize};
use tuwunel_core::{
	Result,
	arrayvec::ArrayVec,
	at, err, implement,
	utils::{math::Expected, rand, stream::TryIgnore},
};
use tuwunel_database::{Cbor, Deserialized, Map};

use super::{DestString, FedDest};

pub struct Cache {
	destinations: Arc<Map>,
	overrides: Arc<Map>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CachedDest {
	pub dest: FedDest,
	pub host: DestString,
	pub expire: SystemTime,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CachedOverride {
	pub ips: IpAddrs,
	pub port: u16,
	pub expire: SystemTime,
	pub overriding: Option<DestString>,
}

pub type IpAddrs = ArrayVec<IpAddr, MAX_IPS>;
pub(crate) const MAX_IPS: usize = 3;

impl Cache {
	pub(super) fn new(args: &crate::Args<'_>) -> Arc<Self> {
		Arc::new(Self {
			destinations: args.db["servername_destination"].clone(),
			overrides: args.db["servername_override"].clone(),
		})
	}
}

#[implement(Cache)]
pub async fn clear(&self) -> Result {
	let (a, b) = join(self.clear_destinations(), self.clear_overrides()).await;
	a.and(b)
}

#[implement(Cache)]
pub async fn clear_destinations(&self) -> Result { self.destinations.clear().await }

#[implement(Cache)]
pub async fn clear_overrides(&self) -> Result { self.overrides.clear().await }

#[implement(Cache)]
pub async fn del_destination(&self, name: &ServerName) -> Result {
	self.destinations.remove(name).await
}

#[implement(Cache)]
pub async fn del_override(&self, name: &str) -> Result { self.overrides.remove(name).await }

#[implement(Cache)]
pub async fn set_destination(&self, name: &ServerName, dest: &CachedDest) -> Result {
	self.destinations.raw_put(name, Cbor(dest)).await
}

#[implement(Cache)]
pub async fn set_override(&self, name: &str, over: &CachedOverride) -> Result {
	self.overrides.raw_put(name, Cbor(over)).await
}

#[implement(Cache)]
#[must_use]
pub async fn has_destination(&self, destination: &ServerName) -> bool {
	self.get_destination(destination).await.is_ok()
}

#[implement(Cache)]
#[must_use]
pub async fn has_override(&self, destination: &str) -> bool {
	self.get_override(destination)
		.await
		.iter()
		.any(CachedOverride::valid)
}

#[implement(Cache)]
pub async fn get_destination(&self, name: &ServerName) -> Result<CachedDest> {
	self.destinations
		.get(name)
		.await
		.deserialized::<Cbor<_>>()
		.map(at!(0))
		.into_iter()
		.find(CachedDest::valid)
		.ok_or(err!(Request(NotFound("Expired from cache"))))
}

#[implement(Cache)]
pub async fn get_override(&self, name: &str) -> Result<CachedOverride> {
	self.overrides
		.get(name)
		.await
		.deserialized::<Cbor<_>>()
		.map(at!(0))
}

#[implement(Cache)]
pub fn destinations(&self) -> impl Stream<Item = (&ServerName, CachedDest)> + Send + '_ {
	self.destinations
		.stream()
		.ignore_err()
		.map(|item: (&ServerName, Cbor<_>)| (item.0, item.1.0))
}

#[implement(Cache)]
pub fn overrides(&self) -> impl Stream<Item = (&ServerName, CachedOverride)> + Send + '_ {
	self.overrides
		.stream()
		.ignore_err()
		.map(|item: (&ServerName, Cbor<_>)| (item.0, item.1.0))
}

impl CachedDest {
	#[inline]
	#[must_use]
	pub fn valid(&self) -> bool { self.expire > SystemTime::now() }

	#[must_use]
	pub(crate) fn default_expire() -> SystemTime {
		rand::time_from_now_secs(60 * 60 * 18..60 * 60 * 36)
	}

	#[inline]
	#[must_use]
	pub fn size(&self) -> usize {
		self.dest
			.size()
			.expected_add(self.host.len())
			.expected_add(size_of_val(&self.expire))
	}
}

impl CachedOverride {
	#[inline]
	#[must_use]
	pub fn valid(&self) -> bool { self.expire > SystemTime::now() }

	#[must_use]
	pub(crate) fn default_expire() -> SystemTime {
		rand::time_from_now_secs(60 * 60 * 6..60 * 60 * 12)
	}

	#[inline]
	#[must_use]
	pub fn size(&self) -> usize { size_of_val(self) }
}
