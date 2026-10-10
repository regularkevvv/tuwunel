use std::sync::{Arc, OnceLock, Weak};

use crate::Services;

macro_rules! services_stream {
	($guard:ident, $root:ident, $body:block) => {
		async_stream::stream! {
			let $root = $guard.as_ref();
			// Keep early returns inside stream construction.
			let build_stream = || $body;
			let stream = build_stream();
			futures::pin_mut!(stream);
			while let Some(item) = futures::StreamExt::next(&mut stream).await {
				yield item;
			}
		}
	};
}

pub(crate) use services_stream;

#[derive(Default)]
pub(crate) struct OnceServices {
	lock: OnceLock<Weak<Services>>,
}

impl OnceServices {
	pub(super) fn set(&self, services: &Arc<Services>) -> Arc<Services> {
		self.lock
			.get_or_init(|| Arc::downgrade(services))
			.upgrade()
			.expect("services root must be owned during initialization")
	}

	#[inline]
	pub(crate) fn get(&self) -> Arc<Services> {
		self.try_get()
			.expect("services must be initialized and alive")
	}

	/// Own the root for the current operation. Before initialization and after
	/// the root has been released, there is no graph to upgrade.
	#[inline]
	pub(crate) fn try_get(&self) -> Option<Arc<Services>> { self.lock.get()?.upgrade() }
}

#[cfg(not(tuwunel_always_prove_sendness))]
// SAFETY: Services has a lot of circularity inherited from Conduit's original
// design. This stresses the trait solver which twists itself into a knot
// proving Sendness. This issue was a lot worse in conduwuit where we used an
// instance of `Dep` for each Service rather than a single instance of
// `OnceServices` like now. The problem still exists though greatly reduced, and
// the same solution now has greater impact because OnceServices is the single
// unified focal-point for the entire Services call-web.
//
// The prior incarnation required this unsafety or it would blow through the
// recursion_limit; that no longer happens. Nevertheless compile times are
// still substantially reduced by asserting Sendness here. Prove sendness
// by simply commenting this out or using `--cfg tuwunel_always_prove_sendness`,
// it will just take longer.
unsafe impl Send for OnceServices {}

#[cfg(not(tuwunel_always_prove_syncness))]
// SAFETY: Similar to Send as explained above, we further reduce compile-times
// by manually asserting Syncness of this type. The only threading contention
// concerns for this would be on startup but this server has a very well defined
// initialization sequence. After that this structure is purely read-only shared
// without concern.
//
// Proof can be verified by using `--cfg tuwunel_always_prove_syncness` at the
// cost of additional build time.
unsafe impl Sync for OnceServices {}
