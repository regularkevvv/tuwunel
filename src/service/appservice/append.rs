//! Frozen appservice recipient admission coupled to canonical PDU acceptance.
use std::sync::Arc;

use ruma::{UserId, events::TimelineEventType};
use tokio::sync::OwnedMutexGuard;
use tuwunel_core::{
	Error, Result, implement,
	matrix::{PduEvent, RawPduId, event::Event},
};
use tuwunel_database::Txn;

pub(crate) struct PreparedPdu {
	admission: crate::sending::DeviceListAdmission,
	sending: Arc<crate::sending::Service>,
}

impl PreparedPdu {
	pub(crate) fn sender_guard(&self) -> &OwnedMutexGuard<()> { self.admission.sender_guard() }

	pub(crate) fn append(&self, txn: &mut Txn) -> Result {
		self.sending
			.append_appservice_admission(&self.admission, txn);
		txn.check_bridge_admission().map(|_| ())
	}

	pub(crate) fn finish(self) -> Result {
		self.sending
			.dispatch_device_list_admission(self.admission)
	}
}

#[implement(super::Service)]
pub(crate) async fn prepare_pdu(
	&self,
	raw: RawPduId,
	pdu: &PduEvent,
	state: Option<crate::rooms::short::ShortStateHash>,
) -> Result<PreparedPdu> {
	let services_guard = self.services.get();
	let services = services_guard.as_ref();
	let registrations = self.read_owned().await;
	let mut bytes = 0_usize;
	for (count, info) in registrations.values().enumerate() {
		bytes = bytes.saturating_add(info.registration.id.len());
		if count >= 128 || bytes > 128 * 1024 {
			return Err(Error::bad_database("Canonical appservice inventory exceeds budget"));
		}
	}
	let members = if registrations.is_empty() {
		Vec::new()
	} else {
		let state = match state {
			| Some(state) => state,
			| None =>
				services
					.state
					.get_room_shortstatehash(pdu.room_id())
					.await?,
		};
		services
			.state_accessor
			.notification_members_for_append(state, pdu)
			.await?
	};
	let aliases = if registrations.is_empty() {
		Vec::new()
	} else {
		services
			.alias
			.bounded_local_aliases_for_room(pdu.room_id())
			.await?
	};
	let membership = (*pdu.kind() == TimelineEventType::RoomMember)
		.then(|| {
			pdu.state_key
				.as_ref()
				.and_then(|key| UserId::parse(key.as_str()).ok())
		})
		.flatten();
	let ids = registrations
		.values()
		.filter(|info| {
			info.is_user_match(pdu.sender())
				|| membership
					.as_ref()
					.is_some_and(|user| info.is_user_match(user))
				|| members
					.iter()
					.any(|user| user == &info.sender || info.is_user_match(user))
				|| aliases
					.iter()
					.any(|alias| info.aliases.is_match(alias.as_str()))
				|| info.rooms.is_match(pdu.room_id().as_str())
		})
		.map(|info| info.registration.id.clone())
		.collect();
	let admission = services
		.sending
		.prepare_pdu_appservice_admission(raw, ids, registrations)
		.await?;
	Ok(PreparedPdu {
		admission,
		sending: services.sending.clone(),
	})
}
