use std::collections::HashMap;

use ruma::{
	CanonicalJsonValue, OwnedEventId, OwnedRoomId, OwnedServerName,
	api::federation::event::get_room_state,
};
use tuwunel_core::{
	Err, Result, debug_error, err, info,
	matrix::{Event, pdu::PduEvent},
	warn,
};
use tuwunel_service::rooms::state_compressor::HashSetCompressStateEvent;

use crate::admin_command;

#[admin_command]
#[tracing::instrument(level = "debug", skip(self))]
pub(super) async fn force_set_room_state_from_server(
	&self,
	room_id: OwnedRoomId,
	server_name: OwnedServerName,
) -> Result {
	// TODO: diverged from join remote

	if !self
		.services
		.state_cache
		.server_in_room(&self.services.server.name, &room_id)
		.await
	{
		return Err!("We are not participating in the room / we don't know about the room ID.");
	}

	let first_pdu = self
		.services
		.timeline
		.latest_pdu_in_room(&room_id)
		.await
		.map_err(|_| err!(Database("Failed to find the latest PDU in database")))?;

	let room_version = self
		.services
		.state
		.get_room_version(&room_id)
		.await?;

	let mut state: HashMap<u64, OwnedEventId> = HashMap::new();

	let remote_state_response = self
		.services
		.federation
		.execute(&server_name, get_room_state::v1::Request {
			room_id: room_id.clone(),
			event_id: first_pdu.event_id().to_owned(),
		})
		.await?;

	for pdu in remote_state_response.pdus.clone() {
		match self
			.services
			.event_handler
			.parse_incoming_pdu(&pdu)
			.await
		{
			| Ok(t) => t,
			| Err(e) => {
				warn!("Could not parse PDU, ignoring: {e}");
				continue;
			},
		};
	}

	info!("Going through room_state response PDUs");
	for result in remote_state_response.pdus.iter().map(|pdu| {
		self.services
			.server_keys
			.validate_and_add_event_id(pdu, &room_version)
	}) {
		let Ok((event_id, mut value)) = result.await else {
			continue;
		};

		let invalid_pdu_err = |e| {
			debug_error!("Invalid PDU in fetching remote room state PDUs response: {value:#?}");
			err!(BadServerResponse(debug_error!("Invalid PDU in send_join response: {e:?}")))
		};

		let pdu = if value["type"] == "m.room.create" {
			PduEvent::from_object_and_roomid_and_eventid(&room_id, &event_id, value.clone())
				.map_err(invalid_pdu_err)?
		} else {
			PduEvent::from_object_and_eventid(&event_id, value.clone())
				.map_err(invalid_pdu_err)?
		};

		if !value.contains_key("room_id") {
			let room_id = CanonicalJsonValue::String(room_id.as_str().into());
			value.insert("room_id".into(), room_id);
		}

		self.services
			.timeline
			.add_pdu_outlier(&event_id, &value)
			.await?;

		if let Some(state_key) = &pdu.state_key {
			let shortstatekey = self
				.services
				.short
				.get_or_create_shortstatekey(&pdu.kind.to_string().into(), state_key)
				.await;

			state.insert(shortstatekey, pdu.event_id.clone());
		}
	}

	info!("Going through auth_chain response");
	for result in remote_state_response
		.auth_chain
		.iter()
		.map(|pdu| {
			self.services
				.server_keys
				.validate_and_add_event_id(pdu, &room_version)
		}) {
		let Ok((event_id, value)) = result.await else {
			continue;
		};

		self.services
			.timeline
			.add_pdu_outlier(&event_id, &value)
			.await?;
	}

	let new_room_state = self
		.services
		.event_handler
		.resolve_state(&room_id, &room_version, state)
		.await?;

	info!("Forcing new room state");
	let HashSetCompressStateEvent {
		shortstatehash: short_state_hash,
		added,
		removed,
	} = self
		.services
		.state_compressor
		.save_state(room_id.clone().as_ref(), new_room_state)
		.await?;

	let state_lock = self.services.state.mutex.lock(&*room_id).await;

	self.services
		.state
		.force_state(room_id.clone().as_ref(), short_state_hash, added, removed, &state_lock)
		.await?;

	info!(
		"Updating joined counts for room just in case (e.g. we may have found a difference in \
		 the room's m.room.member state"
	);
	self.services
		.state_cache
		.update_joined_count(&room_id)
		.await;

	self.write_str("Successfully forced the room state from the requested remote server.")
		.await
}
