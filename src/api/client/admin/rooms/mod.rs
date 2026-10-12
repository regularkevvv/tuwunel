//! Synapse admin API: room endpoints.

mod block;
mod context;
mod delete_room;
mod delete_status;
mod forward_extremities;
mod hierarchy;
mod join;
mod list_rooms;
mod make_room_admin;
mod messages;
mod purge_history;
mod room_details;
mod room_members;
mod row;
mod state;
mod timestamp_to_event;

use ruma::{UInt, uint};

use self::row::{RoomRowBudget, room_row};
pub(crate) use self::{
	block::{admin_get_room_block_route, admin_set_room_block_route},
	context::admin_room_context_route,
	delete_room::{admin_delete_room_v1_route, admin_delete_room_v2_route},
	delete_status::{admin_delete_status_by_id_route, admin_delete_status_by_room_route},
	forward_extremities::{
		admin_delete_forward_extremities_route, admin_get_forward_extremities_route,
	},
	hierarchy::admin_room_hierarchy_route,
	join::admin_join_room_route,
	list_rooms::admin_list_rooms_route,
	make_room_admin::admin_make_room_admin_route,
	messages::admin_room_messages_route,
	purge_history::{
		admin_purge_history_by_event_route, admin_purge_history_route,
		admin_purge_history_status_route,
	},
	room_details::admin_room_details_route,
	room_members::admin_room_members_route,
	state::admin_room_state_route,
	timestamp_to_event::admin_room_timestamp_to_event_route,
};

/// Action name recorded for the async room-shutdown task, mirroring Synapse's
/// `SHUTDOWN_AND_PURGE_ROOM`. The delete-status endpoints filter on it.
const DELETE_ROOM_ACTION: &str = "shutdown_and_purge_room";

/// Action name recorded for the history-purge task, filtered on by the
/// purge-status endpoint.
const PURGE_HISTORY_ACTION: &str = "purge_history";

fn usize_to_uint(count: usize) -> UInt { count.try_into().unwrap_or_else(|_| uint!(0)) }
