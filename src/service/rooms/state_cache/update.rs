use ruma::{
	OwnedServerName, RoomId, UserId,
	events::{
		AnyStrippedStateEvent, AnySyncStateEvent, GlobalAccountDataEventType,
		RoomAccountDataEventType, StateEventType,
		direct::DirectEvent,
		room::{
			create::RoomCreateEventContent,
			member::{MembershipState, RoomMemberEventContent},
		},
	},
	serde::Raw,
};
use tuwunel_core::{
	Error, Result, implement, is_not_empty, matrix::PduCount, utils::result::LogErr,
};
use tuwunel_database::{Json, Txn, serialize_key, serialize_val};

pub(super) const RECOUNT_PENDING: &str = "membership_recount_pending";

/// Optional stripped room state attached to invite and knock transitions.
pub type StrippedRoomState = Option<Vec<Raw<AnyStrippedStateEvent>>>;

/// Parameters for one membership cache transition.
///
/// Borrowed identifiers remain valid only for the duration of the update. Owned
/// event data is consumed by the selected transition.
pub struct MembershipUpdate<'a> {
	/// Room whose membership changed.
	///
	/// Membership indexes and aggregate counts are updated for this room.
	pub room_id: &'a RoomId,

	/// User whose membership changed.
	///
	/// Both local and remote users are represented in the membership indexes.
	pub user_id: &'a UserId,

	/// Membership event content driving the transition.
	///
	/// The membership state selects which indexes are written and cleared.
	pub membership_event: RoomMemberEventContent,

	/// User who sent the membership event.
	///
	/// Invite handling uses the sender when applying the ignored-user policy.
	pub sender: &'a UserId,

	/// Stripped room state associated with an invite or knock.
	///
	/// Other membership transitions leave this value unused.
	pub last_state: StrippedRoomState,

	/// Servers supplied as routing hints for an invite.
	///
	/// Invite handling stores only non-empty lists. The routing hints commit
	/// with the membership indexes.
	pub invite_via: Option<Vec<OwnedServerName>>,

	/// Whether to rebuild the room's aggregate membership counts.
	///
	/// Bulk state updates can defer this rebuild until all transitions are
	/// applied.
	pub update_joined_count: bool,

	/// Stream position associated with the membership event.
	///
	/// Count-indexed membership rows store its unsigned representation.
	pub count: PduCount,
}

/// Update current membership data.
#[implement(super::Service)]
#[tracing::instrument(
		level = "debug",
		skip_all,
		fields(
			%room_id,
			%user_id,
			%sender,
			%count,
			?membership_event,
		),
	)]
pub async fn update_membership(
	&self,
	MembershipUpdate {
		room_id,
		user_id,
		membership_event,
		sender,
		last_state,
		invite_via,
		update_joined_count,
		count,
	}: MembershipUpdate<'_>,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let membership = membership_event.membership;

	self.ensure_remote_user(user_id).await?;

	match membership {
		| MembershipState::Join => {
			self.handle_join(room_id, user_id, count).await?;
		},
		| MembershipState::Invite => {
			if services_root
				.users
				.user_is_ignored(sender, user_id)
				.await
			{
				return Ok(());
			}

			self.mark_as_invited(user_id, room_id, count, last_state, invite_via)
				.await?;
		},
		| MembershipState::Leave | MembershipState::Ban => {
			self.handle_leave(room_id, user_id, count).await?;

			// A departure drops the room from the account-wide badge total.
			if services_root.globals.user_is_local(user_id) {
				services_root
					.sending
					.refresh_push_badge(user_id)
					.await
					.log_err()
					.ok();
			}
		},
		| MembershipState::Knock => {
			self.mark_as_knocked(user_id, room_id, count, last_state)
				.await?;
		},
		| _ => {},
	}

	// The membership is durable and the room's cached appservice answers are
	// gone (`commit_membership`), so a failed recount leaves the aggregates
	// behind but no cached answer that contradicts the membership.
	if update_joined_count {
		self.update_joined_count(room_id).await?;
	}

	Ok(())
}

/// Recounts a room's aggregates: its joined, invited and knocked counts and
/// the servers in it.
///
/// They derive from the membership indexes and are recomputed whole, so any
/// later recount of the room repairs a failed one. Every membership commit
/// leaves a durable pending marker; this aggregate commit removes it
/// atomically. A refusal or restart leaves the marker for the room's next event
/// to repair ([`Self::repair_joined_count`]), even when a bulk update deferred
/// recounting. Public aggregate reads also repair it before returning counts.
/// Membership commits cannot overlap the complete scans or aggregate commit.
///
/// This commit changes no membership index, so it invalidates nothing in the
/// appservice-in-room cache; the membership commit before it did.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn update_joined_count(&self, room_id: &RoomId) -> Result {
	let guard = self.membership_mutex.lock(room_id).await;
	Box::pin(self.update_joined_count_locked(room_id, &guard)).await
}

/// Holds the membership exclusion through all scans and the aggregate commit.
#[implement(super::Service)]
pub(super) async fn update_joined_count_locked(
	&self,
	room_id: &RoomId,
	_guard: &super::MembershipGuard,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	self.ensure_recount_pending(room_id).await?;
	// Refuse malformed reconciliation metadata before any aggregate mutation.
	// A missing or prior-process stamp is normal and will be replaced below.
	self.recount_is_current(room_id).await?;
	let inventory = self.prepare_recount_inventory(room_id).await?;
	let mut joined_servers = inventory.joined_servers;
	let joinedcount = inventory.joined.to_be_bytes();
	let invitedcount = inventory.invited.to_be_bytes();
	let knockedcount = inventory.knocked.to_be_bytes();
	let mut txn = services_root.db.txn();

	txn.insert_raw(&self.db.roomid_joinedcount, room_id, joinedcount);
	txn.insert_raw(&self.db.roomid_invitedcount, room_id, invitedcount);
	txn.insert_raw(&self.db.roomid_knockedcount, room_id, knockedcount);

	for old_joined_server in &inventory.old_servers {
		if joined_servers.remove(old_joined_server) {
			continue;
		}

		// Server not in room anymore
		let roomserver_id = (room_id, old_joined_server);
		let serverroom_id = (old_joined_server, room_id);

		txn.del(&self.db.roomserverids, roomserver_id);
		txn.del(&self.db.serverroomids, serverroom_id);
	}

	// Now only new servers are in joined_servers anymore
	for server in &joined_servers {
		let roomserver_id = (room_id, server);
		let serverroom_id = (server, room_id);
		let roomserver_id = serialize_key(roomserver_id)?;
		let serverroom_id = serialize_key(serverroom_id)?;

		txn.insert_raw(&self.db.roomserverids, roomserver_id, []);
		txn.insert_raw(&self.db.serverroomids, serverroom_id, []);
	}

	txn.put_raw(
		&services_root.db["global"],
		(super::recount::RECOUNT_GENERATION, room_id),
		self.recount_generation.as_bytes(),
	);
	txn.del(&services_root.db["global"], (RECOUNT_PENDING, room_id));
	txn.execute().await
}

/// Also retain the obligation when an explicit recount of older data fails.
#[implement(super::Service)]
async fn ensure_recount_pending(&self, room_id: &RoomId) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let key = (RECOUNT_PENDING, room_id);
	let global = &services_root.db["global"];
	match global.qry(&key).await {
		| Ok(value) if value.is_empty() => Ok(()),
		| Ok(_) => Err(Error::bad_database("Invalid membership recount marker")),
		| Err(error) if error.is_not_found() => global.put(key, &[0_u8; 0][..]).await,
		| Err(error) => Err(error),
	}
}

/// Recounts a durably marked room after a refused or deferred aggregate commit.
/// A missing marker is normal; other read failures and malformed markers
/// refuse.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn repair_joined_count(&self, room_id: &RoomId) -> Result {
	let guard = self.membership_mutex.lock(room_id).await;
	Box::pin(self.repair_joined_count_locked(room_id, &guard)).await
}

/// Marker inspection and any rebuild share the caller's membership exclusion.
#[implement(super::Service)]
pub(super) async fn repair_joined_count_locked(
	&self,
	room_id: &RoomId,
	guard: &super::MembershipGuard,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	match services_root.db["global"]
		.qry(&(RECOUNT_PENDING, room_id))
		.await
	{
		| Ok(value) if value.is_empty() =>
			Box::pin(self.update_joined_count_locked(room_id, guard)).await,
		| Ok(_) => Err(Error::bad_database("Invalid membership recount marker")),
		| Err(error) if error.is_not_found() => Ok(()),
		| Err(error) => Err(error),
	}
}

/// Commits one change to a room's membership indexes, then drops the room's
/// cached appservice answers.
///
/// They are dropped whatever the outcome: a failed commit is no proof that
/// nothing applied, and an answer that outlived a membership change would
/// stand until the room's next one. Dropping them here rather than after the
/// recount that follows means a failed recount cannot leave the cache
/// contradicting durable membership.
#[implement(super::Service)]
pub(super) async fn commit_membership(&self, room_id: &RoomId, txn: Txn) -> Result {
	let guard = self.membership_mutex.lock(room_id).await;
	self.commit_membership_locked(room_id, txn, &guard)
		.await
}

/// The caller retains the same room's exclusion until the commit completes.
#[implement(super::Service)]
pub(super) async fn commit_membership_locked(
	&self,
	room_id: &RoomId,
	mut txn: Txn,
	guard: &super::MembershipGuard,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	// Never publish membership without its repair obligation, including bulk
	// updates that defer recounting and a kill before the first recount starts.
	txn.put(&services_root.db["global"], (RECOUNT_PENDING, room_id), &[0_u8; 0][..]);
	self.commit_membership_txn_locked(room_id, txn, guard)
		.await
}

/// Counter erasure must not leave an obligation that recreates deleted counts.
/// Clear the pending marker and generation in the same counter/index commit.
#[implement(super::Service)]
pub(super) async fn commit_membership_erasure_locked(
	&self,
	room_id: &RoomId,
	mut txn: Txn,
	guard: &super::MembershipGuard,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	txn.del(&services_root.db["global"], (RECOUNT_PENDING, room_id));
	txn.del(&services_root.db["global"], (super::recount::RECOUNT_GENERATION, room_id));
	self.commit_membership_txn_locked(room_id, txn, guard)
		.await
}

/// Invalidate cached membership even when the commit outcome is uncertain.
#[implement(super::Service)]
async fn commit_membership_txn_locked(
	&self,
	room_id: &RoomId,
	txn: Txn,
	_guard: &super::MembershipGuard,
) -> Result {
	let committed = txn.execute().await;

	self.appservice_in_room_cache
		.write()
		.expect("locked")
		.invalidate(room_id);

	committed
}

/// Direct DB function to directly mark a user as joined. It is not
/// recommended to use this directly. You most likely should use
/// `update_membership` instead
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub(crate) async fn mark_as_joined(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	count: PduCount,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let userroom_id = (user_id, room_id);
	let userroom_id = serialize_key(userroom_id).expect("failed to serialize userroom_id");

	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");

	let count = count.into_unsigned().to_be_bytes();
	let mut txn = services_root.db.txn();

	txn.insert_raw(&self.db.userroomid_joinedcount, &userroom_id, count);
	txn.insert_raw(&self.db.roomuserid_joinedcount, &roomuser_id, count);
	txn.del_raw(&self.db.userroomid_invitestate, &userroom_id);
	txn.del_raw(&self.db.roomuserid_invitecount, &roomuser_id);
	txn.del_raw(&self.db.userroomid_leftstate, &userroom_id);
	txn.del_raw(&self.db.roomuserid_leftcount, &roomuser_id);
	txn.del_raw(&self.db.userroomid_knockedstate, &userroom_id);
	txn.del_raw(&self.db.roomuserid_knockedcount, &roomuser_id);
	self.commit_membership(room_id, txn).await
}

/// Direct DB function to directly mark a user as left. It is not
/// recommended to use this directly. You most likely should use
/// `update_membership` instead
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub(crate) async fn mark_as_left(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	count: PduCount,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let userroom_id = (user_id, room_id);
	let userroom_id = serialize_key(userroom_id).expect("failed to serialize userroom_id");

	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");

	let leftstate = serialize_val(Json(Vec::<Raw<AnySyncStateEvent>>::new()))
		.expect("failed to serialize left state");

	let count = count.into_unsigned().to_be_bytes();
	let mut txn = services_root.db.txn();

	txn.insert_raw(&self.db.userroomid_leftstate, &userroom_id, leftstate);
	txn.insert_raw(&self.db.roomuserid_leftcount, &roomuser_id, count);
	txn.del_raw(&self.db.userroomid_joinedcount, &userroom_id);
	txn.del_raw(&self.db.roomuserid_joinedcount, &roomuser_id);
	txn.del_raw(&self.db.userroomid_invitestate, &userroom_id);
	txn.del_raw(&self.db.roomuserid_invitecount, &roomuser_id);
	txn.del_raw(&self.db.userroomid_knockedstate, &userroom_id);
	txn.del_raw(&self.db.roomuserid_knockedcount, &roomuser_id);
	self.commit_membership(room_id, txn).await
}

/// Direct DB function to directly mark a user as knocked. It is not
/// recommended to use this directly. You most likely should use
/// `update_membership` instead
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub(crate) async fn mark_as_knocked(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	count: PduCount,
	knocked_state: StrippedRoomState,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let userroom_id = (user_id, room_id);
	let userroom_id = serialize_key(userroom_id).expect("failed to serialize userroom_id");

	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");

	let knocked_state = serialize_val(Json(knocked_state.unwrap_or_default()))
		.expect("failed to serialize knocked state");

	let count = count.into_unsigned().to_be_bytes();
	let mut txn = services_root.db.txn();

	txn.insert_raw(&self.db.userroomid_knockedstate, &userroom_id, knocked_state);
	txn.insert_raw(&self.db.roomuserid_knockedcount, &roomuser_id, count);
	txn.del_raw(&self.db.userroomid_joinedcount, &userroom_id);
	txn.del_raw(&self.db.roomuserid_joinedcount, &roomuser_id);
	txn.del_raw(&self.db.userroomid_invitestate, &userroom_id);
	txn.del_raw(&self.db.roomuserid_invitecount, &roomuser_id);
	txn.del_raw(&self.db.userroomid_leftstate, &userroom_id);
	txn.del_raw(&self.db.roomuserid_leftcount, &roomuser_id);
	self.commit_membership(room_id, txn).await
}

/// Makes a user forget a room.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn forget(&self, room_id: &RoomId, user_id: &UserId) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let userroom_id = (user_id, room_id);
	let roomuser_id = (room_id, user_id);
	let mut txn = services_root.db.txn();

	txn.del(&self.db.userroomid_leftstate, userroom_id);
	txn.del(&self.db.roomuserid_leftcount, roomuser_id);
	txn.execute().await
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
async fn mark_as_once_joined(&self, user_id: &UserId, room_id: &RoomId) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let key = (user_id, room_id);
	let key = serialize_key(key).expect("failed to serialize roomuseroncejoinedid");
	let mut txn = services_root.db.txn();

	txn.insert_raw(&self.db.roomuseroncejoinedids, key, []);
	txn.execute().await
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self, last_state, invite_via))]
pub(crate) async fn mark_as_invited(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	count: PduCount,
	last_state: StrippedRoomState,
	invite_via: Option<Vec<OwnedServerName>>,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let userroom_id = (user_id, room_id);
	let userroom_id = serialize_key(userroom_id).expect("failed to serialize userroom_id");

	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");

	let invite_state = serialize_val(Json(last_state.unwrap_or_default()))
		.expect("failed to serialize invite state");

	let count = count.into_unsigned().to_be_bytes();
	let mut txn = services_root.db.txn();

	txn.insert_raw(&self.db.userroomid_invitestate, &userroom_id, invite_state);
	txn.insert_raw(&self.db.roomuserid_invitecount, &roomuser_id, count);
	txn.del_raw(&self.db.userroomid_joinedcount, &userroom_id);
	txn.del_raw(&self.db.roomuserid_joinedcount, &roomuser_id);
	txn.del_raw(&self.db.userroomid_leftstate, &userroom_id);
	txn.del_raw(&self.db.roomuserid_leftcount, &roomuser_id);
	txn.del_raw(&self.db.userroomid_knockedstate, &userroom_id);
	txn.del_raw(&self.db.roomuserid_knockedcount, &roomuser_id);

	if let Some(servers) = invite_via.filter(is_not_empty!()) {
		self.add_servers_invite_via(&mut txn, room_id, servers)
			.await;
	}

	self.commit_membership(room_id, txn).await
}

#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
async fn ensure_remote_user(&self, user_id: &UserId) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	if services_root.globals.user_is_local(user_id) || services_root.users.exists(user_id).await {
		return Ok(());
	}

	services_root
		.users
		.create(user_id, None, None)
		.await
}

#[implement(super::Service)]
async fn handle_join(&self, room_id: &RoomId, user_id: &UserId, count: PduCount) -> Result {
	if !self.once_joined(user_id, room_id).await {
		self.mark_as_once_joined(user_id, room_id).await?;
		self.copy_predecessor_data(room_id, user_id)
			.await?;
	}

	self.mark_as_joined(user_id, room_id, count)
		.await?;

	Ok(())
}

#[implement(super::Service)]
async fn copy_predecessor_data(&self, room_id: &RoomId, user_id: &UserId) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let predecessor = services_root
		.state_accessor
		.room_state_get_content(room_id, &StateEventType::RoomCreate, "")
		.await
		.map(|content: RoomCreateEventContent| content.predecessor);

	let Ok(Some(predecessor)) = predecessor else {
		return Ok(());
	};

	self.copy_predecessor_tags(room_id, user_id, &predecessor.room_id)
		.await;

	self.copy_predecessor_direct(room_id, user_id, &predecessor.room_id)
		.await
}

#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
async fn copy_predecessor_tags(&self, room_id: &RoomId, user_id: &UserId, predecessor: &RoomId) {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let Ok(tag_event) = services_root
		.account_data
		.get_room(predecessor, user_id, RoomAccountDataEventType::Tag)
		.await
	else {
		return;
	};

	services_root
		.account_data
		.update(Some(room_id), user_id, RoomAccountDataEventType::Tag, &tag_event)
		.await
		.ok();
}

#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
async fn copy_predecessor_direct(
	&self,
	room_id: &RoomId,
	user_id: &UserId,
	predecessor: &RoomId,
) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let Ok(mut direct_event) = services_root
		.account_data
		.get_global::<DirectEvent>(user_id, GlobalAccountDataEventType::Direct)
		.await
	else {
		return Ok(());
	};

	let room_ids_updated =
		direct_event
			.content
			.0
			.values_mut()
			.fold(false, |updated, room_ids| {
				if !room_ids
					.iter()
					.any(|direct_room_id| direct_room_id == predecessor)
				{
					return updated;
				}

				room_ids.push(room_id.to_owned());

				true
			});

	if !room_ids_updated {
		return Ok(());
	}

	let event_type = GlobalAccountDataEventType::Direct
		.to_string()
		.into();

	let direct_event = serde_json::to_value(&direct_event).expect("to json always works");

	services_root
		.account_data
		.update(None, user_id, event_type, &direct_event)
		.await
}

#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
async fn handle_leave(&self, room_id: &RoomId, user_id: &UserId, count: PduCount) -> Result {
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	self.mark_as_left(user_id, room_id, count).await?;

	if services_root.globals.user_is_local(user_id)
		&& (services_root.config.forget_forced_upon_leave
			|| services_root.metadata.is_banned(room_id).await
			|| services_root.metadata.is_disabled(room_id).await)
	{
		self.forget(room_id, user_id).await?;
	}

	Ok(())
}
