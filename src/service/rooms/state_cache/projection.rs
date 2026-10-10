//! Accepted current state owns its remaining membership-index work. Each
//! transition and its cursor commit together, before aggregate repair retires
//! the plan. No retry allocates another stream position.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::{TryStreamExt, pin_mut};
use ruma::{
	OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
};
use serde::{Deserialize, Serialize};
use tuwunel_core::{
	Error, Result, error,
	matrix::{Event, Pdu, PduCount},
};
use tuwunel_database::{Ignore, Interfix, Json, Txn};

use super::{Service, StrippedRoomState, update::RECOUNT_PENDING};
use crate::rooms::state::RoomMutexGuard;

pub(super) const PENDING: &str = "membership_projection_v1";
pub(super) const WITNESS: &str = "membership_projection_witness_v1";
pub(super) const CURSOR: &str = "membership_projection_cursor_v1";
const MAX_MEMBERS: usize = 4096;
const MAX_PLAN_BYTES: usize = 1_500_000;
const MAX_PENDING: usize = 64;
const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Plan {
	format: u8,
	room: OwnedRoomId,
	before: Option<u64>,
	after: u64,
	count: u64,
	next: usize,
	members: Vec<Member>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Member {
	user: OwnedUserId,
	event: OwnedEventId,
	removed: bool,
	ignored: bool,
	forgotten: bool,
	stripped: StrippedRoomState,
}

fn bad() -> Error { Error::bad_database("Invalid accepted membership projection") }

impl Plan {
	fn digest(&self) -> Result<[u8; 32]> {
		Ok(tuwunel_core::utils::hash::sha256::hash(&serde_json::to_vec(&(
			self.format,
			&self.room,
			self.before,
			self.after,
			self.count,
			&self.members,
		))?))
	}

	fn encode(&self) -> Result<Vec<u8>> {
		let value = serde_json::to_vec(self)?;
		if value.len() > MAX_PLAN_BYTES {
			return Err(bad());
		}
		Ok(value)
	}

	fn decode(room: &RoomId, value: &[u8]) -> Result<Self> {
		if value.len() > MAX_PLAN_BYTES {
			return Err(bad());
		}
		let plan: Self = serde_json::from_slice(value).map_err(|_| bad())?;
		if plan.format != 1
			|| plan.room != room
			|| plan.after == 0
			|| plan.count == 0
			|| plan.before == Some(0)
			|| plan.members.is_empty()
			|| plan.members.len() > MAX_MEMBERS
			|| plan.next != 0
			|| plan
				.members
				.windows(2)
				.any(|pair| pair[0].user >= pair[1].user)
		{
			return Err(bad());
		}
		Ok(plan)
	}
}

impl Service {
	/// The caller holds room state. Complete prior work before another
	/// publication can replace the immutable state used by its journal.
	pub(crate) async fn resume_membership_projection(
		&self,
		room: &RoomId,
		_state: &RoomMutexGuard,
	) -> Result {
		let services_guard = self.services.get();
		let services = services_guard.as_ref();
		let value = match services.db["global"].qry(&(PENDING, room)).await {
			| Ok(value) => value,
			| Err(error) if error.is_not_found() => return Ok(()),
			| Err(error) => return Err(error),
		};
		let mut plan = self.load_projection(room, &value).await?;
		let members = self.validate_projection(&plan).await?;
		while plan.next < plan.members.len() {
			let member = &plan.members[plan.next];
			let pdu = members.get(&member.user).ok_or_else(bad)?;
			let mut content: RoomMemberEventContent = pdu.get_content()?;
			if member.removed {
				content.membership = MembershipState::Leave;
			}
			self.ensure_remote_user(&member.user).await?;
			if content.membership == MembershipState::Join
				&& !self.once_joined(&member.user, room).await
			{
				self.copy_predecessor_data(room, &member.user)
					.await?;
			}
			let guard = self.membership_mutex.lock(room).await;
			let mut txn = services.db.txn();
			if !member.ignored {
				self.stage_projected_member(
					&mut txn,
					room,
					&member.user,
					plan.count,
					&content.membership,
					&member.stripped,
					member.forgotten,
				)?;
			}
			plan.next = plan.next.saturating_add(1);
			txn.put_raw(
				&services.db["global"],
				(CURSOR, room),
				u64::try_from(plan.next)
					.map_err(|_| bad())?
					.to_be_bytes(),
			);
			txn.put(&services.db["global"], (RECOUNT_PENDING, room), &[0_u8; 0][..]);
			txn.check_bridge_admission()?;
			let committed = txn.execute_flushed().await;
			self.appservice_in_room_cache
				.write()
				.expect("locked")
				.invalidate(room);
			drop(guard);
			committed?;
		}
		self.repair_joined_count(room).await?;
		for member in &plan.members {
			let pdu = members.get(&member.user).ok_or_else(bad)?;
			let content: RoomMemberEventContent = pdu.get_content()?;
			if services.globals.user_is_local(&member.user)
				&& (member.removed
					|| matches!(
						content.membership,
						MembershipState::Leave | MembershipState::Ban
					)) {
				services
					.sending
					.refresh_push_badge(&member.user)
					.await?;
			}
		}
		let mut txn = services.db.txn();
		txn.del(&services.db["global"], (PENDING, room));
		txn.del(&services.db["global"], (WITNESS, room));
		txn.del(&services.db["global"], (CURSOR, room));
		txn.execute_flushed().await
	}

	/// Freeze changed and removed membership cells before canonical admission.
	/// The plan is staged in that same transaction, never in a later effect.
	pub(crate) async fn stage_membership_projection(
		&self,
		txn: &mut Txn,
		room: &RoomId,
		after: Option<u64>,
		pending: Option<&Pdu>,
		count: PduCount,
		_state: &RoomMutexGuard,
	) -> Result<Option<tokio::sync::OwnedMutexGuard<()>>> {
		let services_guard = self.services.get();
		let services = services_guard.as_ref();
		let admission = self
			.projection_admission
			.clone()
			.lock_owned()
			.await;
		if services.db["global"]
			.contains_checked(&(PENDING, room))
			.await? || services.db["global"]
			.contains_checked(&(WITNESS, room))
			.await?
		{
			return Err(bad());
		}
		if services.db["global"]
			.contains_checked(&(CURSOR, room))
			.await?
		{
			return Err(bad());
		}
		let before = match services.state.get_room_shortstatehash(room).await {
			| Ok(state) => Some(state),
			| Err(error) if error.is_not_found() => None,
			| Err(error) => return Err(error),
		};
		let Some(after) = after.or(before) else {
			return Ok(None);
		};
		if before == Some(after) {
			return Ok(None);
		}
		let old = self.projection_state(room, before, None).await?;
		let new = self
			.projection_state(room, Some(after), pending)
			.await?;
		let changed = difference(&old, &new);
		if changed.is_empty() {
			return Ok(None);
		}
		if changed.len() > MAX_MEMBERS {
			return Err(bad());
		}
		let mut members = Vec::with_capacity(changed.len());
		let mut member_bytes = 1024_usize;
		for (user, (pdu, removed)) in changed {
			let content: RoomMemberEventContent = pdu.get_content()?;
			let membership = if removed {
				MembershipState::Leave
			} else {
				content.membership
			};
			if !matches!(
				membership,
				MembershipState::Join
					| MembershipState::Invite
					| MembershipState::Leave
					| MembershipState::Ban
					| MembershipState::Knock
			) {
				return Err(bad());
			}
			let ignored = membership == MembershipState::Invite
				&& services
					.users
					.user_is_ignored_checked(&pdu.sender, &user)
					.await?;
			let forgotten = services.globals.user_is_local(&user)
				&& matches!(membership, MembershipState::Leave | MembershipState::Ban)
				&& (services.config.forget_forced_upon_leave
					|| services.metadata.is_banned(room).await
					|| services.metadata.is_disabled(room).await);
			let stripped =
				if matches!(membership, MembershipState::Invite | MembershipState::Knock) {
					Some(
						self.projection_stripped(after, pdu, pending)
							.await?,
					)
				} else {
					None
				};
			let member = Member {
				user,
				event: pdu.event_id.clone(),
				removed,
				ignored,
				forgotten,
				stripped,
			};
			member_bytes =
				member_bytes.saturating_add(tuwunel_core::utils::json::serialized_len(&member)?);
			if member_bytes > MAX_PLAN_BYTES {
				return Err(bad());
			}
			members.push(member);
		}
		let plan = Plan {
			format: 1,
			room: room.to_owned(),
			before,
			after,
			count: count.into_unsigned(),
			next: 0,
			members,
		};
		let encoded = plan.encode()?;
		let rooms = self.pending_projection_rooms().await?;
		if rooms.len() >= MAX_PENDING {
			return Err(bad());
		}
		let mut bytes = encoded.len().saturating_add(4096);
		for existing in rooms {
			let value = services.db["global"]
				.qry(&(PENDING, &existing))
				.await?;
			Plan::decode(&existing, &value)?;
			bytes = bytes.saturating_add(value.len());
			if bytes > MAX_PENDING_BYTES {
				return Err(bad());
			}
		}
		txn.put_raw(&services.db["global"], (PENDING, room), encoded);
		txn.put_raw(&services.db["global"], (WITNESS, room), plan.digest()?);
		txn.put_raw(&services.db["global"], (CURSOR, room), 0_u64.to_be_bytes());
		Ok(Some(admission))
	}

	async fn projection_stripped(
		&self,
		state: u64,
		pdu: &Pdu,
		pending: Option<&Pdu>,
	) -> Result<Vec<ruma::serde::Raw<ruma::events::AnyStrippedStateEvent>>> {
		let services_guard = self.services.get();
		let services = services_guard.as_ref();
		let mut stripped = Vec::new();
		let mut bytes = 0_usize;
		for (kind, key) in [
			(StateEventType::RoomCreate, ""),
			(StateEventType::RoomJoinRules, ""),
			(StateEventType::RoomCanonicalAlias, ""),
			(StateEventType::RoomName, ""),
			(StateEventType::RoomAvatar, ""),
			(StateEventType::RoomMember, pdu.sender.as_str()),
			(StateEventType::RoomEncryption, ""),
			(StateEventType::RoomTopic, ""),
		] {
			let Some(event) = services
				.state_accessor
				.state_get_optional_for_append(state, &kind, key, pending)
				.await?
			else {
				continue;
			};
			if event.room_id != pdu.room_id {
				return Err(bad());
			}
			let raw = event.to_format();
			bytes = bytes.saturating_add(tuwunel_core::utils::json::serialized_len(&raw)?);
			if bytes > 128 * 1024 {
				return Err(bad());
			}
			stripped.push(raw);
		}
		let raw = pdu.to_format();
		bytes = bytes.saturating_add(tuwunel_core::utils::json::serialized_len(&raw)?);
		if bytes > 128 * 1024 {
			return Err(bad());
		}
		stripped.push(raw);
		Ok(stripped)
	}

	async fn projection_state(
		&self,
		room: &RoomId,
		state: Option<u64>,
		pending: Option<&Pdu>,
	) -> Result<BTreeMap<OwnedUserId, Pdu>> {
		let services_guard = self.services.get();
		let services = services_guard.as_ref();
		let mut members = BTreeMap::new();
		let Some(state) = state else {
			return Ok(members);
		};
		let entries = services.state_accessor.state_full_shortids(state);
		pin_mut!(entries);
		let mut bytes = 0_usize;
		let mut payload_bytes = 0_usize;
		while let Some((key, event)) = entries.try_next().await? {
			let (kind, state_key) = services
				.short
				.get_statekey_from_short(key)
				.await?;
			let event_id: OwnedEventId = services
				.short
				.get_eventid_from_short(event)
				.await?;
			bytes = bytes
				.saturating_add(kind.to_cow_str().len())
				.saturating_add(state_key.as_str().len())
				.saturating_add(event_id.as_str().len());
			if bytes > 512 * 1024
				|| services
					.short
					.get_shortstatekey(&kind, state_key.as_str())
					.await? != key
				|| services.short.get_shorteventid(&event_id).await? != event
			{
				return Err(bad());
			}
			if kind != StateEventType::RoomMember {
				continue;
			}
			let pdu = services
				.state_accessor
				.state_event_for_append(&event_id, pending)
				.await?;
			if pdu.event_id != event_id
				|| pdu.room_id != room
				|| pdu.kind.to_cow_str() != kind.to_cow_str()
				|| pdu.state_key.as_deref() != Some(state_key.as_str())
			{
				return Err(bad());
			}
			let _: RoomMemberEventContent = pdu.get_content()?;
			payload_bytes =
				payload_bytes.saturating_add(tuwunel_core::utils::json::serialized_len(&pdu)?);
			if payload_bytes > MAX_PLAN_BYTES {
				return Err(bad());
			}
			let user = UserId::parse(state_key.as_str())?;
			if members.len() >= MAX_MEMBERS || members.insert(user, pdu).is_some() {
				return Err(bad());
			}
		}
		Ok(members)
	}

	async fn load_projection(&self, room: &RoomId, value: &[u8]) -> Result<Plan> {
		let services_guard = self.services.get();
		let services = services_guard.as_ref();
		let mut plan = Plan::decode(room, value)?;
		let cursor = services.db["global"].qry(&(CURSOR, room)).await?;
		plan.next =
			usize::try_from(u64::from_be_bytes(cursor.as_ref().try_into().map_err(|_| bad())?))
				.map_err(|_| bad())?;
		if plan.next > plan.members.len() {
			return Err(bad());
		}
		Ok(plan)
	}

	async fn validate_projection(&self, plan: &Plan) -> Result<BTreeMap<OwnedUserId, Pdu>> {
		let services_guard = self.services.get();
		let services = services_guard.as_ref();
		if services.db["global"]
			.qry(&(WITNESS, &plan.room))
			.await?
			.as_ref() != plan.digest()?
		{
			return Err(bad());
		}
		// send_join/send_knock install immutable state before the first local
		// timeline PDU. The room dictionary and published state bind recovery;
		// requiring a timeline here would strand a valid first join.
		services.short.get_shortroomid(&plan.room).await?;
		if services
			.state
			.get_room_shortstatehash(&plan.room)
			.await? != plan.after
		{
			return Err(bad());
		}
		let old = self
			.projection_state(&plan.room, plan.before, None)
			.await?;
		let new = self
			.projection_state(&plan.room, Some(plan.after), None)
			.await?;
		let changed = difference(&old, &new);
		if changed.len() != plan.members.len() {
			return Err(bad());
		}
		for (index, (member, (user, (pdu, removed)))) in plan
			.members
			.iter()
			.zip(changed.iter())
			.enumerate()
		{
			let content: RoomMemberEventContent = pdu.get_content()?;
			let membership = if *removed {
				MembershipState::Leave
			} else {
				content.membership
			};
			if member.user != *user
				|| member.event != pdu.event_id
				|| member.removed != *removed
				|| (member.ignored && membership != MembershipState::Invite)
				|| (member.forgotten
					&& (!services.globals.user_is_local(user)
						|| !matches!(membership, MembershipState::Leave | MembershipState::Ban)))
				|| (member.stripped.is_some()
					!= matches!(membership, MembershipState::Invite | MembershipState::Knock))
			{
				return Err(bad());
			}
			if index < plan.next && !member.ignored {
				self.validate_projected_member(&plan.room, member, plan.count, &membership)
					.await?;
			}
		}
		Ok(changed
			.into_iter()
			.map(|(user, (pdu, _))| (user, pdu.clone()))
			.collect())
	}

	/// Exactly one member's indexes fit comfortably inside the bridge batch.
	/// Progress is added by the caller to this same unexecuted transaction.
	#[expect(clippy::too_many_arguments)]
	fn stage_projected_member(
		&self,
		txn: &mut Txn,
		room: &RoomId,
		user: &UserId,
		count: u64,
		membership: &MembershipState,
		stripped: &StrippedRoomState,
		forgotten: bool,
	) -> Result {
		let target = member_slot(membership, forgotten)?;
		for (index, (forward, reverse)) in [
			(&self.db.roomuserid_joinedcount, &self.db.userroomid_joinedcount),
			(&self.db.roomuserid_invitecount, &self.db.userroomid_invitestate),
			(&self.db.roomuserid_leftcount, &self.db.userroomid_leftstate),
			(&self.db.roomuserid_knockedcount, &self.db.userroomid_knockedstate),
		]
		.into_iter()
		.enumerate()
		{
			if target != Some(index) {
				txn.del(forward, (room, user));
				txn.del(reverse, (user, room));
			} else {
				txn.put_raw(forward, (room, user), count.to_be_bytes());
				if index == 0 {
					txn.put_raw(reverse, (user, room), count.to_be_bytes());
				} else if index == 2 {
					txn.put(reverse, (user, room), Json(Vec::<serde_json::Value>::new()));
				} else {
					txn.put(reverse, (user, room), Json(stripped.as_deref().unwrap_or_default()));
				}
			}
		}
		if membership == &MembershipState::Join {
			txn.put_raw(&self.db.roomuseroncejoinedids, (user, room), []);
		}
		Ok(())
	}

	async fn validate_projected_member(
		&self,
		room: &RoomId,
		member: &Member,
		count: u64,
		membership: &MembershipState,
	) -> Result {
		let target = member_slot(membership, member.forgotten)?;
		for (index, (forward, reverse)) in [
			(&self.db.roomuserid_joinedcount, &self.db.userroomid_joinedcount),
			(&self.db.roomuserid_invitecount, &self.db.userroomid_invitestate),
			(&self.db.roomuserid_leftcount, &self.db.userroomid_leftstate),
			(&self.db.roomuserid_knockedcount, &self.db.userroomid_knockedstate),
		]
		.into_iter()
		.enumerate()
		{
			if target != Some(index) {
				if forward
					.contains_checked(&(room, &member.user))
					.await? || reverse
					.contains_checked(&(&member.user, room))
					.await?
				{
					return Err(bad());
				}
			} else {
				if forward.qry(&(room, &member.user)).await?.as_ref() != count.to_be_bytes() {
					return Err(bad());
				}
				let expected = if index == 0 {
					count.to_be_bytes().to_vec()
				} else if index == 2 {
					tuwunel_database::serialize_val(Json(Vec::<serde_json::Value>::new()))?
						.into_vec()
				} else {
					tuwunel_database::serialize_val(Json(
						member.stripped.as_deref().unwrap_or_default(),
					))?
					.into_vec()
				};
				if reverse.qry(&(&member.user, room)).await?.as_ref() != expected {
					return Err(bad());
				}
			}
		}
		if membership == &MembershipState::Join
			&& !self
				.db
				.roomuseroncejoinedids
				.contains_checked(&(&member.user, room))
				.await?
		{
			return Err(bad());
		}
		Ok(())
	}

	async fn pending_projection_rooms(&self) -> Result<Vec<OwnedRoomId>> {
		let rooms = self.projection_rooms(PENDING).await?;
		if rooms != self.projection_rooms(WITNESS).await?
			|| rooms != self.projection_rooms(CURSOR).await?
		{
			return Err(bad());
		}
		Ok(rooms)
	}

	async fn projection_rooms(&self, name: &str) -> Result<Vec<OwnedRoomId>> {
		let services_guard = self.services.get();
		let services = services_guard.as_ref();
		let prefix = (name, Interfix);
		let keys = services.db["global"]
			.keys_prefix_capped::<(Ignore, &RoomId), _>(&prefix, MAX_PENDING.saturating_add(1));
		pin_mut!(keys);
		let mut rooms = Vec::new();
		let mut bytes = 0_usize;
		while let Some((_, room)) = keys.try_next().await? {
			bytes = bytes.saturating_add(room.as_bytes().len());
			if rooms.len() >= MAX_PENDING || bytes > 128 * 1024 {
				return Err(bad());
			}
			rooms.push(room.to_owned());
		}
		Ok(rooms)
	}

	pub(super) async fn restore_membership_projections(&self) -> Result {
		let services_guard = self.services.get();
		let services = services_guard.as_ref();
		let rooms = self.pending_projection_rooms().await?;
		let mut bytes = 0_usize;
		// Validate the entire pending inventory before the first repair writes.
		for room in &rooms {
			let value = services.db["global"]
				.qry(&(PENDING, room))
				.await?;
			bytes = bytes.saturating_add(value.len());
			if bytes > MAX_PENDING_BYTES {
				return Err(bad());
			}
			self.validate_projection(&self.load_projection(room, &value).await?)
				.await?;
		}
		for room in rooms {
			let state = services.state.mutex.lock(&room).await;
			self.resume_membership_projection(&room, &state)
				.await?;
		}
		Ok(())
	}

	pub(super) async fn projection_worker(self: Arc<Self>) -> Result {
		let services_guard = self.services.get();
		let services = services_guard.as_ref();
		if services.config.maintenance {
			return Ok(());
		}
		let mut interval = tokio::time::interval(Duration::from_secs(15));
		loop {
			tokio::select! {
				() = self.projection_stop.notified() => return Ok(()),
				_ = interval.tick() => {},
			}
			let repair = self.restore_pending_recounts();
			tokio::select! {
				() = self.projection_stop.notified() => return Ok(()),
				result = repair => if let Err(error) = result { error!("Membership recovery refused: {error}"); },
			}
		}
	}
}

fn member_slot(membership: &MembershipState, forgotten: bool) -> Result<Option<usize>> {
	Ok(match membership {
		| MembershipState::Join => Some(0),
		| MembershipState::Invite => Some(1),
		| MembershipState::Leave | MembershipState::Ban if !forgotten => Some(2),
		| MembershipState::Leave | MembershipState::Ban => None,
		| MembershipState::Knock => Some(3),
		| _ => return Err(bad()),
	})
}

fn difference<'a>(
	old: &'a BTreeMap<OwnedUserId, Pdu>,
	new: &'a BTreeMap<OwnedUserId, Pdu>,
) -> BTreeMap<OwnedUserId, (&'a Pdu, bool)> {
	let mut changed = BTreeMap::new();
	for (user, pdu) in new {
		if old
			.get(user)
			.is_none_or(|old| old.event_id != pdu.event_id)
		{
			changed.insert(user.clone(), (pdu, false));
		}
	}
	for (user, pdu) in old {
		if !new.contains_key(user) {
			changed.insert(user.clone(), (pdu, true));
		}
	}
	changed
}
