use std::sync::Arc;

use futures::{Stream, StreamExt};
use ruma::{
	OwnedRoomId, OwnedServerName, OwnedUserId, RoomAliasId, RoomId, RoomOrAliasId, UserId,
	api::federation::query::get_room_information::v1::Request, events::StateEventType,
};
use tokio::sync::Mutex;
use tuwunel_core::{Err, Result, err, matrix::Event, utils::stream::TryIgnore};
use tuwunel_database::{Deserialized, Ignore, Interfix, Map};

use crate::appservice::RegistrationInfo;

mod inventory;

pub struct Service {
	db: Data,
	services: Arc<crate::services::OnceServices>,
	mutation: Mutex<()>,
}

struct Data {
	alias_userid: Arc<Map>,
	alias_roomid: Arc<Map>,
	aliasid_alias: Arc<Map>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				alias_userid: args.db["alias_userid"].clone(),
				alias_roomid: args.db["alias_roomid"].clone(),
				aliasid_alias: args.db["aliasid_alias"].clone(),
			},
			services: args.services.clone(),
			mutation: Mutex::new(()),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	pub async fn set_alias(&self, alias: &RoomAliasId, room_id: &RoomId) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		self.check_alias_local(alias)?;

		self.set_alias_by(alias, room_id, &services_root.globals.server_user)
			.await
	}

	#[tracing::instrument(skip(self))]
	pub async fn set_alias_by(
		&self,
		alias: &RoomAliasId,
		room_id: &RoomId,
		user_id: &UserId,
	) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		self.check_alias_local(alias)?;

		if alias == services_root.admin.admin_alias
			&& user_id != services_root.globals.server_user
		{
			return Err!(Request(Forbidden("Only the server user can set this alias")));
		}

		let _guard = self.mutation.lock().await;
		let localpart = alias.alias();
		let mut txn = services_root.db.txn();
		match self.resolve_local_alias(alias).await {
			| Ok(previous) =>
				self.stage_removed_alias(alias, &previous, &mut txn)
					.await?,
			| Err(error) if error.is_not_found() => {},
			| Err(error) => return Err(error),
		}
		let count = services_root.globals.next_count().await?;
		txn.insert_raw(&self.db.alias_userid, localpart, user_id);
		txn.insert_raw(&self.db.alias_roomid, localpart, room_id);
		txn.put_raw(&self.db.aliasid_alias, (room_id, *count), alias);
		txn.execute().await
	}

	pub async fn remove_alias_by(&self, alias: &RoomAliasId, user_id: &UserId) -> Result {
		if !self.user_can_remove_alias(alias, user_id).await? {
			return Err!(Request(Forbidden("User is not permitted to remove this alias.")));
		}

		self.remove_alias(alias).await
	}

	#[tracing::instrument(skip(self))]
	pub async fn remove_alias(&self, alias: &RoomAliasId) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		self.check_alias_local(alias)?;
		let _guard = self.mutation.lock().await;
		let room_id = self.resolve_local_alias(alias).await?;
		let mut txn = services_root.db.txn();
		self.stage_removed_alias(alias, &room_id, &mut txn)
			.await?;
		txn.execute().await
	}

	#[inline]
	pub async fn maybe_resolve(&self, room: &RoomOrAliasId) -> Result<OwnedRoomId> {
		match <&RoomId>::try_from(room) {
			| Ok(room_id) => Ok(room_id.to_owned()),
			| Err(alias) => Ok(self.resolve_alias(alias).await?.0),
		}
	}

	pub async fn maybe_resolve_with_servers(
		&self,
		room: &RoomOrAliasId,
		servers: Option<&[OwnedServerName]>,
	) -> Result<(OwnedRoomId, Vec<OwnedServerName>)> {
		match <&RoomId>::try_from(room) {
			| Ok(room_id) => Ok((room_id.to_owned(), Vec::from(servers.unwrap_or_default()))),
			| Err(alias) => self.resolve_alias(alias).await,
		}
	}

	#[tracing::instrument(skip(self), name = "resolve")]
	pub async fn resolve_alias(
		&self,
		room_alias: &RoomAliasId,
	) -> Result<(OwnedRoomId, Vec<OwnedServerName>)> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		if services_root.globals.alias_is_local(room_alias) {
			if let Ok(room_id) = self.resolve_local_alias(room_alias).await {
				return Ok((room_id, Vec::new()));
			}

			if let Ok(room_id) = self.resolve_appservice_alias(room_alias).await {
				return Ok((room_id, Vec::new()));
			}

			return Err!(Request(NotFound("Room with alias not found.")));
		}

		return self.remote_resolve(room_alias).await;
	}

	async fn remote_resolve(
		&self,
		room_alias: &RoomAliasId,
	) -> Result<(OwnedRoomId, Vec<OwnedServerName>)> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		let server = room_alias.server_name();

		let request = Request { room_alias: room_alias.to_owned() };

		let response = services_root
			.federation
			.execute(server, request)
			.await?;

		Ok((response.room_id, response.servers))
	}

	#[tracing::instrument(skip(self), level = "trace")]
	pub async fn resolve_local_alias(&self, alias: &RoomAliasId) -> Result<OwnedRoomId> {
		self.check_alias_local(alias)?;
		self.db
			.alias_roomid
			.get(alias.alias())
			.await
			.deserialized()
	}

	#[tracing::instrument(skip(self), level = "debug")]
	pub fn local_aliases_for_room<'a>(
		&'a self,
		room_id: &'a RoomId,
	) -> impl Stream<Item = &RoomAliasId> + Send + 'a {
		let prefix = (room_id, Interfix);
		self.db
			.aliasid_alias
			.stream_prefix(&prefix)
			.ignore_err()
			.map(|(_, alias): (Ignore, &RoomAliasId)| alias)
	}

	#[tracing::instrument(skip(self), level = "debug")]
	pub fn all_local_aliases(&self) -> impl Stream<Item = (&RoomId, &str)> + Send + '_ {
		self.db
			.alias_roomid
			.stream()
			.ignore_err()
			.map(|(alias_localpart, room_id): (&str, &RoomId)| (room_id, alias_localpart))
	}

	async fn user_can_remove_alias(&self, alias: &RoomAliasId, user_id: &UserId) -> Result<bool> {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		self.check_alias_local(alias)?;

		let room_id = self
			.resolve_local_alias(alias)
			.await
			.map_err(|_| err!(Request(NotFound("Alias not found."))))?;

		// The creator of an alias can remove it
		if self
            .who_created_alias(alias).await
            .is_ok_and(|user| user == user_id)
            // Server admins can remove any local alias
            || services_root.admin.user_is_admin(user_id).await
		{
			return Ok(true);
		}

		// Checking whether the user is able to change canonical aliases of the room
		if let Ok(power_levels) = services_root
			.state_accessor
			.get_power_levels(&room_id)
			.await
		{
			return Ok(
				power_levels.user_can_send_state(user_id, StateEventType::RoomCanonicalAlias)
			);
		}

		// If there is no power levels event, only the room creator can change
		// canonical aliases
		if let Ok(event) = services_root
			.state_accessor
			.room_state_get(&room_id, &StateEventType::RoomCreate, "")
			.await
		{
			return Ok(event.sender() == user_id);
		}

		Err!(Database("Room has no m.room.create event"))
	}

	async fn who_created_alias(&self, alias: &RoomAliasId) -> Result<OwnedUserId> {
		self.check_alias_local(alias)?;

		self.db
			.alias_userid
			.get(alias.alias())
			.await
			.deserialized()
	}

	async fn resolve_appservice_alias(&self, room_alias: &RoomAliasId) -> Result<OwnedRoomId> {
		use ruma::api::appservice::query::query_room_alias;

		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		self.check_alias_local(room_alias)?;

		for appservice in services_root.appservice.read().await.values() {
			if appservice.aliases.is_match(room_alias.as_str())
				&& matches!(
					services_root
						.appservice
						.send_request(
							appservice.registration.clone(),
							query_room_alias::v1::Request { room_alias: room_alias.to_owned() },
						)
						.await,
					Ok(Some(_opt_result))
				) {
				return self
					.resolve_local_alias(room_alias)
					.await
					.map_err(|_| err!(Request(NotFound("Room does not exist."))));
			}
		}

		Err!(Request(NotFound("Room does not exist.")))
	}

	fn check_alias_local(&self, alias: &RoomAliasId) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		if !services_root.globals.alias_is_local(alias) {
			return Err!(Request(InvalidParam("Alias is from another server.")));
		}

		Ok(())
	}

	#[tracing::instrument(skip(self, appservice_info), level = "trace")]
	pub async fn appservice_checks(
		&self,
		room_alias: &RoomAliasId,
		appservice_info: &Option<RegistrationInfo>,
	) -> Result {
		let services_guard = self.services.get();
		let services_root = services_guard.as_ref();

		self.check_alias_local(room_alias)?;
		if let Some(info) = appservice_info {
			if !info.aliases.is_match(room_alias.as_str()) {
				return Err!(Request(Exclusive("Room alias is not in namespace.")));
			}
		} else if services_root
			.appservice
			.is_exclusive_alias(room_alias)
			.await
		{
			return Err!(Request(Exclusive("Room alias reserved by appservice.")));
		}

		Ok(())
	}
}
