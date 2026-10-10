mod account_data;
mod appservice;
mod feds;
mod globals;
mod oauth;
mod peer_status;
mod presence;
mod pusher;
mod raw;
mod resolver;
mod room_alias;
mod room_state_cache;
mod room_timeline;
mod sending;
mod short;
mod storage;
mod sync;
mod threepid;
mod users;

use clap::Subcommand;
use tuwunel_core::Result;

use self::{
	account_data::AccountDataCommand, appservice::AppserviceCommand, feds::FedsCommand,
	globals::GlobalsCommand, oauth::OauthCommand, peer_status::PeerStatusCommand,
	presence::PresenceCommand, pusher::PusherCommand, raw::RawCommand, resolver::ResolverCommand,
	room_alias::RoomAliasCommand, room_state_cache::RoomStateCacheCommand,
	room_timeline::RoomTimelineCommand, sending::SendingCommand, short::ShortCommand,
	storage::StorageCommand, sync::SyncCommand, threepid::ThreepidCommand, users::UsersCommand,
};
use crate::{
	admin_command_dispatch,
	event_fetcher::{self as fetch, EventFetcherCommand},
};

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
/// Query tables from database
pub(super) enum QueryCommand {
	/// - account_data.rs iterators and getters
	#[command(subcommand)]
	AccountData(AccountDataCommand),

	/// - appservice.rs iterators and getters
	#[command(subcommand)]
	Appservice(AppserviceCommand),

	/// - federation fanout diagnostics
	#[command(subcommand)]
	Feds(FedsCommand),

	/// - Drive the federation event-fetcher service directly (diagnostic)
	#[command(subcommand)]
	Fetch(EventFetcherCommand),

	/// - presence.rs iterators and getters
	#[command(subcommand)]
	Presence(PresenceCommand),

	/// - rooms/alias.rs iterators and getters
	#[command(subcommand)]
	RoomAlias(RoomAliasCommand),

	/// - rooms/state_cache iterators and getters
	#[command(subcommand)]
	RoomStateCache(RoomStateCacheCommand),

	/// - rooms/timeline iterators and getters
	#[command(subcommand)]
	RoomTimeline(RoomTimelineCommand),

	/// - globals.rs iterators and getters
	#[command(subcommand)]
	Globals(GlobalsCommand),

	/// - sending.rs iterators and getters
	#[command(subcommand)]
	Sending(SendingCommand),

	/// - users.rs iterators and getters
	#[command(subcommand)]
	Users(UsersCommand),

	/// - threepid service
	#[command(subcommand)]
	Threepid(ThreepidCommand),

	/// - resolver service
	#[command(subcommand)]
	Resolver(ResolverCommand),

	/// - per-server reachability store on the federation service
	#[command(subcommand)]
	PeerStatus(PeerStatusCommand),

	/// - pusher service
	#[command(subcommand)]
	Pusher(PusherCommand),

	/// - short service
	#[command(subcommand)]
	Short(ShortCommand),

	/// - storage service
	#[command(subcommand)]
	Storage(StorageCommand),

	/// - sync service
	#[command(subcommand)]
	Sync(SyncCommand),

	/// - oauth service
	#[command(subcommand)]
	Oauth(OauthCommand),

	/// - raw service
	#[command(subcommand)]
	Raw(RawCommand),
}

/// Production diagnostics must have an audited finite source. New diagnostic
/// variants default to refusal on D1 until their bounds have been reviewed.
pub(super) fn remote_scan_allowed(command: &QueryCommand) -> bool {
	match command {
		| QueryCommand::Raw(command) => match command {
			| RawCommand::Maps
			| RawCommand::Sequence
			| RawCommand::Get { .. }
			| RawCommand::Put { .. }
			| RawCommand::Del { .. }
			| RawCommand::Flush => true,
			| RawCommand::Keys { limit: Some(limit), .. }
			| RawCommand::Iter { limit: Some(limit), .. } => (1..=16).contains(limit),
			| _ => false,
		},
		| QueryCommand::Globals(_) | QueryCommand::Short(_) => true,
		| QueryCommand::AccountData(AccountDataCommand::AccountDataGet { .. })
		| QueryCommand::Presence(PresenceCommand::GetPresence { .. })
		| QueryCommand::Appservice(AppserviceCommand::GetRegistration { .. })
		| QueryCommand::Sending(SendingCommand::GetLatestEduCount { .. }) => true,
		| QueryCommand::Users(command) => matches!(
			command,
			UsersCommand::CountUsers
				| UsersCommand::IterUsers { .. }
				| UsersCommand::ListDevices { .. }
				| UsersCommand::ListDevicesMetadata { .. }
				| UsersCommand::PasswordHash { .. }
				| UsersCommand::GetDeviceMetadata { .. }
				| UsersCommand::GetDevicesVersion { .. }
				| UsersCommand::GetDeviceKeys { .. }
				| UsersCommand::GetUserSigningKey { .. }
				| UsersCommand::GetMasterKey { .. }
		),
		| QueryCommand::Oauth(command) => matches!(
			command,
			OauthCommand::ListProviders | OauthCommand::ShowProvider { .. }
			| OauthCommand::ShowSession { .. } | OauthCommand::TokenInfo { .. }
			// Required immediate operator revocation is an audited behavior flow,
			// not a diagnostic dump. Its service bounds need separate review.
			| OauthCommand::RevokeSessions { .. }
		),
		| _ => false,
	}
}
