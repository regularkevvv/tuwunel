mod adopt;
mod associate;
mod delete;
mod list_providers;
mod list_sessions;
mod list_users;
mod revoke;
mod revoke_sessions;
mod show_provider;
mod show_session;
mod show_user;
mod token_info;

use clap::Subcommand;
use ruma::OwnedUserId;
use tuwunel_core::{
	Result,
	either::{Either, Left, Right},
};
use tuwunel_service::oauth::{ProviderId, SessionId};

use crate::admin_command_dispatch;

#[admin_command_dispatch(handler_prefix = "oauth")]
#[derive(Debug, Subcommand)]
/// Query OAuth service state
pub(crate) enum OauthCommand {
	/// Adopt provider subjects from a migrated database.
	///
	/// The provider must use the same issuer and subject space as the source.
	/// If the source changed providers, associate each user individually.
	Adopt {
		/// ID of the configured provider that issued the subjects.
		///
		/// The provider must represent the issuer for every stored subject.
		provider: String,
	},

	/// Associate existing user with future authorization claims.
	Associate {
		/// ID of configured provider to listen on.
		provider: String,

		/// MXID of local user to associate.
		user_id: OwnedUserId,

		/// List of claims to match in key=value format.
		#[arg(long, required = true)]
		claim: Vec<String>,

		/// Replace existing committed SSO sessions before recording the claim;
		/// without it, associate refuses when committed sessions exist.
		#[arg(long)]
		force: bool,
	},

	/// List configured OAuth providers.
	ListProviders,

	/// List users associated with any OAuth session
	ListUsers,

	/// List session ID's
	ListSessions {
		#[arg(long)]
		user: Option<OwnedUserId>,
	},

	/// Show active configuration of a provider.
	ShowProvider {
		id: ProviderId,

		#[arg(long)]
		config: bool,
	},

	/// Show session state
	ShowSession {
		id: SessionId,
	},

	/// Show user sessions
	ShowUser {
		user_id: OwnedUserId,
	},

	/// Token introspection request to provider.
	TokenInfo {
		id: SessionId,
	},

	/// Revoke token for user_id or sess_id.
	Revoke {
		#[arg(value_parser = session_or_user_id)]
		id: Either<SessionId, OwnedUserId>,
	},

	/// Immediately revoke every Matrix session of a user and drop the stored
	/// upstream grant, leaving the account active.
	RevokeSessions {
		/// MXID of the local user whose sessions end now.
		user_id: OwnedUserId,
	},

	/// Remove oauth state (DANGER!)
	Delete {
		#[arg(value_parser = session_or_user_id)]
		id: Either<SessionId, OwnedUserId>,

		#[arg(long)]
		force: bool,
	},
}

type SessionOrUserId = Either<SessionId, OwnedUserId>;

fn session_or_user_id(input: &str) -> Result<SessionOrUserId> {
	OwnedUserId::parse(input)
		.map(Right)
		.or_else(|_| Ok(Left(input.to_owned())))
}
