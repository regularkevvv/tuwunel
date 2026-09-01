use std::str::from_utf8;

use futures::StreamExt;
use tuwunel_core::{Result, err, implement, utils::random_string, warn};
use tuwunel_database::Cbor;

use super::{SESSION_ID_LENGTH, Session, Sessions};
use crate::{
	migrations::local_user_id,
	oauth::{Provider, UserInfo, unique_id_sub},
};

/// Results from adopting provider subjects stored by another database.
///
/// The counters distinguish new writes from safe skips. A missing source
/// column is reported separately from rows that could not be interpreted.
#[derive(Clone, Copy, Debug, Default)]
pub struct Counts {
	/// New durable associations written.
	pub adopted: usize,

	/// Associations that already resolve to the intended user.
	pub already_bound: usize,

	/// Associations left untouched because the identity key is occupied.
	pub collision: usize,

	/// Rows whose local account is absent or unusable.
	pub absent: usize,

	/// Rows whose subject or localpart is not valid UTF-8.
	pub invalid: usize,

	/// Whether the foreign identity column exists.
	pub foreign_column: bool,

	unreadable: usize,
}

#[derive(Clone, Copy)]
enum Adoption {
	Adopted,
	AlreadyBound,
	Collision,
	Absent,
	Invalid,
}

impl Counts {
	fn tally(&mut self, result: Result<Adoption>) {
		match result {
			| Ok(Adoption::Adopted) => self.adopted = self.adopted.saturating_add(1),
			| Ok(Adoption::Absent) => self.absent = self.absent.saturating_add(1),
			| Ok(Adoption::Invalid) => self.invalid = self.invalid.saturating_add(1),
			| Ok(Adoption::Collision) => self.collision = self.collision.saturating_add(1),
			| Ok(Adoption::AlreadyBound) => {
				self.already_bound = self.already_bound.saturating_add(1);
			},
			| Err(e) => {
				warn!(error = %e, "a provider subject could not be read");
				self.unreadable = self.unreadable.saturating_add(1);
			},
		}
	}
}

#[implement(Sessions)]
/// Adopts foreign provider subjects as durable, one-time session bridges.
///
/// Existing identity keys are never replaced. Each new association commits
/// independently, so a read error leaves earlier writes available to an
/// idempotent retry.
#[tracing::instrument(level = "debug", skip(self, provider))]
pub async fn adopt_foreign_subjects(&self, provider: &Provider) -> Result<Counts> {
	unique_id_sub((provider, ""))?;

	let Some(subjects) = self
		.db
		.database
		.open_cf("openidsubject_localpart")?
	else {
		return Ok(Counts::default());
	};

	let server_name = self.services.globals.server_name();
	let cork = self.db.database.cork_and_sync();
	let counts = Counts {
		foreign_column: true,
		..Default::default()
	};

	let counts = subjects
		.raw_stream()
		.fold(counts, async |mut counts, row| {
			let result = match row {
				| Err(e) => Err(e),
				| Ok((sub, localpart)) => match (from_utf8(sub), from_utf8(localpart)) {
					| (Ok(sub), Ok(localpart)) =>
						self.adopt_foreign_subject(provider, sub, localpart, server_name)
							.await,

					| _ => Ok(Adoption::Invalid),
				},
			};

			counts.tally(result);
			counts
		})
		.await;

	drop(cork);

	let unreadable = counts.unreadable;

	unreadable
		.eq(&0)
		.then_some(counts)
		.ok_or_else(|| err!(Database("{unreadable} provider subjects could not be read")))
}

#[implement(Sessions)]
async fn adopt_foreign_subject(
	&self,
	provider: &Provider,
	sub: &str,
	localpart: &str,
	server_name: &ruma::ServerName,
) -> Result<Adoption> {
	let Some(user_id) = local_user_id(localpart, server_name) else {
		return Ok(Adoption::Absent);
	};

	if user_id == self.services.globals.server_user || !self.services.users.exists(&user_id).await
	{
		return Ok(Adoption::Absent);
	}

	let unique_id = unique_id_sub((provider, sub))?;
	let _write_guard = self.write_locks.lock(&unique_id).await;
	match self.get_sess_id_by_unique_id(&unique_id).await {
		| Err(e) if e.is_not_found() => (),
		| Err(e) => return Err(e),
		| Ok(sess_id) => {
			match self.get(&sess_id).await {
				| Err(e) if !e.is_not_found() => return Err(e),
				| Ok(session) if session.user_id.as_deref() == Some(&user_id) => {
					return Ok(Adoption::AlreadyBound);
				},
				| Ok(_) | Err(_) => (),
			}

			warn!(%user_id, %unique_id, "provider subject association collides");
			return Ok(Adoption::Collision);
		},
	}

	let session = Session {
		idp_id: Some(provider.id().to_owned()),
		sess_id: Some(random_string(SESSION_ID_LENGTH)),
		user_id: Some(user_id),
		user_info: Some(UserInfo {
			sub: sub.to_owned(),
			..Default::default()
		}),
		..Default::default()
	};

	let sess_id = session
		.sess_id
		.as_deref()
		.expect("adopted session id was just initialized");

	let mut txn = self.db.database.txn();

	txn.raw_put(&self.db.oauthid_session, sess_id, Cbor(&session));
	txn.insert_raw(&self.db.oauthuniqid_oauthid, &unique_id, sess_id);

	txn.execute().await?;

	Ok(Adoption::Adopted)
}
