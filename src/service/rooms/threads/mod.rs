use std::{collections::BTreeMap, sync::Arc};

use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use ruma::{
	CanonicalJsonValue, EventId, OwnedEventId, OwnedUserId, RoomId, UserId,
	api::{
		Direction,
		client::threads::get_threads::v1::IncludeThreads,
		error::{ErrorKind, LimitExceededErrorData},
	},
	events::{
		TimelineEventType,
		relation::{BundledThread, RelationType},
	},
	uint,
};
use serde::Deserialize;
use serde_json::json;
use tuwunel_core::{
	Error, Event, Result, err,
	matrix::pdu::{PduCount, PduEvent, PduId, RawPduId},
	utils::{
		bytes::u64_from_bytes,
		stream::{TryReadyExt, automatic_width},
	},
};
use tuwunel_database::{Map, Txn};

#[cfg(test)]
mod tests;

/// Maximum relation hops walked when resolving thread membership, per
/// the Matrix v1.4 spec recommendation (also MSC3771/MSC3773).
const MAX_THREAD_HOPS: usize = 3;
const MAX_THREAD_SCAN_ROWS: usize = 4096;
const MAX_THREAD_SCAN_BYTES: usize = 512 * 1024;
const MAX_PARTICIPANTS_BYTES: usize = 128 * 1024;

#[derive(Deserialize)]
struct ExtractThreadRelation {
	#[serde(rename = "m.relates_to")]
	relates_to: ThreadRelation,
}

#[derive(Deserialize)]
struct ThreadRelation {
	rel_type: RelationType,
	event_id: OwnedEventId,
}

pub struct Service {
	db: Data,
	services: Arc<crate::services::OnceServices>,
}

pub(super) struct Data {
	threadid_userids: Arc<Map>,
	threadactivityid_rootid: Arc<Map>,
	threadrootid_latestcount: Arc<Map>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				threadid_userids: args.db["threadid_userids"].clone(),
				threadactivityid_rootid: args.db["threadactivityid_rootid"].clone(),
				threadrootid_latestcount: args.db["threadrootid_latestcount"].clone(),
			},
			services: args.services.clone(),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Resolves the thread root for `event` by walking up `m.relates_to`
	/// links, bounded at `MAX_THREAD_HOPS`. Returns `None` for events
	/// that belong to the main timeline. Redaction events carry no
	/// `m.relates_to` of their own; their thread is resolved from the
	/// redacted target event per MSC3771/MSC3773.
	pub async fn get_thread_id<E>(&self, event: &E) -> Option<OwnedEventId>
	where
		E: Event,
	{
		let initial = match event.get_content::<ExtractThreadRelation>() {
			| Ok(t) => Some(t.relates_to),
			| Err(_) => self.relates_to_via_redaction_target(event).await,
		};

		let mut relates_to = initial?;

		for _ in 0..MAX_THREAD_HOPS {
			if relates_to.rel_type == RelationType::Thread {
				return Some(relates_to.event_id);
			}

			relates_to = self
				.services
				.timeline
				.get_pdu(&relates_to.event_id)
				.await
				.ok()?
				.get_content::<ExtractThreadRelation>()
				.ok()?
				.relates_to;
		}

		None
	}

	pub(crate) async fn get_thread_id_checked<E: Event>(
		&self,
		event: &E,
	) -> Result<Option<OwnedEventId>> {
		let mut relation = event
			.get_content::<ExtractThreadRelation>()
			.ok()
			.map(|value| value.relates_to);
		if relation.is_none() && *event.kind() == TimelineEventType::RoomRedaction {
			let rules = self
				.services
				.state
				.get_room_version_rules(event.room_id())
				.await?;
			if let Some(target) = event.redacts_id(&rules) {
				match self.services.timeline.get_pdu(&target).await {
					| Ok(pdu) if pdu.room_id() == event.room_id() =>
						relation = pdu
							.get_content::<ExtractThreadRelation>()
							.ok()
							.map(|value| value.relates_to),
					| Ok(_) => return Ok(None),
					| Err(error) if error.is_not_found() => return Ok(None),
					| Err(error) => return Err(error),
				}
			}
		}
		let Some(mut relation) = relation else {
			return Ok(None);
		};
		for _ in 0..MAX_THREAD_HOPS {
			let pdu = match self
				.services
				.timeline
				.get_pdu(&relation.event_id)
				.await
			{
				| Ok(pdu) => pdu,
				| Err(error) if error.is_not_found() => return Ok(None),
				| Err(error) => return Err(error),
			};
			if pdu.room_id() != event.room_id() {
				return Ok(None);
			}
			if relation.rel_type == RelationType::Thread {
				return Ok(Some(relation.event_id));
			}
			let Ok(next) = pdu.get_content::<ExtractThreadRelation>() else {
				return Ok(None);
			};
			relation = next.relates_to;
		}
		Ok(None)
	}

	/// Resolve a redaction event's thread by looking through to the
	/// redacted target. Returns `None` for non-redaction events and for
	/// redactions whose target is unknown or carries no thread relation.
	async fn relates_to_via_redaction_target<E>(&self, event: &E) -> Option<ThreadRelation>
	where
		E: Event,
	{
		if *event.kind() != TimelineEventType::RoomRedaction {
			return None;
		}

		let room_rules = self
			.services
			.state
			.get_room_version_rules(event.room_id())
			.await
			.ok()?;

		let target_id = event.redacts_id(&room_rules)?;

		self.services
			.timeline
			.get_pdu(&target_id)
			.await
			.ok()?
			.get_content::<ExtractThreadRelation>()
			.ok()
			.map(|t| t.relates_to)
	}

	/// `get_thread_id` for an event referenced by id; events missing
	/// locally resolve to `None` (the main timeline).
	pub async fn get_thread_id_for_event(&self, event_id: &EventId) -> Option<OwnedEventId> {
		let pdu = self
			.services
			.timeline
			.get_pdu(event_id)
			.await
			.ok()?;

		self.get_thread_id(&pdu).await
	}

	pub async fn add_to_thread<E>(
		&self,
		root_event_id: &EventId,
		pdu_id: RawPduId,
		event: &E,
	) -> Result
	where
		E: Event,
	{
		let root_id = self
			.services
			.timeline
			.get_pdu_id(root_event_id)
			.await
			.map_err(|e| {
				err!(Request(InvalidParam("Invalid event_id in thread message: {e:?}")))
			})?;

		let root_pdu = self
			.services
			.timeline
			.get_pdu_from_id(&root_id)
			.await
			.map_err(|e| err!(Request(InvalidParam("Thread root not found: {e:?}"))))?;
		if root_pdu.room_id() != event.room_id() || root_id.shortroomid() != pdu_id.shortroomid()
		{
			return Err(err!(Request(InvalidParam("Thread root belongs to another room"))));
		}

		let mut root_pdu_json = self
			.services
			.timeline
			.get_pdu_json_from_id(&root_id)
			.await
			.map_err(|e| err!(Request(InvalidParam("Thread root pdu not found: {e:?}"))))?;

		let mut users = match self.get_participants(&root_id).await {
			| Ok(users) => users,
			| Err(error) if error.kind() == ErrorKind::NotFound =>
				vec![root_pdu.sender().to_owned()],
			| Err(error) => return Err(error),
		};

		users.push(event.sender().to_owned());
		users.sort_unstable();
		users.dedup();
		if users.len() > MAX_THREAD_SCAN_ROWS
			|| users
				.iter()
				.map(|user| user.as_str().len().saturating_add(1))
				.sum::<usize>()
				> MAX_PARTICIPANTS_BYTES
		{
			return Err(thread_read_limit());
		}

		// Commit participants and activity before the bundle so concurrent MSC3816
		// readers never observe stale participation.
		let mut txn = self.services.db.txn();

		self.update_participants(&mut txn, &root_id, &users);

		let count = pdu_id.pdu_count();

		if matches!(count, PduCount::Normal(_)) {
			txn.insert_raw(&self.db.threadactivityid_rootid, pdu_id, root_id);
			txn.insert_raw(&self.db.threadrootid_latestcount, root_id, count.to_be_bytes());
		}

		txn.execute().await?;

		if let CanonicalJsonValue::Object(unsigned) = root_pdu_json
			.entry("unsigned".into())
			.or_insert_with(|| CanonicalJsonValue::Object(BTreeMap::default()))
		{
			if let Some(mut relations) = unsigned
				.get("m.relations")
				.and_then(|r| r.as_object())
				.and_then(|r| r.get("m.thread"))
				.and_then(|relations| {
					serde_json::from_value::<BundledThread>(relations.clone().into()).ok()
				}) {
				// Thread already existed
				relations.count = relations.count.saturating_add(uint!(1));
				relations.latest_event = event.to_format();

				let content = serde_json::to_value(relations).expect("to_value always works");

				unsigned.insert(
					"m.relations".into(),
					json!({ "m.thread": content })
						.try_into()
						.expect("thread is valid json"),
				);
			} else {
				// New thread
				let relations = BundledThread {
					latest_event: event.to_format(),
					count: uint!(1),
					current_user_participated: true,
				};

				let content = serde_json::to_value(relations).expect("to_value always works");

				unsigned.insert(
					"m.relations".into(),
					json!({ "m.thread": content })
						.try_into()
						.expect("thread is valid json"),
				);
			}

			self.services
				.timeline
				.replace_pdu(&root_id, &root_pdu_json)
				.await?;
		}

		Ok(())
	}

	pub fn threads_until<'a>(
		&'a self,
		user_id: &'a UserId,
		room_id: &'a RoomId,
		count: PduCount,
		include: &'a IncludeThreads,
	) -> impl Stream<Item = Result<(PduCount, PduEvent)>> + Send {
		let participated = matches!(include, IncludeThreads::Participated);

		self.services
			.short
			.get_shortroomid(room_id)
			.map_ok(move |shortroomid| PduId {
				shortroomid,
				count: count.saturating_sub(1),
			})
			.map_ok(Into::into)
			.map_ok(move |current: RawPduId| {
				let mut rows = 0_usize;
				let mut bytes = 0_usize;
				self.db
					.threadactivityid_rootid
					.rev_raw_stream_from(&current)
					.ready_try_take_while(move |(key, _)| {
						Ok(key.starts_with(&current.shortroomid()))
					})
					.map(move |row| {
						let (key, value) = row?;
						rows = rows.saturating_add(1);
						bytes = bytes
							.saturating_add(key.len())
							.saturating_add(value.len());
						if rows > MAX_THREAD_SCAN_ROWS || bytes > MAX_THREAD_SCAN_BYTES {
							return Err(thread_read_limit());
						}
						let activity_id = RawPduId::from_bytes(key)?;
						let root_id = RawPduId::from_bytes(value)?;
						if activity_id.shortroomid() != root_id.shortroomid() {
							return Err(Error::bad_database("Mismatched thread index room"));
						}
						Ok((activity_id, root_id))
					})
					.try_filter_map(move |(activity_id, root_id)| {
						self.live_thread(user_id, room_id, participated, activity_id, root_id)
					})
			})
			.try_flatten_stream()
	}

	/// Resolve one activity row to its thread root, skipping and reaping rows
	/// the validity pointer has left behind.
	async fn live_thread(
		&self,
		user_id: &UserId,
		room_id: &RoomId,
		participated: bool,
		activity_id: RawPduId,
		root_id: RawPduId,
	) -> Result<Option<(PduCount, PduEvent)>> {
		let count = activity_id.pdu_count();

		let value = self
			.db
			.threadrootid_latestcount
			.get(&root_id)
			.await
			.map_err(|error| {
				if error.kind() == ErrorKind::NotFound {
					Error::bad_database("Missing thread activity pointer")
				} else {
					error
				}
			})?;
		let pointer = u64_from_bytes(value.as_ref())
			.map(PduCount::from_unsigned)
			.map_err(|_| Error::bad_database("Invalid thread activity pointer"))?;

		if count != pointer {
			// A row ahead of the pointer is a write in flight; only rows behind
			// the pointer are dead and safe to reap.
			if count < pointer {
				self.db
					.threadactivityid_rootid
					.remove(&activity_id)
					.await?;
			}

			return Ok(None);
		}

		if participated && !self.is_participant(&root_id, user_id).await? {
			return Ok(None);
		}

		let mut pdu = self
			.services
			.timeline
			.get_pdu_from_id(&root_id)
			.await
			.map_err(|error| {
				if error.kind() == ErrorKind::NotFound {
					Error::bad_database("Missing thread root event")
				} else {
					error
				}
			})?;
		let canonical = self
			.services
			.timeline
			.get_pdu_id(pdu.event_id())
			.await
			.map_err(|error| {
				if error.kind() == ErrorKind::NotFound {
					Error::bad_database("Missing thread root reverse index")
				} else {
					error
				}
			})?;
		if pdu.room_id() != room_id || canonical != root_id {
			return Err(Error::bad_database("Mismatched thread root event"));
		}

		pdu.remove_transaction_id_unless_sender(Some(user_id));

		Ok(Some((count, pdu)))
	}

	async fn is_participant(&self, root_id: &RawPduId, user_id: &UserId) -> Result<bool> {
		let users = self
			.get_participants(root_id)
			.await
			.map_err(|error| {
				if error.kind() == ErrorKind::NotFound {
					Error::bad_database("Missing thread participants")
				} else {
					error
				}
			})?;
		Ok(users.iter().any(|user| user == user_id))
	}

	pub(super) fn update_participants(
		&self,
		txn: &mut Txn,
		root_id: &RawPduId,
		participants: &[OwnedUserId],
	) {
		let users = participants
			.iter()
			.map(|user| user.as_bytes())
			.collect::<Vec<_>>()
			.join(&[0xFF][..]);

		txn.insert_raw(&self.db.threadid_userids, root_id, &users);
	}

	pub(super) async fn get_participants(&self, root_id: &RawPduId) -> Result<Vec<OwnedUserId>> {
		let value = self.db.threadid_userids.get(root_id).await?;
		if value.len() > MAX_PARTICIPANTS_BYTES {
			return Err(thread_read_limit());
		}
		let mut users = Vec::new();
		for bytes in value.split(|&byte| byte == 0xFF) {
			if users.len() >= MAX_THREAD_SCAN_ROWS {
				return Err(thread_read_limit());
			}
			let user = std::str::from_utf8(bytes)
				.map_err(|_| Error::bad_database("Invalid thread participant encoding"))?;
			users.push(
				UserId::parse(user)
					.map_err(|_| Error::bad_database("Invalid thread participant"))?,
			);
		}
		Ok(users)
	}

	/// MSC3816: whether `user_id` has participated in the thread rooted at
	/// `root_event_id`, having sent the root event or a threaded reply to it.
	pub async fn user_participated(
		&self,
		root_event_id: &EventId,
		user_id: &UserId,
	) -> Result<bool> {
		let root_id = self
			.services
			.timeline
			.get_pdu_id(root_event_id)
			.await
			.map_err(|error| {
				if error.kind() == ErrorKind::NotFound {
					Error::bad_database("Missing thread participation root mapping")
				} else {
					error
				}
			})?;

		self.is_participant(&root_id, user_id).await
	}

	/// Rebuild the thread activity index from every thread root. Run once at
	/// startup behind a `global` marker, and on demand from the admin command.
	/// Clears first so a partial or stale index is replaced wholesale.
	pub async fn rebuild_thread_activity(&self) -> Result {
		self.db.threadactivityid_rootid.clear().await?;
		self.db.threadrootid_latestcount.clear().await?;

		self.db
			.threadid_userids
			.raw_keys()
			.map(|key| key.and_then(RawPduId::from_bytes))
			.try_for_each_concurrent(automatic_width(), async |root_id| {
				self.index_thread_activity(root_id).await
			})
			.await
	}

	async fn index_thread_activity(&self, root_id: RawPduId) -> Result {
		let root: PduId = root_id.into();

		let mut replies = self
			.services
			.pdu_metadata
			.get_relations(root.shortroomid, root.count, None, Direction::Backward, None)
			.await?
			.into_iter()
			.filter_map(|(count, pdu)| {
				pdu.get_content()
					.is_ok_and(|content: ExtractThreadRelation| {
						content.relates_to.rel_type == RelationType::Thread
					})
					.then_some(count)
			});

		let latest = replies.next().unwrap_or(root.count);

		let activity_id: RawPduId = PduId {
			shortroomid: root.shortroomid,
			count: latest,
		}
		.into();

		let mut txn = self.services.db.txn();

		txn.insert_raw(&self.db.threadactivityid_rootid, activity_id, root_id);
		txn.insert_raw(&self.db.threadrootid_latestcount, root_id, latest.to_be_bytes());
		txn.execute().await
	}
}

fn thread_read_limit() -> Error {
	Error::Request(
		ErrorKind::LimitExceeded(LimitExceededErrorData { retry_after: None }),
		"Thread index read limit reached".into(),
		http::StatusCode::TOO_MANY_REQUESTS,
	)
}
