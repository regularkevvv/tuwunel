use ruma::{
	RoomId, ServerName,
	events::{StateEventType, room::server_acl::RoomServerAclEventContent},
};
use serde_json::Value;
use tuwunel_core::{Err, Result, debug, implement, trace};

/// Returns Ok if the acl allows the server
///
/// Evaluation follows the `m.room.server_acl` schema: with no ACL event in the
/// room state, allow; otherwise an IP literal while `allow_ip_literals` is
/// `false`, or a `deny` match, denies, an `allow` match allows, and anything
/// else denies. Content that cannot be read denies rather than voids the ACL.
#[implement(super::Service)]
#[tracing::instrument(skip_all, level = "debug")]
pub async fn acl_check(&self, server_name: &ServerName, room_id: &RoomId) -> Result {
	let content = match self
		.services
		.state_accessor
		.room_state_get_content::<Value>(room_id, &StateEventType::RoomServerAcl, "")
		.await
	{
		| Ok(content) => content,
		| Err(e) if e.is_not_found() => {
			trace!(%room_id, "No ACL content found: {e:?}");
			return Ok(());
		},
		| Err(e) => return Err(e),
	};

	let acl_event_content = acl_from_content(&content);
	trace!(%room_id, "ACL content found: {acl_event_content:?}");

	if acl_event_content.is_allowed(server_name) {
		trace!("server {server_name} is allowed by ACL");
		Ok(())
	} else {
		debug!("Server {server_name} was denied by room ACL in {room_id}");
		Err!(Request(Forbidden("Server was denied by room ACL")))
	}
}

/// Reads `m.room.server_acl` content with the defaults its schema gives: a
/// missing `allow` or `deny` is an empty list, and `allow_ip_literals` is
/// `true` "if missing or otherwise not a boolean". A list that is not an array
/// reads as missing and an entry that is not a string is ignored, so a broken
/// `allow` leaves nothing allowed.
fn acl_from_content(content: &Value) -> RoomServerAclEventContent {
	let patterns = |field: &str| -> Vec<String> {
		content
			.get(field)
			.and_then(Value::as_array)
			.into_iter()
			.flatten()
			.filter_map(Value::as_str)
			.map(ToOwned::to_owned)
			.collect()
	};

	let allow_ip_literals = content
		.get("allow_ip_literals")
		.and_then(Value::as_bool)
		.unwrap_or(true);

	RoomServerAclEventContent::new(allow_ip_literals, patterns("allow"), patterns("deny"))
}

#[cfg(test)]
mod tests {
	use ruma::server_name;
	use serde_json::json;

	use super::acl_from_content;

	#[test]
	fn an_acl_allowing_nothing_readable_denies_every_server() {
		for content in [
			json!({}),
			json!({"allow": []}),
			json!({"allow": "*"}),
			json!({"allow": {"*": true}}),
			json!({"allow": [1, null, {"*": true}]}),
			json!({"allow": ["*"], "deny": ["*"]}),
			json!("not an object"),
		] {
			assert!(
				!acl_from_content(&content).is_allowed(server_name!("remote.test")),
				"{content} allowed a server"
			);
		}
	}

	#[test]
	fn entries_that_are_not_strings_are_ignored() {
		let acl = acl_from_content(&json!({"allow": ["*", 5], "deny": [null, "evil.test"]}));

		assert!(acl.is_allowed(server_name!("remote.test")));
		assert!(!acl.is_allowed(server_name!("evil.test")));
	}

	#[test]
	fn allow_ip_literals_is_true_unless_a_boolean() {
		let literal = server_name!("1.2.3.4");

		assert!(acl_from_content(&json!({"allow": ["*"]})).is_allowed(literal));
		assert!(
			acl_from_content(&json!({"allow": ["*"], "allow_ip_literals": "no"}))
				.is_allowed(literal)
		);
		assert!(
			!acl_from_content(&json!({"allow": ["*"], "allow_ip_literals": false}))
				.is_allowed(literal)
		);
	}
}
