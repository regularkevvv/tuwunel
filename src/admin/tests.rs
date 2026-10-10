#![cfg(test)]

use clap::Parser;

use crate::{admin::AdminCommand, media::MediaCommand, query::QueryCommand, user::UserCommand};

#[test]
fn get_help_short() { get_help_inner("-h"); }

#[test]
fn get_help_long() { get_help_inner("--help"); }

#[test]
fn get_help_subcommand() { get_help_inner("help"); }

#[test]
fn delete_backups_requires_keep() {
	assert!(
		parse_err(&["argv[0] doesn't matter", "server", "delete-backups"]).contains("KEEP"),
		"deleting every backup must not be the default"
	);
}

#[test]
fn delete_range_requires_direction() {
	assert!(
		parse_err(&["argv[0] doesn't matter", "media", "delete-range", "7d"])
			.contains("--older-than|--newer-than"),
		"a direction flag must be required"
	);
}

#[test]
fn delete_range_rejects_both_directions() {
	assert!(
		parse_err(&[
			"argv[0] doesn't matter",
			"media",
			"delete-range",
			"7d",
			"--older-than",
			"--newer-than",
		])
		.contains("cannot be used with"),
		"the direction flags must be exclusive"
	);
}

#[test]
fn delete_range_accepts_one_direction() {
	for direction in ["--older-than", "-o"] {
		let AdminCommand::Media(MediaCommand::DeleteRange { older_than, newer_than, .. }) =
			parse_ok(&["argv[0] doesn't matter", "media", "delete-range", "7d", direction])
		else {
			panic!("{direction} must parse as a media delete-range command");
		};

		assert!(older_than, "{direction} must select the older-than direction");
		assert!(!newer_than, "{direction} must leave the newer-than direction unset");
	}
}

#[test]
fn query_feds_parse() {
	for survey in ["version", "state", "head"] {
		let command =
			parse_ok(&["argv[0] doesn't matter", "query", "feds", survey, "!room:example.org"]);

		assert!(
			matches!(command, AdminCommand::Query(QueryCommand::Feds(_))),
			"{survey} must parse as a query feds command"
		);
	}
}

#[test]
fn query_feds_event_parse() {
	let command =
		parse_ok(&["argv[0] doesn't matter", "query", "feds", "event", "$event:example.org"]);

	assert!(matches!(command, AdminCommand::Query(QueryCommand::Feds(_))));
}

#[test]
fn query_feds_require_a_room() {
	for survey in ["version", "state", "head"] {
		assert!(
			parse_err(&["argv[0] doesn't matter", "query", "feds", survey]).contains("ROOM"),
			"{survey} must require a room"
		);
	}
}

#[test]
fn query_feds_reject_zero_width() {
	let error = parse_err(&[
		"argv[0] doesn't matter",
		"query",
		"feds",
		"version",
		"!room:example.org",
		"--width",
		"0",
	]);

	assert!(error.contains("invalid value '0'"), "a survey width must be nonzero");
}

#[test]
fn local_user_listing_defaults_to_a_bounded_page_and_accepts_a_cursor() {
	assert!(matches!(
		parse_ok(&["admin", "users", "list-users"]),
		AdminCommand::Users(UserCommand::ListUsers { after: None, limit: 16 })
	));
	let AdminCommand::Users(UserCommand::ListUsers { after, limit }) = parse_ok(&[
		"admin",
		"users",
		"list-users",
		"--after",
		"@disabled:localhost",
		"--limit",
		"32",
	]) else {
		panic!("expected paginated user listing");
	};
	assert_eq!(after.expect("cursor").as_str(), "@disabled:localhost");
	assert_eq!(usize::from(limit), tuwunel_service::users::MAX_LOCAL_USER_PAGE_ROWS);
}

#[test]
fn local_user_listing_refuses_unbounded_limits_and_invalid_cursors() {
	for limit in ["0", "33", "65536"] {
		assert!(
			parse_err(&["admin", "users", "list-users", "--limit", limit])
				.contains("invalid value")
		);
	}
	assert!(
		parse_err(&["admin", "users", "list-users", "--after", "not-a-user"])
			.contains("invalid value")
	);
}

#[test]
fn last_active_defaults_to_48_and_refuses_unbounded_output() {
	assert!(matches!(
		parse_ok(&["admin", "users", "last-active"]),
		AdminCommand::Users(UserCommand::LastActive { limit: 48 })
	));
	assert!(matches!(
		parse_ok(&["admin", "users", "last-active", "--limit", "64"]),
		AdminCommand::Users(UserCommand::LastActive { limit: 64 })
	));
	for limit in ["0", "65", "65536"] {
		assert!(
			parse_err(&["admin", "users", "last-active", "--limit", limit])
				.contains("invalid value")
		);
	}
}

#[test]
fn query_user_inventory_requires_bounded_limits_and_valid_cursors() {
	for flags in [&[][..], &["--historical", "--after", "@user:localhost", "--limit", "32"][..]] {
		let mut args = vec!["admin", "query", "users", "iter-users"];
		args.extend_from_slice(flags);
		assert!(matches!(parse_ok(&args), AdminCommand::Query(QueryCommand::Users(_))));
	}
	for limit in ["0", "33", "65536"] {
		assert!(
			parse_err(&["admin", "query", "users", "iter-users", "--limit", limit])
				.contains("invalid value")
		);
	}
	assert!(
		parse_err(&["admin", "query", "users", "iter-users", "--after", "invalid-user"])
			.contains("invalid value")
	);
}

fn get_help_inner(input: &str) {
	let error = parse_err(&["argv[0] doesn't matter", input]);

	// Search for a handful of keywords that suggest the help printed properly
	assert!(error.contains("Usage:"));
	assert!(error.contains("Commands:"));
	assert!(error.contains("Options:"));
}

fn parse_err(argv: &[&str]) -> String {
	let Err(error) = AdminCommand::try_parse_from(argv) else {
		panic!("parsing {argv:?} must fail");
	};

	error.to_string()
}

fn parse_ok(argv: &[&str]) -> AdminCommand {
	AdminCommand::try_parse_from(argv)
		.unwrap_or_else(|error| panic!("parsing {argv:?} must succeed: {error}"))
}

#[test]
fn production_diagnostic_boundary_preserves_bounded_queries_and_operator_commands() {
	for args in [
		vec!["query", "raw", "count"],
		vec!["query", "raw", "keys", "missing-map"],
		vec!["query", "raw", "iter", "missing-map", "--limit", "17"],
		vec!["query", "raw", "vals-total"],
		vec!["query", "sending", "active-requests"],
		vec!["query", "room-alias", "all-local-aliases"],
		vec!["query", "presence", "presence-since", "0"],
		vec!["query", "storage", "list"],
		vec!["query", "storage", "sync", "missing-source", "missing-destination"],
		vec!["query", "users", "get-to-device-events", "@user:localhost", "device"],
		vec!["rooms", "list-joined-members", "!room:localhost"],
		vec!["users", "list-joined-rooms", "@user:localhost"],
	] {
		let command = parse_ok(&[vec!["admin"], args].concat());
		assert!(
			crate::admin::check_remote_scan_command(&command, true).is_err(),
			"D1 must refuse unaudited scans before resolving maps/providers: {command:?}"
		);
		assert!(
			crate::admin::check_remote_scan_command(&command, false).is_ok(),
			"reference diagnostics must remain available: {command:?}"
		);
	}
	for args in [
		vec!["query", "raw", "get", "missing-map", "key"],
		vec!["query", "raw", "keys", "missing-map", "--limit", "16"],
		vec!["query", "users", "iter-users"],
		vec!["query", "users", "list-devices", "@user:localhost"],
		vec!["query", "users", "list-devices-metadata", "@user:localhost"],
		vec!["query", "oauth", "revoke-sessions", "@user:localhost"],
		vec![
			"query",
			"oauth",
			"associate",
			"provider",
			"@user:localhost",
			"--claim",
			"sub=user",
		],
		vec!["server", "uptime"],
		vec!["server", "rotate-signing-key"],
		vec!["rooms", "list"],
		vec!["rooms", "directory", "list"],
	] {
		let command = parse_ok(&[vec!["admin"], args].concat());
		assert!(
			crate::admin::check_remote_scan_command(&command, true).is_ok(),
			"audited bounded/required operator command remains reachable: {command:?}"
		);
	}
}
