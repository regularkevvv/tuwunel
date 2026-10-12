use std::{path::Path, sync::Arc};

use ruma::server_name;
use tuwunel_core::{
	Result,
	config::{Config, Figment},
};

use super::local_user_id;
use crate::{
	Services,
	bridge_fixture::{Fake, Faults, TOKEN},
	services::startup_tests,
};

#[test]
fn localpart_becomes_a_local_user_id() {
	let user_id = local_user_id("alice", server_name!("example.org"))
		.expect("a plain localpart composes a user id");

	assert_eq!(user_id.as_str(), "@alice:example.org");
}

#[test]
fn localpart_past_the_inline_budget_survives() {
	let localpart = "a".repeat(64);
	let user_id = local_user_id(&localpart, server_name!("example.org"))
		.expect("a spilled buffer still composes a user id");

	assert_eq!(user_id.localpart(), localpart);
}

#[test]
fn unusable_localpart_is_skipped() {
	assert!(local_user_id("alice\0bob", server_name!("example.org")).is_none());
}

fn map_id(name: &str) -> u16 {
	tuwunel_bridge::catalog::map_id(name)
		.expect("fixture catalog map")
		.0
}

async fn remote_services(root: &Path, fake: &Fake, migrations: bool) -> Result<Arc<Services>> {
	let config = Config::new(
		&Figment::new()
			.merge(("server_name", "localhost"))
			.merge(("database_backend", "d1"))
			.merge(("database_path", root.join("database")))
			.merge(("d1_bridge_url", &fake.url))
			.merge(("d1_bridge_token", TOKEN))
			.merge(("d1_read_cache_mb", 1))
			.merge(("d1_lease_ttl_ms", 15_000))
			.merge(("database_migrations", migrations))
			.merge(("create_admin_room", false)),
	)?;
	Services::build(startup_tests::server(config)).await
}

#[test]
fn startup_account_probe_reads_one_provider_key_including_disabled_accounts() -> Result {
	startup_tests::isolated(
		"migrations::tests::startup_account_probe_reads_one_provider_key_including_disabled_accounts",
		async |root| {
			let fake = Fake::start().await?;
			let services = remote_services(root, &fake, false).await?;
			fake.fill(map_id("userid_password"), (0..4096).map(|index| {
				(format!("@account{index:04}:localhost").into_bytes(), Vec::new())
			}));
			assert!(services.users.has_users().await?, "disabled accounts remain existing accounts");
			services.db["global"].insert(super::SERVER_NAME_KEY, "localhost").await?;
			services.globals.db.bump_database_version(super::DATABASE_VERSION).await?;
			let before = fake.served();
			let applied = fake.applied();
			super::migrations(&services).await?;
			assert_eq!(fake.served().saturating_sub(before), 1, "startup cannot fetch a second account");
			assert_eq!(fake.applied(), applied, "disabled migrations perform no fresh writes");
			assert_eq!(fake.rows(map_id("userid_password")).len(), 4096);
			services.stop().await;
			Ok(())
		},
	)
}

#[test]
fn startup_account_read_refusal_cannot_initialize_fresh_state() -> Result {
	startup_tests::isolated(
		"migrations::tests::startup_account_read_refusal_cannot_initialize_fresh_state",
		async |root| {
			let fake = Fake::start().await?;
			let services = remote_services(root, &fake, false).await?;
			services.db["global"]
				.insert(super::SERVER_NAME_KEY, "localhost")
				.await?;
			services
				.globals
				.db
				.bump_database_version(super::DATABASE_VERSION)
				.await?;
			let before = fake.rows(map_id("global"));
			let applied = fake.applied();
			fake.faults(Faults { stale_lease: true, ..Faults::default() });
			super::migrations(&services)
				.await
				.expect_err("failed read cannot establish an empty database");
			assert_eq!(fake.rows(map_id("global")), before);
			assert_eq!(fake.applied(), applied, "no version or marker writes after read refusal");
			services.stop().await;
			Ok(())
		},
	)
}

#[test]
fn startup_account_invalid_key_cannot_initialize_fresh_state() -> Result {
	startup_tests::isolated(
		"migrations::tests::startup_account_invalid_key_cannot_initialize_fresh_state",
		async |root| {
			let fake = Fake::start().await?;
			let services = remote_services(root, &fake, false).await?;
			services.db["global"]
				.insert(super::SERVER_NAME_KEY, "localhost")
				.await?;
			services
				.globals
				.db
				.bump_database_version(super::DATABASE_VERSION)
				.await?;
			fake.fill(map_id("userid_password"), [(
				vec![b'x'; tuwunel_bridge::MAX_KEY_BYTES + 1],
				Vec::new(),
			)]);
			let before = fake.rows(map_id("global"));
			let accounts = fake.rows(map_id("userid_password"));
			let applied = fake.applied();
			super::migrations(&services)
				.await
				.expect_err("invalid key is neither an account nor absence");
			assert_eq!(fake.rows(map_id("global")), before);
			assert_eq!(fake.rows(map_id("userid_password")), accounts);
			assert_eq!(fake.applied(), applied);
			services.stop().await;
			Ok(())
		},
	)
}

#[test]
fn checked_empty_accounts_initialize_fresh_but_cannot_downgrade_native_schema() -> Result {
	startup_tests::isolated(
		"migrations::tests::checked_empty_accounts_initialize_fresh_but_cannot_downgrade_native_schema",
		async |root| {
			let fake = Fake::start().await?;
			let services = remote_services(root, &fake, true).await?;
			assert!(!services.users.has_users().await?);
			super::migrations(&services).await?;
			assert_eq!(services.globals.db.database_version().await, super::DATABASE_VERSION);
			assert_eq!(services.db["global"].get(super::SERVER_NAME_KEY).await?.as_ref(), b"localhost");
			assert!(services.db["global"].get(b"rebuild_roomid_tscount_pducount").await?.is_empty());
			services.globals.db.bump_database_version(super::DATABASE_VERSION + 1).await?;
			let before = fake.rows(map_id("global"));
			let applied = fake.applied();
			super::migrations(&services).await.expect_err("checked emptiness cannot bypass the native version gate");
			assert_eq!(fake.rows(map_id("global")), before);
			assert_eq!(fake.applied(), applied);
			services.stop().await;
			Ok(())
		},
	)
}

#[test]
fn disabled_account_and_disabled_migrations_preserve_unsupported_schema_refusal() -> Result {
	startup_tests::isolated(
		"migrations::tests::disabled_account_and_disabled_migrations_preserve_unsupported_schema_refusal",
		async |root| {
			let fake = Fake::start().await?;
			let services = remote_services(root, &fake, false).await?;
			fake.fill(map_id("userid_password"), [(b"@disabled:localhost".to_vec(), Vec::new())]);
			services.db["global"].insert(super::SERVER_NAME_KEY, "localhost").await?;
			services.globals.db.bump_database_version(12).await?;
			let before = fake.rows(map_id("global"));
			let applied = fake.applied();
			let error = super::migrations(&services).await.expect_err("disabled account still makes legacy schema nonempty");
			assert!(error.to_string().contains("no longer supported"));
			assert_eq!(fake.rows(map_id("global")), before);
			assert_eq!(fake.applied(), applied);
			services.stop().await;
			Ok(())
		},
	)
}
