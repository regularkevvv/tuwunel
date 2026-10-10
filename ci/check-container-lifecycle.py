#!/usr/bin/env python3
"""Reject empty/ignored lifecycle harnesses and record the declared release gate."""
import argparse
import json
import platform
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = {
    "tasks::tests::cancelled_join_retains_completion_and_owner",
    "tasks::tests::native_completion_retains_failure_across_cancelled_join",
    "tasks::tests::concurrent_joiners_both_wait_for_completion",
    "tasks::tests::nested_cleanup_is_also_joined",
    "tasks::tests::cancelled_join_preserves_panic_and_drains_survivor",
    "tasks::tests::closed_runtime_spawn_keeps_registration_visible_and_allows_nested_cleanup",
    "tasks::native::tests::native_executor_survives_panic_and_joins_without_runtime",
    "backend::remote::close_tests::close_joins_interrupted_renewal_before_returning",
    "backend::remote::close_tests::cancelled_close_retains_renewal_join_completion",
    "backend::remote::close_tests::cancelled_close_retains_dispatched_release_completion",
    "backend::remote::close_tests::dropped_backend_retains_dispatched_close_for_shutdown_join",
    "pool::startup_tests::first_spawn_failure_releases_the_unstarted_pool",
    "pool::startup_tests::later_spawn_failure_joins_all_started_workers",
    "pool::startup_tests::spawn_panic_joins_workers_despite_the_poisoned_inventory",
    "pool::startup_tests::simultaneous_close_waits_for_the_original_worker_joins",
    "pool::owner_teardown_tests::cancelled_point_read_releases_last_native_owner",
    "pool::owner_teardown_tests::cancelled_batch_read_releases_last_native_owner",
    "pool::owner_teardown_tests::cancelled_seek_releases_last_native_owner",
    "pool::owner_teardown_tests::last_native_owner_releases_after_runtime_shutdown",
    "pool::owner_teardown_tests::last_native_owner_does_not_deadlock_an_existing_closer",
    "pool::owner_teardown_tests::drain_tests::native_close_waits_for_cancelled_accepted_read",
    "pool::owner_teardown_tests::drain_tests::native_close_waits_for_active_read",
    "pool::owner_teardown_tests::drain_tests::cancelled_native_close_is_rejoinable",
    "pool::owner_teardown_tests::drain_tests::concurrent_native_closers_wait_for_accepted_read",
    "pool::owner_teardown_tests::drain_tests::native_drain_fences_later_read_admission",
    "manager::failure_tests::fatal_worker_drains_survivors_and_releases_database",
    "manager::failure_tests::unexpected_worker_abort_is_reported_and_survivors_are_joined",
    "manager::failure_tests::installed_manager_fatal_error_drains_real_workers_before_final_teardown",
    "manager::failure_tests::admin_worker_started_after_shutdown_exits_and_releases_database",
    "serve::tests::listener_error_drains_surviving_listeners",
    "serve::tests::listener_panic_drains_surviving_listeners",
    "serve::tests::dropped_listener_set_retains_child_joins",
    "lifecycle::tests::cancelled_started_graph_handoff_is_joined",
    "restart::tests::exec_restart_keeps_pid_and_configuration_and_drops_activation",
    "restart::tests::listen_fds_never_survive",
    "restart::tests::restore_backup_never_survives",
    "restart::tests::other_arguments_survive",
    "run_releases_services_mutex_while_waiting",
    "cancelled_run_is_joined_before_stop_returns",
    "cancelled_exec_joins_signals_and_releases_database",
    "startup_command_error_releases_database_before_runtime_exit",
    "durable_pending_and_active_deliveries_survive_kill_and_disabled_startup",
}


def feature_contract(root=ROOT):
    raw = (root / "ci/container-features.txt").read_text().strip()
    features = raw.split(",")
    defaults = tomllib.loads((root / "src/main/Cargo.toml").read_text())["features"]["default"]
    if (not raw or "\n" in raw or any(not re.fullmatch(r"[a-z0-9_]+", item) for item in features)
            or len(features) != len(set(features))
            or set(features) != set(defaults) - {"systemd", "io_uring"}):
        raise ValueError("Container features must equal declared defaults minus systemd/io_uring")
    return raw


def verify_log(log):
    # Several real-process fixtures write child output between the outer name
    # and its status. Do not require an uninterrupted `name ... ok` line.
    names = set(re.findall(r"(?:^|\n)test ([A-Za-z0-9_:]+) \.\.\. ", log))
    missing = REQUIRED - names
    ignored = set(re.findall(r"(?:^|\n)test ([A-Za-z0-9_:]+) \.\.\. ignored", log)) & REQUIRED
    summaries = re.findall(r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed;", log, re.MULTILINE)
    if missing or ignored or not summaries or any(status != "ok" or failed != "0" for status, _, failed in summaries):
        raise ValueError(f"Incomplete lifecycle gate: missing={sorted(missing)}, ignored={sorted(ignored)}")
    if not any(int(passed) > 0 for _, passed, _ in summaries):
        raise ValueError("Lifecycle gate executed no tests")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", action="store_true")
    parser.add_argument("log", nargs="?", type=Path)
    parser.add_argument("report", nargs="?", type=Path)
    args = parser.parse_args()
    features = feature_contract()
    if args.features:
        print(features)
        return
    if args.log is None or args.report is None:
        parser.error("log and report are required")
    verify_log(args.log.read_text())
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip())
    args.report.write_text(json.dumps({
        "schema": 1, "status": "passed", "source_commit": commit, "source_dirty": dirty,
        "os": platform.system(), "architecture": platform.machine(), "cargo_profile": "release",
        "default_features": False, "features": features.split(","),
        "test_dev_dependency_features": ["tuwunel_database/commit_refusals", "tuwunel_service/notification_recovery_tests"],
        "required_cases": sorted(REQUIRED), "required_case_count": len(REQUIRED),
        "scope": "Linux/amd64 release-feature lifecycle; local RocksDB and loopback remote controls",
        "excludes": ["real D1/R2", "staging deployment", "hot modules", "io_uring", "systemd"],
    }, indent=2) + "\n")
    print(f"Container lifecycle: {len(REQUIRED)} required controls passed")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
