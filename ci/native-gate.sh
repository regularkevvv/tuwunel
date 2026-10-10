#!/usr/bin/env bash
# The fork owns native qualification. No deployment or private inputs are used.
set -euo pipefail
cd "$(dirname "$0")/.."
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_NET_GIT_FETCH_WITH_CLI=true
# Each native service fixture owns a RocksDB graph until its process exits.
# Cap simultaneous fixture construction while retaining each test's own tasks.
export RUST_TEST_THREADS=${RUST_TEST_THREADS:-2}

mode=${1:?usage: native-gate.sh lint|regressions|test|compatibility|traces|release|container-lifecycle}
test_root=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/tuwunel-native-gate.XXXXXX")
trap 'rm -rf -- "$test_root"' EXIT
export TMPDIR="$test_root" TUWUNEL_DATABASE_PATH="$test_root/default-database"
unset TUWUNEL_TRACE_OUT TUWUNEL_TRACE_GOLDEN TUWUNEL_TRACE_BACKEND

prepare_predecessor() {
  local revision=8b0659fb9c3218b4fe964793dfbdba0c24b2466a
  # Build only the predecessor's committed fixture, from reachable history.
  # Its own .cargo configuration must apply; candidate code is never overlaid.
  git merge-base --is-ancestor "$revision" HEAD
  mkdir "$test_root/predecessor"
  git archive "$revision" | tar -x -C "$test_root/predecessor"
  # Reuse dependency outputs, but freeze the old executable before candidate
  # compilation can overwrite an artifact with the same Cargo target name.
  local target_directory=${CARGO_TARGET_DIR:-"$PWD/target"}
  mkdir -p -- "$target_directory"
  target_directory=$(cd -- "$target_directory" && pwd -P)
  (
    cd "$test_root/predecessor"
    CARGO_TARGET_DIR="$target_directory" cargo test --locked --no-run \
      -p tuwunel --test admin_task_journal --message-format=json
  ) > "$test_root/predecessor-artifacts.jsonl"
  local executable
  executable=$(python3 - "$test_root/predecessor-artifacts.jsonl" <<'PY'
import json, os, sys
from pathlib import Path

artifacts = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines()]
executables = [item['executable'] for item in artifacts
               if item.get('reason') == 'compiler-artifact'
               and item.get('target', {}).get('name') == 'admin_task_journal'
               and item.get('target', {}).get('kind') == ['test']
               and item.get('profile', {}).get('test') is True
               and item.get('executable')]
if len(executables) != 1 or not Path(executables[0]).is_file() or not os.access(executables[0], os.X_OK):
    sys.exit('Expected one executable predecessor journal test artifact')
print(executables[0])
PY
  )
  cp -- "$executable" "$test_root/older-journal"
  chmod 500 "$test_root/older-journal"
  export TUWUNEL_HISTORY_OLDER_JOURNAL_BINARY="$test_root/older-journal"
  unset TUWUNEL_HISTORY_RESUME_PHASE TUWUNEL_HISTORY_RESUME_DIRECTORY
  unset TUWUNEL_ADMIN_JOURNAL_PHASE TUWUNEL_ADMIN_JOURNAL_DIRECTORY
}

history_compatibility() {
  local case=older_journal_refuses_schema_and_record_changes_without_mutation
  cargo test --locked -p tuwunel --test admin_history_resume "$case" -- --ignored --exact --color never \
    | tee "$test_root/history-compatibility.log"
  # Child logs may appear between the outer test name and its final status.
  # Require the selected name and the last (outer) summary, not a child pass.
  python3 - "$test_root/history-compatibility.log" "$case" <<'PY_HISTORY'
import re, sys
from pathlib import Path

log = Path(sys.argv[1]).read_text()
summaries = [line for line in log.splitlines() if line.startswith('test result: ')]
summary = re.match(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', summaries[-1]) if summaries else None
if f'test {sys.argv[2]} ... ' not in log or summary is None or summary.groups() != ('ok', '1', '0', '0'):
    sys.exit('Expected one passing outer predecessor compatibility test')
PY_HISTORY
}

case "$mode" in
  lint)
    cargo +nightly-2026-08-18 fmt --all --check
    cargo clippy --workspace --all-targets --locked --features tuwunel/direct_tls -- -D warnings
    ;;
  regressions)
    cargo test --locked -p tuwunel -p tuwunel_service --lib \
      rooms::state_cache::recount_tests::membership_recount_and_count_reads_share_the_actual_room_exclusion \
      -- --exact | tee "$test_root/exclusion.log"
    grep -Fq 'test rooms::state_cache::recount_tests::membership_recount_and_count_reads_share_the_actual_room_exclusion ... ok' "$test_root/exclusion.log"
    grep -Fq 'test result: ok. 1 passed; 0 failed;' "$test_root/exclusion.log"
    cargo test --locked -p tuwunel --test short_id_allocation \
      --test state_atomic_allocation --test state_local_build \
      --test thread_read_bounds --test relation_read_bounds \
      --test relation_bundle_bounds --test sync_cursor_refusal \
      --test sync_state_completeness --test sync_state_after_corruption \
      --test membership_recount_refusal --test membership_recount_restart \
      --test membership_recount_inventory --test membership_recount_legacy \
      --test sync_v5_state_completeness
    ;;
  test)
    prepare_predecessor
    cargo test --workspace --locked
    history_compatibility
    # These targets compile to zero tests without direct_tls. Require each
    # named case so a successful empty harness cannot qualify federation.
    cargo test --locked -p tuwunel --features direct_tls \
      --test feds_loopback --test federation_transaction_backoff \
      | tee "$test_root/direct-tls.log"
    grep -Fq 'test tests::feds_queries_report_this_server ... ok' "$test_root/direct-tls.log"
    grep -Fq 'test tests::a_refused_transaction_waits_for_the_backoff ... ok' "$test_root/direct-tls.log"
    ;;
  compatibility)
    prepare_predecessor
    history_compatibility
    ;;
  traces)
    for backend in rocksdb remote; do
      TUWUNEL_TRACE_BACKEND="$backend" TUWUNEL_TRACE_OUT="$test_root/$backend.json" \
        cargo test -p tuwunel_database --lib --locked tests::trace::trace_golden \
          -- --ignored --exact --format pretty --color never | tee "$test_root/$backend.log"
      grep -Fxq 'test tests::trace::trace_golden ... ok' "$test_root/$backend.log"
      test -s "$test_root/$backend.json"
    done
    cmp "$test_root/rocksdb.json" "$test_root/remote.json"
    # Job metadata carries these digests to private CI. It compares them with
    # its own frozen golden; the golden is never copied into this public repo.
    for backend in rocksdb remote; do
      digest=$(shasum -a 256 "$test_root/$backend.json" | cut -d ' ' -f 1)
      echo "$backend=$digest"
      if [[ -n ${GITHUB_OUTPUT:-} ]]; then echo "$backend=$digest" >> "$GITHUB_OUTPUT"; fi
    done
    ;;
  container-lifecycle)
    # Qualify the actual non-systemd, non-io_uring Container feature closure.
    # A native macOS run or an arm64 Linux run must not claim this gate.
    [[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] || {
      echo 'Container lifecycle requires native Linux/x86_64 execution' >&2
      exit 1
    }
    features=$(python3 ci/check-container-lifecycle.py --features)
    workspace_features="tuwunel/${features//,/,tuwunel/}"
    [[ -n "$features" && "$features" != *$'\n'* ]]
    unset TUWUNEL_QUEUE_RESTART_PHASE TUWUNEL_QUEUE_RESTART_DIRECTORY TUWUNEL_QUEUE_RESTART_SCENARIO
    unset TUWUNEL_CANCELLATION_CASE
    # --no-default-features applies to every selected workspace package.
    # These are the production release optimizer and feature choices, not a
    # fault-injection image or the default systemd/io_uring build.
    cargo test --locked --release --no-default-features --features "$workspace_features" \
      -p tuwunel -p tuwunel_core -p tuwunel_database -p tuwunel_service -p tuwunel_router \
      --lib -- --color never | tee "$test_root/container-lifecycle.log"
    cargo test --locked --release --no-default-features --features "$features" \
      -p tuwunel --test cancellation_lifecycle --test fatal_lifecycle --test sending_queue_restart \
      -- --color never | tee -a "$test_root/container-lifecycle.log"
    python3 ci/check-container-lifecycle.py "$test_root/container-lifecycle.log" \
      "${TUWUNEL_LIFECYCLE_REPORT:-$test_root/container-lifecycle.json}"
    ;;
  release)
    # Reuse the release dependency closure already built by container-lifecycle,
    # rather than rebuilding native dependencies with a different feature set.
    features=$(python3 ci/check-container-lifecycle.py --features)
    workspace_features="tuwunel/${features//,/,tuwunel/}"
    cargo test --locked --release --no-default-features --features "$workspace_features" \
      -p tuwunel -p tuwunel_database --lib de_record_ -- --nocapture \
      | tee "$test_root/codec.log"
    grep -Fq 'test result: ok. 7 passed; 0 failed;' "$test_root/codec.log"
    ;;
  *) echo "unknown native gate: $mode" >&2; exit 2 ;;
esac
