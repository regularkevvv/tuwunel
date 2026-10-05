#!/usr/bin/env bash
# The fork owns native qualification. No deployment or private inputs are used.
set -euo pipefail
cd "$(dirname "$0")/.."
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_NET_GIT_FETCH_WITH_CLI=true

mode=${1:?usage: native-gate.sh lint|regressions|test|traces|release}
test_root=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/tuwunel-native-gate.XXXXXX")
trap 'rm -rf -- "$test_root"' EXIT
export TMPDIR="$test_root" TUWUNEL_DATABASE_PATH="$test_root/default-database"
unset TUWUNEL_TRACE_OUT TUWUNEL_TRACE_GOLDEN TUWUNEL_TRACE_BACKEND

case "$mode" in
  lint)
    cargo +nightly-2026-08-18 fmt --all --check
    cargo clippy --workspace --all-targets --locked -- -D warnings
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
    cargo test --workspace --locked
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
  release)
    cargo test --locked --release -p tuwunel_database --lib de_record_ -- --nocapture \
      | tee "$test_root/codec.log"
    grep -Fq 'test result: ok. 7 passed; 0 failed;' "$test_root/codec.log"
    ;;
  *) echo "unknown native gate: $mode" >&2; exit 2 ;;
esac
