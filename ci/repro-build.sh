#!/usr/bin/env bash
# Reproducible homeserver build: two clean targets and identical flag vectors.
#
# Builds the `tuwunel` binary twice into two clean target directories with
# a pinned SOURCE_DATE_EPOCH and path remapping, then compares SHA-256
# digests. A mismatch fails the gate.
#
# Known nondeterminism sources this script controls for:
#   - build paths (RUSTFLAGS --remap-path-prefix for repo, CARGO_HOME,
#     rustc sysroot, and each per-run target dir)
#   - timestamps (SOURCE_DATE_EPOCH pinned to the fork HEAD commit time)
#   - incremental compilation (disabled)
# Known residual risks (documented, will fail the gate if they bite):
#   - host linker nondeterminism (LTO object ordering, macOS ad-hoc
#     signatures). CI runs this on Linux; macOS results are advisory
#     because codesign timestamps/UUIDs differ per link.
#   - build scripts embedding host paths outside the remapped roots.
#
# Env overrides:
#   REPRO_PROFILE   cargo profile (default: release)
#   REPRO_PACKAGE   package to build (default: tuwunel)
#   PROVENANCE_OUT  save the result and failed-comparison diagnostics
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FORK="$ROOT"
PROFILE="${REPRO_PROFILE:-release}"
PACKAGE="${REPRO_PACKAGE:-tuwunel}"
PROVENANCE_OUT="${PROVENANCE_OUT:-$ROOT/evidence/provenance}"
REPRO_OUT="$PROVENANCE_OUT/diagnostics"

# Cargo discovers .cargo/config.toml and rustup selects its toolchain from
# the working directory, not --manifest-path. Match the deployed fork build.
cd "$FORK"

EPOCH="$(git -C "$FORK" log -1 --format=%ct)"
SYSROOT="$(rustc --print sysroot)"
CARGO_HOME_DIR="${CARGO_HOME:-$HOME/.cargo}"

sha() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

build_once() {
  local target_dir="$1"
  local rustflags
  rustflags="--remap-path-prefix=$FORK=/build"
  rustflags+=" --remap-path-prefix=$CARGO_HOME_DIR=/cargo"
  rustflags+=" --remap-path-prefix=$SYSROOT=/rustc"
  # Keep the complete flag vector identical between builds. Cargo includes
  # rustflags in crate fingerprints; a different source mapping per run
  # creates different dependency filenames even when paths are remapped.
  rustflags+=" --remap-path-prefix=$A=/target"
  rustflags+=" --remap-path-prefix=$B=/target"

  env \
    SOURCE_DATE_EPOCH="$EPOCH" \
    CARGO_INCREMENTAL=0 \
    CARGO_TARGET_DIR="$target_dir" \
    RUSTFLAGS="$rustflags" \
    cargo build --manifest-path "$FORK/Cargo.toml" \
      --profile "$PROFILE" --locked -p "$PACKAGE"
}

WORK="$(mktemp -d "${TMPDIR:-/tmp}/repro.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
A="$WORK/a"
B="$WORK/b"

echo "==> build 1/2 (target: $A)"
build_once "$A"
echo "==> build 2/2 (target: $B)"
build_once "$B"

# `release` profile artifacts land under target/<profile-dir>; the dir for
# custom profiles is the profile name itself.
PROFILE_DIR="$PROFILE"
[[ "$PROFILE" == "dev" ]] && PROFILE_DIR="debug"

BIN_A="$A/$PROFILE_DIR/$PACKAGE"
BIN_B="$B/$PROFILE_DIR/$PACKAGE"
[[ -f "$BIN_A" && -f "$BIN_B" ]] || { echo "built binaries not found" >&2; exit 1; }

HASH_A="$(sha "$BIN_A")"
HASH_B="$(sha "$BIN_B")"
echo "build A: $HASH_A"
echo "build B: $HASH_B"

if [[ "$HASH_A" != "$HASH_B" ]]; then
  if [[ -n "${REPRO_OUT:-}" ]]; then
    mkdir -p "$REPRO_OUT"
    cp "$BIN_A" "$REPRO_OUT/build-a"
    cp "$BIN_B" "$REPRO_OUT/build-b"
    printf 'build A: %s\nbuild B: %s\n' "$HASH_A" "$HASH_B" > "$REPRO_OUT/digests.txt"
    if command -v strings >/dev/null 2>&1; then
      LC_ALL=C strings "$BIN_A" | LC_ALL=C sort > "$REPRO_OUT/a.strings"
      LC_ALL=C strings "$BIN_B" | LC_ALL=C sort > "$REPRO_OUT/b.strings"
      diff -u "$REPRO_OUT/a.strings" "$REPRO_OUT/b.strings" > "$REPRO_OUT/strings.diff" || true
    fi
  fi
  echo "FAIL: builds are not bit-identical." >&2
  exit 1
fi
echo "Reproducible-build gate: PASS ($HASH_A)"

mkdir -p "$PROVENANCE_OUT"
python3 - "$PROVENANCE_OUT/reproducibility.json" "$HASH_A" "$HASH_B" "$(git rev-parse HEAD)" "$EPOCH" "$PROFILE" "$PACKAGE" <<'PY_REPORT'
import json, sys
from pathlib import Path
path, first, second, source, epoch, profile, package = sys.argv[1:]
Path(path).write_text(json.dumps({'source_commit': source, 'source_date_epoch': int(epoch), 'profile': profile, 'package': package, 'build_a_sha256': first, 'build_b_sha256': second}, indent=2) + '\n')
PY_REPORT
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then printf 'binary=%s\n' "$HASH_A" >> "$GITHUB_OUTPUT"; fi
