#!/usr/bin/env bash
# Install the checksum-pinned Linux CI tool without compiling another Rust graph.
set -euo pipefail
[[ $# == 2 ]] || { echo 'usage: install-cyclonedx.sh PIN DESTINATION' >&2; exit 2; }
[[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || {
  echo 'This tool pin is for Linux x86_64 CI runners.' >&2; exit 1;
}
read -r version checksum extra < "$1"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ && "$checksum" =~ ^[0-9a-f]{64}$ && -z "$extra" ]] || {
  echo 'Invalid CycloneDX version/checksum pin.' >&2; exit 1;
}
destination=$2
mkdir -p "$destination"
archive_name=cargo-cyclonedx-x86_64-unknown-linux-gnu
archive="$destination/$archive_name.tar.xz"
curl -fsSL --retry 3 -o "$archive" \
  "https://github.com/CycloneDX/cyclonedx-rust-cargo/releases/download/cargo-cyclonedx-$version/$archive_name.tar.xz"
printf '%s  %s\n' "$checksum" "$archive" | sha256sum -c -
tar -xJf "$archive" -C "$destination" --strip-components=1 "$archive_name/cargo-cyclonedx"
chmod 755 "$destination/cargo-cyclonedx"
if [[ -n "${GITHUB_PATH:-}" ]]; then
  printf '%s\n' "$destination" >> "$GITHUB_PATH"
fi
