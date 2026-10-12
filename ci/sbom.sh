#!/usr/bin/env bash
# One BOM per workspace member, collected outside the source tree.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
out=${PROVENANCE_OUT:-"$root/evidence/provenance"}
mkdir -p "$out/sbom"
export SOURCE_DATE_EPOCH
SOURCE_DATE_EPOCH=$(git log -1 --format=%ct)
cargo metadata --locked --no-deps --format-version 1 > "$out/workspace.json"
cargo cyclonedx --manifest-path "$root/Cargo.toml" --format json --spec-version 1.5 --describe crate
python3 - "$out" <<'PY'
import json, shutil, sys
from pathlib import Path

out = Path(sys.argv[1])
metadata = json.loads((out / 'workspace.json').read_text())
members = set(metadata['workspace_members'])
packages = [p for p in metadata['packages'] if p['id'] in members]
if not packages or len(packages) != len(members):
    sys.exit('Incomplete workspace inventory')
for package in packages:
    directory = Path(package['manifest_path']).parent
    files = list(directory.glob('*.cdx.json'))
    matches = []
    for file in files:
        bom = json.loads(file.read_text())
        if (bom.get('bomFormat') == 'CycloneDX' and bom.get('specVersion') == '1.5'
                and bom.get('metadata', {}).get('component', {}).get('name') == package['name']):
            matches.append(file)
    if len(matches) != 1:
        sys.exit(f'Expected one CycloneDX 1.5 BOM for {package["name"]}')
    shutil.move(matches[0], out / 'sbom' / (package['name'] + '.cdx.json'))
PY
tar -czf "$out/homeserver-sbom.tar.gz" -C "$out/sbom" .
digest=$(sha256sum "$out/homeserver-sbom.tar.gz" | cut -d' ' -f1)
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then printf 'sbom=%s\n' "$digest" >> "$GITHUB_OUTPUT"; fi
echo "Homeserver SBOM SHA-256: $digest"
