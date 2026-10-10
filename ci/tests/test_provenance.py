"""Exercise provenance drivers offline with isolated tool and Git fixtures."""
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class ProvenanceDrivers(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(prefix="provenance-driver-")
        self.addCleanup(scratch.cleanup)
        self.base = Path(scratch.name).resolve()
        self.root = self.base / "source"
        (self.root / "ci").mkdir(parents=True)
        for name in ("repro-build.sh", "sbom.sh", "install-cyclonedx.sh", "container-features.txt", "check-container-lifecycle.py"):
            shutil.copy2(ROOT / "ci" / name, self.root / "ci" / name)
        (self.root / "src/main").mkdir(parents=True)
        shutil.copy2(ROOT / "src/main/Cargo.toml", self.root / "src/main/Cargo.toml")
        (self.root / "Cargo.toml").write_text('[workspace]\nmembers = ["member"]\n')
        (self.root / "member").mkdir()
        (self.root / "member/Cargo.toml").write_text('[package]\nname = "member"\nversion = "0.1.0"\n')
        (self.root / ".cargo").mkdir()
        (self.root / ".cargo/config.toml").write_text('[env]\nFIXTURE = "selected"\n')
        for args in [("init", "--quiet"), ("add", "."), ("commit", "--quiet", "-m", "fixture")]:
            subprocess.run(["git", "-c", "user.name=Provenance", "-c", "user.email=test@example.invalid",
                            "-c", "commit.gpgsign=false", *args], cwd=self.root, check=True, capture_output=True)
        self.tools = self.base / "tools"
        self.tools.mkdir()
        stub = '''import json, os, pathlib, shutil, sys
name = pathlib.Path(sys.argv[0]).name
root = pathlib.Path(os.environ["TEST_ROOT"])
if name == "uname":
    print(os.environ.get("TEST_PLATFORM", "Linux") if sys.argv[1] == "-s" else "x86_64")
    sys.exit(0)
if name == "curl":
    shutil.copyfile(os.environ["TEST_ARCHIVE"], sys.argv[sys.argv.index("-o") + 1])
    sys.exit(0)
assert pathlib.Path.cwd() == root, "workspace configuration must be selected"
assert (root / ".cargo/config.toml").read_text().endswith('FIXTURE = "selected"\\n')
if name == "rustc":
    print("/fixture/rust")
    sys.exit(0)
with open(os.environ["TEST_LOG"], "a") as log:
    log.write(json.dumps({"args": sys.argv[1:], "flags": os.environ.get("RUSTFLAGS"),
                         "target": os.environ.get("CARGO_TARGET_DIR"),
                         "epoch": os.environ.get("SOURCE_DATE_EPOCH")}) + "\\n")
if sys.argv[1] == "metadata":
    print(json.dumps({"workspace_members": ["member@0.1.0"], "packages": [
        {"id": "member@0.1.0", "name": "member", "manifest_path": str(root / "member/Cargo.toml") }]}))
elif sys.argv[1] == "cyclonedx":
    if not os.environ.get("TEST_MISSING_BOM"):
        name = "foreign" if os.environ.get("TEST_FOREIGN_BOM") else "member"
        (root / "member/member.cdx.json").write_text(json.dumps({"bomFormat": "CycloneDX", "specVersion": "1.5",
            "metadata": {"component": {"name": name}}}))
else:
    assert sys.argv[1] == "build" and "--locked" in sys.argv
    assert os.environ["CARGO_INCREMENTAL"] == "0" and os.environ["SOURCE_DATE_EPOCH"].isdigit()
    target = pathlib.Path(os.environ["CARGO_TARGET_DIR"]) / "release"
    assert not target.exists(), "each build must have a clean target"
    target.mkdir(parents=True)
    value = "same-binary"
    if os.environ.get("TEST_MISMATCH"):
        value += str(target)
    (target / "tuwunel").write_text(value)
'''
        for name in ("cargo", "rustc", "uname", "curl"):
            path = self.tools / name
            path.write_text(f"#!{sys.executable}\n" + stub)
            path.chmod(0o755)
        self.out = self.base / "output"
        self.env = dict(os.environ, PATH=str(self.tools) + os.pathsep + os.environ["PATH"],
                        TMPDIR=str(self.base), TEST_ROOT=str(self.root), TEST_LOG=str(self.base / "calls.jsonl"),
                        PROVENANCE_OUT=str(self.out), GITHUB_OUTPUT=str(self.base / "github-output"),
                        GITHUB_PATH=str(self.base / "github-path"), REPRO_PROFILE="release", REPRO_PACKAGE="tuwunel")

    def run_driver(self, name, *args):
        return subprocess.run(["bash", str(self.root / "ci" / name), *args], cwd=self.base,
                              env=self.env, text=True, capture_output=True)

    def test_repro_uses_two_clean_targets_and_identical_flags(self):
        result = self.run_driver("repro-build.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(line) for line in (self.base / "calls.jsonl").read_text().splitlines()]
        self.assertEqual(len(calls), 2)
        self.assertNotEqual(calls[0]["target"], calls[1]["target"])
        self.assertEqual(calls[0]["flags"], calls[1]["flags"])
        expected_features = (ROOT / "ci/container-features.txt").read_text().strip().split(",")
        for call in calls:
            self.assertIn("--no-default-features", call["args"])
            self.assertEqual(call["args"][call["args"].index("--features") + 1].split(","), expected_features)
            self.assertEqual(call["args"][call["args"].index("--bin") + 1], "tuwunel")
            self.assertIn("--remap-path-prefix=" + call["target"] + "=/target", calls[0]["flags"])
            self.assertFalse(Path(call["target"]).exists())
        report = json.loads((self.out / "reproducibility.json").read_text())
        self.assertEqual(report["build_a_sha256"], report["build_b_sha256"])
        self.assertFalse(report["default_features"])
        self.assertEqual(report["features"], expected_features)
        self.assertIn("signed image identity qualified separately", report["scope"])
        source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root, text=True).strip()
        self.assertEqual(report["source_commit"], source)
        self.assertIn("binary=" + report["build_a_sha256"], (self.base / "github-output").read_text())

    def test_repro_refuses_feature_contract_drift_before_building(self):
        path = self.root / "ci/container-features.txt"
        path.write_text(path.read_text().strip() + ",io_uring\n")
        result = self.run_driver("repro-build.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.base / "calls.jsonl").exists())
        self.assertFalse((self.out / "reproducibility.json").exists())

    def test_repro_refuses_a_different_package_before_building(self):
        self.env["REPRO_PACKAGE"] = "member"
        result = self.run_driver("repro-build.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.base / "calls.jsonl").exists())
        self.assertFalse((self.out / "reproducibility.json").exists())

    def test_repro_mismatch_preserves_both_binaries_and_refuses_receipt(self):
        self.env["TEST_MISMATCH"] = "1"
        result = self.run_driver("repro-build.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not bit-identical", result.stderr)
        self.assertNotEqual((self.out / "diagnostics/build-a").read_bytes(), (self.out / "diagnostics/build-b").read_bytes())
        self.assertFalse((self.out / "reproducibility.json").exists())
        self.assertFalse((self.base / "github-output").exists())

    def test_sbom_contains_the_workspace_member_and_emits_its_digest(self):
        result = self.run_driver("sbom.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        archive = self.out / "homeserver-sbom.tar.gz"
        with tarfile.open(archive) as tar:
            self.assertEqual([name for name in tar.getnames() if name.endswith(".json")], ["./member.cdx.json"])
        self.assertFalse((self.root / "member/member.cdx.json").exists())
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        self.assertEqual((self.base / "github-output").read_text(), "sbom=" + digest + "\n")

    def test_missing_or_foreign_member_sbom_refuses_receipt(self):
        for variable in ("TEST_MISSING_BOM", "TEST_FOREIGN_BOM"):
            with self.subTest(variable=variable):
                self.env.pop("TEST_MISSING_BOM", None)
                self.env[variable] = "1"
                result = self.run_driver("sbom.sh")
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((self.out / "homeserver-sbom.tar.gz").exists())
                self.assertFalse((self.base / "github-output").exists())

    def archive_fixture(self, wrong_digest=False):
        archive = self.base / "tool.tar.xz"
        with tarfile.open(archive, "w:xz") as tar:
            value = b"verified-tool"
            info = tarfile.TarInfo("cargo-cyclonedx-x86_64-unknown-linux-gnu/cargo-cyclonedx")
            info.size = len(value)
            tar.addfile(info, io.BytesIO(value))
        digest = "0" * 64 if wrong_digest else hashlib.sha256(archive.read_bytes()).hexdigest()
        pin = self.base / "tool.pin"
        pin.write_text("0.5.9 " + digest + "\n")
        self.env["TEST_ARCHIVE"] = str(archive)
        return pin

    def test_installer_extracts_only_checksum_verified_tool(self):
        pin = self.archive_fixture()
        destination = self.base / "installed"
        result = self.run_driver("install-cyclonedx.sh", str(pin), str(destination))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((destination / "cargo-cyclonedx").read_bytes(), b"verified-tool")
        self.assertEqual((self.base / "github-path").read_text().strip(), str(destination))

    def test_installer_checksum_failure_does_not_install_or_update_path(self):
        pin = self.archive_fixture(wrong_digest=True)
        destination = self.base / "installed"
        result = self.run_driver("install-cyclonedx.sh", str(pin), str(destination))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FAILED", result.stdout)
        self.assertFalse((destination / "cargo-cyclonedx").exists())
        self.assertFalse((self.base / "github-path").exists())

    def test_installer_refuses_an_unsupported_platform(self):
        self.env["TEST_PLATFORM"] = "Darwin"
        result = self.run_driver("install-cyclonedx.sh", "absent", str(self.base / "installed"))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.base / "github-path").exists())


if __name__ == "__main__":
    unittest.main()
