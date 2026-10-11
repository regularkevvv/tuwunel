"""Exercise lifecycle gate refusal and invocation without compiling Rust."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("container_lifecycle", ROOT / "ci/check-container-lifecycle.py")
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


class LifecycleReceipt(unittest.TestCase):
    def log(self):
        return "\n".join(f"test {name} ... ok" for name in sorted(GATE.REQUIRED)) + "\ntest result: ok. 42 passed; 0 failed; 0 ignored;\n"

    def test_complete_required_controls_pass(self):
        GATE.verify_log(self.log())

    def test_empty_harness_refuses(self):
        with self.assertRaises(ValueError):
            GATE.verify_log("test result: ok. 0 passed; 0 failed; 0 ignored;\n")

    def test_omitted_required_control_refuses(self):
        with self.assertRaises(ValueError):
            GATE.verify_log(self.log().replace(f"test {sorted(GATE.REQUIRED)[0]} ... ok\n", ""))

    def test_ignored_required_control_refuses(self):
        with self.assertRaises(ValueError):
            GATE.verify_log(self.log().replace(" ... ok", " ... ignored", 1))

    def test_maximum_singleton_replay_requires_execution(self):
        case = "sending::sender::retry_restart_tests::maximum_producer_singleton_with_full_key_rows_replays_and_drains"
        self.assertIn(case, GATE.REQUIRED)
        for replacement in ("", f"test {case} ... ignored\n"):
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                GATE.verify_log(self.log().replace(f"test {case} ... ok\n", replacement))

    def test_http_storage_and_admin_controls_each_require_execution(self):
        for case in ("room_storage_erasure_is_atomic_and_protects_operator_access",
                     "administrative_inventories_refuse_before_output_or_key_markers",
                     "http_inbox_pages_preserve_backlog_retries_and_failed_read_claims"):
            with self.subTest(case=case):
                self.assertIn(case, GATE.REQUIRED)
                for replacement in ("", f"test {case} ... ignored\n"):
                    with self.assertRaises(ValueError):
                        GATE.verify_log(self.log().replace(f"test {case} ... ok\n", replacement))

    def test_failed_outer_harness_refuses_even_after_passing_child(self):
        with self.assertRaises(ValueError):
            GATE.verify_log(self.log() + "test result: FAILED. 0 passed; 1 failed; 0 ignored;\n")

    def stress_log(self, **changes):
        report = {"phase": "sender-qualification", "case": "maximum_sender_inventory",
                  "source_revision": "current", "profile": "release", "os": "linux", "arch": "x86_64",
                  "provider_acceptance": False, "linux_vm_hwm_kib": 256000, "elapsed_ms": 10000,
                  "measurements": {"inventory_workers": 4, "slots_per_worker": 16, "deliveries": 64,
                                   "active_rows": 128, "payload_bytes_per_row": 1_400_047,
                                   "replayed_identical_transactions": 64, "remaining_rows": 0,
                                   "shutdown_ms": 4000}}
        report.update(changes)
        return (f"test {GATE.STRESS_CASE} ... child diagnostics\n" + json.dumps(report) +
                "\ntest result: ok. 1 passed; 0 failed; 0 ignored;\nok\n"
                "test result: ok. 1 passed; 0 failed; 0 ignored;\n")

    def test_executed_stress_requires_matching_source_and_measurements(self):
        self.assertEqual(GATE.verify_sender_stress(self.stress_log(), "current")["measurements"]["deliveries"], 64)
        for changes in ({"source_revision": "older"}, {"linux_vm_hwm_kib": None},
                        {"profile": "debug"}, {"provider_acceptance": True}, {"measurements": {}}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                GATE.verify_sender_stress(self.stress_log(**changes), "current")

    def test_ignored_or_failed_stress_cannot_qualify_metrics(self):
        for log in (self.stress_log().replace(" ... child diagnostics", " ... ignored"),
                    self.stress_log() + "test result: FAILED. 0 passed; 1 failed; 0 ignored;\n",
                    f"test {GATE.STRESS_CASE} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n"):
            with self.assertRaises(ValueError):
                GATE.verify_sender_stress(log, "current")

    def test_interleaved_real_process_output_is_accepted(self):
        name = sorted(GATE.REQUIRED)[0]
        log = self.log().replace(f"test {name} ... ok", f"test {name} ... child diagnostics\ntest result: ok. 1 passed; 0 failed;\nok")
        GATE.verify_log(log)


class LifecycleDriver(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(prefix="native-gate-control-")
        self.addCleanup(scratch.cleanup)
        self.base = Path(scratch.name)
        self.root = self.base / "source"
        (self.root / "ci").mkdir(parents=True)
        (self.root / "src/main").mkdir(parents=True)
        for name in ("native-gate.sh", "container-features.txt", "check-container-lifecycle.py"):
            shutil.copy2(ROOT / "ci" / name, self.root / "ci" / name)
        shutil.copy2(ROOT / "src/main/Cargo.toml", self.root / "src/main/Cargo.toml")
        for args in (("init", "--quiet"), ("add", "."), ("commit", "--quiet", "-m", "fixture")):
            subprocess.run(["git", "-c", "user.name=Lifecycle", "-c", "user.email=test@example.invalid",
                            "-c", "commit.gpgsign=false", *args], cwd=self.root, check=True, capture_output=True)
        self.tools = self.base / "tools"
        self.tools.mkdir()
        stub = '''import json, os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
if name == "uname":
    print(os.environ.get("TEST_OS", "Linux") if sys.argv[1] == "-s" else os.environ.get("TEST_ARCH", "x86_64"))
    sys.exit(0)
with open(os.environ["TEST_CARGO_LOG"], "a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\\n")
if os.environ.get("TEST_CARGO_FAIL") == "1":
    sys.exit(1)
if os.environ["TEST_STRESS_CASE"] in sys.argv:
    case = os.environ["TEST_STRESS_CASE"]
    print(f"test {case} ... ok")
    if os.environ.get("TEST_STRESS_SKIP") == "1":
        print("test result: ok. 0 passed; 0 failed; 1 ignored;")
        sys.exit(0)
    report = {"phase": "sender-qualification", "case": "maximum_sender_inventory",
              "source_revision": os.environ["TUWUNEL_QUALIFICATION_REVISION"],
              "profile": "release", "os": "linux", "arch": "x86_64",
              "provider_acceptance": False, "linux_vm_hwm_kib": 256000, "elapsed_ms": 10000,
              "measurements": {"inventory_workers": 4, "slots_per_worker": 16, "deliveries": 64,
                               "active_rows": 128, "payload_bytes_per_row": 1_400_047,
                               "replayed_identical_transactions": 64, "remaining_rows": 0, "shutdown_ms": 4000}}
    print(json.dumps(report))
    print("test result: ok. 1 passed; 0 failed; 0 ignored;")
    sys.exit(0)
if "de_record_" in sys.argv:
    print("test result: ok. 0 passed; 0 failed; 0 ignored;")
    print("test result: ok. 7 passed; 0 failed; 0 ignored;")
    sys.exit(0)
for case in json.loads(os.environ["TEST_REQUIRED"]):
    print(f"test {case} ... ok")
print("test result: ok. 42 passed; 0 failed; 0 ignored;")
'''
        for name in ("cargo", "uname"):
            path = self.tools / name
            path.write_text(f"#!{sys.executable}\n" + stub)
            path.chmod(0o755)
        self.env = {**os.environ, "PATH": f"{self.tools}:{os.environ['PATH']}",
                    "TEST_CARGO_LOG": str(self.base / "cargo.jsonl"),
                    "TEST_REQUIRED": json.dumps(sorted(GATE.REQUIRED)),
                    "TEST_STRESS_CASE": GATE.STRESS_CASE,
                    "TUWUNEL_LIFECYCLE_REPORT": str(self.base / "receipt.json"),
                    "TUWUNEL_QUEUE_RESTART_SCENARIO": "corrupt-pdu"}

    def run_gate(self, mode="container-lifecycle"):
        return subprocess.run(["bash", "ci/native-gate.sh", mode], cwd=self.root,
                              env=self.env, text=True, capture_output=True)

    def test_linux_amd64_driver_uses_declared_release_features(self):
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        commands = [json.loads(line) for line in (self.base / "cargo.jsonl").read_text().splitlines()]
        self.assertEqual(len(commands), 3)
        for command in commands:
            for flag in ("--locked", "--release", "--no-default-features"):
                self.assertIn(flag, command)
            features = command[command.index("--features") + 1].replace("tuwunel/", "").split(",")
            self.assertEqual(features, GATE.feature_contract().split(","))
        for target in ("cancellation_lifecycle", "fatal_lifecycle", "sending_queue_restart",
                       "room_storage_erasure", "admin_diagnostic_inventory", "inbox_paging"):
            self.assertIn(target, commands[1])
        self.assertIn(GATE.STRESS_CASE, commands[2])
        for flag in ("--ignored", "--exact", "--nocapture"):
            self.assertIn(flag, commands[2])
        receipt = json.loads((self.base / "receipt.json").read_text())
        self.assertEqual(receipt["required_case_count"], len(GATE.REQUIRED))
        self.assertEqual(receipt["cargo_profile"], "release")
        self.assertEqual(receipt["sender_stress"]["measurements"]["replayed_identical_transactions"], 64)

    def test_skipped_stress_produces_no_passing_receipt(self):
        self.env["TEST_STRESS_SKIP"] = "1"
        result = self.run_gate()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.base / "receipt.json").exists())

    def test_record_decoding_reuses_the_container_release_feature_closure(self):
        result = self.run_gate("release")
        self.assertEqual(result.returncode, 0, result.stderr)
        commands = [json.loads(line) for line in (self.base / "cargo.jsonl").read_text().splitlines()]
        self.assertEqual(len(commands), 1)
        command = commands[0]
        for argument in ("--release", "--no-default-features", "tuwunel", "tuwunel_database", "de_record_"):
            self.assertIn(argument, command)
        features = command[command.index("--features") + 1].replace("tuwunel/", "").split(",")
        self.assertEqual(features, GATE.feature_contract().split(","))

    def test_darwin_refuses_before_cargo(self):
        self.env["TEST_OS"] = "Darwin"
        self.assertNotEqual(self.run_gate().returncode, 0)
        self.assertFalse((self.base / "cargo.jsonl").exists())

    def test_arm64_linux_refuses_before_cargo(self):
        self.env["TEST_ARCH"] = "aarch64"
        self.assertNotEqual(self.run_gate().returncode, 0)
        self.assertFalse((self.base / "cargo.jsonl").exists())

    def test_unexpected_feature_refuses_before_cargo(self):
        path = self.root / "ci/container-features.txt"
        path.write_text(path.read_text().strip() + ",io_uring\n")
        self.assertNotEqual(self.run_gate().returncode, 0)
        self.assertFalse((self.base / "cargo.jsonl").exists())

    def test_cargo_failure_produces_no_passing_receipt(self):
        self.env["TEST_CARGO_FAIL"] = "1"
        self.assertNotEqual(self.run_gate().returncode, 0)
        self.assertFalse((self.base / "receipt.json").exists())

    def test_source_revision_failure_refuses_before_cargo(self):
        git = self.tools / "git"
        git.write_text("#!/bin/sh\nexit 1\n")
        git.chmod(0o755)
        self.assertNotEqual(self.run_gate().returncode, 0)
        self.assertFalse((self.base / "cargo.jsonl").exists())
        self.assertFalse((self.base / "receipt.json").exists())


class PredecessorIsolation(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(prefix="predecessor-isolation-")
        self.addCleanup(scratch.cleanup)
        self.base = Path(scratch.name)
        self.root = self.base / "source"
        (self.root / "ci").mkdir(parents=True)
        shutil.copy2(ROOT / "ci/native-gate.sh", self.root / "ci/native-gate.sh")
        self.tools = self.base / "tools"
        self.tools.mkdir()
        stub = r'''import io, json, os, pathlib, sys, tarfile
args = sys.argv[1:]
if pathlib.Path(sys.argv[0]).name == "git":
    if args[0] == "merge-base":
        sys.exit(0)
    assert args[0] == "archive", args
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as archive:
        content = b"committed predecessor"
        info = tarfile.TarInfo("Cargo.toml")
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))
    sys.exit(0)
with open(os.environ["TEST_CARGO_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
target = pathlib.Path(os.environ["CARGO_TARGET_DIR"])
target.mkdir(exist_ok=True)
members = ["path+file:///predecessor/core#tuwunel_core@1.9.0",
           "path+file:///predecessor/database#tuwunel_database@1.9.0",
           "path+file:///predecessor/main#tuwunel@1.9.0"]
external = "registry+https://github.com/rust-lang/crates.io-index#rocksdb@1.0.0"
if args[0] == "metadata":
    packages = [{"id": package} for package in members + [external]]
    if os.environ.get("TEST_INCOMPLETE_METADATA") == "1":
        packages.pop(0)
    print(json.dumps({"workspace_members": members, "packages": packages}))
elif "--no-run" in args:
    assert pathlib.Path("Cargo.toml").read_text() == "committed predecessor"
    binary = target / "admin_task_journal"
    binary.write_text("frozen predecessor executable")
    binary.chmod(0o755)
    (target / "workspace-stale").write_text("old library")
    (target / "external-cache").write_text("keep native dependencies")
    print(json.dumps({"reason": "compiler-artifact", "target": {"name": "admin_task_journal", "kind": ["test"]},
                      "profile": {"test": True}, "executable": str(binary)}))
elif args[0] == "clean":
    assert args[1::2] == ["--package"] * len(members), args
    assert args[2::2] == members, args
    frozen = pathlib.Path.cwd().parent / "older-journal"
    assert frozen.is_file() and frozen.read_text() == "frozen predecessor executable"
    assert frozen.stat().st_mode & 0o777 == 0o500
    (target / "admin_task_journal").unlink()
    (target / "workspace-stale").unlink()
elif args[0] == "test":
    assert not (target / "workspace-stale").exists(), "candidate saw predecessor library"
    assert (target / "external-cache").read_text() == "keep native dependencies"
    assert pathlib.Path(os.environ["TUWUNEL_HISTORY_OLDER_JOURNAL_BINARY"]).read_text() == "frozen predecessor executable"
    print("test older_journal_refuses_schema_and_record_changes_without_mutation ... ok")
    print("test result: ok. 1 passed; 0 failed; 0 ignored;")
else:
    raise AssertionError(args)
'''
        for name in ("cargo", "git"):
            path = self.tools / name
            path.write_text(f"#!{sys.executable}\n" + stub)
            path.chmod(0o755)
        self.env = {**os.environ, "PATH": f"{self.tools}:{os.environ['PATH']}",
                    "TEST_CARGO_LOG": str(self.base / "cargo.jsonl"),
                    "CARGO_TARGET_DIR": str(self.base / "shared-target"), "RUNNER_TEMP": str(self.base)}

    def run_gate(self):
        result = subprocess.run(["bash", "ci/native-gate.sh", "compatibility"], cwd=self.root,
                                env=self.env, text=True, capture_output=True)
        commands = [json.loads(line) for line in (self.base / "cargo.jsonl").read_text().splitlines()]
        return result, commands

    def test_old_workspace_outputs_retire_after_freezing_before_candidate_without_cleaning_dependencies(self):
        result, commands = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([command[0] for command in commands], ["test", "metadata", "clean", "test"])
        self.assertTrue((self.base / "shared-target/external-cache").is_file())
        self.assertFalse((self.base / "shared-target/workspace-stale").exists())

    def test_incomplete_workspace_inventory_refuses_cleanup_and_candidate(self):
        self.env["TEST_INCOMPLETE_METADATA"] = "1"
        result, commands = self.run_gate()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([command[0] for command in commands], ["test", "metadata"])
        self.assertTrue((self.base / "shared-target/external-cache").is_file())
        self.assertTrue((self.base / "shared-target/workspace-stale").is_file())


if __name__ == "__main__":
    unittest.main()
