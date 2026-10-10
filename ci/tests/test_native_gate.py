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

    def test_failed_outer_harness_refuses_even_after_passing_child(self):
        with self.assertRaises(ValueError):
            GATE.verify_log(self.log() + "test result: FAILED. 0 passed; 1 failed; 0 ignored;\n")

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
                    "TUWUNEL_LIFECYCLE_REPORT": str(self.base / "receipt.json"),
                    "TUWUNEL_QUEUE_RESTART_SCENARIO": "corrupt-pdu"}

    def run_gate(self):
        return subprocess.run(["bash", "ci/native-gate.sh", "container-lifecycle"], cwd=self.root,
                              env=self.env, text=True, capture_output=True)

    def test_linux_amd64_driver_uses_declared_release_features(self):
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        commands = [json.loads(line) for line in (self.base / "cargo.jsonl").read_text().splitlines()]
        self.assertEqual(len(commands), 2)
        for command in commands:
            for flag in ("--locked", "--release", "--no-default-features"):
                self.assertIn(flag, command)
            features = command[command.index("--features") + 1].replace("tuwunel/", "").split(",")
            self.assertEqual(features, GATE.feature_contract().split(","))
        for target in ("cancellation_lifecycle", "fatal_lifecycle", "sending_queue_restart"):
            self.assertIn(target, commands[1])
        receipt = json.loads((self.base / "receipt.json").read_text())
        self.assertEqual(receipt["required_case_count"], len(GATE.REQUIRED))
        self.assertEqual(receipt["cargo_profile"], "release")

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


if __name__ == "__main__":
    unittest.main()
