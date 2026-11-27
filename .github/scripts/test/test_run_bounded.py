import importlib.util
import os
import signal
import subprocess
from pathlib import Path
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("run_bounded", Path(__file__).with_name("run_bounded.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class DeadlineTests(unittest.TestCase):
    def test_exit_status_and_output_are_preserved(self):
        with tempfile.TemporaryDirectory() as root:
            code = runner.run([sys.executable, "-c", "print('evidence', flush=True); raise SystemExit(7)"], 10, "failure", root=root)
            self.assertEqual(7, code)
            logs = list(Path(root).rglob("attempt-1.log"))
            self.assertEqual(1, len(logs))
            self.assertIn("evidence", logs[0].read_text())

    def test_timeout_terminates_the_command(self):
        with tempfile.TemporaryDirectory() as root:
            code = runner.run([sys.executable, "-c", "import time; time.sleep(60)"], 0.2, "timeout", root=root)
            self.assertEqual(124, code)
            self.assertEqual(1, len(list(Path(root).rglob("timeout.txt"))))

    def test_timeout_terminates_descendants_in_separate_sessions(self):
        with tempfile.TemporaryDirectory() as root:
            pidfile = Path(root) / "child.pid"
            script = (
                "import os, subprocess, sys, time; from pathlib import Path; "
                "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], "
                "start_new_session=(os.name != 'nt')); "
                "Path(sys.argv[1]).write_text(str(child.pid)); time.sleep(60)"
            )
            code = runner.run([sys.executable, "-c", script, str(pidfile)], 1, "descendants", root=root)
            self.assertEqual(124, code)
            pid = int(pidfile.read_text())
            try:
                if os.name == "nt":
                    result = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                                            capture_output=True, text=True, timeout=5)
                    self.assertNotIn(f'"{pid}"', result.stdout)
                else:
                    result = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)],
                                            capture_output=True, text=True, timeout=5)
                    self.assertTrue(not result.stdout.strip() or result.stdout.strip().startswith("Z"),
                                    "Detached descendant is still running: " + result.stdout)
            finally:
                try:
                    os.kill(pid, signal.SIGTERM)
                except (ProcessLookupError, PermissionError):
                    pass

    def test_assertions_are_never_retried_as_download_failures(self):
        network = "[ERROR] Could not transfer artifact: HTTP Status: 429"
        self.assertTrue(runner.download_failure(network))
        self.assertFalse(runner.download_failure(network + "\nTests run: 3, Failures: 1"))
        self.assertFalse(runner.download_failure("[ERROR] There are test failures"))


if __name__ == "__main__":
    unittest.main()
