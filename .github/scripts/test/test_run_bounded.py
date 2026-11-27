import importlib.util
import os
import signal
import subprocess
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

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

    def test_build_clean_cannot_remove_diagnostics(self):
        with tempfile.TemporaryDirectory() as root:
            target = Path(root) / "target"
            target.mkdir()
            script = "import pathlib, sys; pathlib.Path(sys.argv[1]).rmdir(); print('clean completed', flush=True); raise SystemExit(7)"
            self.assertEqual(7, runner.run([sys.executable, "-c", script, str(target)], 10, "clean", root=root))
            logs = list(Path(root).rglob("attempt-1.log"))
            self.assertEqual(1, len(logs))
            self.assertIn("clean completed", logs[0].read_text())

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

    def test_central_rate_limit_uses_mirror_but_assertions_do_not_retry(self):
        with tempfile.TemporaryDirectory() as root:
            executable = Path(root) / ("mvn.cmd" if os.name == "nt" else "mvn")
            error = "[ERROR] Could not transfer artifact from https://repo.maven.apache.org/maven2: HTTP Status: 429"
            if os.name == "nt":
                executable.write_text('@echo off\nif "%1"=="--settings" (\n echo mirror-retry\n exit /b 0\n)\necho ' + error + '\nexit /b 1\n')
            else:
                executable.write_text('#!/bin/sh\nif [ "$1" = "--settings" ]; then echo mirror-retry; exit 0; fi\necho "' + error + '"\nexit 1\n')
                executable.chmod(0o700)
            with patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}), patch.object(runner.time, "sleep"):
                self.assertEqual(0, runner.run([str(executable), "test"], 60, "mirror", retries=1, root=root))
            logs = list(Path(root).rglob("attempt-2.log"))
            self.assertEqual(1, len(logs))
            self.assertIn("mirror-retry", logs[0].read_text())
            with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}):
                self.assertEqual([str(executable), "-U", "test"], runner.retry_command([str(executable), "test"], error))

    def test_missing_release_retry_refreshes_maven_failure_cache(self):
        with tempfile.TemporaryDirectory() as root:
            executable = Path(root) / ("mvn.cmd" if os.name == "nt" else "mvn")
            error = "[ERROR] PluginResolutionException: release was not found during a previous attempt"
            if os.name == "nt":
                executable.write_text('@echo off\nif "%1"=="-U" (\n echo refreshed-repository\n exit /b 0\n)\necho ' + error + '\nexit /b 1\n')
            else:
                executable.write_text('#!/bin/sh\nif [ "$1" = "-U" ]; then echo refreshed-repository; exit 0; fi\necho "' + error + '"\nexit 1\n')
                executable.chmod(0o700)
            with patch.object(runner.time, "sleep"):
                self.assertEqual(0, runner.run([str(executable), "test"], 60, "missing-release", retries=1, root=root))
            logs = list(Path(root).rglob("attempt-2.log"))
            self.assertEqual(1, len(logs))
            self.assertIn("refreshed-repository", logs[0].read_text())
            command = [str(executable), "-U", "test"]
            self.assertEqual(command, runner.retry_command(command, error))

    def test_plugin_prefix_download_failure_is_retried_but_invalid_goal_is_not(self):
        error = "[ERROR] NoPluginFoundForPrefixException"
        self.assertFalse(runner.download_failure(error))
        warning = "[WARNING] Could not transfer metadata from https://repo.maven.apache.org: 429\n"
        self.assertTrue(runner.download_failure(warning + error))
        self.assertFalse(runner.download_failure(warning + error + "\nTests run: 1, Failures: 1"))


if __name__ == "__main__":
    unittest.main()
