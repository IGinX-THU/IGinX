import importlib.util
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

    def test_assertions_are_never_retried_as_download_failures(self):
        network = "[ERROR] Could not transfer artifact: HTTP Status: 429"
        self.assertTrue(runner.download_failure(network))
        self.assertFalse(runner.download_failure(network + "\nTests run: 3, Failures: 1"))
        self.assertFalse(runner.download_failure("[ERROR] There are test failures"))


if __name__ == "__main__":
    unittest.main()
