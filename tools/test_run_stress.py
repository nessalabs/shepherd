"""Guard against false-green results from the external stress watchdog."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class WatchdogTests(unittest.TestCase):
    def run_command(self, code, expected):
        with tempfile.TemporaryDirectory(prefix="shepherd watchdog ") as directory:
            log = Path(directory) / "stress.log"
            result = subprocess.run(
                [sys.executable, str(Path(__file__).with_name("run-stress.py")),
                 "--log", str(log), "--idle-timeout", "1", "--", sys.executable, "-c", code],
                capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
            if expected == 124:
                self.assertIn("WATCHDOG TIMEOUT", log.read_text())
                self.assertTrue(log.with_suffix(".diagnostics.txt").exists())

    def test_success(self):
        self.run_command('print("complete")', 0)

    def test_preserves_failure_exit(self):
        self.run_command("raise SystemExit(7)", 7)

    def test_eof_does_not_mean_process_has_exited(self):
        self.run_command("import os,time;os.close(1);os.close(2);time.sleep(0.2)", 0)

    def test_stall_fails_and_retains_diagnostics(self):
        self.run_command('import time;print("started",flush=True);time.sleep(20)', 124)


if __name__ == "__main__":
    unittest.main()
