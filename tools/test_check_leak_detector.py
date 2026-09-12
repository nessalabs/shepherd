"""A disabled detector or unrelated crash must never pass its positive control."""
import subprocess
import unittest
from unittest.mock import patch
from check_leak_detector import verify


class DetectorControlTests(unittest.TestCase):
    def result(self, code, report=""):
        return subprocess.CompletedProcess([], code, "", report)

    def test_detected_tiny_leak(self):
        with patch("check_leak_detector.subprocess.run", side_effect=[
            self.result(0), self.result(23, "LeakSanitizer: detected memory leaks\n37 byte(s)")
        ]):
            verify("sentinel")

    def test_disabled_detector_fails(self):
        with patch("check_leak_detector.subprocess.run", return_value=self.result(0)):
            with self.assertRaises(RuntimeError):
                verify("sentinel")

    def test_unrelated_crash_fails(self):
        with patch("check_leak_detector.subprocess.run", side_effect=[self.result(0), self.result(-11)]):
            with self.assertRaises(RuntimeError):
                verify("sentinel")

    def test_clean_control_failure_fails(self):
        with patch("check_leak_detector.subprocess.run", return_value=self.result(23)):
            with self.assertRaises(RuntimeError):
                verify("sentinel")
