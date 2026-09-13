"""Require the real detector to accept a clean process and reject a 37-byte leak."""
import subprocess
import sys


def verify(binary):
    clean = subprocess.run([binary], capture_output=True, text=True, timeout=30)
    if clean.returncode != 0:
        raise RuntimeError(f"clean control failed:\n{clean.stdout}{clean.stderr}")
    leaked = subprocess.run([binary, "leak"], capture_output=True, text=True, timeout=30)
    report = leaked.stdout + leaked.stderr
    if (leaked.returncode == 0 or "LeakSanitizer: detected memory leaks" not in report
            or "37 byte(s)" not in report):
        raise RuntimeError(f"detector did not identify the injected 37-byte leak:\n{report}")
    print("LeakSanitizer control passed: clean exit accepted, 37-byte leak rejected.")


if __name__ == "__main__":
    verify(sys.argv[1])
