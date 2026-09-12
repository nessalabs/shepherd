#!/usr/bin/env python3
"""Stream test output and diagnose stalls independently of the Tokio runtime."""
import argparse
import os
from pathlib import Path
import queue
import signal
import subprocess
import sys
import threading
import time


def diagnose(process, log):
    lines = [f"watchdog parent PID={process.pid}\n"]
    if sys.platform != "win32":
        try:
            result = subprocess.run(["ps", "-axo", "pid=,ppid=,stat=,comm="], capture_output=True, text=True, timeout=10)
            lines.append(result.stdout)
            if sys.platform == "darwin":
                rows = [line.split(None, 3) for line in result.stdout.splitlines()]
                # Only inspect a direct test child of the cargo command we launched.
                for row in rows:
                    if len(row) == 4 and row[1] == str(process.pid) and "leak_stress" in row[3]:
                        sample = subprocess.run(["sample", row[0], "3"], capture_output=True, text=True, timeout=15)
                        lines.extend([sample.stdout, sample.stderr])
        except (OSError, subprocess.TimeoutExpired) as error:
            lines.append(f"diagnostics unavailable: {error}\n")
    path = log.with_suffix(".diagnostics.txt")
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Watchdog diagnostics: {path}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--idle-timeout", type=float, default=120)
    parser.add_argument("--overall-timeout", type=float, default=2400)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or args.idle_timeout <= 0 or args.overall_timeout <= 0:
        parser.error("provide a command and positive timeouts")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, encoding="utf-8", errors="replace", bufsize=1,
                               start_new_session=sys.platform != "win32")
    output = queue.Queue()

    def read():
        for line in process.stdout:
            output.put(line)
        output.put(None)

    threading.Thread(target=read, daemon=True).start()
    started = last_output = time.monotonic()
    eof = False
    with args.log.open("w", encoding="utf-8") as log:
        while True:
            if eof and process.poll() is not None:
                return process.returncode
            now = time.monotonic()
            if now - started > args.overall_timeout or now - last_output > args.idle_timeout:
                message = f"WATCHDOG TIMEOUT: idle={now-last_output:.1f}s elapsed={now-started:.1f}s\n"
                print(message, end="", flush=True)
                log.write(message)
                log.flush()
                diagnose(process, args.log)
                if sys.platform == "win32":
                    subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True, timeout=15)
                else:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                process.wait(timeout=15)
                return 124
            try:
                line = output.get(timeout=0.2)
            except queue.Empty:
                continue
            if line is None:
                # EOF alone does not imply the command exited. Keep the watchdog alive.
                eof = True
                continue
            last_output = time.monotonic()
            print(line, end="", flush=True)
            log.write(line)
            log.flush()


if __name__ == "__main__":
    sys.exit(main())
