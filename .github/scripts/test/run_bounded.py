#!/usr/bin/env python3
"""Bound a CI process tree, retain its output and collect JVM stacks before killing it."""
import argparse
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import threading
import time


def capture_stacks(directory):
    # CI runners are dedicated to this checkout. Do not inspect unrelated local JVMs.
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    try:
        listing = subprocess.run(["jps", "-l"], capture_output=True, text=True, timeout=8)
        (directory / "jps.txt").write_text(listing.stdout + listing.stderr, encoding="utf-8")
        deadline = time.monotonic() + 35
        for line in listing.stdout.splitlines():
            if not any(name in line for name in ("iginx", "surefire", "ForkedBooter", "classworlds")):
                continue
            pid = line.split()[0]
            if not pid.isdigit() or time.monotonic() >= deadline:
                continue
            with (directory / ("jstack-" + pid + ".txt")).open("w", encoding="utf-8") as out:
                try:
                    subprocess.run(["jstack", "-l", pid], stdout=out, stderr=subprocess.STDOUT,
                                   timeout=min(8, max(1, deadline - time.monotonic())))
                except (OSError, subprocess.TimeoutExpired) as error:
                    out.write(str(error))
    except (OSError, subprocess.TimeoutExpired) as error:
        (directory / "diagnostics-error.txt").write_text(str(error), encoding="utf-8")


def stop_tree(process):
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
    else:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            # Descendants can outlive a launcher even if the launcher has exited.
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait(timeout=10)


def download_failure(output):
    if re.search(r"Failures:\s*[1-9]|Errors:\s*[1-9]|<<< FAILURE|There are test failures", output):
        return False
    return bool(re.search(
        r"\[ERROR\].*(Could not transfer|Could not resolve dependencies|PluginResolutionException|PluginVersionResolutionException)",
        output))


def run(command, timeout, label, retries=0, root=None):
    root = Path(root) if root else Path(__file__).resolve().parents[3]
    label = re.sub(r"[^A-Za-z0-9_.-]", "_", label)[:80]
    directory = root / "target/ci-diagnostics" / (label + "-" + str(time.time_ns()))
    directory.mkdir(parents=True, exist_ok=True)
    executable = shutil.which(command[0])
    if not executable:
        raise FileNotFoundError(command[0])
    command = [executable, *command[1:]]
    deadline = time.monotonic() + timeout
    for attempt in range(retries + 1):
        print(f"[{label}] attempt {attempt + 1}; deadline {timeout}s; logs {directory}", flush=True)
        options = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {"start_new_session": True}
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **options)
        logfile = directory / f"attempt-{attempt + 1}.log"

        def copy_output(source=process.stdout, destination=logfile):
            with source, destination.open("wb") as out:
                for data in iter(source.readline, b""):
                    out.write(data)
                    out.flush()
                    sys.stdout.write(data.decode("utf-8", errors="replace"))
                    sys.stdout.flush()

        copier = threading.Thread(target=copy_output, daemon=True)
        copier.start()
        try:
            code = process.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            print(f"::error::[{label}] timed out; capturing JVM stacks before terminating the process tree", flush=True)
            capture_stacks(directory)
            stop_tree(process)
            copier.join(timeout=5)
            (directory / "timeout.txt").write_text(f"{label}: exceeded {timeout}s\n", encoding="utf-8")
            return 124
        copier.join(timeout=5)
        if code == 0 or attempt == retries:
            return code
        output = logfile.read_text(encoding="utf-8", errors="replace")
        delay = 15 * (attempt + 1)
        if not download_failure(output) or time.monotonic() + delay >= deadline:
            return code
        print(f"[{label}] dependency resolution failed; retrying in {delay}s within the original deadline", flush=True)
        time.sleep(delay)
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--retry-downloads", type=int, default=0)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or args.timeout <= 0 or args.retry_downloads < 0:
        parser.error("a command, positive timeout and nonnegative retry count are required")
    return run(command, args.timeout, args.label, args.retry_downloads)


if __name__ == "__main__":
    sys.exit(main())
