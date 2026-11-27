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
        # A Controller launches this wrapper again for each class. Those nested
        # commands start their own sessions, so killing only the outer group
        # would leave the inner Maven/JVM running without its deadline monitor.
        listing = subprocess.run(["ps", "-e", "-o", "pid=,ppid="],
                                 capture_output=True, text=True, timeout=5)
        children = {}
        for line in listing.stdout.splitlines():
            pid, parent = map(int, line.split())
            children.setdefault(parent, []).append(pid)
        pending, descendants = [process.pid], []
        while pending:
            pid = pending.pop()
            descendants.append(pid)
            pending.extend(children.get(pid, []))
        groups = {process.pid}
        for pid in descendants:
            try:
                groups.add(os.getpgid(pid))
            except ProcessLookupError:
                pass
        groups.discard(os.getpgrp())
        for group in groups:
            try:
                os.killpg(group, signal.SIGTERM)
            except ProcessLookupError:
                pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        for group in groups:
            try:
                os.killpg(group, signal.SIGKILL)
            except ProcessLookupError:
                pass
    process.wait(timeout=10)


def download_failure(output):
    if re.search(r"Failures:\s*[1-9]|Errors:\s*[1-9]|<<< FAILURE|There are test failures", output):
        return False
    return bool(re.search(
        r"\[ERROR\].*(Could not transfer|Could not resolve dependencies|PluginResolutionException|PluginVersionResolutionException)",
        output))


def retry_command(command, output):
    # A shared hosted-runner address can remain rate limited for every retry.
    # Switch only Central downloads to its documented Google-hosted mirror;
    # never retry an assertion or change a developer's Maven settings.
    maven = Path(command[0]).name.lower() in {"mvn", "mvn.cmd", "mvn.bat"}
    if (os.environ.get("GITHUB_ACTIONS") == "true" and maven
            and "repo.maven.apache.org" in output and "429" in output
            and not any(arg in {"-s", "--settings"} for arg in command)):
        settings = Path(__file__).resolve().parents[2] / "maven-central-mirror.xml"
        print("Maven Central returned 429; using its Google-hosted mirror for the bounded retry", flush=True)
        return [command[0], "--settings", str(settings), "-U", *command[1:]]
    return command


def run(command, timeout, label, retries=0, root=None):
    root = Path(root) if root else Path(__file__).resolve().parents[3]
    label = re.sub(r"[^A-Za-z0-9_.-]", "_", label)[:80]
    # Maven clean removes every module's target directory, including the root one.
    directory = root / ".ci-diagnostics" / (label + "-" + str(time.time_ns()))
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
        command = retry_command(command, output)
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
