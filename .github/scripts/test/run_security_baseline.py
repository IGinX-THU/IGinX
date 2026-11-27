#!/usr/bin/env python3
"""Compile the audit's original revision; do not substitute only selected patched classes."""
from pathlib import Path
import argparse
import json
import os
import shutil
import subprocess
import zipfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[3]
BASELINE = "51b58d9f6"
DIRECTORY = ROOT / "target/security-baseline"


def run(command, cwd, log, allow_test_failures=False):
    print("Running:", " ".join(str(arg) for arg in command), flush=True)
    with (DIRECTORY / log).open("w", encoding="utf-8") as out:
        result = subprocess.run(command, cwd=cwd, stdout=out, stderr=subprocess.STDOUT)
    if result.returncode and not allow_test_failures:
        print((DIRECTORY / log).read_text(encoding="utf-8", errors="replace")[-12000:])
        raise SystemExit(result.returncode)
    return result.returncode


def filesystem_results(root):
    reports = root / "dataSource/filesystem/target/surefire-reports"
    failures, methods = {}, set()
    for report in reports.glob("TEST-*.xml"):
        suite = ET.parse(report).getroot()
        if ".filesystem." not in suite.get("name", ""):
            continue
        for case in suite.iter("testcase"):
            if not case.get("name").startswith("test") and suite.get("name", "").endswith("InternalAuthTest"):
                continue
            key = case.get("classname") + "#" + case.get("name")
            assert case.find("skipped") is None, key
            methods.add(key)
            for kind in ("failure", "error"):
                problem = case.find(kind)
                if problem is not None:
                    failures[key] = {"kind": kind, "type": problem.get("type"), "message": problem.get("message")}
    assert any("ServerTest#" in key for key in methods), "ServerTest was not executed"
    assert len(methods) >= 43, "The complete original FileSystem suite must execute"
    return {"methods": sorted(methods), "failures": failures}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jupiter", action="store_true", help="Compare all original FileSystem JUnit 5 tests; do not hide pre-existing failures")
    args = parser.parse_args()
    DIRECTORY.mkdir(parents=True, exist_ok=True)
    original = DIRECTORY / "original"
    archive = DIRECTORY / "original.zip"
    subprocess.run(["git", "archive", "--format=zip", "--output=" + str(archive), BASELINE], cwd=ROOT, check=True)
    with zipfile.ZipFile(archive) as source:
        for member in source.infolist():
            assert not Path(member.filename).is_absolute() and ".." not in Path(member.filename).parts
        source.extractall(original)
    maven = shutil.which("mvn.cmd" if os.name == "nt" else "mvn")
    assert maven, "Maven is required"
    run([maven, "-B", "-P!format", "-pl", "dependency,core,dataSource/filesystem", "-am", "test-compile", "-DskipTests"], original, "build-original.log")
    if args.jupiter:
        command = [maven, "-B", "-P!format", "-pl", "dependency,dataSource/filesystem", "-am", "test-compile", "org.apache.maven.plugins:maven-surefire-plugin:3.5.2:test", "-Duser.timezone=Asia/Shanghai", "-Dmaven.test.failure.ignore=true"]
        run(command, original, "jupiter-original.log")
        run(command, ROOT, "jupiter-current.log")
        before, after = filesystem_results(original), filesystem_results(ROOT)
        assert before == after, "FileSystem Jupiter results differ from the original revision"
        (DIRECTORY / "jupiter-comparison.json").write_text(json.dumps({"baseline": BASELINE, "result": after}, indent=2) + "\n")
        print(f"FileSystem Jupiter comparison: {len(after['methods'])} methods; {len(after['failures'])} pre-existing failures (see JSON).")
        return
    run([maven, "-B", "-P!format", "-pl", "dependency,dataSource/filesystem", "-am", "test", "-Dtest=BaselineNegativeControlsIT,MqttLegacyIT", "-DfailIfNoTests=false", "-Diginx.acceptance.baselineRoot=" + str(original)], ROOT, "counterexamples.log")
    import sys
    subprocess.run([sys.executable, str(ROOT / ".github/scripts/test/assert_test_reports.py"), "dataSource/filesystem/target/surefire-reports/TEST-cn.edu.tsinghua.iginx.filesystem.security.BaselineNegativeControlsIT.xml:2", "dataSource/filesystem/target/surefire-reports/TEST-cn.edu.tsinghua.iginx.filesystem.security.MqttLegacyIT.xml:1"], cwd=ROOT, check=True)


if __name__ == "__main__":
    main()
