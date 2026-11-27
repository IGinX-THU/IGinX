#!/usr/bin/env python3
"""Run a bounded group and reject missing/renamed/skipped acceptance scenarios."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[3]
GROUPS = {
    "entry": "SecurityConfigTest,SessionManagerTest,ResultResponseTest,UserChangeTrackerTest,StatementAuthTableTest,UploadRegistryTest,SecurityConfigAcceptanceIT,SqlSecurityAcceptanceIT,PermissionMatrixIT,CodeRegistrationIT,ReservedPrefixAcceptanceIT,InternalRequestIT",
    "gateways": "RestSecurityAcceptanceIT,RootPasswordIT,MqttSecurityAcceptanceIT,MqttLegacyIT",
    "metadata": "ZkAclTest,ZkSecurityAcceptanceIT,ZkRollingMigrationIT,StartupScriptIT",
    "cluster": "NaivePolicyReplicaTest,InternalAuthTest,SessionRevokeAcceptanceIT,ClusterRegressionAcceptanceIT,ReplicaNumIT,FsEngineAuthIT",
    "etcd": "EtcdAuthIT,EtcdClusterAuthIT,SessionRevokeAcceptanceIT",
}
MODULES = ["shared", "core", "dataSource/filesystem"]
STAGE = json.loads((ROOT / "docs/quickStarts/security-patch-stage.json").read_text(encoding="utf-8"))
GROUPS = {
    group: ",".join(name for name in names.split(",") if name in STAGE["classes"])
    for group, names in GROUPS.items()
}
GROUPS = {group: names for group, names in GROUPS.items() if names}


def matrix():
    result = json.loads((ROOT / "docs/quickStarts/security-test-matrix.json").read_text(encoding="utf-8"))["cases"]
    expected = {f"IGT-{n:02}" for n in list(range(1, 23)) + list(range(23, 38)) + list(range(40, 46)) + [50, 60, 61, 62, 63]}
    assert {r["id"] for r in result} == expected and len(result) == 48
    if STAGE["stage"] == 8:
        assert all(row["tests"] for row in result), "Final PR must map all 48 scenarios"
        assert not any(row.get("deferred_tests") for row in result)
    sources = {}
    for module in MODULES + ["test"]:
        for source in (ROOT / module / "src/test/java").rglob("*.java"):
            sources[source.stem] = source.read_text(encoding="utf-8")
    for row in result:
        for ref in row["tests"]:
            if "#" in ref:
                name, method = ref.split("#")
                assert re.search(r"\b" + re.escape(method) + r"\s*\(", sources[name]), ref
    return result


def verify(group):
    classes = set(GROUPS[group].split(","))
    executed = {}
    for module in MODULES:
        for report in (ROOT / module / "target/surefire-reports").glob("TEST-*.xml"):
            suite = ET.parse(report).getroot()
            if suite.get("name", "").split(".")[-1] not in classes:
                continue
            for case in suite.iter("testcase"):
                key = case.get("classname", suite.get("name")).split(".")[-1] + "#" + case.get("name")
                assert case.find("failure") is None and case.find("error") is None and case.find("skipped") is None, key
                executed[key] = float(case.get("time", 0))
    found = {key.split("#")[0] for key in executed}
    assert found == classes, f"Missing reports: {classes - found}"
    cases = []
    for row in matrix():
        required = [r for r in row["tests"] if r.split("#")[0] in classes]
        if required:
            assert all(ref in executed for ref in required), (row["id"], required)
            cases.append(row["id"])
    evidence = ROOT / "target/security-evidence"
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / f"{group}.json").write_text(json.dumps({"stage": STAGE["stage"], "group": group, "backend": "etcd" if group == "etcd" else "zookeeper", "cases": cases, "methods": executed}, indent=2) + "\n")
    print(f"{group}: {len(executed)} methods passed, {len(cases)} documented scenarios checked")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("group", choices=GROUPS)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    matrix()
    if not args.verify_only:
        executable = shutil.which("mvn.cmd" if os.name == "nt" else "mvn")
        assert executable, "Maven is required"
        command = [executable, "-B", "-P!format", "-pl", "dependency,dataSource/filesystem", "-am", "test", "-DfailIfNoTests=false", "-Dtest=" + GROUPS[args.group], "-Duser.timezone=Asia/Shanghai"]
        if args.group == "etcd":
            assert os.environ.get("IGINX_TEST_ETCD"), "Install etcd 3.5.13 and set IGINX_TEST_ETCD"
            command.append("-Diginx.security.backend=etcd")
        subprocess.run([os.sys.executable, str(ROOT / ".github/scripts/test/run_bounded.py"), "--timeout", "1500", "--label", args.group, "--retry-downloads", "2", "--", *command], cwd=ROOT, check=True)
    verify(args.group)


if __name__ == "__main__":
    main()
