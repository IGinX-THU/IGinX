#!/usr/bin/env python3
"""Archive CI evidence without globbing through databases and Python environments."""
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile


def modules(root):
    pending = [root]
    while pending:
        module = pending.pop()
        yield module
        pom = module / "pom.xml"
        if pom.is_file():
            for child in ET.parse(pom).findall("./{*}modules/{*}module"):
                pending.append(module / child.text.strip())


def files(path):
    if path.is_symlink():
        return
    if path.is_file():
        yield path
    elif path.is_dir():
        for directory, children, names in os.walk(path, followlinks=False):
            children[:] = [name for name in children if not (Path(directory) / name).is_symlink()]
            for name in names:
                item = Path(directory) / name
                if not item.is_symlink():
                    yield item


def collect(root):
    root = Path(root).resolve()
    selected = [root / ".ci-diagnostics", root / "target/ci-diagnostics", root / "target/security-evidence",
                root / "test/src/test/resources/testResult.txt"]
    for project in [root, root / "target/security-baseline/original"]:
        if project.exists():
            for module in modules(project):
                selected.extend([module / "target/surefire-reports", module / "target/security-acceptance"])
    selected.extend(root.glob("iginx-*.log"))
    selected.extend((root / "target/security-baseline").glob("*.log"))
    selected.extend((root / "target/security-baseline").glob("*.json"))
    for location in [root / "core/target", *root.glob("IGinX-*")]:
        selected.extend(location.glob("iginx-core-*/logs"))
    destination = root / "target/ci-artifacts/evidence.zip"
    destination.parent.mkdir(parents=True, exist_ok=True)
    archived = set()
    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
        for selection in selected:
            for path in files(selection):
                relative = path.relative_to(root).as_posix()
                if relative not in archived:
                    archive.write(path, relative)
                    archived.add(relative)
        archive.writestr("evidence-files.txt", "\n".join(sorted(archived)) + "\n")
    print(f"Archived {len(archived)} evidence files in {destination}")
    return destination


if __name__ == "__main__":
    collect(Path(__file__).resolve().parents[3])
