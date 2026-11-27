import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

spec = importlib.util.spec_from_file_location("collect_ci_evidence", Path(__file__).with_name("collect_ci_evidence.py"))
collector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collector)


class EvidenceTests(unittest.TestCase):
    def test_retains_reports_and_stacks_without_scanning_database_or_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "pom.xml").write_text('<project><modules><module>test</module></modules></project>')
            evidence = ["test/target/surefire-reports/TEST-example.xml",
                        ".ci-diagnostics/UDFIT/jstack-1.txt",
                        "test/target/security-acceptance/node-0/node.log", "iginx-6888.log"]
            for name in evidence + ["test/database/points.bin", "core/target/python/lib/site-packages/module.py"]:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("evidence", encoding="utf-8")
            original = collector.os.walk

            def limited_walk(path, **kwargs):
                self.assertNotIn(str(path), [str(root), str(root / "test"), str(root / "core/target")])
                return original(path, **kwargs)

            with patch.object(collector.os, "walk", side_effect=limited_walk):
                result = collector.collect(root)
            with zipfile.ZipFile(result) as archive:
                self.assertEqual(set(evidence) | {"evidence-files.txt"}, set(archive.namelist()))
                self.assertEqual(b"evidence", archive.read(evidence[1]))


if __name__ == "__main__":
    unittest.main()
