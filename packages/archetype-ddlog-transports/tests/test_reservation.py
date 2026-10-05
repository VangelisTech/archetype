"""Counterexamples for the optional transport distribution's import boundary."""

import runpy
import shutil
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
CHECK = runpy.run_path(str(REPO / "scripts/check_ddlog_transports.py"))["check"]


class TransportReservationTests(unittest.TestCase):
    def test_forbidden_dependency_package_and_dynamic_loader(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for relative in (
                "quality/ddlog-transports.toml",
                "packages/archetype-ddlog-transports/pyproject.toml",
                "docs/guide/ddlog-transports-preview.md",
            ):
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(REPO / relative, target)
            source = root / "packages/archetype-ddlog-transports/src"
            module = source / "archetype_ddlog_transports/__init__.py"
            module.parent.mkdir(parents=True)
            module.write_text("from mcp.server.lowlevel import Server\n")
            self.assertEqual(CHECK(root), [])
            extra = source / "archetype"
            extra.mkdir()
            self.assertTrue(CHECK(root))
            extra.rmdir()
            for forbidden in (
                "import archetype.runtime\n",
                "import daft\n",
                "import ctypes\nimport importlib as loader\n",
                '__import__("archetype.core")\n',
            ):
                module.write_text(forbidden)
                self.assertTrue(CHECK(root), forbidden)
            module.write_text("import json\n")
            project = root / "packages/archetype-ddlog-transports/pyproject.toml"
            project.write_text(project.read_text().replace('"mcp==2.3.0"', '"mcp>=2"'))
            self.assertTrue(CHECK(root))
