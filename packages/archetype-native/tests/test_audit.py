"""Executable counterexamples for the independent package's import reservation."""

import runpy
import shutil
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
CHECK = runpy.run_path(str(REPO / "scripts/check_ddlog_preview.py"))["check"]


class PreviewAuditTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        for relative in [
            "quality/ddlog-preview.toml",
            "packages/archetype-native/pyproject.toml",
            "crates/archetype-ddlog-python/Cargo.toml",
            "docs/guide/ddlog-python-preview.md",
        ]:
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPO / relative, path)
        self.source = self.root / "packages/archetype-native/src"
        self.module = self.source / "archetype_native/__init__.py"
        self.module.parent.mkdir(parents=True)
        self.module.write_text("import ctypes\nimport json\n")
        self.assertEqual(CHECK(self.root), [])

    def test_stale_reservation_and_missing_module(self):
        metadata = self.root / "quality/ddlog-preview.toml"
        original = metadata.read_text()
        metadata.write_text(original.replace('module = "archetype_native"', 'module = "other"'))
        self.assertTrue(CHECK(self.root))
        metadata.write_text(original)
        self.module.unlink()
        self.assertTrue(CHECK(self.root))

    def test_extra_package_and_loader_aliases(self):
        extra = self.source / "unregistered.py"
        extra.write_text("import json\n")
        self.assertTrue(CHECK(self.root))
        extra.unlink()
        for forbidden in [
            "import archetype.runtime\n",
            'from importlib import import_module as load\nload("archetype.runtime")\n',
            'import builtins\nbuiltins.__import__("daft")\n',
        ]:
            self.module.write_text(forbidden)
            self.assertTrue(CHECK(self.root), forbidden)
