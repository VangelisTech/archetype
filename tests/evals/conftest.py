"""Make the top-level ``evals`` package importable from tests.

Only pre-imports leaf modules that have no side-effects on the Component
subclass registry.  ``evals.run`` and ``evals.suites.*`` are NOT imported
because they register Component subclasses (``Tag``, ``Health``) that
collide with identically-named locals in other test files.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path


def pytest_configure(config):  # noqa: ARG001
    repo_root = str(Path(__file__).resolve().parents[2])

    need_cleanup = repo_root not in sys.path
    if need_cleanup:
        sys.path.append(repo_root)

    for mod_name in ["evals", "evals.types", "evals.harness", "evals.graders"]:
        if mod_name not in sys.modules:
            importlib.import_module(mod_name)

    if need_cleanup:
        sys.path.remove(repo_root)
