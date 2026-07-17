# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The public-surface boundary rules must FIRE, not just pass on a clean tree."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "check_api_import_boundaries",
    Path(__file__).resolve().parents[1] / "scripts" / "check_api_import_boundaries.py",
)
checker = importlib.util.module_from_spec(_SPEC)
sys.modules["check_api_import_boundaries"] = checker
_SPEC.loader.exec_module(checker)


def _write(tmp_path: Path, body: str) -> Path:
    target = tmp_path / "src" / "archetype" / "experiments" / "sample.py"
    target.parent.mkdir(parents=True)
    target.write_text(body)
    return target


def test_public_api_rule_fires_on_service_typed_param(tmp_path, monkeypatch):
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "async def bad(world_service, x: int = 0): ...\n",
    )
    violations = checker._public_api_violations(path)
    assert len(violations) == 1
    assert "world_service" in violations[0]
    assert "ArchetypeRuntime" in violations[0]


def test_public_api_rule_fires_on_annotation(tmp_path, monkeypatch):
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "def bad(svc: 'SimulationService'): ...\n",
    )
    assert len(checker._public_api_violations(path)) == 1


def test_bridge_allowlist_suppresses_with_deadline(tmp_path, monkeypatch):
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "async def bridged(world_service=None): ...\n",
    )
    monkeypatch.setitem(
        checker.PUBLIC_API_BRIDGE_PARAMS,
        "src/archetype/experiments/sample.py::bridged",
        {"world_service"},
    )
    assert checker._public_api_violations(path) == []


def test_experiments_scope_blocks_service_imports(tmp_path, monkeypatch):
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    path = _write(tmp_path, "from archetype.app.simulation_service import SimulationService\n")
    violations = checker._import_violations(
        path, checker.ALLOWED_APP_IMPORTS_EXPERIMENTS, set(), "experiments"
    )
    assert len(violations) == 1
    assert "allowed app imports for experiments" in violations[0]


def test_experiments_scope_allows_models(tmp_path, monkeypatch):
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    path = _write(tmp_path, "from archetype.app.models import EpisodeConfig\n")
    assert (
        checker._import_violations(
            path, checker.ALLOWED_APP_IMPORTS_EXPERIMENTS, set(), "experiments"
        )
        == []
    )


def test_import_scope_ignores_dotted_sibling_packages(tmp_path, monkeypatch):
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    path = _write(tmp_path, "import archetype.application\n")
    assert (
        checker._import_violations(
            path, checker.ALLOWED_APP_IMPORTS_EXPERIMENTS, set(), "experiments"
        )
        == []
    )


def test_experiments_scope_scans_nested_subdirectories(tmp_path, monkeypatch):
    """Nested experiment modules must not escape the import scan (the reviewer
    caught the original non-recursive glob)."""
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    nested = tmp_path / "src" / "archetype" / "experiments" / "vla" / "bridge.py"
    nested.parent.mkdir(parents=True)
    nested.write_text("from archetype.app.world_service import WorldService\n")
    targets = sorted((tmp_path / "src/archetype/experiments").rglob("*.py"))
    assert nested in targets
    violations = [
        v
        for path in targets
        for v in checker._import_violations(
            path, checker.ALLOWED_APP_IMPORTS_EXPERIMENTS, set(), "experiments"
        )
    ]
    assert len(violations) == 1


def test_annotation_match_is_whole_token_not_substring(tmp_path, monkeypatch):
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "def fine(cfg: 'SimulationServiceConfig'): ...\n"
        "@public_api\n"
        "def bad(svc: 'SimulationService'): ...\n",
    )
    violations = checker._public_api_violations(path)
    assert len(violations) == 1 and "bad" in violations[0]


def test_public_api_class_constructor_is_checked(tmp_path, monkeypatch):
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "class Bad:\n"
        "    def __init__(self, world_service): ...\n",
    )
    violations = checker._public_api_violations(path)
    assert len(violations) == 1
    assert "Bad.__init__" in violations[0]
