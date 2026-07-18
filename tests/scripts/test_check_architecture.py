# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

CHECKER_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_architecture.py"
SPEC = importlib.util.spec_from_file_location("check_architecture", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
checker = importlib.util.module_from_spec(SPEC)
sys.modules["check_architecture"] = checker
SPEC.loader.exec_module(checker)


def _write_policy(root: Path, *, exception: str = "") -> Path:
    (root / "pyproject.toml").write_text(
        '[project]\nname = "fixture"\nversion = "0.4.0"\n',
        encoding="utf-8",
    )
    policy = root / "architecture.toml"
    policy.write_text(
        """
version = 1
source_root = "src"

[concrete_services]
types = ["WorldService"]
composition_roots = ["archetype.app.container"]

[[package_rule]]
name = "app-outward"
consumer = "archetype.app"
forbidden = ["archetype.experiments"]

[[module_rule]]
module = "archetype.app.probe"
allowed_interfaces = []
allowed_app = []
"""
        + exception,
        encoding="utf-8",
    )
    return policy


def test_negative_fixture_detects_each_enforced_rule(tmp_path: Path) -> None:
    probe = tmp_path / "src" / "archetype" / "app" / "probe.py"
    probe.parent.mkdir(parents=True)
    (probe.parent / "interfaces.py").write_text(
        "class iWorldService:\n    pass\n",
        encoding="utf-8",
    )
    probe.write_text(
        """
from archetype.app.interfaces import iWorldService
from archetype.app.world.service import WorldService
from archetype.experiments import trajectories

class Derived(WorldService):
    pass

instance = WorldService()
""",
        encoding="utf-8",
    )

    result = checker.audit_repository(_write_policy(tmp_path), repo_root=tmp_path)

    assert {violation.rule for violation in result.violations} == {
        "package_dependency",
        "module_dependency",
        "interface_dependency",
        "concrete_construction",
        "concrete_inheritance",
    }
    assert not result.policy_errors


def test_exception_must_be_exact_and_non_stale(tmp_path: Path) -> None:
    probe = tmp_path / "src" / "archetype" / "app" / "probe.py"
    probe.parent.mkdir(parents=True)
    probe.write_text("value = 1\n", encoding="utf-8")
    exception = """

[[exception]]
rule = "module_dependency"
consumer = "archetype.app.probe"
target = "archetype.app.world.service"
owner = "architecture"
reason = "fixture"
expires = "v1"
"""

    result = checker.audit_repository(
        _write_policy(tmp_path, exception=exception),
        repo_root=tmp_path,
    )

    assert not result.violations
    assert result.policy_errors == [
        "stale architecture exception matched no violation: "
        "module_dependency | archetype.app.probe | archetype.app.world.service"
    ]


def test_exception_fails_at_its_release_deadline(tmp_path: Path) -> None:
    probe = tmp_path / "src" / "archetype" / "app" / "probe.py"
    probe.parent.mkdir(parents=True)
    probe.write_text(
        "from archetype.app.world.service import WorldService\n",
        encoding="utf-8",
    )
    exception = """

[[exception]]
rule = "module_dependency"
consumer = "archetype.app.probe"
target = "archetype.app.world.service"
owner = "architecture"
reason = "fixture"
expires = "v0.4"
"""

    result = checker.audit_repository(
        _write_policy(tmp_path, exception=exception),
        repo_root=tmp_path,
    )

    assert not result.violations
    assert len(result.policy_errors) == 1
    assert "expired at v0.4 (project is 0.4.0)" in result.policy_errors[0]


def test_concrete_constructor_annotation_requires_a_port(tmp_path: Path) -> None:
    probe = tmp_path / "src" / "archetype" / "app" / "probe.py"
    probe.parent.mkdir(parents=True)
    probe.write_text(
        "class WorldService:\n"
        "    def __init__(self, parent: WorldService) -> None:\n"
        "        self.parent = parent\n",
        encoding="utf-8",
    )

    result = checker.audit_repository(_write_policy(tmp_path), repo_root=tmp_path)

    assert [violation.rule for violation in result.violations] == ["concrete_annotation"]


def test_every_concrete_service_requires_a_module_rule(tmp_path: Path) -> None:
    service = tmp_path / "src" / "archetype" / "app" / "service.py"
    service.parent.mkdir(parents=True)
    service.write_text("class WorldService:\n    pass\n", encoding="utf-8")

    result = checker.audit_repository(_write_policy(tmp_path), repo_root=tmp_path)

    assert result.policy_errors == [
        "module rule references missing module: archetype.app.probe",
        "concrete service WorldService in archetype.app.service has no module rule",
    ]


def test_repository_architecture_policy_passes() -> None:
    completed = subprocess.run(
        [sys.executable, str(CHECKER_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "Architecture audit passed" in completed.stdout
