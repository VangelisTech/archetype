# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The public-surface boundary rules must FIRE, not just pass on a clean tree."""

from __future__ import annotations

import importlib.util
import sys
import tomllib
from dataclasses import replace
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "check_api_import_boundaries",
    _ROOT / "scripts" / "check_api_import_boundaries.py",
)
checker = importlib.util.module_from_spec(_SPEC)
sys.modules["check_api_import_boundaries"] = checker
_SPEC.loader.exec_module(checker)

_POLICY = checker.load_policy(_ROOT / "quality" / "api_import_boundaries.toml")
_API_COMPOSITION_SURFACE = next(
    surface for surface in _POLICY.import_surfaces if surface.name == "api-composition"
)
_API_ROUTES_SURFACE = next(
    surface for surface in _POLICY.import_surfaces if surface.name == "api-routes"
)
_MISSIONS_API_SURFACE = next(
    surface for surface in _POLICY.import_surfaces if surface.name == "missions-api-router"
)


def _write(tmp_path: Path, body: str) -> Path:
    target = tmp_path / "src" / "archetype" / "probes" / "sample.py"
    target.parent.mkdir(parents=True)
    target.write_text(body)
    return target


def _policy_document() -> dict[str, object]:
    return tomllib.loads(
        (_ROOT / "quality" / "api_import_boundaries.toml").read_text(encoding="utf-8")
    )


def test_policy_loads_surface_dependencies_and_owner_types_from_data():
    assert _API_ROUTES_SURFACE.dependency_roots == {
        "archetype.app",
        "archetype.commands",
        "archetype.world",
    }
    assert "archetype.commands.dispatch" in _API_ROUTES_SURFACE.allowed_dependencies
    assert "archetype.commands.models" in _API_ROUTES_SURFACE.allowed_dependencies
    assert "archetype.missions" not in _API_ROUTES_SURFACE.dependency_roots
    assert "archetype.commands.scheduler" in _API_ROUTES_SURFACE.forbidden_dependencies
    assert "archetype.world.models" in _API_ROUTES_SURFACE.allowed_dependencies
    assert "archetype.world.simulation" in _API_ROUTES_SURFACE.forbidden_dependencies
    assert _API_COMPOSITION_SURFACE.dependency_roots == {
        "archetype.app",
        "archetype.commands",
        "archetype.wiring",
        "archetype.world",
        "archetype.world_libraries",
    }
    assert set(_API_COMPOSITION_SURFACE.targets) == {
        "packages/archetype-ecs/src/archetype/api/app.py",
        "packages/archetype-ecs/src/archetype/api/deps.py",
    }
    assert {
        "archetype.commands.dispatch",
        "archetype.commands.models",
        "archetype.wiring",
        "archetype.world_libraries",
    } <= _API_COMPOSITION_SURFACE.allowed_dependencies
    assert _MISSIONS_API_SURFACE.targets == (
        "packages/archetype-missions/src/archetype/missions/api.py",
    )
    assert "archetype.missions.components" in _MISSIONS_API_SURFACE.allowed_dependencies
    assert "archetype.missions.service" in _MISSIONS_API_SURFACE.forbidden_dependencies
    assert "WorldLifecycle" in _POLICY.public_api.forbidden_owner_types
    assert "MissionService" in _POLICY.public_api.forbidden_owner_types
    assert "mission_service" in _POLICY.public_api.forbidden_parameter_names

    architecture = tomllib.loads(
        (_ROOT / "quality" / "architecture.toml").read_text(encoding="utf-8")
    )
    assert _POLICY.public_api.forbidden_owner_types == frozenset(
        architecture["concrete_services"]["types"]
    )


def test_public_api_rule_fires_on_service_typed_param(tmp_path):
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "async def bad(world_lifecycle, x: int = 0): ...\n",
    )
    violations = checker._public_api_violations(path, _POLICY.public_api, root=tmp_path)
    assert len(violations) == 1
    assert "world_lifecycle" in violations[0]
    assert "supported runtime or gateway boundary" in violations[0]


def test_public_api_rule_fires_on_annotation(tmp_path):
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n@public_api\ndef bad(svc: 'WorldLifecycle'): ...\n",
    )
    assert len(checker._public_api_violations(path, _POLICY.public_api, root=tmp_path)) == 1


@pytest.mark.parametrize(
    ("signature", "parameter"),
    [
        ("*world_lifecycle", "world_lifecycle"),
        ("**container", "container"),
    ],
)
def test_public_api_rule_checks_variadic_parameters(
    tmp_path: Path,
    signature: str,
    parameter: str,
) -> None:
    path = _write(
        tmp_path,
        f"from archetype._api import public_api\n@public_api\ndef bad({signature}): ...\n",
    )

    violations = checker._public_api_violations(path, _POLICY.public_api, root=tmp_path)

    assert len(violations) == 1
    assert parameter in violations[0]


def test_architecture_owner_registry_drives_type_and_parameter_checks(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "def typed(owner: 'MissionService'): ...\n"
        "@public_api\n"
        "def named(mission_service): ...\n",
    )

    violations = checker._public_api_violations(path, _POLICY.public_api, root=tmp_path)

    assert len(violations) == 2
    assert any("typed" in violation for violation in violations)
    assert any("named" in violation for violation in violations)


def test_bridge_allowlist_suppresses_with_deadline(tmp_path, monkeypatch):
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "async def bridged(world_lifecycle=None): ...\n",
    )
    monkeypatch.setitem(
        checker.PUBLIC_API_BRIDGE_PARAMS,
        "src/archetype/probes/sample.py::bridged",
        {"world_lifecycle"},
    )
    assert checker._public_api_violations(path, _POLICY.public_api, root=tmp_path) == []


def test_api_scope_blocks_world_family_behavior_imports(tmp_path):
    path = _write(tmp_path, "from archetype.world.simulation import step\n")
    violations = checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path)
    assert len(violations) == 1
    assert "imports forbidden archetype.world.simulation" in violations[0]


def test_api_scope_rejects_unapproved_application_imports(tmp_path):
    path = _write(tmp_path, "from archetype.app.missions.service import MissionService\n")
    violations = checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path)
    assert len(violations) == 1


def test_composition_surface_governs_process_wiring(tmp_path: Path) -> None:
    path = _write(tmp_path, "from archetype.wiring import build_runtime_resources\n")

    assert checker._import_violations(path, _API_COMPOSITION_SURFACE, root=tmp_path) == []
    assert "archetype.wiring" not in _API_ROUTES_SURFACE.dependency_roots


@pytest.mark.parametrize(
    ("source", "governed_roots"),
    [
        ("from ..commands.scheduler import CommandScheduler\n", ("archetype.commands",)),
        ("from archetype import commands\n", ("archetype.commands",)),
        (
            "from archetype import *\n",
            (
                "archetype.app",
                "archetype.commands",
                "archetype.world",
            ),
        ),
    ],
    ids=["relative", "root-parent", "root-star"],
)
def test_governed_import_roots_cannot_be_bypassed_by_import_spelling(
    tmp_path: Path,
    source: str,
    governed_roots: tuple[str, ...],
) -> None:
    path = _write(tmp_path, source)

    violations = checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path)

    assert len(violations) == len(governed_roots)
    for governed_root in governed_roots:
        assert any(governed_root in violation for violation in violations)


def test_import_policy_data_controls_allowed_dependency(tmp_path):
    path = _write(tmp_path, "import archetype.commands.future_models\n")
    assert len(checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path)) == 1

    updated = replace(
        _API_ROUTES_SURFACE,
        allowed_dependencies=(
            _API_ROUTES_SURFACE.allowed_dependencies | {"archetype.commands.future_models"}
        ),
    )
    assert checker._import_violations(path, updated, root=tmp_path) == []


def test_allowed_dependency_imported_from_parent_package_is_accepted(tmp_path: Path) -> None:
    path = _write(tmp_path, "from archetype.commands import dispatch\n")

    assert checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path) == []


def test_symbol_import_from_allowed_leaf_stays_bound_to_the_leaf(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "from archetype.commands.dispatch import CommandDispatcher\n",
    )

    assert checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path) == []


def test_forbidden_dependency_imported_from_parent_package_is_classified_exactly(
    tmp_path: Path,
) -> None:
    path = _write(tmp_path, "from archetype.commands import scheduler\n")

    violations = checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path)

    assert len(violations) == 1
    assert "imports forbidden archetype.commands.scheduler" in violations[0]


def test_owner_type_policy_data_controls_public_signature(tmp_path):
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "def bad(owner: 'FutureResources'): ...\n",
    )
    assert checker._public_api_violations(path, _POLICY.public_api, root=tmp_path) == []

    updated = replace(
        _POLICY.public_api,
        forbidden_owner_types=(_POLICY.public_api.forbidden_owner_types | {"FutureResources"}),
    )
    assert len(checker._public_api_violations(path, updated, root=tmp_path)) == 1


def test_policy_rejects_dependencies_outside_governed_roots():
    with pytest.raises(
        checker.BoundaryPolicyError,
        match="dependencies fall outside dependency_roots",
    ):
        checker._parse_policy(
            {
                "version": 1,
                "import_surface": [
                    {
                        "name": "api",
                        "targets": ["src/archetype/api/deps.py"],
                        "dependency_roots": ["archetype.commands"],
                        "allowed_dependencies": ["archetype.runtime"],
                        "forbidden_dependencies": [],
                        "rationale": "test boundary",
                    }
                ],
                "public_api": {
                    "targets": ["src/archetype/**/*.py"],
                    "forbidden_owner_types_from": "quality/architecture.toml",
                    "forbidden_parameter_names": ["owner"],
                },
            },
            repo_root=_ROOT,
        )


def test_policy_rejects_unknown_top_level_keys():
    document = _policy_document()
    document["import_surfaces"] = []

    with pytest.raises(checker.BoundaryPolicyError, match="policy contains unknown keys"):
        checker._parse_policy(document, repo_root=_ROOT)


def test_policy_rejects_unknown_import_surface_keys():
    document = _policy_document()
    surfaces = document["import_surface"]
    assert isinstance(surfaces, list)
    surfaces[0]["allowed_dependency"] = "archetype.commands.dispatch"

    with pytest.raises(
        checker.BoundaryPolicyError,
        match=r"import_surface\[0\] contains unknown keys",
    ):
        checker._parse_policy(document, repo_root=_ROOT)


def test_policy_rejects_unknown_public_api_keys():
    document = _policy_document()
    public_api = document["public_api"]
    assert isinstance(public_api, dict)
    public_api["forbidden_owner_types"] = ["WorldLifecycle"]

    with pytest.raises(checker.BoundaryPolicyError, match="public_api contains unknown keys"):
        checker._parse_policy(document, repo_root=_ROOT)


def test_import_scope_ignores_dotted_sibling_packages(tmp_path):
    path = _write(tmp_path, "import archetype.application\n")
    assert checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path) == []


def test_annotation_match_is_whole_token_not_substring(tmp_path):
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "def fine(cfg: 'WorldLifecycleConfig'): ...\n"
        "@public_api\n"
        "def bad(svc: 'WorldLifecycle'): ...\n",
    )
    violations = checker._public_api_violations(path, _POLICY.public_api, root=tmp_path)
    assert len(violations) == 1 and "bad" in violations[0]


def test_public_api_class_constructor_is_checked(tmp_path):
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "class Bad:\n"
        "    def __init__(self, world_lifecycle): ...\n",
    )
    violations = checker._public_api_violations(path, _POLICY.public_api, root=tmp_path)
    assert len(violations) == 1
    assert "Bad.__init__" in violations[0]


def test_workspace_source_module_identity_covers_each_distribution() -> None:
    targets = {
        "packages/archetype-ecs/src/archetype/api/app.py": "archetype.api.app",
        "packages/archetype-missions/src/archetype/missions/api.py": "archetype.missions.api",
        "packages/archetype-physical-ai/src/archetype/physical_ai/runtime.py": (
            "archetype.physical_ai.runtime"
        ),
        "packages/archetype-research/src/archetype/research/runtime.py": (
            "archetype.research.runtime"
        ),
        "packages/archetype-smol/src/archetype/smol/world.py": "archetype.smol.world",
    }

    for relative, expected in targets.items():
        assert checker._source_module(_ROOT / relative, _ROOT) == (expected, False)


def test_missions_router_surface_rejects_workflow_service_import(tmp_path: Path) -> None:
    path = _write(tmp_path, "from archetype.missions.service import MissionService\n")

    violations = checker._import_violations(path, _MISSIONS_API_SURFACE, root=tmp_path)

    assert len(violations) == 1
    assert "imports forbidden archetype.missions.service" in violations[0]
