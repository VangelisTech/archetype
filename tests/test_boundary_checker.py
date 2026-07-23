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
    assert _API_ROUTES_SURFACE.dependency_roots == {"archetype.app"}
    assert "archetype.app.gateway.interfaces" in _API_ROUTES_SURFACE.allowed_dependencies
    assert "archetype.app.world.service" in _API_ROUTES_SURFACE.forbidden_dependencies
    assert "WorldService" in _POLICY.public_api.forbidden_owner_types
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
        "async def bad(world_service, x: int = 0): ...\n",
    )
    violations = checker._public_api_violations(path, _POLICY.public_api, root=tmp_path)
    assert len(violations) == 1
    assert "world_service" in violations[0]
    assert "supported runtime or gateway boundary" in violations[0]


def test_public_api_rule_fires_on_annotation(tmp_path):
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "def bad(svc: 'SimulationService'): ...\n",
    )
    assert len(checker._public_api_violations(path, _POLICY.public_api, root=tmp_path)) == 1


@pytest.mark.parametrize(
    ("signature", "parameter"),
    [
        ("*world_service", "world_service"),
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
        "async def bridged(world_service=None): ...\n",
    )
    monkeypatch.setitem(
        checker.PUBLIC_API_BRIDGE_PARAMS,
        "src/archetype/probes/sample.py::bridged",
        {"world_service"},
    )
    assert checker._public_api_violations(path, _POLICY.public_api, root=tmp_path) == []


def test_api_scope_blocks_service_imports(tmp_path):
    path = _write(tmp_path, "from archetype.app.world.simulation import SimulationService\n")
    violations = checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path)
    assert len(violations) == 1
    assert "never construct or consume concrete app services" in violations[0]


def test_api_scope_rejects_unapproved_application_imports(tmp_path):
    path = _write(tmp_path, "from archetype.app.artifacts.service import ArtifactService\n")
    violations = checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path)
    assert len(violations) == 1


def test_only_api_composition_may_import_the_container(tmp_path: Path) -> None:
    path = _write(tmp_path, "from archetype.app.container import ServiceContainer\n")

    assert checker._import_violations(path, _API_COMPOSITION_SURFACE, root=tmp_path) == []
    violations = checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path)
    assert len(violations) == 1
    assert "imports forbidden archetype.app.container" in violations[0]


@pytest.mark.parametrize(
    "source",
    [
        "from ..app.world.service import WorldService\n",
        "from archetype import app\n",
        "from archetype import *\n",
    ],
    ids=["relative", "root-parent", "root-star"],
)
def test_governed_import_roots_cannot_be_bypassed_by_import_spelling(
    tmp_path: Path,
    source: str,
) -> None:
    path = _write(tmp_path, source)

    violations = checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path)

    assert len(violations) == 1
    assert "archetype.app" in violations[0]


def test_import_policy_data_controls_allowed_dependency(tmp_path):
    path = _write(tmp_path, "import archetype.app.future_models\n")
    assert len(checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path)) == 1

    updated = replace(
        _API_ROUTES_SURFACE,
        allowed_dependencies=(
            _API_ROUTES_SURFACE.allowed_dependencies | {"archetype.app.future_models"}
        ),
    )
    assert checker._import_violations(path, updated, root=tmp_path) == []


def test_allowed_dependency_imported_from_parent_package_is_accepted(tmp_path: Path) -> None:
    path = _write(tmp_path, "from archetype.app.gateway import interfaces\n")

    assert checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path) == []


def test_symbol_import_from_allowed_leaf_stays_bound_to_the_leaf(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "from archetype.app.gateway.interfaces import iCommandGateway\n",
    )

    assert checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path) == []


def test_forbidden_dependency_imported_from_parent_package_is_classified_exactly(
    tmp_path: Path,
) -> None:
    path = _write(tmp_path, "from archetype.app.world import service\n")

    violations = checker._import_violations(path, _API_ROUTES_SURFACE, root=tmp_path)

    assert len(violations) == 1
    assert "imports forbidden archetype.app.world.service" in violations[0]


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
                        "dependency_roots": ["archetype.app"],
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
    surfaces[0]["allowed_dependency"] = "archetype.app.container"

    with pytest.raises(
        checker.BoundaryPolicyError,
        match=r"import_surface\[0\] contains unknown keys",
    ):
        checker._parse_policy(document, repo_root=_ROOT)


def test_policy_rejects_unknown_public_api_keys():
    document = _policy_document()
    public_api = document["public_api"]
    assert isinstance(public_api, dict)
    public_api["forbidden_owner_types"] = ["WorldService"]

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
        "def fine(cfg: 'SimulationServiceConfig'): ...\n"
        "@public_api\n"
        "def bad(svc: 'SimulationService'): ...\n",
    )
    violations = checker._public_api_violations(path, _POLICY.public_api, root=tmp_path)
    assert len(violations) == 1 and "bad" in violations[0]


def test_public_api_class_constructor_is_checked(tmp_path):
    path = _write(
        tmp_path,
        "from archetype._api import public_api\n"
        "@public_api\n"
        "class Bad:\n"
        "    def __init__(self, world_service): ...\n",
    )
    violations = checker._public_api_violations(path, _POLICY.public_api, root=tmp_path)
    assert len(violations) == 1
    assert "Bad.__init__" in violations[0]
