# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

CHECKER_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_architecture.py"
SPEC = importlib.util.spec_from_file_location("check_architecture", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
checker = importlib.util.module_from_spec(SPEC)
sys.modules["check_architecture"] = checker
SPEC.loader.exec_module(checker)

DEFAULT_RESERVED_INFRASTRUCTURE = (
    "archetype._api",
    "archetype._logging",
    "archetype._obs",
    "archetype._storage_uri",
    "archetype.api",
    "archetype.app",
    "archetype.cli",
    "archetype.contrib",
    "archetype.core",
    "archetype.runtime",
)


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


def _write_family_policy(
    root: Path,
    *,
    rules: str,
    exception: str = "",
    reserved_infrastructure: tuple[str, ...] = DEFAULT_RESERVED_INFRASTRUCTURE,
) -> Path:
    (root / "pyproject.toml").write_text(
        '[project]\nname = "fixture"\nversion = "0.4.0"\n',
        encoding="utf-8",
    )
    policy = root / "architecture.toml"
    reserved = "\n".join(f'  "{scope}",' for scope in reserved_infrastructure)
    policy.write_text(
        f"""
version = 3
source_root = "src"

[top_level_family_policy]
forbidden_outward = [
  "archetype.app",
  "archetype.runtime",
  "archetype.api",
  "archetype.cli",
]
reserved_infrastructure = [
{reserved}
]
"""
        + rules
        + exception,
        encoding="utf-8",
    )
    return policy


def _write_root_export_fixture(root: Path) -> None:
    package = root / "src" / "archetype"
    runtime = package / "runtime" / "__init__.py"
    package.mkdir(parents=True, exist_ok=True)
    runtime.parent.mkdir(parents=True, exist_ok=True)
    (package / "__init__.py").write_text(
        '_EXPORTS = {"ArchetypeRuntime": ("archetype.runtime", "ArchetypeRuntime")}\n',
        encoding="utf-8",
    )
    runtime.write_text("class ArchetypeRuntime:\n    pass\n", encoding="utf-8")


def _write_component_family_fixture(root: Path) -> str:
    family = root / "src" / "archetype" / "alpha" / "contracts.py"
    models = root / "src" / "archetype" / "app" / "widgets" / "models.py"
    family.parent.mkdir(parents=True)
    models.parent.mkdir(parents=True)
    family.write_text("value = 1\n", encoding="utf-8")
    models.write_text(
        "from archetype.core.component import Component\n\n"
        "class DurableWidget(Component):\n"
        "    value: int = 0\n",
        encoding="utf-8",
    )
    return """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = []
"""


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


def test_top_level_family_rejects_every_outward_package(tmp_path: Path) -> None:
    contracts = tmp_path / "src" / "archetype" / "alpha" / "contracts.py"
    contracts.parent.mkdir(parents=True)
    contracts.write_text(
        """
from archetype import app
from archetype.api import deps
from archetype.cli import main
from archetype.runtime import ArchetypeRuntime
""",
        encoding="utf-8",
    )
    rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = []
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert not result.policy_errors
    assert {(violation.rule, violation.target) for violation in result.violations} == {
        ("top_level_family_outward_dependency", "archetype.app"),
        ("top_level_family_outward_dependency", "archetype.api"),
        ("top_level_family_outward_dependency", "archetype.cli"),
        ("top_level_family_outward_dependency", "archetype.runtime"),
    }


def test_artifacts_family_scope_rejects_synthetic_reverse_app_dependency(
    tmp_path: Path,
) -> None:
    """#558 acceptance: an artifacts-family module importing app authority fails."""
    bundles = tmp_path / "src" / "archetype" / "artifacts" / "bundles.py"
    bundles.parent.mkdir(parents=True)
    bundles.write_text(
        "from archetype.app.storage.catalog import artifact_publication_key\n",
        encoding="utf-8",
    )
    rules = """

[[top_level_family_rule]]
name = "artifacts-domain-family"
consumer = "archetype.artifacts"
allowed_families = []
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert not result.policy_errors
    assert {(violation.rule, violation.target) for violation in result.violations} == {
        ("top_level_family_outward_dependency", "archetype.app.storage.catalog"),
    }


def test_evaluation_family_scope_rejects_synthetic_reverse_app_dependency(
    tmp_path: Path,
) -> None:
    """#557 acceptance: an evaluation-family module importing app authority fails."""
    contracts = tmp_path / "src" / "archetype" / "evaluation" / "contracts.py"
    contracts.parent.mkdir(parents=True)
    contracts.write_text(
        "from archetype.app.evaluation.service import EvaluationService\n",
        encoding="utf-8",
    )
    rules = """

[[top_level_family_rule]]
name = "evaluation-domain-family"
consumer = "archetype.evaluation"
allowed_families = []
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert not result.policy_errors
    assert {(violation.rule, violation.target) for violation in result.violations} == {
        ("top_level_family_outward_dependency", "archetype.app.evaluation.service"),
    }


def test_root_package_and_facade_imports_match_explicit_forbidden_imports(
    tmp_path: Path,
) -> None:
    import_forms = (
        "from archetype import runtime\n",
        "from archetype import ArchetypeRuntime\n",
        "from archetype.runtime import ArchetypeRuntime\n",
        "import archetype\nvalue = archetype.ArchetypeRuntime\n",
        "import archetype as root\nvalue = root.ArchetypeRuntime\n",
        "import archetype.core\nvalue = archetype.ArchetypeRuntime\n",
        "import archetype.core\nvalue = archetype.runtime.ArchetypeRuntime\n",
    )
    consumers = {
        "core": "package_dependency",
        "app": "package_dependency",
        "alpha": "top_level_family_outward_dependency",
    }

    for consumer_scope, expected_rule in consumers.items():
        for index, statement in enumerate(import_forms):
            root = tmp_path / f"{consumer_scope}-{index}"
            _write_root_export_fixture(root)
            alpha = root / "src" / "archetype" / "alpha" / "contracts.py"
            alpha.parent.mkdir(parents=True, exist_ok=True)
            alpha.write_text("value = 1\n", encoding="utf-8")
            consumer = root / "src" / "archetype" / consumer_scope / "probe.py"
            consumer.parent.mkdir(parents=True, exist_ok=True)
            consumer.write_text(statement, encoding="utf-8")
            package_rule = ""
            if consumer_scope in {"core", "app"}:
                package_rule = f"""

[[package_rule]]
name = "{consumer_scope}-outward"
consumer = "archetype.{consumer_scope}"
forbidden = ["archetype.runtime"]
"""
            rules = (
                """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = []
"""
                + package_rule
            )

            result = checker.audit_repository(
                _write_family_policy(root, rules=rules),
                repo_root=root,
            )

            assert not result.policy_errors
            assert [(violation.rule, violation.target) for violation in result.violations] == [
                (expected_rule, "archetype.runtime")
            ]


def test_unparsable_root_export_map_fails_closed(tmp_path: Path) -> None:
    invalid_exports = (
        "value = 1\n",
        '_EXPORTS = dict(ArchetypeRuntime=("archetype.runtime", "ArchetypeRuntime"))\n',
        "_EXPORTS = []\n",
        '_EXPORTS = {"ArchetypeRuntime": ("archetype.runtime",)}\n',
        '_EXPORTS = {"": ("archetype.runtime", "ArchetypeRuntime")}\n',
    )
    for index, source in enumerate(invalid_exports):
        root = tmp_path / str(index)
        _write_root_export_fixture(root)
        package = root / "src" / "archetype"
        (package / "__init__.py").write_text(source, encoding="utf-8")
        alpha = package / "alpha" / "contracts.py"
        core = package / "core" / "probe.py"
        alpha.parent.mkdir(parents=True)
        core.parent.mkdir(parents=True)
        alpha.write_text("value = 1\n", encoding="utf-8")
        core.write_text("from archetype import ArchetypeRuntime\n", encoding="utf-8")
        rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = []

[[package_rule]]
name = "core-outward"
consumer = "archetype.core"
forbidden = ["archetype.runtime"]
"""

        result = checker.audit_repository(
            _write_family_policy(root, rules=rules),
            repo_root=root,
        )

        assert result.policy_errors == [
            "unable to statically parse archetype._EXPORTS; "
            "root-facade import enforcement is degraded"
        ]


def test_core_rejects_every_registered_family_without_static_enumeration(
    tmp_path: Path,
) -> None:
    core = tmp_path / "src" / "archetype" / "core" / "probe.py"
    alpha = tmp_path / "src" / "archetype" / "alpha" / "contracts.py"
    graph = tmp_path / "src" / "archetype" / "graph" / "contracts.py"
    core.parent.mkdir(parents=True)
    alpha.parent.mkdir(parents=True)
    graph.parent.mkdir(parents=True)
    core.write_text("from archetype.graph import contracts\n", encoding="utf-8")
    alpha.write_text("value = 1\n", encoding="utf-8")
    graph.write_text("value = 1\n", encoding="utf-8")
    rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = []

[[top_level_family_rule]]
name = "graph"
consumer = "archetype.graph"
allowed_families = []

[[package_rule]]
name = "core-outward"
consumer = "archetype.core"
forbidden = [
  "archetype.app",
  "archetype.runtime",
  "archetype.api",
  "archetype.cli",
]
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert not result.policy_errors
    assert [(violation.rule, violation.target) for violation in result.violations] == [
        ("package_dependency", "archetype.graph")
    ]


def test_undeclared_top_level_family_dependency_fails(tmp_path: Path) -> None:
    alpha = tmp_path / "src" / "archetype" / "alpha" / "contracts.py"
    beta = tmp_path / "src" / "archetype" / "beta" / "contracts.py"
    alpha.parent.mkdir(parents=True)
    beta.parent.mkdir(parents=True)
    alpha.write_text("from archetype import beta\n", encoding="utf-8")
    beta.write_text("value = 1\n", encoding="utf-8")
    rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = []

[[top_level_family_rule]]
name = "beta"
consumer = "archetype.beta"
allowed_families = []
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert not result.policy_errors
    assert [
        (violation.rule, violation.consumer, violation.target) for violation in result.violations
    ] == [
        (
            "top_level_family_dependency",
            "archetype.alpha.contracts",
            "archetype.beta",
        )
    ]


def test_unclassified_top_level_package_fails_and_reserved_package_passes(
    tmp_path: Path,
) -> None:
    for name, reserved, expected_error in (
        ("unclassified", DEFAULT_RESERVED_INFRASTRUCTURE, True),
        (
            "classified",
            (*DEFAULT_RESERVED_INFRASTRUCTURE, "archetype.graph"),
            False,
        ),
    ):
        root = tmp_path / name
        alpha = root / "src" / "archetype" / "alpha" / "contracts.py"
        graph = root / "src" / "archetype" / "graph" / "contracts.py"
        alpha.parent.mkdir(parents=True)
        graph.parent.mkdir(parents=True)
        alpha.write_text("value = 1\n", encoding="utf-8")
        graph.write_text("value = 1\n", encoding="utf-8")
        rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = []
"""

        result = checker.audit_repository(
            _write_family_policy(
                root,
                rules=rules,
                reserved_infrastructure=reserved,
            ),
            repo_root=root,
        )

        if expected_error:
            assert result.policy_errors == [
                "unclassified first-party top-level scopes: archetype.graph"
            ]
        else:
            assert result.ok


def test_unclassified_top_level_module_fails_and_reserved_module_passes(
    tmp_path: Path,
) -> None:
    for name, reserved, expected_error in (
        ("unclassified", DEFAULT_RESERVED_INFRASTRUCTURE, True),
        (
            "classified",
            (*DEFAULT_RESERVED_INFRASTRUCTURE, "archetype.helpers"),
            False,
        ),
    ):
        root = tmp_path / name
        alpha = root / "src" / "archetype" / "alpha" / "contracts.py"
        helpers = root / "src" / "archetype" / "helpers.py"
        alpha.parent.mkdir(parents=True)
        alpha.write_text("value = 1\n", encoding="utf-8")
        helpers.write_text("from archetype import app\n", encoding="utf-8")
        rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = []
"""

        result = checker.audit_repository(
            _write_family_policy(
                root,
                rules=rules,
                reserved_infrastructure=reserved,
            ),
            repo_root=root,
        )

        if expected_error:
            assert result.policy_errors == [
                "unclassified first-party top-level scopes: archetype.helpers"
            ]
        else:
            assert result.ok


def test_registered_top_level_module_enforces_family_direction(tmp_path: Path) -> None:
    alpha = tmp_path / "src" / "archetype" / "alpha" / "contracts.py"
    helpers = tmp_path / "src" / "archetype" / "helpers.py"
    alpha.parent.mkdir(parents=True)
    alpha.write_text("value = 1\n", encoding="utf-8")
    helpers.write_text("from archetype import app\n", encoding="utf-8")
    rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = []

[[top_level_family_rule]]
name = "helpers"
consumer = "archetype.helpers"
allowed_families = []
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert not result.policy_errors
    assert [(violation.rule, violation.target) for violation in result.violations] == [
        ("top_level_family_outward_dependency", "archetype.app")
    ]


def test_unclassified_internal_import_fails_and_declared_lower_family_passes(
    tmp_path: Path,
) -> None:
    reserved = tuple(
        scope for scope in DEFAULT_RESERVED_INFRASTRUCTURE if scope != "archetype.contrib"
    )
    for name, declare_contrib, should_pass in (
        ("unclassified", False, False),
        ("declared", True, True),
    ):
        root = tmp_path / name
        alpha = root / "src" / "archetype" / "alpha" / "contracts.py"
        contrib = root / "src" / "archetype" / "contrib" / "contracts.py"
        alpha.parent.mkdir(parents=True)
        contrib.parent.mkdir(parents=True)
        alpha.write_text("from archetype.contrib import contracts\n", encoding="utf-8")
        contrib.write_text("value = 1\n", encoding="utf-8")
        allowed = '["archetype.contrib"]' if declare_contrib else "[]"
        rules = f"""

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = {allowed}
"""
        if declare_contrib:
            rules += """

[[top_level_family_rule]]
name = "contrib"
consumer = "archetype.contrib"
allowed_families = []
"""

        result = checker.audit_repository(
            _write_family_policy(
                root,
                rules=rules,
                reserved_infrastructure=reserved,
            ),
            repo_root=root,
        )

        if should_pass:
            assert result.ok
        else:
            assert result.policy_errors == [
                "unclassified first-party top-level scopes: archetype.contrib"
            ]
            assert [(violation.rule, violation.target) for violation in result.violations] == [
                ("top_level_family_dependency", "archetype.contrib")
            ]


def test_top_level_family_dependency_cycle_fails(tmp_path: Path) -> None:
    for family in ("alpha", "beta"):
        module = tmp_path / "src" / "archetype" / family / "contracts.py"
        module.parent.mkdir(parents=True)
        module.write_text("value = 1\n", encoding="utf-8")
    rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = ["archetype.beta"]

[[top_level_family_rule]]
name = "beta"
consumer = "archetype.beta"
allowed_families = ["archetype.alpha"]
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert result.policy_errors == [
        "top-level family dependency cycle: archetype.alpha -> archetype.beta -> archetype.alpha"
    ]


def test_multi_level_top_level_family_dag_passes(tmp_path: Path) -> None:
    for family in ("alpha", "beta", "gamma"):
        module = tmp_path / "src" / "archetype" / family / "contracts.py"
        module.parent.mkdir(parents=True)
        module.write_text("value = 1\n", encoding="utf-8")
    rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = ["archetype.beta"]

[[top_level_family_rule]]
name = "beta"
consumer = "archetype.beta"
allowed_families = ["archetype.gamma"]

[[top_level_family_rule]]
name = "gamma"
consumer = "archetype.gamma"
allowed_families = []
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert result.ok


def test_declared_family_edge_and_app_contract_import_pass(tmp_path: Path) -> None:
    alpha = tmp_path / "src" / "archetype" / "alpha" / "contracts.py"
    beta = tmp_path / "src" / "archetype" / "beta" / "contracts.py"
    app = tmp_path / "src" / "archetype" / "app" / "consumer.py"
    alpha.parent.mkdir(parents=True)
    beta.parent.mkdir(parents=True)
    app.parent.mkdir(parents=True)
    alpha.write_text("from archetype.beta import contracts\n", encoding="utf-8")
    beta.write_text("value = 1\n", encoding="utf-8")
    app.write_text("from archetype.alpha import contracts\n", encoding="utf-8")
    rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = ["archetype.beta"]

[[top_level_family_rule]]
name = "beta"
consumer = "archetype.beta"
allowed_families = []

[[package_rule]]
name = "app-outward"
consumer = "archetype.app"
forbidden = [
  "archetype.runtime",
  "archetype.api",
  "archetype.cli",
]
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert result.ok


def test_direct_component_anywhere_in_app_fails(tmp_path: Path) -> None:
    family = tmp_path / "src" / "archetype" / "alpha" / "contracts.py"
    models = tmp_path / "src" / "archetype" / "app" / "widgets" / "models.py"
    components = tmp_path / "src" / "archetype" / "app" / "widgets" / "components.py"
    family.parent.mkdir(parents=True)
    models.parent.mkdir(parents=True)
    family.write_text("value = 1\n", encoding="utf-8")
    for path, class_name in (
        (models, "DurableWidgetModel"),
        (components, "DurableWidgetComponent"),
    ):
        path.write_text(
            "from archetype.core.component import Component as ECSComponent\n\n"
            f"class {class_name}(ECSComponent):\n"
            "    value: int = 0\n",
            encoding="utf-8",
        )
    rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = []
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert not result.policy_errors
    assert [(violation.rule, violation.target) for violation in result.violations] == [
        (
            "app_component_model",
            "archetype.app.widgets.components.DurableWidgetComponent",
        ),
        (
            "app_component_model",
            "archetype.app.widgets.models.DurableWidgetModel",
        ),
    ]


def test_version_three_requires_registered_family_scope(tmp_path: Path) -> None:
    module = tmp_path / "src" / "archetype" / "unrelated.py"
    module.parent.mkdir(parents=True)
    module.write_text("value = 1\n", encoding="utf-8")

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=""),
        repo_root=tmp_path,
    )

    assert result.policy_errors == [
        "architecture policy registers no top-level family scopes",
        "unclassified first-party top-level scopes: archetype.unrelated",
    ]


def test_registered_family_scopes_reject_missing_empty_stale_and_duplicate(
    tmp_path: Path,
) -> None:
    alpha = tmp_path / "src" / "archetype" / "alpha" / "contracts.py"
    alpha.parent.mkdir(parents=True)
    alpha.write_text("value = 1\n", encoding="utf-8")
    rules = """

[[top_level_family_rule]]
name = "missing"
allowed_families = []

[[top_level_family_rule]]
name = "empty"
consumer = ""
allowed_families = []

[[top_level_family_rule]]
name = "stale"
consumer = "archetype.stale"
allowed_families = []

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
allowed_families = []

[[top_level_family_rule]]
name = "duplicate"
consumer = "archetype.alpha"
allowed_families = []
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert "top-level family rule 'missing' is missing its consumer scope" in result.policy_errors
    assert "top-level family rule 'empty' has an empty consumer scope" in result.policy_errors
    assert (
        "top-level family rule 'stale' references stale scope: archetype.stale"
        in result.policy_errors
    )
    assert "duplicate top-level family scope: archetype.alpha" in result.policy_errors


def test_registered_family_rejects_empty_source_scope(tmp_path: Path) -> None:
    empty = tmp_path / "src" / "archetype" / "empty" / "__init__.py"
    empty.parent.mkdir(parents=True)
    empty.write_text("", encoding="utf-8")
    rules = """

[[top_level_family_rule]]
name = "empty"
consumer = "archetype.empty"
allowed_families = []
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert result.policy_errors == [
        "top-level family rule 'empty' matched an empty source scope: archetype.empty"
    ]


def test_registered_family_requires_exact_dependency_disposition(tmp_path: Path) -> None:
    alpha = tmp_path / "src" / "archetype" / "alpha" / "contracts.py"
    alpha.parent.mkdir(parents=True)
    alpha.write_text("value = 1\n", encoding="utf-8")
    rules = """

[[top_level_family_rule]]
name = "alpha"
consumer = "archetype.alpha"
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules),
        repo_root=tmp_path,
    )

    assert result.policy_errors == [
        "top-level family rule 'alpha' lacks an exact allowed_families disposition"
    ]


def test_version_three_exception_requires_issue_and_expiry_condition(tmp_path: Path) -> None:
    rules = _write_component_family_fixture(tmp_path)
    exception = """

[[exception]]
rule = "app_component_model"
consumer = "archetype.app.widgets.models"
target = "archetype.app.widgets.models.DurableWidget"
owner = "widgets"
reason = "fixture"
expires = "v1"
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules, exception=exception),
        repo_root=tmp_path,
    )

    assert not result.violations
    assert result.policy_errors == [
        "architecture exception ('app_component_model', "
        "'archetype.app.widgets.models', "
        "'archetype.app.widgets.models.DurableWidget') lacks issue, expiry_condition"
    ]


def test_version_three_exact_issue_owned_exception_passes(tmp_path: Path) -> None:
    rules = _write_component_family_fixture(tmp_path)
    exception = """

[[exception]]
rule = "app_component_model"
consumer = "archetype.app.widgets.models"
target = "archetype.app.widgets.models.DurableWidget"
owner = "widgets"
issue = 999
reason = "fixture"
expiry_condition = "Remove when #999 relocates the fixture component."
expires = "v1"
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules, exception=exception),
        repo_root=tmp_path,
    )

    assert result.ok
    assert [violation.target for violation in result.exempted] == [
        "archetype.app.widgets.models.DurableWidget"
    ]


def test_version_three_rejects_wildcard_exception(tmp_path: Path) -> None:
    rules = _write_component_family_fixture(tmp_path)
    exception = """

[[exception]]
rule = "app_component_model"
consumer = "archetype.app.widgets.models"
target = "archetype.app.widgets.models.*"
owner = "widgets"
issue = 999
reason = "fixture"
expiry_condition = "Remove when #999 relocates the fixture component."
expires = "v1"
"""

    result = checker.audit_repository(
        _write_family_policy(tmp_path, rules=rules, exception=exception),
        repo_root=tmp_path,
    )

    assert [violation.target for violation in result.violations] == [
        "archetype.app.widgets.models.DurableWidget"
    ]
    assert any(
        "uses a wildcard instead of an exact edge" in error for error in result.policy_errors
    )


def test_repository_architecture_policy_passes() -> None:
    completed = subprocess.run(
        [sys.executable, str(CHECKER_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "Architecture audit passed" in completed.stdout


def _write_fragment(policy: Path, name: str, body: str) -> Path:
    fragment_dir = policy.parent / f"{policy.stem}.d"
    fragment_dir.mkdir(exist_ok=True)
    fragment = fragment_dir / f"{name}.toml"
    fragment.write_text(body, encoding="utf-8")
    return fragment


def test_fragment_exception_merges_into_the_policy(tmp_path: Path) -> None:
    probe = tmp_path / "src" / "archetype" / "app" / "probe.py"
    probe.parent.mkdir(parents=True)
    probe.write_text("value = 1\n", encoding="utf-8")
    policy = _write_policy(tmp_path)
    _write_fragment(
        policy,
        "probe",
        """
[[exception]]
rule = "module_dependency"
consumer = "archetype.app.probe"
target = "archetype.app.world.service"
owner = "architecture"
reason = "fixture"
expires = "v1"
""",
    )

    result = checker.audit_repository(policy, repo_root=tmp_path)

    assert result.policy_errors == [
        "stale architecture exception matched no violation: "
        "module_dependency | archetype.app.probe | archetype.app.world.service"
    ]


def test_fragment_rejects_scalar_policy_sections(tmp_path: Path) -> None:
    policy = _write_policy(tmp_path)
    _write_fragment(policy, "rogue", "version = 9\n")

    with pytest.raises(ValueError, match="fragments may declare only"):
        checker.audit_repository(policy, repo_root=tmp_path)


def test_fragment_rejects_non_list_rule_sections(tmp_path: Path) -> None:
    policy = _write_policy(tmp_path)
    _write_fragment(policy, "rogue", 'exception = "not-a-list"\n')

    with pytest.raises(ValueError, match="must be an array of tables"):
        checker.audit_repository(policy, repo_root=tmp_path)


def test_duplicate_rule_names_across_fragments_fail(tmp_path: Path) -> None:
    policy = _write_policy(tmp_path)
    _write_fragment(
        policy,
        "dupe",
        """
[[package_rule]]
name = "app-outward"
consumer = "archetype.app"
forbidden = []
""",
    )

    with pytest.raises(ValueError, match="duplicate package_rule name"):
        checker.audit_repository(policy, repo_root=tmp_path)
