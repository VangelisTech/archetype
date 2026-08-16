# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest

CHECKER_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_observability.py"
SPEC = importlib.util.spec_from_file_location("check_observability", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
checker = importlib.util.module_from_spec(SPEC)
sys.modules["check_observability"] = checker
SPEC.loader.exec_module(checker)


OBS_VOCABULARY = """
from types import MappingProxyType
from typing import Final, Mapping

SPAN_NAMES: Final[frozenset[str]] = frozenset({"probe.run", "probe.legacy"})
LEGACY_SPAN_NAMES: Final[frozenset[str]] = frozenset({"probe.legacy"})
SPAN_NAME_ALIASES: Final[Mapping[str, str]] = MappingProxyType({})
TRACE_ATTRIBUTE_KEYS: Final[frozenset[str]] = frozenset(
    {
        "archetype.failure.disposition",
        "archetype.operation",
        "archetype.outcome",
        "archetype.world.id",
        "error.type",
    }
)
TRACE_ATTRIBUTE_ALIASES: Final[Mapping[str, str]] = MappingProxyType(
    {"operation": "archetype.operation"}
)
METRIC_NAMES: Final[frozenset[str]] = frozenset(
    {"archetype.operation.failures", "archetype.operation.outcomes"}
)
METRIC_LABEL_KEYS: Final[frozenset[str]] = frozenset(
    {
        "archetype.failure.disposition",
        "archetype.operation",
        "archetype.outcome",
        "error.type",
    }
)
RECORDER_METRIC_NAMES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "record_failure": "archetype.operation.failures",
        "record_outcome": "archetype.operation.outcomes",
    }
)
RECORDER_METRIC_LABEL_KEYS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "record_failure.disposition": "archetype.failure.disposition",
        "record_failure.error_type": "error.type",
        "record_outcome.operation": "archetype.operation",
        "record_outcome.outcome": "archetype.outcome",
    }
)
EVENT_NAMES: Final[frozenset[str]] = frozenset({"archetype.outcome"})
FAILURE_DISPOSITIONS: Final[frozenset[str]] = frozenset({"handled", "retrying"})
OUTCOMES: Final[frozenset[str]] = frozenset({"rejected", "succeeded"})
ERROR_TYPES: Final[frozenset[str]] = frozenset({"internal", "validation"})

def span(name: str, attributes: object = None, **values: object) -> object: ...
def instrument(name: str) -> object: ...
def counter_add(name: str, amount: int = 1, *, attributes: object = None) -> None: ...
def record_failure(error: BaseException, *, disposition: str) -> None: ...
def record_outcome(outcome: str, *, operation: str | None = None) -> None: ...
def configure_tracing(*, service_name: str, debug_console: bool = False) -> None: ...
"""

INTERFACES = """
from typing import Protocol

class iProbe(Protocol):
    @property
    def enabled(self) -> bool: ...

    async def run(self) -> None: ...
"""

SERVICE = """
from typing import Protocol
from archetype import _obs

class LocalPort(Protocol):
    def flush(self) -> None: ...

class Probe:
    async def run(self) -> None:
        with _obs.span("probe.run", attributes={"archetype.outcome": "succeeded"}):
            return None
"""

CONCRETE_SERVICE = """
class ProbeService:
    @property
    def active(self) -> bool:
        return True

    async def execute(self) -> None:
        return None

    def close(self) -> None:
        return None

    def _internal(self) -> None:
        return None
"""

MODULE_OPERATIONS = """
async def dispatch() -> None:
    return None

def _helper() -> None:
    return None
"""

MANIFEST = """
version = 1
owner = "probe"
family = "probe"

[[disposition]]
operations = [
  "interfaces.iProbe.enabled",
  "interfaces.iProbe.run",
  "service.LocalPort.flush",
]
signals = ["child"]
emission_workflows = ["probe.run"]
outcomes = ["propagated_failure", "handled_outcome"]
authority = "The fixture receipt and typed result remain authoritative."
evidence = ["docs/evidence.md"]
span_names = ["probe.run"]
attribute_keys = ["archetype.outcome"]

[[workflow]]
id = "probe.run"
qualified_scope = "archetype.app.probe.service.Probe.run"
signals = [
  "child",
]
outcomes = [
  "propagated_failure",
  "handled_outcome",
]
authority = "The fixture workflow receipt and typed result remain authoritative."
evidence = [
  "docs/evidence.md",
]
span_names = [
  "probe.run",
]
attribute_keys = [
  "archetype.outcome",
]
"""

HOSTS = """
version = 1
owner = "hosts"
"""

ARCHITECTURE_POLICY = """
version = 3
source_root = "src"
"""


def _write_fixture(
    root: Path,
    *,
    interfaces: str = INTERFACES,
    service: str = SERVICE,
    manifest: str = MANIFEST,
    hosts: str | None = None,
    obs: str = OBS_VOCABULARY,
) -> Path:
    source = root / "src" / "archetype"
    family = source / "app" / "probe"
    manifests = root / "quality" / "observability"
    family.mkdir(parents=True)
    manifests.mkdir(parents=True)
    (root / "docs").mkdir()
    (root / "pyproject.toml").write_text(
        '[project]\nname = "fixture"\nversion = "0.4.0"\n',
        encoding="utf-8",
    )
    (root / "quality" / "architecture.toml").write_text(
        dedent(ARCHITECTURE_POLICY).lstrip(),
        encoding="utf-8",
    )
    (root / "docs" / "evidence.md").write_text("# Fixture evidence\n", encoding="utf-8")
    (source / "_obs.py").write_text(dedent(obs).lstrip(), encoding="utf-8")
    (family / "interfaces.py").write_text(dedent(interfaces).lstrip(), encoding="utf-8")
    (family / "service.py").write_text(dedent(service).lstrip(), encoding="utf-8")
    (manifests / "probe.toml").write_text(dedent(manifest).lstrip(), encoding="utf-8")
    if hosts is not None:
        (manifests / "hosts.toml").write_text(dedent(hosts).lstrip(), encoding="utf-8")
    return manifests


def _write_source_module(root: Path, relative_path: str, source: str) -> None:
    path = root / "src" / "archetype" / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(source).lstrip(), encoding="utf-8")


def _register_top_level_family(root: Path, family: str) -> None:
    fragment_root = root / "quality" / "architecture.d"
    fragment_root.mkdir(parents=True, exist_ok=True)
    (fragment_root / f"{family}.toml").write_text(
        dedent(
            f"""
            [[top_level_family_rule]]
            name = "{family}-domain-family"
            consumer = "archetype.{family}"
            allowed_families = []
            """
        ).lstrip(),
        encoding="utf-8",
    )


def _review_concrete_service(root: Path, service_name: str) -> None:
    policy = root / "quality" / "architecture.toml"
    policy.write_text(
        policy.read_text(encoding="utf-8")
        + dedent(
            f"""

            [concrete_services]
            types = ["{service_name}"]
            """
        ),
        encoding="utf-8",
    )


def _audit(root: Path):
    return checker.audit_repository(
        manifest_root=root / "quality" / "observability",
        repo_root=root,
    )


def _assert_rejected(result) -> None:
    assert not result.ok
    assert result.violations or result.policy_errors


def _replace_once(value: str, old: str, new: str) -> str:
    assert value.count(old) == 1
    return value.replace(old, new)


def _with_concrete_service_surface(
    *,
    surface: str = "archetype.probe.service.ProbeService",
    replaces: tuple[str, ...] = (),
    operations: tuple[str, ...] = (
        "service.ProbeService.active",
        "service.ProbeService.execute",
        "service.ProbeService.close",
    ),
) -> str:
    replacement_rows = (
        "replaces = [\n" + "".join(f'  "{protocol}",\n' for protocol in replaces) + "]\n"
        if replaces
        else ""
    )
    manifest = _replace_once(
        MANIFEST,
        'family = "probe"\n',
        (
            'family = "probe"\n\n'
            "[[concrete_operation_surface]]\n"
            f'qualified_scope = "{surface}"\n'
            f"{replacement_rows}"
        ),
    )
    encoded = "".join(f'  "{operation}",\n' for operation in operations)
    return _replace_once(manifest, "operations = [\n", "operations = [\n" + encoded)


def _with_module_operation_surface(
    *,
    surface: str = "archetype.probe.operations",
    operations: tuple[str, ...] = ("operations.dispatch",),
) -> str:
    manifest = _replace_once(
        MANIFEST,
        'family = "probe"\n',
        (f'family = "probe"\n\n[[module_operation_surface]]\nqualified_scope = "{surface}"\n'),
    )
    encoded = "".join(f'  "{operation}",\n' for operation in operations)
    return _replace_once(manifest, "operations = [\n", "operations = [\n" + encoded)


def _write_concrete_service_fixture(
    root: Path,
    manifest: str,
    *,
    reviewed_name: str | None = "ProbeService",
    interfaces: str = INTERFACES,
) -> None:
    _write_fixture(root, manifest=manifest, interfaces=interfaces)
    _register_top_level_family(root, "probe")
    if reviewed_name is not None:
        _review_concrete_service(root, reviewed_name)
    _write_source_module(root, "probe/service.py", CONCRETE_SERVICE)


def _write_module_operation_fixture(
    root: Path,
    manifest: str,
    *,
    source: str = MODULE_OPERATIONS,
) -> None:
    _write_fixture(root, manifest=manifest)
    _register_top_level_family(root, "probe")
    _write_source_module(root, "probe/operations.py", source)


def _with_metric_claims(manifest: str, metric_name: str, labels: tuple[str, ...]) -> str:
    inline_labels = ", ".join(f'"{label}"' for label in labels)
    block_labels = "".join(f'  "{label}",\n' for label in labels)
    result = _replace_once(manifest, 'signals = ["child"]', 'signals = ["child", "metric"]')
    result = _replace_once(
        result,
        'signals = [\n  "child",\n]\n',
        'signals = [\n  "child",\n  "metric",\n]\n',
    )
    result = _replace_once(
        result,
        'attribute_keys = ["archetype.outcome"]\n',
        'attribute_keys = ["archetype.outcome"]\n'
        f'metric_names = ["{metric_name}"]\n'
        f"metric_label_keys = [{inline_labels}]\n",
    )
    return _replace_once(
        result,
        'attribute_keys = [\n  "archetype.outcome",\n]\n',
        'attribute_keys = [\n  "archetype.outcome",\n]\n'
        f'metric_names = [\n  "{metric_name}",\n]\n'
        f"metric_label_keys = [\n{block_labels}]\n",
    )


def _with_host(
    qualified_scope: str,
    capability: str,
    *,
    rationale: str = "The fixture process host explicitly owns this configuration.",
) -> str:
    return (
        HOSTS
        + f'''

[[host_callable]]
qualified_scope = "{qualified_scope}"
capabilities = ["{capability}"]
evidence = ["docs/evidence.md"]
rationale = "{rationale}"
'''
    )


def test_valid_fixture_covers_properties_and_protocols_outside_interfaces(tmp_path: Path) -> None:
    _write_fixture(tmp_path)

    result = _audit(tmp_path)

    assert result.ok, result.violations + result.policy_errors
    assert not result.violations
    assert not result.policy_errors
    assert result.protocols_scanned == 2
    assert result.operations_scanned == 3


def test_registered_top_level_protocol_requires_an_exact_disposition(
    tmp_path: Path,
) -> None:
    _write_fixture(tmp_path)
    _register_top_level_family(tmp_path, "probe")
    _write_source_module(
        tmp_path,
        "probe/contracts.py",
        """
        from typing import Protocol

        class DomainPort(Protocol):
            async def dispatch(self) -> None: ...
        """,
    )

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert result.protocols_scanned == 3
    assert result.operations_scanned == 4
    assert any(
        "missing disposition for probe:contracts.DomainPort.dispatch" in error
        for error in result.policy_errors
    )


def test_registered_generic_top_level_protocol_requires_an_exact_disposition(
    tmp_path: Path,
) -> None:
    _write_fixture(tmp_path)
    _register_top_level_family(tmp_path, "probe")
    _write_source_module(
        tmp_path,
        "probe/contracts.py",
        """
        from typing import Protocol, TypeVar

        T = TypeVar("T")

        class GenericPort(Protocol[T]):
            async def dispatch(self, value: T) -> T: ...
        """,
    )

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert result.protocols_scanned == 3
    assert result.operations_scanned == 4
    assert any(
        "missing disposition for probe:contracts.GenericPort.dispatch" in error
        for error in result.policy_errors
    )


def test_registered_top_level_protocols_join_temporary_app_compatibility(
    tmp_path: Path,
) -> None:
    manifest = _replace_once(
        MANIFEST,
        '  "interfaces.iProbe.enabled",\n',
        '  "contracts.DomainPort.dispatch",\n  "interfaces.iProbe.enabled",\n',
    )
    _write_fixture(tmp_path, manifest=manifest)
    _register_top_level_family(tmp_path, "probe")
    _write_source_module(
        tmp_path,
        "probe/contracts.py",
        """
        from typing import Protocol

        class DomainPort(Protocol):
            async def dispatch(self) -> None: ...
        """,
    )

    result = _audit(tmp_path)

    assert result.ok, result.violations + result.policy_errors
    assert result.protocols_scanned == 3
    assert result.operations_scanned == 4


def test_app_and_top_level_protocol_definitions_cannot_collapse_to_one_operation(
    tmp_path: Path,
) -> None:
    _write_fixture(tmp_path)
    _register_top_level_family(tmp_path, "probe")
    _write_source_module(
        tmp_path,
        "probe/interfaces.py",
        """
        from typing import Protocol

        class iProbe(Protocol):
            async def run(self) -> None: ...
        """,
    )

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert result.protocols_scanned == 3
    assert result.operations_scanned == 3
    assert any(
        "duplicate discovered protocol operation probe:interfaces.iProbe.run" in error
        and "archetype.app.probe.interfaces.iProbe.run" in error
        and "archetype.probe.interfaces.iProbe.run" in error
        for error in result.policy_errors
    )


def test_unregistered_top_level_protocol_is_not_a_family_operation(
    tmp_path: Path,
) -> None:
    _write_fixture(tmp_path)
    _write_source_module(
        tmp_path,
        "unregistered/contracts.py",
        """
        from typing import Protocol

        class InternalPort(Protocol):
            async def dispatch(self) -> None: ...
        """,
    )

    result = _audit(tmp_path)

    assert result.ok, result.violations + result.policy_errors
    assert result.protocols_scanned == 2
    assert result.operations_scanned == 3


def test_registered_module_functions_can_own_an_exact_operation_surface(
    tmp_path: Path,
) -> None:
    _write_module_operation_fixture(tmp_path, _with_module_operation_surface())

    result = _audit(tmp_path)

    assert result.ok, result.violations + result.policy_errors
    assert result.protocols_scanned == 2
    assert result.operations_scanned == 4


@pytest.mark.parametrize(
    ("manifest", "source", "expected_error"),
    [
        (
            _with_module_operation_surface(operations=()),
            MODULE_OPERATIONS,
            "missing disposition for probe:operations.dispatch",
        ),
        (
            _with_module_operation_surface(
                operations=("operations.dispatch", "operations.missing")
            ),
            MODULE_OPERATIONS,
            "phantom disposition for probe:operations.missing",
        ),
        (
            _with_module_operation_surface(surface="archetype.probe.missing"),
            MODULE_OPERATIONS,
            "references unknown module operation surface",
        ),
        (
            _with_module_operation_surface(),
            "def _helper() -> None:\n    return None\n",
            "surface has no public operations",
        ),
    ],
    ids=["missing", "phantom", "unknown-module", "empty-public-surface"],
)
def test_module_operation_surfaces_fail_closed(
    tmp_path: Path,
    manifest: str,
    source: str,
    expected_error: str,
) -> None:
    _write_module_operation_fixture(tmp_path, manifest, source=source)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any(expected_error in error for error in result.policy_errors)


def test_reviewed_concrete_service_can_own_an_exact_operation_surface(
    tmp_path: Path,
) -> None:
    _write_concrete_service_fixture(tmp_path, _with_concrete_service_surface())

    result = _audit(tmp_path)

    assert result.ok, result.violations + result.policy_errors
    assert result.protocols_scanned == 2
    assert result.operations_scanned == 6


def test_reviewed_concrete_service_can_explicitly_replace_a_matching_protocol(
    tmp_path: Path,
) -> None:
    matching_protocol = """
    from typing import Protocol

    class iProbe(Protocol):
        @property
        def active(self) -> bool: ...

        async def execute(self) -> None: ...

        def close(self) -> None: ...
    """
    manifest = _with_concrete_service_surface(replaces=("interfaces.iProbe",))
    manifest = manifest.replace('  "interfaces.iProbe.enabled",\n', "")
    manifest = manifest.replace('  "interfaces.iProbe.run",\n', "")
    _write_concrete_service_fixture(
        tmp_path,
        manifest,
        interfaces=matching_protocol,
    )

    result = _audit(tmp_path)

    assert result.ok, result.violations + result.policy_errors
    assert result.protocols_scanned == 2
    assert result.operations_scanned == 4


def test_concrete_service_surface_rejects_a_protocol(tmp_path: Path) -> None:
    protocol_service = """
    from typing import Protocol

    class ProbeService(Protocol):
        def execute(self) -> None: ...
    """
    _write_fixture(tmp_path, manifest=_with_concrete_service_surface(operations=()))
    _register_top_level_family(tmp_path, "probe")
    _review_concrete_service(tmp_path, "ProbeService")
    _write_source_module(tmp_path, "probe/service.py", protocol_service)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any(
        "must select a concrete service, not a Protocol" in error for error in result.policy_errors
    )


@pytest.mark.parametrize(
    ("replacement", "expected_error"),
    [
        ("interfaces.MissingPort", "references unknown Protocol surface"),
        ("interfaces.iProbe", "does not exactly match concrete service operations"),
    ],
    ids=["unknown-protocol", "operation-mismatch"],
)
def test_concrete_service_protocol_replacement_is_fail_closed(
    tmp_path: Path,
    replacement: str,
    expected_error: str,
) -> None:
    _write_concrete_service_fixture(
        tmp_path,
        _with_concrete_service_surface(replaces=(replacement,)),
    )

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any(expected_error in error for error in result.policy_errors)


@pytest.mark.parametrize(
    ("manifest", "reviewed_name", "expected_error"),
    [
        (
            _with_concrete_service_surface(
                surface="archetype.probe.service.MissingService",
                operations=("service.MissingService.execute",),
            ),
            "MissingService",
            "unknown concrete operation surface",
        ),
        (
            _with_concrete_service_surface(),
            None,
            "is not a reviewed concrete service",
        ),
        (
            _with_concrete_service_surface(
                operations=(
                    "service.ProbeService.active",
                    "service.ProbeService.execute",
                    "service.ProbeService.close",
                    "service.ProbeService.missing",
                )
            ),
            "ProbeService",
            "phantom disposition for probe:service.ProbeService.missing",
        ),
        (
            _with_concrete_service_surface(
                operations=(
                    "service.ProbeService.active",
                    "service.ProbeService.execute",
                    "service.ProbeService.close",
                    "service.ProbeService.close",
                )
            ),
            "ProbeService",
            "must not contain duplicates",
        ),
        (
            _with_concrete_service_surface(
                operations=(
                    "service.ProbeService.active",
                    "service.ProbeService.execute",
                )
            ),
            "ProbeService",
            "missing disposition for probe:service.ProbeService.close",
        ),
    ],
    ids=["unknown", "non-service", "phantom", "duplicate", "missing"],
)
def test_concrete_service_operation_surfaces_fail_closed(
    tmp_path: Path,
    manifest: str,
    reviewed_name: str | None,
    expected_error: str,
) -> None:
    _write_concrete_service_fixture(
        tmp_path,
        manifest,
        reviewed_name=reviewed_name,
    )

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any(expected_error in error for error in result.policy_errors)


@pytest.mark.parametrize(
    "manifest",
    [
        _replace_once(MANIFEST, '  "interfaces.iProbe.enabled",\n', ""),
        _replace_once(
            MANIFEST,
            '  "interfaces.iProbe.enabled",\n',
            '  "interfaces.iProbe.enabled",\n  "interfaces.iProbe.enabled",\n',
        ),
        _replace_once(
            MANIFEST,
            '  "interfaces.iProbe.enabled",\n',
            '  "interfaces.iProbe.missing",\n',
        ),
        _replace_once(MANIFEST, '  "service.LocalPort.flush",\n', ""),
    ],
    ids=["missing", "duplicate", "phantom", "non-interfaces-protocol"],
)
def test_protocol_manifest_is_an_exact_bijection(tmp_path: Path, manifest: str) -> None:
    _write_fixture(tmp_path, manifest=manifest)

    _assert_rejected(_audit(tmp_path))


@pytest.mark.parametrize(
    "manifest",
    [
        _replace_once(MANIFEST, 'signals = ["child"]', 'signals = ["root", "child"]'),
        _replace_once(MANIFEST, 'signals = ["child"]', 'signals = ["none", "metric"]'),
        _replace_once(MANIFEST, 'signals = ["child"]', 'signals = ["unknown"]'),
        _replace_once(
            MANIFEST,
            'outcomes = ["propagated_failure", "handled_outcome"]',
            'outcomes = ["mystery"]',
        ),
        _replace_once(
            MANIFEST,
            'authority = "The fixture receipt and typed result remain authoritative."',
            'authority = ""',
        ),
        _replace_once(MANIFEST, 'evidence = ["docs/evidence.md"]', "evidence = []"),
        _replace_once(
            MANIFEST, 'evidence = ["docs/evidence.md"]', 'evidence = ["docs/missing.md"]'
        ),
        _replace_once(MANIFEST, 'span_names = ["probe.run"]', "span_names = []"),
        _replace_once(MANIFEST, 'span_names = ["probe.run"]', 'span_names = ["probe.unknown"]'),
        _replace_once(
            MANIFEST, 'attribute_keys = ["archetype.outcome"]', 'attribute_keys = ["password"]'
        ),
    ],
    ids=[
        "root-child-exclusive",
        "none-exclusive",
        "signal-vocabulary",
        "outcome-vocabulary",
        "authority-required",
        "evidence-required",
        "evidence-must-exist",
        "span-name-required",
        "span-name-vocabulary",
        "attribute-key-vocabulary",
    ],
)
def test_disposition_schema_rejects_invalid_rows(tmp_path: Path, manifest: str) -> None:
    _write_fixture(tmp_path, manifest=manifest)

    _assert_rejected(_audit(tmp_path))


def test_root_disposition_is_reserved_for_runtime_or_gateway_ingress(tmp_path: Path) -> None:
    lower_family_root = _replace_once(
        MANIFEST,
        'signals = ["child"]',
        'signals = ["root"]',
    )
    _write_fixture(tmp_path, manifest=lower_family_root)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any(
        "root is reserved for runtime or gateway ingress" in error for error in result.policy_errors
    )


def test_gateway_workflow_may_declare_an_ingress_root(tmp_path: Path) -> None:
    _write_fixture(tmp_path)
    _write_source_module(
        tmp_path,
        "gateway.py",
        """
from archetype import _obs

def ingress() -> None:
    with _obs.span("probe.run"):
        pass
""",
    )
    gateway = """
version = 1
owner = "gateway"

[[workflow]]
id = "gateway.ingress"
qualified_scope = "archetype.gateway.ingress"
signals = ["root"]
outcomes = ["propagated_failure"]
authority = "The typed gateway result remains authoritative."
evidence = ["docs/evidence.md"]
span_names = ["probe.run"]
"""
    (tmp_path / "quality" / "observability" / "gateway.toml").write_text(
        dedent(gateway).lstrip(),
        encoding="utf-8",
    )

    assert _audit(tmp_path).ok


def test_none_disposition_requires_rationale_and_accepts_one(tmp_path: Path) -> None:
    without_rationale = (
        _replace_once(MANIFEST, 'signals = ["child"]', 'signals = ["none"]')
        .replace('emission_workflows = ["probe.run"]\n', "")
        .replace('span_names = ["probe.run"]\n', "")
        .replace('attribute_keys = ["archetype.outcome"]\n', "")
    )
    _write_fixture(tmp_path, manifest=without_rationale)
    _assert_rejected(_audit(tmp_path))

    with_rationale = _replace_once(
        without_rationale,
        'evidence = ["docs/evidence.md"]\n',
        'evidence = ["docs/evidence.md"]\n'
        'rationale = "No direct signal is approved; the typed result remains authoritative."\n',
    )
    (tmp_path / "quality" / "observability" / "probe.toml").write_text(
        with_rationale,
        encoding="utf-8",
    )
    assert _audit(tmp_path).ok


def test_metric_disposition_requires_fixed_names_and_bounded_labels(tmp_path: Path) -> None:
    valid = _replace_once(MANIFEST, 'signals = ["child"]', 'signals = ["child", "metric"]')
    valid = _replace_once(
        valid,
        'attribute_keys = ["archetype.outcome"]\n',
        'attribute_keys = ["archetype.outcome"]\n'
        'metric_names = ["archetype.operation.outcomes"]\n'
        'metric_label_keys = ["archetype.outcome"]\n',
    )
    valid = _replace_once(
        valid,
        'signals = [\n  "child",\n]\n',
        'signals = [\n  "child",\n  "metric",\n]\n',
    )
    valid = _replace_once(
        valid,
        'attribute_keys = [\n  "archetype.outcome",\n]\n',
        'attribute_keys = [\n  "archetype.outcome",\n]\n'
        'metric_names = [\n  "archetype.operation.outcomes",\n]\n'
        'metric_label_keys = [\n  "archetype.outcome",\n]\n',
    )
    metric_service = _replace_once(
        SERVICE,
        '        with _obs.span("probe.run", attributes={"archetype.outcome": "succeeded"}):\n'
        "            return None\n",
        '        with _obs.span("probe.run", attributes={"archetype.outcome": "succeeded"}):\n'
        "            _obs.counter_add(\n"
        '                "archetype.operation.outcomes",\n'
        '                attributes={"archetype.outcome": "succeeded"},\n'
        "            )\n"
        "            return None\n",
    )
    _write_fixture(tmp_path, manifest=valid, service=metric_service)
    assert _audit(tmp_path).ok

    invalid_name = _replace_once(
        valid,
        'metric_names = ["archetype.operation.outcomes"]',
        'metric_names = ["probe.dynamic"]',
    )
    (tmp_path / "quality" / "observability" / "probe.toml").write_text(
        invalid_name,
        encoding="utf-8",
    )
    _assert_rejected(_audit(tmp_path))

    invalid_label = _replace_once(
        valid,
        'metric_label_keys = ["archetype.outcome"]',
        'metric_label_keys = ["archetype.world.id"]',
    )
    (tmp_path / "quality" / "observability" / "probe.toml").write_text(
        invalid_label,
        encoding="utf-8",
    )
    _assert_rejected(_audit(tmp_path))

    wrong_bounded_label = _replace_once(
        valid,
        'metric_label_keys = ["archetype.outcome"]',
        'metric_label_keys = ["error.type"]',
    )
    (tmp_path / "quality" / "observability" / "probe.toml").write_text(
        wrong_bounded_label,
        encoding="utf-8",
    )
    result = _audit(tmp_path)
    _assert_rejected(result)
    assert any(
        "disposition[0].metric_label_keys does not match source emissions" in error
        for error in result.policy_errors
    )


def test_workflow_scope_must_exist_and_be_callable(tmp_path: Path) -> None:
    _write_fixture(tmp_path)
    assert _audit(tmp_path).ok

    stale = _replace_once(
        MANIFEST,
        "archetype.app.probe.service.Probe.run",
        "archetype.app.probe.service.Probe.missing",
    )
    (tmp_path / "quality" / "observability" / "probe.toml").write_text(
        stale,
        encoding="utf-8",
    )
    _assert_rejected(_audit(tmp_path))


@pytest.mark.parametrize(
    ("old", "new", "field_name"),
    [
        (
            'span_names = [\n  "probe.run",\n]',
            'span_names = [\n  "probe.legacy",\n]',
            "span_names",
        ),
        (
            'attribute_keys = [\n  "archetype.outcome",\n]',
            'attribute_keys = [\n  "archetype.world.id",\n]',
            "attribute_keys",
        ),
    ],
    ids=["globally-valid-span", "globally-safe-attribute"],
)
def test_workflow_signal_claims_match_their_exact_source_scope(
    tmp_path: Path,
    old: str,
    new: str,
    field_name: str,
) -> None:
    manifest = _replace_once(MANIFEST, old, new)
    _write_fixture(tmp_path, manifest=manifest)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any(
        f"workflow[0].{field_name} does not match source emissions" in error
        for error in result.policy_errors
    )


def test_disposition_signal_claims_are_backed_by_referenced_workflows(tmp_path: Path) -> None:
    manifest = _replace_once(
        MANIFEST,
        'span_names = ["probe.run"]',
        'span_names = ["probe.legacy"]',
    )
    _write_fixture(tmp_path, manifest=manifest)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any(
        "disposition[0].span_names does not match source emissions" in error
        for error in result.policy_errors
    )


def test_declared_but_absent_workflow_signal_is_rejected(tmp_path: Path) -> None:
    service = _replace_once(
        SERVICE,
        '        with _obs.span("probe.run", attributes={"archetype.outcome": "succeeded"}):\n'
        "            return None\n",
        "        return None\n",
    )
    _write_fixture(tmp_path, service=service)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any("declared-only=['probe.run']" in error for error in result.policy_errors)


def test_emitted_but_undeclared_workflow_field_is_rejected(tmp_path: Path) -> None:
    manifest = _replace_once(
        MANIFEST,
        'attribute_keys = [\n  "archetype.outcome",\n]\n',
        "attribute_keys = []\n",
    )
    _write_fixture(tmp_path, manifest=manifest)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any("observed-only=['archetype.outcome']" in error for error in result.policy_errors)


def test_instrument_decorator_is_attributed_to_the_decorated_callable(tmp_path: Path) -> None:
    service = """
from typing import Protocol
from archetype import _obs

class LocalPort(Protocol):
    def flush(self) -> None: ...

class Probe:
    @_obs.instrument("probe.run")
    async def run(self) -> None:
        return None
"""
    manifest = _replace_once(MANIFEST, 'attribute_keys = ["archetype.outcome"]\n', "")
    manifest = _replace_once(
        manifest,
        'attribute_keys = [\n  "archetype.outcome",\n]\n',
        "",
    )
    _write_fixture(tmp_path, service=service, manifest=manifest)

    assert _audit(tmp_path).ok


@pytest.mark.parametrize(
    "inactive_call",
    [
        '_obs.span("probe.run", attributes={"archetype.outcome": "succeeded"})',
        '_obs.instrument("probe.run")',
    ],
    ids=["span-context-factory", "instrument-decorator-factory"],
)
def test_inactive_emitter_factories_do_not_satisfy_workflow_claims(
    tmp_path: Path,
    inactive_call: str,
) -> None:
    service = _replace_once(
        SERVICE,
        '        with _obs.span("probe.run", attributes={"archetype.outcome": "succeeded"}):\n'
        "            return None\n",
        f"        {inactive_call}\n        return None\n",
    )
    _write_fixture(tmp_path, service=service)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any("declared-only=['probe.run']" in error for error in result.policy_errors)


def test_workflow_attribution_does_not_follow_called_helpers(tmp_path: Path) -> None:
    service = """
from typing import Protocol
from archetype import _obs

class LocalPort(Protocol):
    def flush(self) -> None: ...

class Probe:
    async def run(self) -> None:
        self._emit()

    def _emit(self) -> None:
        with _obs.span("probe.run", attributes={"archetype.outcome": "succeeded"}):
            pass
"""
    _write_fixture(tmp_path, service=service)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any("declared-only=['probe.run']" in error for error in result.policy_errors)


def test_workflow_attribution_does_not_absorb_nested_lambda_emissions(tmp_path: Path) -> None:
    service = _replace_once(
        SERVICE,
        "    async def run(self) -> None:\n",
        "    async def run(self) -> None:\n"
        "        callback = lambda: _obs.counter_add(\n"
        '            "archetype.operation.outcomes",\n'
        '            attributes={"archetype.outcome": "succeeded"},\n'
        "        )\n"
        "        callback()\n",
    )
    _write_fixture(tmp_path, service=service)

    assert _audit(tmp_path).ok


def test_nested_lambda_emissions_remain_safety_checked(tmp_path: Path) -> None:
    service = _replace_once(
        SERVICE,
        "    async def run(self) -> None:\n",
        "    async def run(self) -> None:\n"
        '        callback = lambda suffix: _obs.counter_add(f"probe.{suffix}")\n'
        '        callback("dynamic")\n',
    )
    _write_fixture(tmp_path, service=service)

    result = _audit(tmp_path)

    _assert_rejected(result)
    violation = next(item for item in result.violations if item.rule == "dynamic_signal_name")
    assert ".<lambda>@L" in violation.qualified_scope


def test_lambda_walrus_alias_to_obs_remains_safety_checked(tmp_path: Path) -> None:
    service = _replace_once(
        SERVICE,
        "    async def run(self) -> None:\n",
        "    async def run(self) -> None:\n"
        "        callback = lambda suffix: (\n"
        "            (emit := _obs),\n"
        '            emit.counter_add(f"probe.{suffix}"),\n'
        "        )\n"
        '        callback("dynamic")\n',
    )
    _write_fixture(tmp_path, service=service)

    result = _audit(tmp_path)

    _assert_rejected(result)
    violation = next(item for item in result.violations if item.rule == "dynamic_signal_name")
    assert ".<lambda>@L" in violation.qualified_scope


def test_lambda_parameter_shadowing_does_not_impersonate_obs_adapter(tmp_path: Path) -> None:
    service = _replace_once(
        SERVICE,
        "    async def run(self) -> None:\n",
        "    async def run(self) -> None:\n"
        '        callback = lambda _obs: _obs.counter_add(f"probe.{self}")\n'
        "        del callback\n",
    )
    _write_fixture(tmp_path, service=service)

    assert _audit(tmp_path).ok


def test_lambda_walrus_shadowing_does_not_impersonate_obs_adapter(tmp_path: Path) -> None:
    service = _replace_once(
        SERVICE,
        "    async def run(self) -> None:\n",
        "    async def run(self) -> None:\n"
        "        fake = object()\n"
        "        callback = lambda: (\n"
        "            (_obs := fake),\n"
        '            _obs.counter_add("archetype.operation.outcomes"),\n'
        "        )\n"
        "        del callback\n",
    )
    _write_fixture(tmp_path, service=service)

    assert _audit(tmp_path).ok


@pytest.mark.parametrize(
    ("call", "metric_name", "labels"),
    [
        (
            '_obs.record_outcome("succeeded")',
            "archetype.operation.outcomes",
            ("archetype.outcome",),
        ),
        (
            '_obs.record_outcome("succeeded", operation="probe.run")',
            "archetype.operation.outcomes",
            ("archetype.operation", "archetype.outcome"),
        ),
        (
            '_obs.record_failure(ValueError(), disposition="handled")',
            "archetype.operation.failures",
            ("archetype.failure.disposition", "error.type"),
        ),
    ],
    ids=["outcome", "outcome-with-operation", "failure"],
)
def test_safe_recorders_contribute_their_fixed_metric_contract(
    tmp_path: Path,
    call: str,
    metric_name: str,
    labels: tuple[str, ...],
) -> None:
    service = _replace_once(
        SERVICE,
        '        with _obs.span("probe.run", attributes={"archetype.outcome": "succeeded"}):\n',
        '        with _obs.span("probe.run", attributes={"archetype.outcome": "succeeded"}):\n'
        f"            {call}\n",
    )
    manifest = _with_metric_claims(MANIFEST, metric_name, labels)
    _write_fixture(tmp_path, service=service, manifest=manifest)

    assert _audit(tmp_path).ok

    undeclared = manifest.replace(f'metric_names = ["{metric_name}"]', "metric_names = []", 1)
    (tmp_path / "quality" / "observability" / "probe.toml").write_text(
        undeclared,
        encoding="utf-8",
    )
    result = _audit(tmp_path)
    _assert_rejected(result)
    assert any(
        "disposition[0].metric_names does not match source emissions" in error
        for error in result.policy_errors
    )


def test_safe_unregistered_internal_emitter_remains_allowed(tmp_path: Path) -> None:
    service = (
        SERVICE
        + """

def internal_observation() -> None:
    with _obs.span("probe.run", attributes={"archetype.outcome": "succeeded"}):
        pass
"""
    )
    _write_fixture(tmp_path, service=service)

    assert _audit(tmp_path).ok


def test_stale_emission_workflow_reference_is_rejected(tmp_path: Path) -> None:
    manifest = _replace_once(
        MANIFEST,
        'emission_workflows = ["probe.run"]',
        'emission_workflows = ["probe.missing"]',
    )
    _write_fixture(tmp_path, manifest=manifest)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any(
        "references unknown workflow 'probe.missing'" in error for error in result.policy_errors
    )


def test_cross_owner_emission_workflow_reference_is_rejected(tmp_path: Path) -> None:
    manifest = _replace_once(
        MANIFEST,
        'emission_workflows = ["probe.run"]',
        'emission_workflows = ["gateway.ingress"]',
    )
    _write_fixture(tmp_path, manifest=manifest)
    _write_source_module(
        tmp_path,
        "gateway.py",
        """
from archetype import _obs

def ingress() -> None:
    with _obs.span("probe.run"):
        pass
""",
    )
    gateway = """
version = 1
owner = "gateway"

[[workflow]]
id = "gateway.ingress"
qualified_scope = "archetype.gateway.ingress"
signals = ["root"]
outcomes = ["propagated_failure"]
authority = "The typed gateway result remains authoritative."
evidence = ["docs/evidence.md"]
span_names = ["probe.run"]
"""
    (tmp_path / "quality" / "observability" / "gateway.toml").write_text(
        dedent(gateway).lstrip(),
        encoding="utf-8",
    )

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any("owned by 'gateway', not 'probe'" in error for error in result.policy_errors)


def test_vocabulary_must_be_literal_and_fail_closed(tmp_path: Path) -> None:
    dynamic_vocabulary = _replace_once(
        OBS_VOCABULARY,
        'SPAN_NAMES: Final[frozenset[str]] = frozenset({"probe.run", "probe.legacy"})',
        "SPAN_NAMES: Final[frozenset[str]] = frozenset(_span_names())",
    )
    _write_fixture(tmp_path, obs=dynamic_vocabulary)

    _assert_rejected(_audit(tmp_path))


def test_checker_consumes_new_literal_vocabulary_without_a_duplicate_allowlist(
    tmp_path: Path,
) -> None:
    obs = _replace_once(
        OBS_VOCABULARY,
        '{"probe.run", "probe.legacy"}',
        '{"probe.run", "probe.new", "probe.legacy"}',
    )
    manifest = MANIFEST.replace('"probe.run"', '"probe.new"')
    service = SERVICE.replace('"probe.run"', '"probe.new"')
    _write_fixture(tmp_path, obs=obs, manifest=manifest, service=service)

    assert _audit(tmp_path).ok


@pytest.mark.parametrize(
    "source",
    [
        """
def observed(tick: int) -> None:
    with _obs.span(f"probe.{tick}"):
        pass
""",
        """
def observed() -> None:
    with _obs.span("probe.unknown"):
        pass
""",
        """
def observed() -> None:
    with _obs.span("probe.legacy"):
        pass
""",
        """
def observed(field: str) -> None:
    with _obs.span("probe.run", attributes={field: "succeeded"}):
        pass
""",
        """
def observed() -> None:
    with _obs.span("probe.run", attributes={"password": "secret"}):
        pass
""",
        """
def observed() -> None:
    with _obs.span("probe.run", operation="probe.run"):
        pass
""",
        """
def observed(secret: str) -> None:
    with _obs.span("probe.run", **{"password": secret}):
        pass
""",
        """
def observed(name: str) -> None:
    _obs.counter_add(name)
""",
        """
def observed() -> None:
    _obs.counter_add("probe.unknown")
""",
        """
def observed(world_id: str) -> None:
    _obs.counter_add(
        "archetype.operation.outcomes",
        attributes={"archetype.world.id": world_id},
    )
""",
    ],
    ids=[
        "dynamic-span",
        "unknown-span",
        "legacy-span",
        "dynamic-attribute-key",
        "unsafe-attribute-key",
        "legacy-attribute-key",
        "expanded-attribute-keys",
        "dynamic-metric",
        "unknown-metric",
        "high-cardinality-metric-label",
    ],
)
def test_signal_sites_require_literal_safe_bounded_vocabulary(
    tmp_path: Path,
    source: str,
) -> None:
    service = SERVICE + "\nfrom archetype import _obs\n" + source
    _write_fixture(tmp_path, service=service)

    _assert_rejected(_audit(tmp_path))


@pytest.mark.parametrize(
    "vendor_import",
    [
        "from opentelemetry.sdk.trace import TracerProvider",
        "from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter",
        "import logfire",
    ],
    ids=["otel-sdk", "otel-exporter", "vendor-adapter"],
)
def test_family_modules_cannot_import_vendor_telemetry(
    tmp_path: Path,
    vendor_import: str,
) -> None:
    _write_fixture(tmp_path, service=SERVICE + "\n" + vendor_import + "\n")

    _assert_rejected(_audit(tmp_path))


def test_host_configuration_approval_is_exact_to_the_callable(tmp_path: Path) -> None:
    approved_obs = (
        OBS_VOCABULARY
        + """

def approved() -> None:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    trace.set_tracer_provider(TracerProvider())
"""
    )
    hosts = _with_host(
        "archetype._obs.approved",
        "provider_configuration",
    )
    _write_fixture(tmp_path, obs=approved_obs, hosts=hosts)
    assert _audit(tmp_path).ok

    unapproved_obs = (
        approved_obs
        + """

def sibling() -> None:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    trace.set_tracer_provider(TracerProvider())
"""
    )
    _write_source_module(tmp_path, "_obs.py", unapproved_obs)
    _assert_rejected(_audit(tmp_path))


def test_provider_configuration_alias_is_exact_to_the_callable(tmp_path: Path) -> None:
    source = (
        OBS_VOCABULARY
        + """

def approved() -> None:
    pass

def sibling(provider: object) -> None:
    from opentelemetry import trace
    setter = trace.set_tracer_provider
    setter(provider)
"""
    )
    hosts = _with_host(
        "archetype._obs.approved",
        "provider_configuration",
    )
    _write_fixture(tmp_path, obs=source, hosts=hosts)

    _assert_rejected(_audit(tmp_path))


def test_configuration_aliases_obey_lexical_shadowing(tmp_path: Path) -> None:
    source = (
        OBS_VOCABULARY
        + """

from opentelemetry import trace
setter = trace.set_tracer_provider

def approved(provider: object) -> None:
    setter(provider)

def ordinary(setter, value: object) -> None:
    setter(value)
"""
    )
    hosts = _with_host(
        "archetype._obs.approved",
        "provider_configuration",
    )
    _write_fixture(tmp_path, obs=source, hosts=hosts)

    assert _audit(tmp_path).ok


def test_local_configuration_alias_shadows_a_same_named_definition(tmp_path: Path) -> None:
    source = (
        OBS_VOCABULARY
        + """

def setter(value: object) -> None:
    pass

def sibling(provider: object) -> None:
    from opentelemetry import trace
    setter = trace.set_tracer_provider
    setter(provider)
"""
    )
    _write_fixture(tmp_path, obs=source)

    _assert_rejected(_audit(tmp_path))


def test_configuration_alias_uses_the_latest_assignment(tmp_path: Path) -> None:
    unsafe = (
        OBS_VOCABULARY
        + """

def ordinary(value: object) -> None:
    pass

def sibling(provider: object) -> None:
    from opentelemetry import trace
    setter = ordinary
    setter = trace.set_tracer_provider
    setter(provider)
"""
    )
    _write_fixture(tmp_path, obs=unsafe)
    _assert_rejected(_audit(tmp_path))

    safe = unsafe.replace(
        "setter = ordinary\n    setter = trace.set_tracer_provider",
        "setter = trace.set_tracer_provider\n    setter = ordinary",
    )
    _write_source_module(tmp_path, "_obs.py", safe)
    assert _audit(tmp_path).ok


@pytest.mark.parametrize(
    "control_flow",
    [
        """
    if enabled:
        setter = trace.set_tracer_provider
    else:
        setter = ordinary
""",
        """
    setter = ordinary
    for _item in items:
        setter = trace.set_tracer_provider
""",
        """
    try:
        setter = trace.set_tracer_provider
    except RuntimeError:
        setter = ordinary
""",
        """
    setter = ordinary
    try:
        setter = trace.set_tracer_provider
        risky()
        setter = ordinary
    except RuntimeError:
        pass
""",
        """
    setter = ordinary
    for _item in items:
        setter = trace.set_tracer_provider
        if enabled:
            break
        setter = ordinary
""",
        """
    setter = ordinary
    try:
        if enabled:
            setter = trace.set_tracer_provider
            risky()
            setter = ordinary
        else:
            setter = ordinary
    except RuntimeError:
        pass
""",
    ],
    ids=[
        "if-join",
        "loop-join",
        "try-join",
        "exception-prefix",
        "break-prefix",
        "nested-exception-prefix",
    ],
)
def test_configuration_alias_joins_control_flow_conservatively(
    tmp_path: Path,
    control_flow: str,
) -> None:
    source = (
        OBS_VOCABULARY
        + """

def ordinary(value: object) -> None:
    pass

def sibling(enabled: bool, items: list[object], provider: object) -> None:
    from opentelemetry import trace
"""
        + control_flow
        + "    setter(provider)\n"
    )
    _write_fixture(tmp_path, obs=source)

    _assert_rejected(_audit(tmp_path))


def test_provider_mutator_requires_an_exact_host_without_import_provenance(
    tmp_path: Path,
) -> None:
    source = (
        SERVICE
        + """

def configure(provider: object, processor: object) -> None:
    provider.add_span_processor(processor)
"""
    )
    _write_fixture(tmp_path, service=source)

    _assert_rejected(_audit(tmp_path))


def test_family_callable_cannot_self_declare_as_provider_host(tmp_path: Path) -> None:
    service = (
        SERVICE
        + """

def approved() -> None:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    trace.set_tracer_provider(TracerProvider())
"""
    )
    hosts = _with_host(
        "archetype.app.probe.service.approved",
        "provider_configuration",
    )
    _write_fixture(tmp_path, service=service, hosts=hosts)

    _assert_rejected(_audit(tmp_path))


def test_logging_configuration_is_host_owned(tmp_path: Path) -> None:
    approved_source = """
from __future__ import annotations

def approved() -> None:
    import logging
    logging.basicConfig()
"""
    hosts = _with_host(
        "archetype._logging.approved",
        "logging_configuration",
    )
    _write_fixture(tmp_path, hosts=hosts)
    _write_source_module(tmp_path, "_logging.py", approved_source)
    assert _audit(tmp_path).ok

    unapproved_source = (
        approved_source
        + """

def sibling() -> None:
    import logging
    import logging.config
    logging.config.dictConfig({})
"""
    )
    _write_source_module(tmp_path, "_logging.py", unapproved_source)
    _assert_rejected(_audit(tmp_path))


def test_unrelated_configuration_method_name_is_not_logging(tmp_path: Path) -> None:
    source = (
        SERVICE
        + """

class Feature:
    def disable(self) -> None:
        pass

def update(feature: Feature) -> None:
    feature.disable()
"""
    )
    _write_fixture(tmp_path, service=source)

    assert _audit(tmp_path).ok


def test_print_is_confined_to_cli_or_an_exact_console_export_callable(tmp_path: Path) -> None:
    approved_obs = (
        OBS_VOCABULARY
        + """

def export() -> None:
    print("safe fixed console line")
"""
    )
    hosts = _with_host(
        "archetype._obs.export",
        "console_export",
    )
    _write_fixture(tmp_path, obs=approved_obs, hosts=hosts)
    assert _audit(tmp_path).ok

    unapproved_obs = (
        approved_obs
        + """

def sibling() -> None:
    print("not an approved console boundary")
"""
    )
    _write_source_module(tmp_path, "_obs.py", unapproved_obs)
    _assert_rejected(_audit(tmp_path))


def test_parameterized_logging_passes_but_eager_f_strings_fail(tmp_path: Path) -> None:
    safe = (
        SERVICE
        + """

import logging
diagnostics = logging.getLogger(__name__)

def observed(world_id: str) -> None:
    diagnostics.info("world=%s", world_id)
    diagnostics.log(logging.INFO, "world=%s", world_id)
"""
    )
    _write_fixture(tmp_path, service=safe)
    assert _audit(tmp_path).ok

    eager = safe.replace(
        'diagnostics.info("world=%s", world_id)',
        'message = f"world={world_id}"\n    diagnostics.info(message)',
    )
    (tmp_path / "src" / "archetype" / "app" / "probe" / "service.py").write_text(
        dedent(eager).lstrip(),
        encoding="utf-8",
    )
    _assert_rejected(_audit(tmp_path))


def test_logger_log_audits_its_message_after_the_level(tmp_path: Path) -> None:
    source = (
        SERVICE
        + """

import logging
diagnostics = logging.getLogger(__name__)

def observed(value: str) -> None:
    diagnostics.log(logging.ERROR, f"payload={value}", exc_info=True)
"""
    )
    _write_fixture(tmp_path, service=source)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert {
        "eager_log_interpolation",
        "raw_exception_logging",
        "unsafe_log_value",
    } <= {violation.rule for violation in result.violations}
    assert any("f'payload={value}'" in violation.target for violation in result.violations)


def test_logger_log_keyword_message_is_audited(tmp_path: Path) -> None:
    source = (
        SERVICE
        + """

import logging
diagnostics = logging.getLogger(__name__)

def observed(world_id: str) -> None:
    diagnostics.log(level=logging.INFO, msg=f"world={world_id}")
"""
    )
    _write_fixture(tmp_path, service=source)

    result = _audit(tmp_path)

    _assert_rejected(result)
    assert any(violation.rule == "eager_log_interpolation" for violation in result.violations)


@pytest.mark.parametrize(
    "source",
    [
        """
import logging
base = logging.getLogger(__name__)
diagnostics = base

def observed(world_id: str) -> None:
    diagnostics.info(f"world={world_id}")
""",
        """
import logging

class Observer:
    def __init__(self) -> None:
        self.diagnostics = logging.getLogger(__name__)

    def observed(self, world_id: str) -> None:
        self.diagnostics.info(f"world={world_id}")
""",
    ],
    ids=["assigned-alias", "member-binding"],
)
def test_eager_log_interpolation_follows_logger_aliases(
    tmp_path: Path,
    source: str,
) -> None:
    _write_fixture(tmp_path, service=SERVICE + source)

    _assert_rejected(_audit(tmp_path))


def test_logger_member_bindings_do_not_leak_between_classes(tmp_path: Path) -> None:
    source = (
        SERVICE
        + """

import logging

class LoggerOwner:
    def __init__(self) -> None:
        self.channel = logging.getLogger(__name__)

class DomainOwner:
    def observed(self, world_id: str) -> None:
        self.channel.info(f"domain={world_id}")
"""
    )
    _write_fixture(tmp_path, service=source)

    assert _audit(tmp_path).ok


def test_local_logger_binding_shadows_a_same_named_definition(tmp_path: Path) -> None:
    source = (
        SERVICE
        + """

import logging

def diagnostics() -> None:
    pass

def observed(world_id: str) -> None:
    diagnostics = logging.getLogger(__name__)
    diagnostics.info(f"world={world_id}")
"""
    )
    _write_fixture(tmp_path, service=source)

    _assert_rejected(_audit(tmp_path))


def test_logger_alias_uses_the_latest_assignment(tmp_path: Path) -> None:
    unsafe = (
        SERVICE
        + """

import logging

def helper() -> None:
    pass

def observed(world_id: str) -> None:
    diagnostics = helper
    diagnostics = logging.getLogger(__name__)
    diagnostics.info(f"world={world_id}")
"""
    )
    _write_fixture(tmp_path, service=unsafe)
    _assert_rejected(_audit(tmp_path))

    safe = unsafe.replace(
        "diagnostics = helper\n    diagnostics = logging.getLogger(__name__)",
        "diagnostics = logging.getLogger(__name__)\n    diagnostics = helper",
    )
    (tmp_path / "src" / "archetype" / "app" / "probe" / "service.py").write_text(
        dedent(safe).lstrip(),
        encoding="utf-8",
    )
    assert _audit(tmp_path).ok


def test_logger_alias_joins_mutually_exclusive_branches(tmp_path: Path) -> None:
    source = (
        SERVICE
        + """

import logging

def observed(enabled: bool, domain: object, world_id: str) -> None:
    if enabled:
        diagnostics = logging.getLogger(__name__)
    else:
        diagnostics = domain
    diagnostics.info(f"world={world_id}")
"""
    )
    _write_fixture(tmp_path, service=source)

    _assert_rejected(_audit(tmp_path))


@pytest.mark.parametrize(
    "control_flow",
    [
        """
    diagnostics = domain
    try:
        diagnostics = logging.getLogger(__name__)
        risky()
        diagnostics = domain
    except RuntimeError:
        pass
""",
        """
    diagnostics = domain
    for _item in items:
        diagnostics = logging.getLogger(__name__)
        if enabled:
            break
        diagnostics = domain
""",
    ],
    ids=["exception-prefix", "break-prefix"],
)
def test_logger_alias_retains_reachable_early_exit_bindings(
    tmp_path: Path,
    control_flow: str,
) -> None:
    source = (
        SERVICE
        + """

import logging

def observed(
    enabled: bool,
    items: list[object],
    domain: object,
    world_id: str,
) -> None:
"""
        + control_flow
        + '    diagnostics.info(f"world={world_id}")\n'
    )
    _write_fixture(tmp_path, service=source)

    _assert_rejected(_audit(tmp_path))


@pytest.mark.parametrize(
    "statement",
    [
        'diagnostics.info("event", extra={"password": secret})',
        'diagnostics.info("payload=%s", value)',
    ],
    ids=["unsafe-extra-key", "obvious-payload-value"],
)
def test_structured_logging_rejects_obvious_content_export(
    tmp_path: Path,
    statement: str,
) -> None:
    source = (
        SERVICE
        + f"""

import logging
diagnostics = logging.getLogger(__name__)

def observed(secret: str, value: str) -> None:
    {statement}
"""
    )
    _write_fixture(tmp_path, service=source)

    _assert_rejected(_audit(tmp_path))


@pytest.mark.parametrize(
    "statement",
    [
        'diagnostics.exception("probe failed")',
        'diagnostics.error("probe failed", exc_info=True)',
        'diagnostics.error("probe failed: %s", exc)',
    ],
    ids=["logger-exception", "exc-info", "caught-exception-object"],
)
def test_raw_exception_logging_is_rejected(tmp_path: Path, statement: str) -> None:
    source = (
        SERVICE
        + f"""

import logging
diagnostics = logging.getLogger(__name__)

def observed() -> None:
    try:
        raise ValueError("payload must stay local")
    except ValueError as exc:
        {statement}
"""
    )
    _write_fixture(tmp_path, service=source)

    _assert_rejected(_audit(tmp_path))


def test_raw_exception_attribute_is_rejected(tmp_path: Path) -> None:
    source = (
        SERVICE
        + """

from archetype import _obs

def observed() -> None:
    try:
        raise ValueError("payload must stay local")
    except ValueError as exc:
        with _obs.span("probe.run", attributes={"archetype.outcome": exc}):
            pass
"""
    )
    _write_fixture(tmp_path, service=source)

    result = _audit(tmp_path)
    _assert_rejected(result)
    assert {violation.rule for violation in result.violations} == {"raw_exception_attribute"}


def test_broad_suppression_around_telemetry_requires_an_exact_exception(
    tmp_path: Path,
) -> None:
    source = (
        SERVICE
        + """

from archetype import _obs

def observed() -> None:
    try:
        _obs.counter_add("archetype.operation.outcomes")
    except Exception:
        pass
"""
    )
    _write_fixture(tmp_path, service=source)

    result = _audit(tmp_path)
    _assert_rejected(result)
    assert len(result.violations) == 1


def test_conditional_reraise_does_not_hide_a_telemetry_suppression_path(
    tmp_path: Path,
) -> None:
    source = (
        SERVICE
        + """

from archetype import _obs

def observed(reraise: bool) -> None:
    try:
        _obs.counter_add("archetype.operation.outcomes")
    except Exception:
        if reraise:
            raise
        return None
"""
    )
    _write_fixture(tmp_path, service=source)

    result = _audit(tmp_path)
    _assert_rejected(result)
    assert {violation.rule for violation in result.violations} == {"broad_telemetry_suppression"}


def test_run_debug_cannot_select_telemetry_export_configuration(tmp_path: Path) -> None:
    source = """
from __future__ import annotations

from archetype import _obs

class RunConfig:
    debug: bool

def host(config: RunConfig) -> None:
    _obs.configure_tracing(service_name="fixture", debug_console=config.debug)
"""
    hosts = _with_host(
        "archetype.runtime.host.host",
        "invoke_configuration",
    )
    _write_fixture(tmp_path, hosts=hosts)
    _write_source_module(tmp_path, "runtime/host.py", source)

    _assert_rejected(_audit(tmp_path))


def _legacy_exception(*, rule: str, scope: str, target: str) -> str:
    return f'''

[[legacy]]
rule = "{rule}"
path = "src/archetype/app/probe/service.py"
qualified_scope = "{scope}"
target = "{target}"
owner = "probe"
issue = 999
reason = "The fixture models an exact migration entry."
expiry_condition = "Remove when probe.legacy is deleted."
'''


def test_legacy_exception_requires_an_exact_match_and_then_exempts(tmp_path: Path) -> None:
    source = (
        SERVICE
        + """

from archetype import _obs

def observed() -> None:
    with _obs.span("probe.legacy"):
        pass
"""
    )
    _write_fixture(tmp_path, service=source)
    initial = _audit(tmp_path)
    assert len(initial.violations) == 1
    violation = initial.violations[0]

    manifest = MANIFEST + _legacy_exception(
        rule=violation.rule,
        scope="archetype.app.probe.service.observed",
        target=violation.target,
    )
    (tmp_path / "quality" / "observability" / "probe.toml").write_text(
        manifest,
        encoding="utf-8",
    )

    assert _audit(tmp_path).ok


def test_legacy_exception_cannot_be_owned_by_another_family(tmp_path: Path) -> None:
    source = (
        SERVICE
        + """

from archetype import _obs

def observed() -> None:
    with _obs.span("probe.legacy"):
        pass
"""
    )
    _write_fixture(tmp_path, service=source)
    violation = _audit(tmp_path).violations[0]
    other = f'''
version = 1
owner = "other"

[[legacy]]
rule = "{violation.rule}"
path = "src/archetype/app/probe/service.py"
qualified_scope = "archetype.app.probe.service.observed"
target = "{violation.target}"
owner = "other"
issue = 999
reason = "Another owner must not absorb probe debt."
expiry_condition = "This row must be rejected immediately."
'''
    (tmp_path / "quality" / "observability" / "other.toml").write_text(
        other,
        encoding="utf-8",
    )

    _assert_rejected(_audit(tmp_path))


def test_stale_or_colliding_legacy_exceptions_fail_policy(tmp_path: Path) -> None:
    stale = MANIFEST + _legacy_exception(
        rule="legacy_signal_name",
        scope="archetype.app.probe.service.observed",
        target="span:probe.legacy",
    )
    _write_fixture(tmp_path, manifest=stale)
    _assert_rejected(_audit(tmp_path))

    collision = stale + _legacy_exception(
        rule="legacy_signal_name",
        scope="archetype.app.probe.service.observed",
        target="span:probe.legacy",
    )
    (tmp_path / "quality" / "observability" / "probe.toml").write_text(
        collision,
        encoding="utf-8",
    )
    _assert_rejected(_audit(tmp_path))


def test_workspace_source_roots_are_unioned_and_duplicate_modules_fail_closed(
    tmp_path: Path,
) -> None:
    _write_fixture(tmp_path)
    original_family = tmp_path / "src" / "archetype" / "app" / "probe"
    plugin_family = tmp_path / "packages" / "probe" / "src" / "archetype" / "app" / "probe"
    plugin_family.parent.mkdir(parents=True)
    original_family.rename(plugin_family)
    (tmp_path / "quality" / "architecture.toml").write_text(
        """
version = 3
source_roots = ["src", "packages/probe/src"]
""",
        encoding="utf-8",
    )

    clean = _audit(tmp_path)

    assert clean.ok
    assert clean.files_scanned == 3

    duplicate = plugin_family.parents[1] / "_obs.py"
    duplicate.write_text(dedent(OBS_VOCABULARY).lstrip(), encoding="utf-8")
    collided = _audit(tmp_path)

    assert not collided.ok
    assert any(
        "duplicate first-party module identity archetype._obs" in error
        and "src/archetype/_obs.py" in error
        and "packages/probe/src/archetype/_obs.py" in error
        for error in collided.policy_errors
    )


def test_repository_observability_policy_passes_for_all_protocol_operations() -> None:
    completed = subprocess.run(
        [sys.executable, str(CHECKER_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    expected = checker.audit_repository()
    assert expected.operations_scanned > 0
    assert f"{expected.operations_scanned} operations" in completed.stdout
    assert "Observability audit passed" in completed.stdout
