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
    {"archetype.operation", "archetype.outcome", "archetype.world.id", "error.type"}
)
TRACE_ATTRIBUTE_ALIASES: Final[Mapping[str, str]] = MappingProxyType(
    {"operation": "archetype.operation"}
)
METRIC_NAMES: Final[frozenset[str]] = frozenset({"archetype.operation.outcomes"})
METRIC_LABEL_KEYS: Final[frozenset[str]] = frozenset(
    {"archetype.operation", "archetype.outcome", "error.type"}
)
EVENT_NAMES: Final[frozenset[str]] = frozenset({"archetype.outcome"})
FAILURE_DISPOSITIONS: Final[frozenset[str]] = frozenset({"handled", "retrying"})
OUTCOMES: Final[frozenset[str]] = frozenset({"rejected", "succeeded"})
ERROR_TYPES: Final[frozenset[str]] = frozenset({"internal", "validation"})

def span(name: str, attributes: object = None, **values: object) -> object: ...
def counter_add(name: str, amount: int = 1, *, attributes: object = None) -> None: ...
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

class LocalPort(Protocol):
    def flush(self) -> None: ...

class Probe:
    async def run(self) -> None:
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
outcomes = ["propagated_failure", "handled_outcome"]
authority = "The fixture receipt and typed result remain authoritative."
evidence = ["docs/evidence.md"]
span_names = ["probe.run"]
attribute_keys = ["archetype.outcome"]
"""

HOSTS = """
version = 1
owner = "hosts"
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
        "app/gateway/service.py",
        """
def ingress() -> None:
    pass
""",
    )
    gateway = """
version = 1
owner = "gateway"

[[workflow]]
id = "gateway.ingress"
qualified_scope = "archetype.app.gateway.service.ingress"
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
        .replace('span_names = ["probe.run"]\n', "")
        .replace('attribute_keys = ["archetype.outcome"]\n', "")
    )
    _write_fixture(tmp_path, manifest=without_rationale)
    _assert_rejected(_audit(tmp_path))

    with_rationale = without_rationale + (
        'rationale = "No direct signal is approved; the typed result remains authoritative."\n'
    )
    (tmp_path / "quality" / "observability" / "probe.toml").write_text(
        with_rationale,
        encoding="utf-8",
    )
    assert _audit(tmp_path).ok


def test_metric_disposition_requires_fixed_names_and_bounded_labels(tmp_path: Path) -> None:
    valid = (
        _replace_once(MANIFEST, 'signals = ["child"]', 'signals = ["child", "metric"]')
        + 'metric_names = ["archetype.operation.outcomes"]\n'
        + 'metric_label_keys = ["archetype.outcome"]\n'
    )
    _write_fixture(tmp_path, manifest=valid)
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


def test_workflow_scope_must_exist_and_be_callable(tmp_path: Path) -> None:
    valid = (
        MANIFEST
        + """

[[workflow]]
id = "probe.run"
qualified_scope = "archetype.app.probe.service.Probe.run"
signals = ["child"]
outcomes = ["propagated_failure"]
authority = "The fixture typed result remains authoritative."
evidence = ["docs/evidence.md"]
span_names = ["probe.run"]
"""
    )
    _write_fixture(tmp_path, manifest=valid)
    assert _audit(tmp_path).ok

    stale = _replace_once(
        valid,
        "archetype.app.probe.service.Probe.run",
        "archetype.app.probe.service.Probe.missing",
    )
    (tmp_path / "quality" / "observability" / "probe.toml").write_text(
        stale,
        encoding="utf-8",
    )
    _assert_rejected(_audit(tmp_path))


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
    manifest = _replace_once(MANIFEST, '["probe.run"]', '["probe.new"]')
    service = (
        SERVICE
        + """

from archetype import _obs

def observed() -> None:
    with _obs.span("probe.new", attributes={"archetype.outcome": "succeeded"}):
        pass
"""
    )
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
        'diagnostics.info("payload=%s", payload)',
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

def observed(secret: str, payload: str) -> None:
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


def test_repository_observability_policy_passes_for_all_protocol_operations() -> None:
    completed = subprocess.run(
        [sys.executable, str(CHECKER_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "244 operations" in completed.stdout
    assert "Observability audit passed" in completed.stdout
