# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused contracts for dependency-owned telemetry configuration routing."""

from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace

import pytest

from archetype import _dependency_telemetry

pytestmark = [
    pytest.mark.contract("observability.signals.safe"),
    pytest.mark.unit,
]

_ENVIRONMENT_KEYS = (
    "ARCHETYPE_OTLP_TRACES_ENDPOINT",
    "DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_SERVICE_NAME",
)


@pytest.fixture(autouse=True)
def _isolated_host_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in _ENVIRONMENT_KEYS:
        monkeypatch.delenv(name, raising=False)
    _dependency_telemetry._pending_diagnostics.clear()
    monkeypatch.setattr(_dependency_telemetry, "sys", SimpleNamespace(modules={}))
    yield
    _dependency_telemetry._pending_diagnostics.clear()
    # The subject mutates ``os.environ`` directly, so remove values it created
    # before monkeypatch restores the process's original environment.
    for name in _ENVIRONMENT_KEYS:
        _dependency_telemetry.os.environ.pop(name, None)


@pytest.mark.parametrize(
    ("endpoint", "expected"),
    [
        ("https://collector.example", "https://collector.example/v1/traces"),
        (
            "https://collector.example/otlp/?tenant=a#fragment",
            "https://collector.example/otlp/v1/traces?tenant=a#fragment",
        ),
        ("http://[malformed", None),
    ],
)
def test_generic_endpoint_conversion_updates_only_the_url_path(
    endpoint: str, expected: str | None
) -> None:
    assert _dependency_telemetry._generic_traces_endpoint(endpoint) == expected


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://localhost:4317",
        "https://127.0.0.1/v1/metrics",
        "http://[::1]:4317",
        "https://collector.example/metrics%20path",
    ],
)
def test_valid_metrics_endpoints_are_accepted(endpoint: str) -> None:
    assert _dependency_telemetry._valid_metrics_endpoint(endpoint) is True


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        "ftp://collector.example",
        "http://bad host:4317",
        "http://%zz:4317",
        "http://user@collector.example:4317",
        "http://collector.example:",
        "http://collector.example:99999",
        "http://[invalid]:4317",
        "http://collector.example/path#fragment",
    ],
)
def test_invalid_metrics_endpoints_are_rejected(endpoint: str) -> None:
    assert _dependency_telemetry._valid_metrics_endpoint(endpoint) is False


def test_native_metrics_require_an_explicitly_validated_daft_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_dependency_telemetry, "version", lambda distribution: "0.7.19")
    assert _dependency_telemetry._daft_native_metrics_are_validated() is True

    monkeypatch.setattr(_dependency_telemetry, "version", lambda distribution: "0.7.20")
    assert _dependency_telemetry._daft_native_metrics_are_validated() is False

    def unavailable(distribution: str) -> str:
        raise RuntimeError("version-canary")

    monkeypatch.setattr(_dependency_telemetry, "version", unavailable)
    assert _dependency_telemetry._daft_native_metrics_are_validated() is False


def test_generic_traces_and_validated_metrics_are_routed_to_separate_owners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "https://collector.example/otlp?tenant=a",
    )
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "https://logs.example")
    monkeypatch.setenv("DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT", "https://old.example")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics.example")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "unsafe.label=resource-canary")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "service-canary")
    monkeypatch.setattr(
        _dependency_telemetry,
        "_daft_native_metrics_are_validated",
        lambda: True,
    )

    _dependency_telemetry.prepare_dependency_telemetry()

    assert _dependency_telemetry.os.environ["ARCHETYPE_OTLP_TRACES_ENDPOINT"] == (
        "https://collector.example/otlp/v1/traces?tenant=a"
    )
    assert _dependency_telemetry.os.environ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == (
        "http://metrics.example"
    )
    for name in (
        "DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_RESOURCE_ATTRIBUTES",
        "OTEL_SERVICE_NAME",
    ):
        assert name not in _dependency_telemetry.os.environ
    assert _dependency_telemetry.take_diagnostics() == ()


def test_trace_specific_endpoint_has_precedence_over_generic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://generic.example")
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "https://traces.example/custom",
    )

    assert _dependency_telemetry.archetype_traces_endpoint() == ("https://traces.example/custom")
    assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in _dependency_telemetry.os.environ
    assert "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" not in _dependency_telemetry.os.environ


def test_malformed_endpoints_are_removed_with_fixed_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://[trace-canary")
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "http://metrics-canary invalid:4317",
    )
    monkeypatch.setattr(
        _dependency_telemetry,
        "_daft_native_metrics_are_validated",
        lambda: True,
    )

    _dependency_telemetry.prepare_dependency_telemetry()

    assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in _dependency_telemetry.os.environ
    assert "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" not in _dependency_telemetry.os.environ
    assert _dependency_telemetry.take_diagnostics() == (
        "Malformed generic OTLP trace configuration was disabled.",
        "Unsupported Daft metrics telemetry configuration was disabled.",
    )


def test_unvalidated_version_removes_metrics_with_a_fixed_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics.example:4317")
    monkeypatch.setattr(
        _dependency_telemetry,
        "_daft_native_metrics_are_validated",
        lambda: False,
    )

    _dependency_telemetry.prepare_dependency_telemetry()

    assert "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" not in _dependency_telemetry.os.environ
    assert _dependency_telemetry.take_diagnostics() == (
        "Daft native metrics were disabled for an unvalidated dependency version.",
    )


def test_late_daft_initialization_is_detected_without_restoring_generic_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector.example")
    monkeypatch.setattr(
        _dependency_telemetry,
        "sys",
        SimpleNamespace(modules={"daft.daft": object()}),
    )

    _dependency_telemetry.prepare_dependency_telemetry()

    assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in _dependency_telemetry.os.environ
    assert _dependency_telemetry.take_diagnostics() == (
        "Daft native telemetry initialized before Archetype's host isolation boundary; "
        "dependency signal export may be unsafe.",
    )
