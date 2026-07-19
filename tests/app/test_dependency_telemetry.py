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
    "OTEL_EXPORTER_OTLP_COMPRESSION",
    "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_SERVICE_NAME",
    "OTEL_METRIC_EXPORT_INTERVAL",
)


@pytest.fixture(autouse=True)
def _isolated_host_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in _ENVIRONMENT_KEYS:
        monkeypatch.delenv(name, raising=False)
    _dependency_telemetry._pending_diagnostics.clear()
    monkeypatch.setattr(_dependency_telemetry, "_last_routed_traces_endpoint", None)
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
            "https://collector.example/otlp/",
            "https://collector.example/otlp/v1/traces",
        ),
        ("https://collector.example/otlp?tenant=secret-canary", None),
        ("https://collector.example/otlp#secret-canary", None),
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


@pytest.mark.parametrize("interval", ["1", "10", "500", str(2**64 - 1)])
def test_positive_u64_metrics_intervals_are_accepted(interval: str) -> None:
    assert _dependency_telemetry._valid_metrics_interval(interval) is True


@pytest.mark.parametrize(
    "interval",
    ["", "0", "-1", "1.0", "invalid", str(2**64), "9" * 4_301],
)
def test_nonpositive_or_malformed_metrics_intervals_are_rejected(interval: str) -> None:
    assert _dependency_telemetry._valid_metrics_interval(interval) is False


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
        "http://collector.example/path?token=secret-canary",
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
        "https://collector.example/otlp",
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
        "https://collector.example/otlp/v1/traces"
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


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip"),
        ("OTEL_EXPORTER_OTLP_METRICS_COMPRESSION", "invalid-canary"),
    ],
)
def test_native_metrics_compression_is_removed_while_endpoint_remains_enabled(
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
    value: str,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics.example:4317")
    monkeypatch.setenv(variable, value)
    monkeypatch.setattr(
        _dependency_telemetry,
        "_daft_native_metrics_are_validated",
        lambda: True,
    )

    _dependency_telemetry.prepare_dependency_telemetry()

    assert _dependency_telemetry.os.environ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == (
        "http://metrics.example:4317"
    )
    assert "OTEL_EXPORTER_OTLP_COMPRESSION" not in _dependency_telemetry.os.environ
    assert "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION" not in _dependency_telemetry.os.environ
    assert _dependency_telemetry.take_diagnostics() == (
        "Unsupported Daft metrics compression configuration was removed.",
    )


def test_generic_trace_compression_remains_when_native_metrics_are_not_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector.example")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip")

    _dependency_telemetry.prepare_dependency_telemetry()

    assert _dependency_telemetry.os.environ["OTEL_EXPORTER_OTLP_COMPRESSION"] == "gzip"
    assert _dependency_telemetry.take_diagnostics() == ()


@pytest.mark.parametrize("interval", ["0", "-1", "invalid", str(2**64)])
def test_unsafe_metrics_interval_is_removed_with_fixed_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
    interval: str,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics.example:4317")
    monkeypatch.setenv("OTEL_METRIC_EXPORT_INTERVAL", interval)
    monkeypatch.setattr(
        _dependency_telemetry,
        "_daft_native_metrics_are_validated",
        lambda: True,
    )

    _dependency_telemetry.prepare_dependency_telemetry()

    assert "OTEL_METRIC_EXPORT_INTERVAL" not in _dependency_telemetry.os.environ
    assert _dependency_telemetry.take_diagnostics() == (
        "Unsupported Daft metrics export interval configuration was removed.",
    )


def test_native_metrics_controls_are_untouched_without_a_metrics_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_COMPRESSION", "gzip")
    monkeypatch.setenv("OTEL_METRIC_EXPORT_INTERVAL", "0")

    _dependency_telemetry.prepare_dependency_telemetry()

    assert _dependency_telemetry.os.environ["OTEL_EXPORTER_OTLP_COMPRESSION"] == "gzip"
    assert _dependency_telemetry.os.environ["OTEL_EXPORTER_OTLP_METRICS_COMPRESSION"] == "gzip"
    assert _dependency_telemetry.os.environ["OTEL_METRIC_EXPORT_INTERVAL"] == "0"
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


@pytest.mark.parametrize(
    ("source_variable", "first_value", "second_value", "first_expected", "second_expected"),
    [
        (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "https://first.example/otlp",
            "https://second.example/otlp",
            "https://first.example/otlp/v1/traces",
            "https://second.example/otlp/v1/traces",
        ),
        (
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "https://first.example/v1/traces",
            "https://second.example/v1/traces",
            "https://first.example/v1/traces",
            "https://second.example/v1/traces",
        ),
    ],
)
def test_repeated_prepare_consumes_later_standard_trace_configuration(
    monkeypatch: pytest.MonkeyPatch,
    source_variable: str,
    first_value: str,
    second_value: str,
    first_expected: str,
    second_expected: str,
) -> None:
    monkeypatch.setenv(source_variable, first_value)

    _dependency_telemetry.prepare_dependency_telemetry()

    assert _dependency_telemetry.os.environ["ARCHETYPE_OTLP_TRACES_ENDPOINT"] == first_expected
    assert source_variable not in _dependency_telemetry.os.environ

    monkeypatch.setenv(source_variable, second_value)

    _dependency_telemetry.prepare_dependency_telemetry()

    assert _dependency_telemetry.os.environ["ARCHETYPE_OTLP_TRACES_ENDPOINT"] == second_expected
    assert source_variable not in _dependency_telemetry.os.environ
    assert _dependency_telemetry.take_diagnostics() == ()


def test_changed_private_trace_endpoint_wins_over_later_standard_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://first.example/otlp")
    _dependency_telemetry.prepare_dependency_telemetry()

    monkeypatch.setenv(
        "ARCHETYPE_OTLP_TRACES_ENDPOINT",
        "https://private.example/v1/traces",
    )
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "https://later.example/v1/traces",
    )

    _dependency_telemetry.prepare_dependency_telemetry()

    assert _dependency_telemetry.os.environ["ARCHETYPE_OTLP_TRACES_ENDPOINT"] == (
        "https://private.example/v1/traces"
    )
    assert "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" not in _dependency_telemetry.os.environ
    assert _dependency_telemetry.take_diagnostics() == ()


@pytest.mark.parametrize(
    "variable",
    [
        "ARCHETYPE_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ],
)
def test_secret_bearing_trace_endpoint_is_removed_without_echoing_value(
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
) -> None:
    monkeypatch.setenv(variable, "https://collector.example/path?token=secret-canary")

    _dependency_telemetry.prepare_dependency_telemetry()

    assert "ARCHETYPE_OTLP_TRACES_ENDPOINT" not in _dependency_telemetry.os.environ
    assert _dependency_telemetry.take_diagnostics() == (
        "Unsupported Archetype OTLP trace configuration was removed.",
    )


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
        "Unsupported Archetype OTLP trace configuration was removed.",
        "Unsupported Daft metrics telemetry configuration was removed.",
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
        "Daft native metrics configuration was removed for an unvalidated dependency version.",
    )


@pytest.mark.parametrize("protocol", ["GRPC", " HTTP/PROTOBUF ", "http/json"])
def test_noncanonical_metrics_protocol_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    protocol: str,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics.example:4317")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", protocol)
    monkeypatch.setattr(
        _dependency_telemetry,
        "_daft_native_metrics_are_validated",
        lambda: True,
    )

    _dependency_telemetry.prepare_dependency_telemetry()

    assert "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" not in _dependency_telemetry.os.environ
    assert _dependency_telemetry.take_diagnostics() == (
        "Unsupported Daft metrics telemetry configuration was removed.",
    )


@pytest.mark.parametrize(
    ("generic_protocol", "metrics_protocol"),
    [
        ("grpc", "http/protobuf"),
        ("http/json", "grpc"),
    ],
)
def test_signal_specific_metrics_protocol_is_validated_and_translated_for_daft(
    monkeypatch: pytest.MonkeyPatch,
    generic_protocol: str,
    metrics_protocol: str,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics.example:4317")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", generic_protocol)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", metrics_protocol)
    monkeypatch.setattr(
        _dependency_telemetry,
        "_daft_native_metrics_are_validated",
        lambda: True,
    )

    _dependency_telemetry.prepare_dependency_telemetry()

    assert _dependency_telemetry.os.environ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == (
        "http://metrics.example:4317"
    )
    assert _dependency_telemetry.os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] == metrics_protocol
    assert _dependency_telemetry.os.environ["OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"] == (
        metrics_protocol
    )
    assert _dependency_telemetry.take_diagnostics() == ()


@pytest.mark.parametrize("metrics_protocol", ["http/json", "GRPC", " HTTP/PROTOBUF "])
def test_unsupported_signal_specific_metrics_protocol_rejects_native_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    metrics_protocol: str,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics.example:4317")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", metrics_protocol)
    monkeypatch.setattr(
        _dependency_telemetry,
        "_daft_native_metrics_are_validated",
        lambda: True,
    )

    _dependency_telemetry.prepare_dependency_telemetry()

    assert "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" not in _dependency_telemetry.os.environ
    assert _dependency_telemetry.take_diagnostics() == (
        "Unsupported Daft metrics telemetry configuration was removed.",
    )


def test_rejected_metrics_after_daft_load_reports_unverified_provider_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics.example:4317")
    monkeypatch.setattr(
        _dependency_telemetry,
        "_daft_native_metrics_are_validated",
        lambda: False,
    )
    monkeypatch.setattr(
        _dependency_telemetry,
        "sys",
        SimpleNamespace(modules={"daft.daft": object()}),
    )

    _dependency_telemetry.prepare_dependency_telemetry()

    assert _dependency_telemetry.take_diagnostics() == (
        "Daft native metrics configuration was removed for an unvalidated dependency version.",
        "Rejected Daft metrics configuration may already have initialized a dependency "
        "provider before Archetype isolation.",
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
