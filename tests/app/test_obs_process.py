# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Process-global provider contracts for the internal signal boundary."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest

pytestmark = [
    pytest.mark.contract("observability.signals.safe"),
    pytest.mark.process,
]


def _run(source: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    for key in tuple(env):
        if key.startswith(("ARCHETYPE_OTLP_", "LOGFIRE_", "OTEL_")) or key == ("ARCHETYPE_LOG"):
            env.pop(key)
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def test_noop_configuration_does_not_block_later_host_configuration() -> None:
    result = _run(
        """
        from opentelemetry import trace
        from archetype import _obs

        _obs.configure_tracing(service_name="archetype-test")
        assert _obs._configured is False
        assert isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)

        _obs.configure_tracing(service_name="archetype-test", debug_console=True)
        assert _obs._configured is True
        assert not isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)
        """
    )
    assert result.returncode == 0, result.stderr


def test_host_provider_is_respected() -> None:
    result = _run(
        """
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry import trace

        host = TracerProvider()
        trace.set_tracer_provider(host)
        from archetype import _obs

        _obs.configure_tracing(service_name="archetype-test", debug_console=True)
        assert trace.get_tracer_provider() is host
        assert _obs._owned_tracer_provider is None
        """
    )
    assert result.returncode == 0, result.stderr


def test_otlp_configuration_installs_a_sanitizing_owned_provider() -> None:
    result = _run(
        """
        import os
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http import trace_exporter
        from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

        class CapturingExporter(SpanExporter):
            instances = []

            def __init__(self, *, endpoint):
                self.endpoint = endpoint
                self.spans = []
                self.shutdown_calls = 0
                self.instances.append(self)

            def export(self, spans):
                self.spans.extend(spans)
                return SpanExportResult.SUCCESS

            def shutdown(self):
                self.shutdown_calls += 1

        trace_exporter.OTLPSpanExporter = CapturingExporter
        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = (
            "https://collector.invalid/otlp?tenant=a"
        )

        from archetype import _obs

        _obs.configure_tracing(service_name="archetype-test")
        provider = trace.get_tracer_provider()
        assert _obs._configured is True
        assert _obs._owned_tracer_provider is provider

        with provider.get_tracer("archetype").start_as_current_span(
            "artifact.publish",
            attributes={
                "archetype.operation": "artifact.publish",
                "authorization": "Bearer should-not-be-exported",
            },
        ):
            pass

        assert provider.force_flush()
        (exporter,) = CapturingExporter.instances
        assert exporter.endpoint == (
            "https://collector.invalid/otlp/v1/traces?tenant=a"
        )
        (finished,) = exporter.spans
        assert finished.name == "artifact.publish"
        assert dict(finished.attributes) == {
            "archetype.operation": "artifact.publish",
        }
        assert dict(finished.resource.attributes) == {
            "service.name": "archetype-test",
        }
        provider.shutdown()
        assert exporter.shutdown_calls == 1
        """
    )
    assert result.returncode == 0, result.stderr
    assert "Bearer should-not-be-exported" not in result.stderr


def test_otlp_initialization_failure_is_safe_shutdown_and_retryable() -> None:
    result = _run(
        """
        import os
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http import trace_exporter
        import opentelemetry.sdk.trace as sdk_trace

        real_provider = sdk_trace.TracerProvider

        class TrackingProvider(real_provider):
            instances = []

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.shutdown_calls = 0
                self.instances.append(self)

            def shutdown(self):
                self.shutdown_calls += 1
                return super().shutdown()

        class FailingExporter:
            def __init__(self, *, endpoint):
                raise RuntimeError("Bearer should-not-be-exported")

        sdk_trace.TracerProvider = TrackingProvider
        trace_exporter.OTLPSpanExporter = FailingExporter
        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "https://collector.invalid"

        from archetype import _obs

        _obs.configure_tracing(service_name="archetype-test")
        assert _obs._configured is False
        assert isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)
        (failed_candidate,) = TrackingProvider.instances
        assert failed_candidate.shutdown_calls == 1

        sdk_trace.TracerProvider = real_provider
        del os.environ["ARCHETYPE_OTLP_TRACES_ENDPOINT"]
        _obs.configure_tracing(service_name="archetype-test", debug_console=True)
        assert _obs._configured is True
        assert not isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)
        trace.get_tracer_provider().shutdown()
        """
    )
    assert result.returncode == 0, result.stderr
    assert "Bearer should-not-be-exported" not in result.stderr


def test_invalid_daft_metrics_config_reports_only_a_fixed_host_diagnostic() -> None:
    result = _run(
        """
        import os

        os.environ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] = (
            "http://endpoint-canary invalid:4317"
        )
        from archetype import _obs

        _obs.configure_tracing(service_name="archetype-test")
        assert "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" not in os.environ
        """
    )
    assert result.returncode == 0, result.stderr
    assert "Unsupported Daft metrics telemetry configuration was disabled." in result.stderr
    assert "endpoint-canary" not in result.stderr


def test_malformed_generic_endpoint_reports_only_a_fixed_host_diagnostic() -> None:
    result = _run(
        """
        import os

        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://[endpoint-canary"
        from archetype import _obs

        _obs.configure_tracing(service_name="archetype-test")
        assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in os.environ
        """
    )
    assert result.returncode == 0, result.stderr
    assert "Malformed generic OTLP trace configuration was disabled." in result.stderr
    assert "endpoint-canary" not in result.stderr


def test_unvalidated_daft_version_disables_native_metrics() -> None:
    result = _run(
        """
        import os
        from archetype import _dependency_telemetry

        _dependency_telemetry.version = lambda distribution: "0.7.20"
        os.environ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] = (
            "http://127.0.0.1:4317"
        )
        _dependency_telemetry.prepare_dependency_telemetry()

        assert "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" not in os.environ
        assert _dependency_telemetry.take_diagnostics() == (
            "Daft native metrics were disabled for an unvalidated dependency version.",
        )
        """
    )
    assert result.returncode == 0, result.stderr


def test_host_adapter_failure_is_safe_and_does_not_latch() -> None:
    result = _run(
        """
        import os
        import sys
        import types
        from archetype import _obs

        def fail(**kwargs):
            raise RuntimeError("Bearer should-not-be-exported")

        sys.modules["logfire"] = types.SimpleNamespace(configure=fail)
        os.environ["LOGFIRE_SEND_TO_LOGFIRE"] = "true"
        _obs.configure_tracing(service_name="archetype-test")
        assert _obs._configured is False

        del os.environ["LOGFIRE_SEND_TO_LOGFIRE"]
        _obs.configure_tracing(service_name="archetype-test", debug_console=True)
        assert _obs._configured is True
        """
    )
    assert result.returncode == 0, result.stderr
    assert "Bearer should-not-be-exported" not in result.stderr


def test_noop_logfire_adapter_does_not_latch_configuration() -> None:
    result = _run(
        """
        import os
        import sys
        import types
        from opentelemetry import trace
        from archetype import _obs

        sys.modules["logfire"] = types.SimpleNamespace(configure=lambda **kwargs: None)
        os.environ["LOGFIRE_SEND_TO_LOGFIRE"] = "true"
        _obs.configure_tracing(service_name="archetype-test")
        assert _obs._configured is False
        assert isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)

        del os.environ["LOGFIRE_SEND_TO_LOGFIRE"]
        _obs.configure_tracing(service_name="archetype-test", debug_console=True)
        assert _obs._configured is True
        assert not isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)
        """
    )
    assert result.returncode == 0, result.stderr


def test_logfire_adapter_disables_implicit_content_capture() -> None:
    result = _run(
        """
        import os
        import sys
        import types
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from archetype import _obs

        captured = {}
        candidate = TracerProvider()

        class Instance:
            _tracer_provider = candidate

            def shutdown(self, *, flush):
                raise AssertionError("installed provider must not be shut down")

        def configure(**kwargs):
            captured.update(kwargs)
            trace.set_tracer_provider(candidate)
            return Instance()

        sys.modules["logfire"] = types.SimpleNamespace(configure=configure)
        os.environ["LOGFIRE_SEND_TO_LOGFIRE"] = "true"
        _obs.configure_tracing(service_name="archetype-test")

        assert _obs._configured is True
        assert captured["add_baggage_to_attributes"] is False
        assert captured["inspect_arguments"] is False
        """
    )
    assert result.returncode == 0, result.stderr


def test_losing_logfire_candidate_is_shutdown_and_host_is_respected() -> None:
    result = _run(
        """
        import os
        import sys
        import types
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from archetype import _obs

        host = TracerProvider()
        candidate = TracerProvider()

        class Instance:
            _tracer_provider = candidate
            shutdown_calls = 0

            def shutdown(self, *, flush):
                assert flush is False
                self.shutdown_calls += 1

        instance = Instance()

        def configure(**kwargs):
            trace.set_tracer_provider(host)
            return instance

        sys.modules["logfire"] = types.SimpleNamespace(configure=configure)
        os.environ["LOGFIRE_SEND_TO_LOGFIRE"] = "true"
        _obs.configure_tracing(service_name="archetype-test")

        assert trace.get_tracer_provider() is host
        assert instance.shutdown_calls == 1
        assert _obs._configured is True
        """
    )
    assert result.returncode == 0, result.stderr


def test_logfire_meter_winner_is_not_shutdown_when_host_trace_wins() -> None:
    result = _run(
        """
        import os
        import sys
        import types
        from opentelemetry import metrics, trace
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.trace import TracerProvider
        from archetype import _obs

        host_trace = TracerProvider()
        candidate_trace = TracerProvider()
        candidate_meter = MeterProvider()

        class Instance:
            _tracer_provider = candidate_trace
            _meter_provider = candidate_meter
            shutdown_calls = 0

            def shutdown(self, *, flush):
                self.shutdown_calls += 1
                candidate_meter.shutdown()

        instance = Instance()

        def configure(**kwargs):
            trace.set_tracer_provider(host_trace)
            metrics.set_meter_provider(candidate_meter)
            return instance

        sys.modules["logfire"] = types.SimpleNamespace(configure=configure)
        os.environ["LOGFIRE_SEND_TO_LOGFIRE"] = "true"
        _obs.configure_tracing(service_name="archetype-test")

        assert trace.get_tracer_provider() is host_trace
        assert metrics.get_meter_provider() is candidate_meter
        assert instance.shutdown_calls == 0
        assert _obs._configured is True
        """
    )
    assert result.returncode == 0, result.stderr


def test_real_logfire_losing_logger_provider_is_explicitly_shutdown() -> None:
    result = _run(
        """
        import os
        import logfire
        from opentelemetry import _logs, metrics, trace
        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.trace import TracerProvider
        from archetype import _obs

        host_trace = TracerProvider()
        host_meter = MeterProvider()
        host_logger = LoggerProvider()
        real_configure = logfire.configure
        state = {"logger_shutdown_calls": 0}

        def configure(**kwargs):
            trace.set_tracer_provider(host_trace)
            metrics.set_meter_provider(host_meter)
            _logs.set_logger_provider(host_logger)
            kwargs["send_to_logfire"] = False
            kwargs["metrics"] = False
            instance = real_configure(**kwargs)
            candidate_logger = instance.config._logger_provider
            original_shutdown = candidate_logger.shutdown

            def counted_shutdown(*args, **inner_kwargs):
                state["logger_shutdown_calls"] += 1
                return original_shutdown(*args, **inner_kwargs)

            candidate_logger.shutdown = counted_shutdown
            return instance

        logfire.configure = configure
        os.environ["LOGFIRE_SEND_TO_LOGFIRE"] = "true"
        _obs.configure_tracing(service_name="archetype-test")

        assert trace.get_tracer_provider() is host_trace
        assert metrics.get_meter_provider() is host_meter
        assert _logs.get_logger_provider() is host_logger
        assert state["logger_shutdown_calls"] >= 1
        assert _obs._configured is True
        """
    )
    assert result.returncode == 0, result.stderr


def test_proxy_tracer_records_after_external_host_registers() -> None:
    result = _run(
        """
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
        from archetype import _obs

        _obs.configure_tracing(service_name="archetype-test")
        assert _obs._configured is False

        exporter = InMemorySpanExporter()
        host = TracerProvider()
        host.add_span_processor(SimpleSpanProcessor(exporter))
        trace.set_tracer_provider(host)

        with _obs.span("artifact.publish"):
            pass
        assert [span.name for span in exporter.get_finished_spans()] == ["artifact.publish"]
        """
    )
    assert result.returncode == 0, result.stderr


def test_noop_tracer_preserves_incoming_host_context() -> None:
    result = _run(
        """
        from opentelemetry import context, trace
        from archetype import _obs

        incoming = trace.SpanContext(
            trace_id=1,
            span_id=2,
            is_remote=True,
            trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
        )
        token = context.attach(
            trace.set_span_in_context(trace.NonRecordingSpan(incoming))
        )
        try:
            with _obs.span("artifact.publish"):
                assert trace.get_current_span().get_span_context() == incoming
        finally:
            context.detach(token)
        """
    )
    assert result.returncode == 0, result.stderr


def test_proxy_counter_created_before_provider_records_after_registration() -> None:
    result = _run(
        """
        from opentelemetry import metrics
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader
        from archetype import _obs

        _obs.counter_add(
            "archetype.operation.failures",
            attributes={
                "operation": "artifact.publish",
                "failure_disposition": "handled",
                "error_type": "internal",
                "world_id": "019b6b64-74d5-7bb0-bf10-4bdc6c5e9e31",
            },
        )

        reader = InMemoryMetricReader()
        provider = MeterProvider(metric_readers=(reader,))
        metrics.set_meter_provider(provider)

        _obs.counter_add(
            "archetype.operation.failures",
            attributes={
                "operation": "artifact.publish",
                "failure_disposition": "handled",
                "error_type": "internal",
                "world_id": "019b6b64-74d5-7bb0-bf10-4bdc6c5e9e31",
            },
        )

        data = reader.get_metrics_data()
        assert data is not None
        metric = data.resource_metrics[0].scope_metrics[0].metrics[0]
        point = metric.data.data_points[0]
        assert point.value == 1
        assert dict(point.attributes) == {
            "archetype.failure.disposition": "handled",
            "archetype.operation": "artifact.publish",
            "error.type": "internal",
        }
        """
    )
    assert result.returncode == 0, result.stderr
