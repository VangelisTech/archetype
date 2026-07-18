# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Vendor-neutral observability boundary (archetype._obs).

Archetype emits through the OpenTelemetry API only: a no-op without any
SDK, and any registered provider — bundled, host, or Logfire's — receives
the same spans. Tests inject a local tracer rather than registering a
global provider, so no test poisons the process-wide OTel state.
"""

import asyncio
import inspect
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, cast

import pytest
from opentelemetry import context, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, Status, StatusCode
from uuid_utils import uuid7

from archetype import _obs
from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.models import ActorCtx
from archetype.core.config import WorldConfig

pytestmark = pytest.mark.contract("observability.signals.safe")


def _capture(monkeypatch) -> InMemorySpanExporter:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(_obs, "_tracer", provider.get_tracer("archetype"))
    return exporter


def test_instrument_is_a_noop_without_any_provider():
    @_obs.instrument("noop.sync")
    def add(a: int, b: int) -> int:
        return a + b

    @_obs.instrument("noop.async")
    async def mul(a: int, b: int) -> int:
        return a * b

    assert add(2, 3) == 5
    assert asyncio.run(mul(2, 3)) == 6
    assert inspect.iscoroutinefunction(mul), "async-ness must survive instrumentation"
    assert add.__name__ == "add" and mul.__name__ == "mul"


def test_span_accepts_only_key_specific_safe_attributes(monkeypatch):
    exporter = _capture(monkeypatch)
    world_id = str(uuid7())
    idempotency_key = "Bearer should-not-be-exported"
    bundle_digest = "0123456789abcdef" * 4

    class Opaque:
        def __str__(self) -> str:
            raise AssertionError("telemetry must not stringify arbitrary objects")

    with _obs.span(
        "world.query",
        sig="a_1c_s0123456789abcdef",
        tick=3,
        world_id=world_id,
        idempotency_key=idempotency_key,
        bundle_id=bundle_digest,
        obj=Opaque(),
        password="not-exported",
        skipped=None,
    ) as active:
        assert active is None, "callers must not receive a raw OTel span handle"
        pass

    (span,) = exporter.get_finished_spans()
    assert span.name == "world.query"
    assert span.attributes == {
        "archetype.artifact.bundle.digest": bundle_digest,
        "archetype.component.signature": "a_1c_s0123456789abcdef",
        "archetype.tick": 3,
        "archetype.world.id": world_id,
    }


def test_invalid_names_and_values_are_safe_noops(monkeypatch):
    exporter = _capture(monkeypatch)

    with _obs.span("dynamic.secret.Bearer-token", tick=True, world_id="not-a-uuid"):
        result = "application-result"

    assert result == "application-result"
    assert exporter.get_finished_spans() == ()


def test_keyword_attribute_mapping_and_legacy_keywords_are_both_supported(monkeypatch):
    exporter = _capture(monkeypatch)
    world_id = str(uuid7())

    with _obs.span("world.query", attributes={"world_id": world_id}, tick=4):
        pass

    (finished,) = exporter.get_finished_spans()
    assert finished.attributes == {
        "archetype.tick": 4,
        "archetype.world.id": world_id,
    }


def test_current_span_is_read_only_but_nested_spans_keep_parentage(monkeypatch):
    exporter = _capture(monkeypatch)

    with _obs.span("artifact.publish"):
        visible = trace.get_current_span()
        assert isinstance(visible, trace.NonRecordingSpan)
        visible.set_attribute("password", "Bearer should-not-be-exported")
        visible.add_event("exception", {"exception.message": "Bearer should-not-be-exported"})
        with _obs.span("artifact.index"):
            pass

    inner, outer = exporter.get_finished_spans()
    assert inner.name == "artifact.index"
    assert outer.name == "artifact.publish"
    assert inner.parent == outer.context
    assert outer.attributes == {}
    assert outer.events == ()


def test_propagated_failure_records_only_bounded_error_type(monkeypatch):
    exporter = _capture(monkeypatch)

    class SecretError(RuntimeError):
        def __str__(self) -> str:
            return "Bearer should-not-be-exported"

    error = SecretError()
    with pytest.raises(SecretError) as captured:
        with _obs.span("artifact.publish"):
            raise error

    assert captured.value is error
    (finished,) = exporter.get_finished_spans()
    assert finished.status.status_code is StatusCode.ERROR
    assert finished.status.description is None
    assert finished.attributes == {"error.type": "internal"}
    assert finished.events == ()
    assert "Bearer" not in repr(finished.to_json())


def test_exception_class_metadata_cannot_replace_application_exception(monkeypatch):
    exporter = _capture(monkeypatch)

    class HostileMeta(type):
        def __getattribute__(cls, name):
            if name == "__name__":
                raise RuntimeError("secret class metadata")
            return super().__getattribute__(name)

    class HostileError(RuntimeError, metaclass=HostileMeta):
        def __str__(self):
            raise RuntimeError("secret string")

        def __repr__(self):
            raise RuntimeError("secret representation")

    error = HostileError()
    with pytest.raises(HostileError) as captured:
        with _obs.span("artifact.publish"):
            raise error

    assert captured.value is error
    (finished,) = exporter.get_finished_spans()
    assert finished.attributes == {"error.type": "internal"}


def test_exception_instance_metadata_cannot_replace_application_exception(monkeypatch):
    exporter = _capture(monkeypatch)

    class HostileError(RuntimeError):
        def __getattribute__(self, name):
            if name == "__class__":
                raise RuntimeError("telemetry replacement")
            return super().__getattribute__(name)

    error = HostileError()
    with pytest.raises(HostileError) as captured:
        with _obs.span("artifact.publish"):
            raise error

    assert captured.value is error
    (finished,) = exporter.get_finished_spans()
    assert finished.attributes == {"error.type": "internal"}

    with _obs.span("artifact.upload"):
        _obs.record_failure(error, disposition="handled")


def test_handled_and_retried_failures_do_not_mark_successful_span_failed(monkeypatch):
    exporter = _capture(monkeypatch)

    class SecretError(RuntimeError):
        def __str__(self) -> str:
            raise AssertionError("failure telemetry must not inspect exception messages")

    with _obs.span("artifact.upload"):
        _obs.record_failure(SecretError(), disposition="handled")
        _obs.record_failure(TimeoutError(), disposition="retrying")

    (finished,) = exporter.get_finished_spans()
    assert finished.status.status_code is StatusCode.UNSET
    assert [event.name for event in finished.events] == [
        "archetype.failure",
        "archetype.failure",
    ]
    assert [dict(event.attributes or {}) for event in finished.events] == [
        {"archetype.failure.disposition": "handled", "error.type": "internal"},
        {"archetype.failure.disposition": "retrying", "error.type": "timeout"},
    ]


def test_cancellation_propagates_without_error_telemetry(monkeypatch):
    exporter = _capture(monkeypatch)
    cancellation = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError) as captured:
        with _obs.span("artifact.index"):
            raise cancellation

    assert captured.value is cancellation
    (finished,) = exporter.get_finished_spans()
    assert finished.status.status_code is StatusCode.UNSET
    assert finished.attributes == {}
    assert finished.events == ()


def test_context_binding_is_nested_safe_and_restored(monkeypatch):
    exporter = _capture(monkeypatch)
    world_id = str(uuid7())
    run_id = str(uuid7())
    correlation = "arbitrary possibly secret correlation value"
    correlation_digest = "abcdef0123456789" * 4

    assert _obs.capture_context() == {}
    with _obs.bind_context(
        world_id=world_id,
        correlation_id=correlation,
        correlation_digest=correlation_digest,
        payload=object(),
    ):
        assert _obs.capture_context() == {
            "archetype.correlation.digest": correlation_digest,
            "archetype.world.id": world_id,
        }
        with _obs.bind_context(run_id=run_id):
            assert _obs.capture_context() == {
                "archetype.correlation.digest": correlation_digest,
                "archetype.run.id": run_id,
                "archetype.world.id": world_id,
            }
        assert _obs.capture_context() == {
            "archetype.correlation.digest": correlation_digest,
            "archetype.world.id": world_id,
        }
        with pytest.raises(ValueError):
            with _obs.bind_context(command_id=str(uuid7())):
                raise ValueError("application failure")
        assert _obs.capture_context() == {
            "archetype.correlation.digest": correlation_digest,
            "archetype.world.id": world_id,
        }
        with _obs.span("gate.get_world_info"):
            pass
    assert _obs.capture_context() == {}

    (finished,) = exporter.get_finished_spans()
    assert finished.attributes == {
        "archetype.correlation.digest": correlation_digest,
        "archetype.world.id": world_id,
    }


def test_signal_emission_failure_never_changes_application_behavior(monkeypatch):
    class BrokenTracer:
        def start_span(self, *args, **kwargs):
            raise RuntimeError("provider diagnostic with secret")

    monkeypatch.setattr(_obs, "_tracer", BrokenTracer())

    with _obs.span("artifact.publish"):
        result = 42

    assert result == 42


def test_provider_control_flow_failures_cannot_replace_application_control_flow(monkeypatch):
    provider_interrupt = KeyboardInterrupt("provider only")

    class StartFailureTracer:
        def start_span(self, *args, **kwargs):
            raise provider_interrupt

    monkeypatch.setattr(_obs, "_tracer", StartFailureTracer())
    with _obs.span("artifact.publish"):
        result = "body-ran"
    assert result == "body-ran"

    class EndFailureActive:
        def get_span_context(self):
            return trace.INVALID_SPAN_CONTEXT

        def end(self):
            raise KeyboardInterrupt("provider cleanup only")

    class EndFailureTracer:
        def start_span(self, *args, **kwargs):
            return EndFailureActive()

    application_cancellation = asyncio.CancelledError()
    monkeypatch.setattr(_obs, "_tracer", EndFailureTracer())
    with pytest.raises(asyncio.CancelledError) as captured:
        with _obs.span("artifact.publish"):
            raise application_cancellation
    assert captured.value is application_cancellation


def test_signal_context_and_end_failures_are_semantic_noops(monkeypatch):
    class BrokenActive:
        def get_span_context(self):
            return trace.SpanContext(
                trace_id=1,
                span_id=2,
                is_remote=False,
                trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
            )

        def end(self):
            raise RuntimeError("secret end diagnostic")

    class BrokenTracer:
        def start_span(self, *args, **kwargs):
            return BrokenActive()

    class BrokenManager:
        def __enter__(self):
            raise RuntimeError("secret context diagnostic")

        def __exit__(self, *exc_info):
            raise RuntimeError("secret exit diagnostic")

    monkeypatch.setattr(_obs, "_tracer", BrokenTracer())
    monkeypatch.setattr(_obs.trace, "use_span", lambda *args, **kwargs: BrokenManager())

    with _obs.span("artifact.publish"):
        result = 3

    assert result == 3


def test_status_failures_cannot_replace_application_exception(monkeypatch):
    application_error = ValueError("application secret")

    class BrokenActive:
        def get_span_context(self):
            return trace.INVALID_SPAN_CONTEXT

        def set_attribute(self, *args, **kwargs):
            raise RuntimeError("attribute secret")

        def set_status(self, *args, **kwargs):
            raise RuntimeError("status secret")

        def end(self):
            raise RuntimeError("exit secret")

    class Tracer:
        def start_span(self, *args, **kwargs):
            return BrokenActive()

    monkeypatch.setattr(_obs, "_tracer", Tracer())

    with pytest.raises(ValueError) as captured:
        with _obs.span("artifact.publish"):
            raise application_error
    assert captured.value is application_error


def test_counter_creation_and_add_failures_are_semantic_noops(monkeypatch):
    class BrokenMeter:
        def create_counter(self, name):
            raise RuntimeError("counter creation secret")

    monkeypatch.setattr(_obs, "_meter", BrokenMeter())
    monkeypatch.setattr(_obs, "_counters", {})
    _obs.counter_add("archetype.operation.failures")

    class BrokenCounter:
        def add(self, amount, *, attributes):
            raise KeyboardInterrupt("counter add control flow")

    monkeypatch.setattr(_obs, "_counters", {"archetype.operation.failures": BrokenCounter()})
    _obs.counter_add(
        "archetype.operation.failures",
        attributes={"world_id": str(uuid7()), "error_type": "internal"},
    )

    class BrokenEvent:
        def add_event(self, *args, **kwargs):
            raise KeyboardInterrupt("event control flow")

    token = _obs._active_signal_span.set(BrokenEvent())
    try:
        _obs.record_failure(RuntimeError(), disposition="handled")
    finally:
        _obs._active_signal_span.reset(token)


def test_losing_provider_candidate_is_shutdown_without_touching_host(monkeypatch):
    class Candidate:
        def __init__(self) -> None:
            self.shutdown_calls = 0

        def shutdown(self) -> None:
            self.shutdown_calls += 1

    class Host:
        def __init__(self) -> None:
            self.shutdown_calls = 0

        def shutdown(self) -> None:
            self.shutdown_calls += 1

    candidate = Candidate()
    host = Host()
    monkeypatch.setattr(_obs.trace, "set_tracer_provider", lambda provider: None)
    monkeypatch.setattr(_obs.trace, "get_tracer_provider", lambda: host)
    monkeypatch.setattr(_obs, "_owned_tracer_provider", None)

    assert _obs._install_candidate(candidate) is True
    assert candidate.shutdown_calls == 1
    assert host.shutdown_calls == 0
    assert _obs._owned_tracer_provider is None


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ({"tick": True, "entity_id": -1, "artifact_count": 1 << 63}, {}),
        ({"world_id": "NOT-A-UUID", "run_id": str(uuid7()).upper()}, {}),
        ({"attempt_id": "attempt-1", "idempotency_key": "publish-1"}, {}),
        (
            {
                "attempt_id": "a" * 64,
                "correlation_id": "b" * 64,
                "idempotency_key": "c" * 64,
                "sig": "a_0001c_s0123456789abcdef",
            },
            {},
        ),
        (
            {
                "redaction_rule_ids": ("provider.openai", "provider.openai"),
                "sig": "a_2c_s0123456789abcdef",
            },
            {
                "archetype.component.signature": "a_2c_s0123456789abcdef",
                "archetype.redaction.rule_ids": ("provider.openai",),
            },
        ),
        (
            {"sig": "a_0c_s0123456789abcdef"},
            {"archetype.component.signature": "a_0c_s0123456789abcdef"},
        ),
    ],
)
def test_attribute_validators_are_exact_and_bounded(values, expected):
    assert _obs._attributes(values) == expected


def test_hostile_mapping_proxy_and_malformed_unicode_are_omitted(monkeypatch):
    class HostileMapping(Mapping):
        def __getitem__(self, key):
            raise AssertionError("telemetry must not inspect a hostile mapping")

        def __iter__(self):
            raise AssertionError("telemetry must not iterate a hostile mapping")

        def __len__(self):
            raise AssertionError("telemetry must not size a hostile mapping")

        def items(self):
            raise AssertionError("telemetry must not call hostile mapping methods")

    proxy = MappingProxyType(HostileMapping())
    assert _obs._attributes(proxy) == {}

    exporter = _capture(monkeypatch)
    malformed = "\ud800" * 64
    with _obs.bind_context(bundle_id=malformed):
        assert _obs.capture_context() == {}
        with _obs.span("artifact.publish", bundle_id=malformed):
            result = "unchanged"

    assert result == "unchanged"
    (finished,) = exporter.get_finished_spans()
    assert finished.attributes == {}


def test_signal_vocabulary_is_immutable_and_namespaced():
    assert isinstance(_obs.SPAN_NAMES, frozenset)
    assert isinstance(_obs.LEGACY_SPAN_NAMES, frozenset)
    assert isinstance(_obs.TRACE_ATTRIBUTE_KEYS, frozenset)
    assert isinstance(_obs.METRIC_NAMES, frozenset)
    assert isinstance(_obs.METRIC_LABEL_KEYS, frozenset)
    assert isinstance(_obs.EVENT_NAMES, frozenset)
    assert isinstance(_obs.SPAN_NAME_ALIASES, MappingProxyType)
    assert isinstance(_obs.TRACE_ATTRIBUTE_ALIASES, MappingProxyType)
    assert all(name.startswith("archetype.") for name in _obs.METRIC_NAMES)
    assert all(
        key.startswith("archetype.") or key == "error.type" for key in _obs.TRACE_ATTRIBUTE_KEYS
    )
    assert {
        "archetype.world.id",
        "archetype.run.id",
        "archetype.entity.id",
        "archetype.attempt.digest",
        "archetype.idempotency.digest",
    }.isdisjoint(_obs.METRIC_LABEL_KEYS)


def test_outcome_helper_is_bounded_and_advisory(monkeypatch):
    exporter = _capture(monkeypatch)

    with _obs.span("artifact.publish"):
        _obs.record_outcome("duplicate", operation="artifact.publish")
        _obs.record_outcome(cast(Any, "user-controlled"), operation="secret.dynamic")

    (finished,) = exporter.get_finished_spans()
    assert finished.status.status_code is StatusCode.UNSET
    assert [event.name for event in finished.events] == ["archetype.outcome"]
    assert dict(finished.events[0].attributes or {}) == {
        "archetype.operation": "artifact.publish",
        "archetype.outcome": "duplicate",
    }


def test_gate_operations_emit_otel_spans(monkeypatch):
    """The gate's @instrument decorators ride any OTel provider — no vendor."""
    exporter = _capture(monkeypatch)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    async def drive() -> None:
        c = ServiceContainer()
        try:
            info = await c.command_gateway.create_world(ctx, WorldConfig(name="obs"), None, None)
            await c.command_gateway.get_world_info(ctx, info.world_id)
        finally:
            await c.shutdown()

    asyncio.run(drive())

    names = {s.name for s in exporter.get_finished_spans()}
    assert "gateway.create_world" in names
    assert "gateway.get_world_info" in names


def test_console_processor_prints_one_line_per_span(monkeypatch, capsys):
    provider = TracerProvider()
    provider.add_span_processor(_obs._console_processor())
    monkeypatch.setattr(_obs, "_tracer", provider.get_tracer("archetype"))

    with _obs.span("world.query", tick=7):
        pass

    err = capsys.readouterr().err
    assert "world.query" in err and "archetype.tick=7" in err
    assert "\n" == err[-1] and err.count("world.query") == 1


def test_console_processor_drops_foreign_secret_bearing_spans(capsys):
    provider = TracerProvider()
    provider.add_span_processor(_obs._console_processor())
    foreign = provider.get_tracer("foreign")

    with foreign.start_as_current_span(
        "foreign.request", attributes={"password": "Bearer should-not-be-exported"}
    ):
        pass

    assert capsys.readouterr().err == ""


def test_owned_processor_snapshots_same_scope_spans_before_export():
    secret = "Bearer should-not-be-exported"
    exporter = InMemorySpanExporter()
    provider = TracerProvider(
        resource=Resource({"service.name": "unsafe-host", "password": secret})
    )
    provider.add_span_processor(
        _obs._filtered_processor(SimpleSpanProcessor(exporter), service_name="archetype-test")
    )
    direct = provider.get_tracer(
        "archetype", instrumenting_library_version=secret, schema_url=f"https://{secret}"
    )
    parent_context = trace.SpanContext(
        trace_id=1,
        span_id=2,
        is_remote=True,
        trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
        trace_state=trace.TraceState((("vendor", "secret-trace-state"),)),
    )
    link_context = trace.SpanContext(trace_id=3, span_id=4, is_remote=True)
    token = context.attach(trace.set_span_in_context(trace.NonRecordingSpan(parent_context)))
    try:
        with direct.start_as_current_span(
            "artifact.publish",
            attributes={"archetype.tick": 5, "password": secret},
            links=(trace.Link(link_context, {"password": secret}),),
            kind=SpanKind.CLIENT,
        ) as active:
            active.set_status(Status(StatusCode.ERROR, secret))
            active.add_event("exception", {"exception.message": secret})
            active.add_event(
                "archetype.failure",
                {"error.type": "timeout", "password": secret},
            )
    finally:
        context.detach(token)

    (finished,) = exporter.get_finished_spans()
    assert finished.name == "artifact.publish"
    assert finished.attributes == {"archetype.tick": 5}
    assert finished.kind is SpanKind.INTERNAL
    assert finished.status.status_code is StatusCode.ERROR
    assert finished.status.description is None
    assert finished.resource.attributes == {"service.name": "archetype-test"}
    scope = finished.instrumentation_scope
    assert scope is not None
    assert scope.name == "archetype"
    assert scope.version is None
    assert not scope.schema_url
    assert finished.links == ()
    assert finished.context.trace_state == trace.TraceState()
    assert finished.parent is not None
    assert finished.parent.trace_state == trace.TraceState()
    assert [event.name for event in finished.events] == ["archetype.failure"]
    assert dict(finished.events[0].attributes or {}) == {"error.type": "timeout"}
    assert hasattr(finished._attributes, "dropped")
    assert hasattr(finished._events, "dropped")
    assert hasattr(finished.events[0]._attributes, "dropped")
    assert secret not in finished.to_json()


def test_owned_processor_drops_malformed_same_scope_span_timestamps():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(_obs._filtered_processor(SimpleSpanProcessor(exporter)))
    direct = provider.get_tracer("archetype")

    class HostileTimestamp:
        def __repr__(self):
            raise AssertionError("export must not render a hostile timestamp")

    active = direct.start_span("artifact.publish", start_time=cast(Any, HostileTimestamp()))
    active.end(end_time=cast(Any, HostileTimestamp()))

    assert exporter.get_finished_spans() == ()
