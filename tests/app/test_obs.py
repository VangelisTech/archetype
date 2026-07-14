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

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from uuid_utils import uuid7

from archetype import _obs
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.core.config import WorldConfig


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


def test_span_coerces_attributes_and_drops_none(monkeypatch):
    exporter = _capture(monkeypatch)

    class Opaque:
        def __str__(self) -> str:
            return "opaque-repr"

    with _obs.span("unit.attrs", sig="a_1", tick=3, obj=Opaque(), skipped=None):
        pass

    (span,) = exporter.get_finished_spans()
    assert span.name == "unit.attrs"
    assert span.attributes == {"sig": "a_1", "tick": 3, "obj": "opaque-repr"}


def test_gate_operations_emit_otel_spans(monkeypatch):
    """The gate's @instrument decorators ride any OTel provider — no vendor."""
    exporter = _capture(monkeypatch)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    async def drive() -> None:
        c = ServiceContainer()
        try:
            info = await c.command_service.create_world(ctx, WorldConfig(name="obs"), None, None)
            await c.command_service.get_world_info(ctx, info.world_id)
        finally:
            await c.shutdown()

    asyncio.run(drive())

    names = {s.name for s in exporter.get_finished_spans()}
    assert "gate.create_world" in names
    assert "gate.get_world_info" in names


def test_console_processor_prints_one_line_per_span(monkeypatch, capsys):
    provider = TracerProvider()
    provider.add_span_processor(_obs._console_processor())
    monkeypatch.setattr(_obs, "_tracer", provider.get_tracer("archetype"))

    with _obs.span("debug.line", tick=7):
        pass

    err = capsys.readouterr().err
    assert "debug.line" in err and "tick=7" in err
    assert "\n" == err[-1] and err.count("debug.line") == 1
