# Copyright 2026 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Archetype's observability boundary: vanilla OpenTelemetry, no vendor.

Every span archetype emits goes through this module and the OpenTelemetry
*API* only. With no SDK configured the API is a no-op, so `import archetype`
costs nothing and requires no telemetry vendor. Any OTel SDK that registers
the global tracer provider — the runtime's own setup, a host application's,
or Logfire's (which is an OTel SDK) — receives the same spans.

Nothing under ``src/`` may import ``logfire``; the only logfire-aware code
paths are the optional backend selection in :func:`configure_tracing` (via
importlib, never a hard import) and ``contrib/logfire_observer.py``, which
is explicitly a Logfire integration.
"""

from __future__ import annotations

import functools
import inspect
import logging
import os
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any, TypeVar, cast

from opentelemetry import trace

logger = logging.getLogger(__name__)

# The proxy tracer resolves the global provider at span time, so module
# import order never matters: configure_tracing (or a host SDK) can run
# before or after any instrumented module is imported.
_tracer = trace.get_tracer("archetype")

_F = TypeVar("_F", bound=Callable[..., Any])

# OTel attribute values must be str/bool/int/float or sequences thereof.
_ATTR_TYPES = (str, bool, int, float)


def _attributes(values: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value if isinstance(value, _ATTR_TYPES) else str(value)
        for key, value in values.items()
        if value is not None
    }


@contextmanager
def span(name: str, **attributes: Any) -> Iterator[trace.Span]:
    """Open one span around a block. No-op without a configured SDK."""
    with _tracer.start_as_current_span(name, attributes=_attributes(attributes)) as active:
        yield active


def instrument(name: str) -> Callable[[_F], _F]:
    """Wrap a sync or async callable in one span of the given name."""

    def decorate(fn: _F) -> _F:
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                with _tracer.start_as_current_span(name):
                    return await fn(*args, **kwargs)

            return cast(_F, async_wrapper)  # wraps preserves the signature

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with _tracer.start_as_current_span(name):
                return fn(*args, **kwargs)

        return cast(_F, wrapper)  # wraps preserves the signature

    return decorate


# ─────────────────────────────────────────────────────────────────────────────
# Provider setup — called once, by the runtime (the script boundary)
# ─────────────────────────────────────────────────────────────────────────────

_configured = False


def configure_tracing(*, service_name: str, debug_console: bool = False) -> None:
    """Register a global tracer provider for this process, once.

    Backend selection, in order:

    1. A host application already registered a provider → respect it and do
       nothing. Archetype is a library; the application owns the pipeline.
    2. Logfire installed AND explicitly opted into (``LOGFIRE_TOKEN`` /
       ``LOGFIRE_API_KEY`` / ``LOGFIRE_SEND_TO_LOGFIRE`` truthy) →
       ``logfire.configure()``. Logfire is an OTel SDK: it registers the
       global provider and every archetype span lands there.
    3. ``OTEL_EXPORTER_OTLP_ENDPOINT`` / ``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT``
       set → SDK provider with the standard OTLP/HTTP exporter (which reads
       the ``OTEL_*`` environment variables itself). Works with any
       collector: Jaeger, Grafana, Honeycomb, a Logfire endpoint, ...
    4. ``debug_console`` (ARCHETYPE_LOG=debug) → SDK provider with a terse
       one-line console exporter on stderr, keeping stdout for the script.
    5. Otherwise → leave the no-op OTel API in place: zero overhead, zero
       output.
    """
    global _configured
    if _configured:
        return
    _configured = True

    if not isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider):
        return  # path 1: the application already owns the pipeline

    has_token = bool(os.environ.get("LOGFIRE_TOKEN") or os.environ.get("LOGFIRE_API_KEY"))
    send_env = os.environ.get("LOGFIRE_SEND_TO_LOGFIRE", "").lower()
    if has_token or send_env in ("1", "true", "yes"):
        try:
            import logfire

            logfire.configure(
                service_name=service_name,
                send_to_logfire=True,
                console=None if debug_console else False,
            )
            return  # path 2
        except ImportError:
            logger.warning(
                "LOGFIRE_* environment set but logfire is not installed; "
                "install archetype-ecs[logfire] to send there. Falling back."
            )

    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or os.environ.get(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
    )
    if otlp_endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
            if debug_console:
                provider.add_span_processor(_console_processor())
            trace.set_tracer_provider(provider)
            return  # path 3
        except ImportError:
            logger.warning(
                "OTEL_EXPORTER_OTLP_*ENDPOINT set but no OTLP exporter is "
                "installed; install archetype-ecs[otlp]. Falling back."
            )

    if debug_console:
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
        provider.add_span_processor(_console_processor())
        trace.set_tracer_provider(provider)  # path 4
    # path 5: no-op API stays


def _console_processor():
    """One line per span on stderr: name, duration, attributes."""
    import sys

    from opentelemetry.sdk.trace import ReadableSpan
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult

    class _OneLineExporter(SpanExporter):
        def export(self, spans: Any) -> SpanExportResult:
            for s in spans:
                assert isinstance(s, ReadableSpan)
                dur_ms = ((s.end_time or 0) - (s.start_time or 0)) / 1e6
                attrs = " ".join(f"{k}={v}" for k, v in (s.attributes or {}).items())
                print(f"· {s.name} {dur_ms:.1f}ms {attrs}".rstrip(), file=sys.stderr)
            return SpanExportResult.SUCCESS

        def shutdown(self) -> None:
            return None

    return SimpleSpanProcessor(_OneLineExporter())
