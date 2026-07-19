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

"""Safe, vendor-neutral observability signals for Archetype.

Production code emits only through the OpenTelemetry API and this closed
vocabulary.  Process hosts may install an SDK/provider; without one, proxy
tracers and instruments are semantic no-ops.  Telemetry is diagnostic only:
it never owns behavior, failure classification, retry, or durable authority.

This module is imported by ``archetype.core`` and therefore cannot consume the
application redaction family.  It is safe by construction instead: unknown
names and keys are ignored, values are validated without arbitrary conversion,
and exception messages/stacks are never handed to OpenTelemetry.
"""

from __future__ import annotations

import functools
import inspect
import logging
import math
import os
import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from threading import Lock
from types import MappingProxyType
from typing import Any, Final, Literal, TypeVar, cast
from uuid import UUID

from opentelemetry import metrics, trace
from opentelemetry.trace import Status, StatusCode

logger = logging.getLogger(__name__)

SIGNAL_SCHEMA_VERSION: Final = 1

# Literal, runtime-readable vocabularies are intentionally declared here so
# the deterministic audit introduced by #514 can consume the same authority.
SPAN_NAMES: Final[frozenset[str]] = frozenset(
    {
        "artifact.index",
        "artifact.publish",
        "artifact.upload",
        "gateway.create_entity",
        "gateway.create_world",
        "gateway.get_world_info",
        "world.execute",
        "world.materialize",
        "world.query",
        "world.update",
    }
)

# ``world.execute`` and ``world.materialize`` describe legacy lazy-plan phases,
# not proven execution timing.  #518/#519 own their replacement.
LEGACY_SPAN_NAMES: Final[frozenset[str]] = frozenset(
    {
        "world.execute",
        "world.materialize",
    }
)
SPAN_NAME_ALIASES: Final[Mapping[str, str]] = MappingProxyType({})

TRACE_ATTRIBUTE_KEYS: Final[frozenset[str]] = frozenset(
    {
        "archetype.actor.id",
        "archetype.artifact.bundle.digest",
        "archetype.artifact.count",
        "archetype.attempt.digest",
        "archetype.command.id",
        "archetype.component.signature",
        "archetype.correlation.digest",
        "archetype.entity.id",
        "archetype.failure.disposition",
        "archetype.idempotency.digest",
        "archetype.operation",
        "archetype.outcome",
        "archetype.redaction.rule_ids",
        "archetype.run.id",
        "archetype.tick",
        "archetype.world.id",
        "error.type",
    }
)

TRACE_ATTRIBUTE_ALIASES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "actor_id": "archetype.actor.id",
        "artifact_count": "archetype.artifact.count",
        "attempt_digest": "archetype.attempt.digest",
        "bundle_id": "archetype.artifact.bundle.digest",
        "command_id": "archetype.command.id",
        "correlation_digest": "archetype.correlation.digest",
        "entity_id": "archetype.entity.id",
        "error_type": "error.type",
        "failure_disposition": "archetype.failure.disposition",
        "idempotency_digest": "archetype.idempotency.digest",
        "operation": "archetype.operation",
        "outcome": "archetype.outcome",
        "redaction_rule_ids": "archetype.redaction.rule_ids",
        "run_id": "archetype.run.id",
        "sig": "archetype.component.signature",
        "tick": "archetype.tick",
        "world_id": "archetype.world.id",
    }
)

METRIC_NAMES: Final[frozenset[str]] = frozenset(
    {
        "archetype.operation.failures",
        "archetype.operation.outcomes",
    }
)
METRIC_LABEL_KEYS: Final[frozenset[str]] = frozenset(
    {
        "archetype.failure.disposition",
        "archetype.operation",
        "archetype.outcome",
        "error.type",
    }
)
EVENT_NAMES: Final[frozenset[str]] = frozenset({"archetype.failure", "archetype.outcome"})
FAILURE_DISPOSITIONS: Final[frozenset[str]] = frozenset({"handled", "retrying"})
OUTCOMES: Final[frozenset[str]] = frozenset({"duplicate", "expired", "rejected", "succeeded"})
ERROR_TYPES: Final[frozenset[str]] = frozenset(
    {
        "authorization",
        "conflict",
        "internal",
        "io",
        "multiple",
        "not_found",
        "rate_limit",
        "rejected",
        "timeout",
        "unavailable",
        "validation",
    }
)

FailureDisposition = Literal["handled", "retrying"]
Outcome = Literal["duplicate", "expired", "rejected", "succeeded"]
AttributeValue = str | bool | int | float | tuple[str, ...]

_F = TypeVar("_F", bound=Callable[..., Any])
_MAX_INT = (1 << 63) - 1
_MAX_RULE_IDS = 16
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_SIGNATURE_RE = re.compile(r"a_(?:0|[1-9][0-9]{0,3})c_s[0-9a-f]{16}")
_RULE_ID_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}")
_SERVICE_NAME_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,63}")


def _canonical_span_name(name: object) -> str | None:
    if type(name) is not str:
        return None
    canonical = SPAN_NAME_ALIASES.get(name, name)
    return canonical if canonical in SPAN_NAMES else None


def _uuid(value: object) -> str | None:
    if type(value) is not str or not _UUID_RE.fullmatch(value):
        return None
    try:
        parsed = UUID(value)
    except ValueError:
        return None
    return value if str(parsed) == value else None


def _digest(value: object) -> str | None:
    if type(value) is not str or len(value) != 64:
        return None
    return value if _DIGEST_RE.fullmatch(value) else None


def _nonnegative_int(value: object) -> int | None:
    if type(value) is not int or value < 0 or value > _MAX_INT:
        return None
    return value


def _rule_ids(value: object) -> tuple[str, ...] | None:
    if type(value) is not tuple or not 0 < len(value) <= _MAX_RULE_IDS:
        return None
    if any(type(item) is not str or not _RULE_ID_RE.fullmatch(item) for item in value):
        return None
    return cast(tuple[str, ...], tuple(dict.fromkeys(value)))


def _attribute_value(key: str, value: object) -> AttributeValue | None:
    if key in {
        "archetype.actor.id",
        "archetype.command.id",
        "archetype.run.id",
        "archetype.world.id",
    }:
        return _uuid(value)
    if key in {
        "archetype.artifact.bundle.digest",
        "archetype.attempt.digest",
        "archetype.correlation.digest",
        "archetype.idempotency.digest",
    }:
        return _digest(value)
    if key in {"archetype.artifact.count", "archetype.entity.id", "archetype.tick"}:
        return _nonnegative_int(value)
    if key == "archetype.component.signature":
        return value if type(value) is str and _SIGNATURE_RE.fullmatch(value) else None
    if key == "archetype.failure.disposition":
        return value if type(value) is str and value in FAILURE_DISPOSITIONS else None
    if key == "archetype.operation":
        return _canonical_span_name(value)
    if key == "archetype.outcome":
        return value if type(value) is str and value in OUTCOMES else None
    if key == "archetype.redaction.rule_ids":
        return _rule_ids(value)
    if key == "error.type":
        return value if type(value) is str and value in ERROR_TYPES else None
    return None


def _attributes(values: Mapping[str, Any]) -> dict[str, AttributeValue]:
    """Return validated canonical trace attributes without coercion.

    Only exact dictionaries are inspected.  This prevents a custom mapping,
    key, value, ``__str__``, or iterator from running merely because telemetry
    is enabled.  Internal immutable context is copied before reaching here.
    """
    if type(values) is not dict:
        return {}

    result: dict[str, AttributeValue] = {}
    items = tuple(values.items())

    # Canonical keys win regardless of insertion order when a legacy alias is
    # supplied alongside its replacement.
    for source, value in items:
        if type(source) is not str or source not in TRACE_ATTRIBUTE_KEYS:
            continue
        normalized = _attribute_value(source, value)
        if normalized is not None:
            result[source] = normalized

    for source, value in items:
        if type(source) is not str:
            continue
        target = TRACE_ATTRIBUTE_ALIASES.get(source)
        if target is None or target in result:
            continue
        normalized = _attribute_value(target, value)
        if normalized is not None:
            result[target] = normalized
    return result


def _metric_attributes(values: Mapping[str, Any] | None) -> dict[str, AttributeValue]:
    if values is None:
        return {}
    return {key: value for key, value in _attributes(values).items() if key in METRIC_LABEL_KEYS}


_signal_context: ContextVar[Mapping[str, AttributeValue]] = ContextVar(
    "archetype_signal_context", default=MappingProxyType({})
)
_active_signal_span: ContextVar[Any | None] = ContextVar(
    "archetype_active_signal_span", default=None
)


def capture_context() -> dict[str, AttributeValue]:
    """Return a detached snapshot of the current safe correlation context."""
    try:
        return dict(_signal_context.get())
    except BaseException:
        return {}


@contextmanager
def bind_context(
    attributes: dict[str, Any] | None = None,
    **legacy_attributes: Any,
) -> Iterator[None]:
    """Bind validated correlation fields for nested spans and structured logs."""
    candidates: dict[str, Any] = {}
    if type(attributes) is dict:
        candidates.update(attributes)
    candidates.update(legacy_attributes)
    bound = capture_context()
    bound.update(_attributes(candidates))
    try:
        token = _signal_context.set(MappingProxyType(bound))
    except BaseException:
        yield None
        return
    try:
        yield None
    finally:
        try:
            _signal_context.reset(token)
        except BaseException:
            pass


# Proxy API objects resolve providers installed after this module is imported.
_tracer = trace.get_tracer("archetype")
_meter = metrics.get_meter("archetype")
_counter_lock = Lock()
_counters: dict[str, Any] = {}


def _exception_mro(value: object) -> tuple[type, ...]:
    try:
        return cast(tuple[type, ...], type.__getattribute__(type(value), "__mro__"))
    except BaseException:
        return ()


def _classify_error(error: BaseException) -> str:
    bases = _exception_mro(error)
    names: set[str] = set()
    for base in bases:
        try:
            name = type.__getattribute__(base, "__name__")
        except BaseException:
            continue
        if type(name) is str:
            names.add(name)
    if BaseExceptionGroup in bases:
        return "multiple"
    if "RateLimitError" in names:
        return "rate_limit"
    if "GuardrailError" in names or PermissionError in bases:
        return "authorization"
    if "SecretQuarantineError" in names or "PayloadRejectedError" in names:
        return "rejected"
    if "ConflictError" in names or "ClaimConflictError" in names:
        return "conflict"
    if "AvailabilityError" in names or ConnectionError in bases:
        return "unavailable"
    if TimeoutError in bases:
        return "timeout"
    if "WorldNotFoundError" in names or FileNotFoundError in bases or LookupError in bases:
        return "not_found"
    if TypeError in bases or ValueError in bases:
        return "validation"
    if OSError in bases:
        return "io"
    return "internal"


def _mark_propagated_failure(active: Any, error: Exception) -> None:
    category = _classify_error(error)
    try:
        active.set_attribute("error.type", category)
    except BaseException:
        pass
    try:
        active.set_status(Status(StatusCode.ERROR))
    except BaseException:
        pass


@contextmanager
def span(
    name: str,
    attributes: object = None,
    **legacy_attributes: Any,
) -> Iterator[None]:
    """Open one safe span, or execute as a no-op when telemetry fails.

    The current OTel context receives only a read-only non-recording view, so
    child spans retain parent correlation without exposing the mutable span.
    OpenTelemetry never observes an application exception, so it cannot record
    a message or stack.  Callers receive no raw span handle.
    """
    canonical_name = _canonical_span_name(name)
    if canonical_name is None:
        yield None
        return

    candidates = capture_context()
    if type(attributes) is dict:
        candidates.update(cast(dict[str, Any], attributes))
    candidates.update(legacy_attributes)
    safe_attributes = _attributes(candidates)

    active: Any = None
    current_manager: Any = None
    current_entered = False
    active_token: Any = None
    try:
        active = _tracer.start_span(
            canonical_name,
            attributes=safe_attributes,
        )
    except BaseException:
        active = None
    if active is not None:
        active_context: Any = None
        try:
            candidate_context = active.get_span_context()
            if candidate_context.is_valid:
                active_context = candidate_context
        except BaseException:
            active_context = None
        if active_context is not None:
            try:
                safe_current = trace.NonRecordingSpan(active_context)
                current_manager = trace.use_span(
                    safe_current,
                    end_on_exit=False,
                    record_exception=False,
                    set_status_on_exception=False,
                )
                current_manager.__enter__()
                current_entered = True
            except BaseException:
                current_manager = None
            try:
                active_token = _active_signal_span.set(active)
            except BaseException:
                active_token = None

    try:
        yield None
    except Exception as error:
        if active is not None:
            _mark_propagated_failure(active, error)
        raise
    finally:
        if active_token is not None:
            try:
                _active_signal_span.reset(active_token)
            except BaseException:
                pass
        if current_entered and current_manager is not None:
            try:
                current_manager.__exit__(None, None, None)
            except BaseException:
                pass
        if active is not None:
            try:
                active.end()
            except BaseException:
                pass


def instrument(name: str) -> Callable[[_F], _F]:
    """Wrap a sync or async callable in one safe span."""

    def decorate(fn: _F) -> _F:
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                with span(name):
                    return await fn(*args, **kwargs)

            return cast(_F, async_wrapper)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with span(name):
                return fn(*args, **kwargs)

        return cast(_F, wrapper)

    return decorate


def counter_add(
    name: str,
    amount: int | float = 1,
    *,
    attributes: dict[str, Any] | None = None,
) -> None:
    """Add to one approved counter with bounded labels, otherwise no-op."""
    if type(name) is not str or name not in METRIC_NAMES:
        return
    if type(amount) is int:
        valid_amount = 0 < amount <= _MAX_INT
    elif type(amount) is float:
        valid_amount = math.isfinite(amount) and 0 < amount <= float(_MAX_INT)
    else:
        valid_amount = False
    if not valid_amount:
        return

    try:
        counter = _counters.get(name)
        if counter is None:
            with _counter_lock:
                counter = _counters.get(name)
                if counter is None:
                    counter = _meter.create_counter(name)
                    _counters[name] = counter
        counter.add(amount, attributes=_metric_attributes(attributes))
    except BaseException:
        # A failed creation is retryable; do not retain a partially initialized
        # instrument.  An add failure leaves a valid proxy/SDK counter cached.
        if name not in _counters:
            return


def _safe_event(name: str, attributes: dict[str, Any]) -> None:
    if type(name) is not str or name not in EVENT_NAMES:
        return
    try:
        active = _active_signal_span.get()
        if active is not None:
            active.add_event(name, attributes=_attributes(attributes))
    except BaseException:
        pass


def record_failure(error: BaseException, *, disposition: FailureDisposition) -> None:
    """Record a handled/retrying failure without failing the enclosing span."""
    if (
        Exception not in _exception_mro(error)
        or type(disposition) is not str
        or disposition not in FAILURE_DISPOSITIONS
    ):
        return
    category = _classify_error(error)
    attributes = {
        "error.type": category,
        "archetype.failure.disposition": disposition,
    }
    _safe_event("archetype.failure", attributes)
    counter_add("archetype.operation.failures", attributes=attributes)


def record_outcome(outcome: Outcome, *, operation: str | None = None) -> None:
    """Record a bounded advisory outcome; durable records remain authoritative."""
    if type(outcome) is not str or outcome not in OUTCOMES:
        return
    attributes: dict[str, Any] = {"archetype.outcome": outcome}
    if operation is not None:
        attributes["archetype.operation"] = operation
    _safe_event("archetype.outcome", attributes)
    counter_add("archetype.operation.outcomes", attributes=attributes)


# ─────────────────────────────────────────────────────────────────────────────
# Provider setup — explicit process-host adapter, never an import side effect
# ─────────────────────────────────────────────────────────────────────────────

_configuration_lock = Lock()
_configured = False
_owned_tracer_provider: object | None = None


def _warn_fixed(message: str) -> None:
    try:
        logger.warning(message)
    except BaseException:
        pass


def _safe_shutdown(provider: Any) -> None:
    try:
        provider.shutdown()
    except BaseException:
        pass


def _logfire_provider_state(instance: Any) -> tuple[object | None, bool, bool]:
    """Settle Logfire provider races without shutting down a winning signal."""
    try:
        installed_trace: object | None = trace.get_tracer_provider()
    except BaseException:
        installed_trace = None
    if instance is None:
        return installed_trace, False, False

    candidate_trace: Any = None
    candidate_meter: Any = None
    candidate_logger: Any = None
    installed_meter: Any = None
    installed_logger: Any = None
    try:
        candidate_trace = getattr(instance, "_tracer_provider", None)
    except BaseException:
        pass
    try:
        candidate_meter = getattr(instance, "_meter_provider", None)
        installed_meter = metrics.get_meter_provider()
    except BaseException:
        pass
    try:
        from opentelemetry import _logs

        config = getattr(instance, "config", None)
        candidate_logger = getattr(instance, "_logger_provider", None) or getattr(
            config, "_logger_provider", None
        )
        installed_logger = _logs.get_logger_provider()
    except BaseException:
        pass

    trace_won = candidate_trace is not None and installed_trace is candidate_trace
    meter_won = candidate_meter is not None and installed_meter is candidate_meter
    logger_won = candidate_logger is not None and installed_logger is candidate_logger
    any_won = trace_won or meter_won or logger_won

    if not any_won:
        try:
            instance.shutdown(flush=False)
        except BaseException:
            pass
    for candidate, won in (
        (candidate_trace, trace_won),
        (candidate_meter, meter_won),
        (candidate_logger, logger_won),
    ):
        if candidate is not None and not won:
            _safe_shutdown(candidate)
    return installed_trace, trace_won, any_won


def _install_candidate(provider: Any) -> bool:
    """Install our candidate or discard it if an external host won the race."""
    global _owned_tracer_provider
    try:
        trace.set_tracer_provider(cast(trace.TracerProvider, provider))
    except BaseException:
        _safe_shutdown(provider)
        return False

    try:
        installed = trace.get_tracer_provider()
    except BaseException:
        _safe_shutdown(provider)
        return False
    if installed is provider:
        _owned_tracer_provider = provider
        return True
    _safe_shutdown(provider)
    return type(installed) is not trace.ProxyTracerProvider


def configure_tracing(*, service_name: str, debug_console: bool = False) -> None:
    """Configure tracing from an explicit process host.

    A prior host provider is never replaced or shut down.  With no requested
    backend this leaves the API proxy in place and, importantly, does not latch:
    a later runtime or external host can still install a provider.  Adapter
    failures emit only fixed diagnostics and remain retryable.
    """
    global _configured
    safe_service_name = (
        service_name
        if type(service_name) is str and _SERVICE_NAME_RE.fullmatch(service_name)
        else "archetype"
    )

    with _configuration_lock:
        if _configured:
            return
        try:
            existing = trace.get_tracer_provider()
        except BaseException:
            return
        if type(existing) is not trace.ProxyTracerProvider:
            _configured = True
            return

        has_token = bool(os.environ.get("LOGFIRE_TOKEN") or os.environ.get("LOGFIRE_API_KEY"))
        send_env = os.environ.get("LOGFIRE_SEND_TO_LOGFIRE", "").lower()
        if has_token or send_env in {"1", "true", "yes"}:
            logfire_instance: Any = None
            try:
                import logfire

                logfire_instance = logfire.configure(
                    service_name=safe_service_name,
                    send_to_logfire=True,
                    console=None if debug_console else False,
                    add_baggage_to_attributes=False,
                    inspect_arguments=False,
                )
            except ImportError:
                _warn_fixed(
                    "Logfire telemetry was requested but its optional adapter is unavailable."
                )
            except BaseException:
                _warn_fixed("Logfire telemetry initialization failed; tracing remains retryable.")
            else:
                installed, trace_won, any_won = _logfire_provider_state(logfire_instance)
                if trace_won:
                    _configured = True
                    return
                if installed is not None and type(installed) is not trace.ProxyTracerProvider:
                    _configured = True
                    return

        otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or os.environ.get(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
        )
        if otlp_endpoint:
            candidate: object | None = None
            try:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                    OTLPSpanExporter,
                )
                from opentelemetry.sdk.resources import Resource
                from opentelemetry.sdk.trace import TracerProvider
                from opentelemetry.sdk.trace.export import BatchSpanProcessor

                candidate = TracerProvider(resource=Resource({"service.name": safe_service_name}))
                processor = BatchSpanProcessor(OTLPSpanExporter())
                candidate.add_span_processor(
                    _filtered_processor(processor, service_name=safe_service_name)
                )
                if debug_console:
                    candidate.add_span_processor(_console_processor(service_name=safe_service_name))
            except ImportError:
                if candidate is not None:
                    _safe_shutdown(candidate)
                _warn_fixed("OTLP telemetry was requested but its optional adapter is unavailable.")
            except BaseException:
                if candidate is not None:
                    _safe_shutdown(candidate)
                _warn_fixed("OTLP telemetry initialization failed; tracing remains retryable.")
            else:
                if _install_candidate(candidate):
                    _configured = True
                    return

        if debug_console:
            candidate = None
            try:
                from opentelemetry.sdk.resources import Resource
                from opentelemetry.sdk.trace import TracerProvider

                candidate = TracerProvider(resource=Resource({"service.name": safe_service_name}))
                candidate.add_span_processor(_console_processor(service_name=safe_service_name))
            except BaseException:
                if candidate is not None:
                    _safe_shutdown(candidate)
                _warn_fixed("Console telemetry initialization failed; tracing remains retryable.")
            else:
                if _install_candidate(candidate):
                    _configured = True


def _safe_span_context(value: object) -> Any | None:
    """Clone IDs and sampling only, omitting arbitrary trace-state values."""
    from opentelemetry.trace import SpanContext, TraceFlags, TraceState

    if type(value) is not SpanContext or not value.is_valid:
        return None
    sampled = TraceFlags.SAMPLED if value.trace_flags.sampled else TraceFlags.DEFAULT
    return SpanContext(
        trace_id=value.trace_id,
        span_id=value.span_id,
        is_remote=value.is_remote,
        trace_flags=TraceFlags(sampled),
        trace_state=TraceState(),
    )


def _sanitized_sdk_span(value: object, *, service_name: str) -> Any | None:
    """Return an exporter-safe immutable SDK snapshot, or drop the span."""
    from opentelemetry.attributes import BoundedAttributes
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import Event, ReadableSpan
    from opentelemetry.sdk.util import BoundedList
    from opentelemetry.sdk.util.instrumentation import InstrumentationScope

    if type(value) is not ReadableSpan:
        return None
    try:
        scope = value.instrumentation_scope
        if type(scope) is not InstrumentationScope or scope.name != "archetype":
            return None
        canonical_name = _canonical_span_name(value.name)
        context = _safe_span_context(value.context)
        if canonical_name is None or context is None:
            return None
        if type(value.start_time) is not int or type(value.end_time) is not int:
            return None
        if not 0 <= value.start_time <= value.end_time <= _MAX_INT:
            return None

        parent = None
        if value.parent is not None:
            parent = _safe_span_context(value.parent)
            if parent is None:
                return None

        attributes = _attributes(dict(value.attributes or {}))
        events: list[Event] = []
        for event in value.events:
            if (
                type(event) is not Event
                or type(event.name) is not str
                or event.name not in EVENT_NAMES
            ):
                continue
            if type(event.timestamp) is not int or not 0 <= event.timestamp <= _MAX_INT:
                continue
            event_attributes = _attributes(dict(event.attributes or {}))
            events.append(
                Event(
                    event.name,
                    attributes=BoundedAttributes(attributes=event_attributes),
                    timestamp=event.timestamp,
                )
            )

        status_code = value.status.status_code
        if status_code not in {StatusCode.UNSET, StatusCode.OK, StatusCode.ERROR}:
            status_code = StatusCode.UNSET
        safe_service_name = (
            service_name
            if type(service_name) is str and _SERVICE_NAME_RE.fullmatch(service_name)
            else "archetype"
        )
        return ReadableSpan(
            name=canonical_name,
            context=context,
            parent=parent,
            resource=Resource({"service.name": safe_service_name}),
            attributes=BoundedAttributes(attributes=attributes),
            events=BoundedList.from_seq(128, events),
            links=BoundedList.from_seq(128, ()),
            kind=trace.SpanKind.INTERNAL,
            instrumentation_info=None,
            status=Status(status_code),
            start_time=value.start_time,
            end_time=value.end_time,
            instrumentation_scope=InstrumentationScope("archetype"),
        )
    except BaseException:
        return None


def _approved_sdk_span(value: object) -> bool:
    try:
        scope = getattr(value, "instrumentation_scope", None)
        return (
            getattr(scope, "name", None) == "archetype"
            and _canonical_span_name(getattr(value, "name", None)) is not None
        )
    except BaseException:
        return False


def _filtered_processor(processor: Any, *, service_name: str = "archetype") -> Any:
    """Snapshot safe Archetype spans before any owned enqueue or export."""
    from opentelemetry.sdk.trace import SpanProcessor

    class _ArchetypeSpanProcessor(SpanProcessor):
        def on_start(self, span: Any, parent_context: Any = None) -> None:
            # BatchSpanProcessor and SimpleSpanProcessor have no start work.
            # Never expose their wrapped processor to a mutable SDK span.
            return None

        def on_end(self, span: Any) -> None:
            safe_span = _sanitized_sdk_span(span, service_name=service_name)
            if safe_span is not None:
                try:
                    processor.on_end(safe_span)
                except BaseException:
                    pass

        def shutdown(self) -> None:
            try:
                processor.shutdown()
            except BaseException:
                pass

        def force_flush(self, timeout_millis: int = 30_000) -> bool:
            try:
                return bool(processor.force_flush(timeout_millis))
            except BaseException:
                return False

    return _ArchetypeSpanProcessor()


def _console_processor(*, service_name: str = "archetype") -> Any:
    """Return a safe, Archetype-only one-line console span processor."""
    import sys

    from opentelemetry.sdk.trace import ReadableSpan
    from opentelemetry.sdk.trace.export import (
        SimpleSpanProcessor,
        SpanExporter,
        SpanExportResult,
    )

    class _OneLineExporter(SpanExporter):
        def export(self, spans: Any) -> SpanExportResult:
            try:
                for finished in spans:
                    if type(finished) is not ReadableSpan or not _approved_sdk_span(finished):
                        continue
                    name = _canonical_span_name(finished.name)
                    if name is None:
                        continue
                    duration_ms = ((finished.end_time or 0) - (finished.start_time or 0)) / 1e6
                    attributes = _attributes(dict(finished.attributes or {}))
                    rendered = " ".join(f"{key}={value}" for key, value in attributes.items())
                    print(
                        f"· {name} {duration_ms:.1f}ms {rendered}".rstrip(),
                        file=sys.stderr,
                    )
            except BaseException:
                return SpanExportResult.FAILURE
            return SpanExportResult.SUCCESS

        def shutdown(self) -> None:
            return None

    return _filtered_processor(SimpleSpanProcessor(_OneLineExporter()), service_name=service_name)
