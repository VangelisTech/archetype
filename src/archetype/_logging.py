# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Private, host-owned stdlib logging configuration.

Library and family modules only create normal stdlib log records. Explicit
process hosts may install the one handler in this module; imports never do.
Correlation is advisory and fail-open, while durable records and typed
application outcomes remain authoritative.
"""

from __future__ import annotations

import logging
import os
from threading import Lock
from typing import Final

from opentelemetry import trace

from archetype import _obs

_LOG_LEVELS: Final[dict[str, int]] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}

# The #513 trace vocabulary remains the sole attribute authority. Trace/span
# IDs are log correlation coordinates layered on top; they are never metric
# labels. Callers cannot forge any reserved field through ``extra``: the owned
# filter clears them all, then restores only trusted current values.
LOG_RECORD_FIELDS: Final[frozenset[str]] = _obs.TRACE_ATTRIBUTE_KEYS | frozenset(
    {"span_id", "trace_id"}
)

_configuration_lock = Lock()
_HANDLER_MARKER = "archetype.host.v1"
_OMITTED_ARGUMENT = "<value omitted>"
_SAFE_ARGUMENT_TYPES: Final[frozenset[type]] = frozenset({bool, float, int, str, type(None)})


def resolve_log_level(env: str | None = None) -> int | None:
    """Map an explicit value or ``ARCHETYPE_LOG`` to one stdlib level."""
    try:
        value = env if env is not None else os.environ.get("ARCHETYPE_LOG", "")
        if type(value) is not str:
            return None
        return _LOG_LEVELS.get(value.strip().lower())
    except BaseException:
        return None


class _CorrelationFilter(logging.Filter):
    """Replace reserved fields with trusted current correlation coordinates."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            fields = vars(record)
            for key in LOG_RECORD_FIELDS:
                fields.pop(key, None)
        except BaseException:
            return True

        try:
            current = trace.get_current_span().get_span_context()
            if type(current) is trace.SpanContext and current.is_valid:
                fields["trace_id"] = f"{current.trace_id:032x}"
                fields["span_id"] = f"{current.span_id:016x}"
        except BaseException:
            pass

        try:
            captured = _obs.capture_context()
            safe = _obs._attributes(captured) if type(captured) is dict else {}
            for key, value in safe.items():
                if key in LOG_RECORD_FIELDS:
                    fields[key] = value
        except BaseException:
            pass
        return True


def _safe_arguments(value: object) -> tuple[object, ...] | dict[str, object]:
    """Copy exact logging args without invoking arbitrary object rendering."""

    def safe(item: object) -> object:
        return item if type(item) in _SAFE_ARGUMENT_TYPES else _OMITTED_ARGUMENT

    if type(value) is tuple:
        return tuple(safe(item) for item in value)
    if type(value) is dict:
        return {key: safe(item) for key, item in value.items() if type(key) is str}
    return ()


class _SafeFormatter(logging.Formatter):
    """Format diagnostics without traceback text or arbitrary object calls."""

    def format(self, record: logging.LogRecord) -> str:
        sentinel = object()
        fields: dict[str, object]
        originals: dict[str, object] = {}
        try:
            fields = vars(record)
            for key in ("args", "exc_info", "exc_text", "msg", "stack_info"):
                originals[key] = fields.get(key, sentinel)
            message = fields.get("msg")
            fields["msg"] = message if type(message) is str else "<unsafe log message omitted>"
            fields["args"] = _safe_arguments(fields.get("args"))
            fields["exc_info"] = None
            fields["exc_text"] = None
            fields["stack_info"] = None
            return super().format(record)
        finally:
            try:
                for key, original in originals.items():
                    if original is sentinel:
                        fields.pop(key, None)
                    else:
                        fields[key] = original
            except BaseException:
                pass


class _ArchetypeHandler(logging.StreamHandler):
    """Marker for the sole stderr handler owned by an Archetype host."""

    def __init__(self) -> None:
        super().__init__()
        self._archetype_handler_marker = _HANDLER_MARKER
        self.setFormatter(_SafeFormatter("%(levelname).1s %(name)s: %(message)s"))
        self.addFilter(_CorrelationFilter())

    def emit(self, record: logging.LogRecord) -> None:
        """Write one diagnostic, dropping only handler/formatting failures."""
        try:
            message = self.format(record)
            stream = self.stream
            stream.write(message + self.terminator)
            self.flush()
        except BaseException:
            pass


class _ArchetypeNullHandler(logging.NullHandler):
    """Suppress ``lastResort`` output until a host explicitly enables logs."""

    def __init__(self) -> None:
        super().__init__()
        self._archetype_handler_marker = _HANDLER_MARKER


def _owned_handler_kind(handler: object) -> str | None:
    """Classify this adapter's handler across an in-process module reload."""
    try:
        handler_type = type(handler)
        if (
            type.__getattribute__(handler_type, "__module__") != __name__
            or vars(handler).get("_archetype_handler_marker") != _HANDLER_MARKER
        ):
            return None
        return {
            "_ArchetypeHandler": "stderr",
            "_ArchetypeNullHandler": "null",
        }.get(type.__getattribute__(handler_type, "__qualname__"))
    except BaseException:
        return None


def _is_owned_handler(handler: object) -> bool:
    """Return whether a handler belongs to this private host adapter."""
    return _owned_handler_kind(handler) is not None


def configure_archetype_logging(level: int | None) -> None:
    """Idempotently install one enabled or quiet package-owned handler."""
    if level is not None and (type(level) is not int or level not in _LOG_LEVELS.values()):
        return

    try:
        with _configuration_lock:
            package = logging.getLogger("archetype")
            previous_level = package.level
            previous_propagate = package.propagate
            previous_handlers = tuple(package.handlers)
            candidate: logging.Handler | None = None
            replaced: logging.Handler | None = None
            duplicates: list[logging.Handler] = []
            try:
                recognized = [handler for handler in package.handlers if _is_owned_handler(handler)]
                if level is None:
                    owned = next(
                        (
                            handler
                            for handler in recognized
                            if _owned_handler_kind(handler) == "stderr"
                        ),
                        recognized[0] if recognized else None,
                    )
                else:
                    owned = recognized[0] if recognized else None
                duplicates = [handler for handler in recognized if handler is not owned]
                desired_type: type[logging.Handler] = (
                    _ArchetypeNullHandler if level is None else _ArchetypeHandler
                )
                if owned is None:
                    candidate = desired_type()
                    package.addHandler(candidate)
                    owned = candidate
                elif (level is not None or _owned_handler_kind(owned) == "null") and type(
                    owned
                ) is not desired_type:
                    candidate = desired_type()
                    owned_index = next(
                        index for index, handler in enumerate(package.handlers) if handler is owned
                    )
                    package.handlers[owned_index] = candidate
                    replaced = owned
                    owned = candidate
                for duplicate in duplicates:
                    duplicate_index = next(
                        index
                        for index, handler in enumerate(package.handlers)
                        if handler is duplicate
                    )
                    package.handlers.pop(duplicate_index)
                if level is not None:
                    owned_index = next(
                        index for index, handler in enumerate(package.handlers) if handler is owned
                    )
                    if owned_index:
                        package.handlers.insert(0, package.handlers.pop(owned_index))
                    package.setLevel(level)
                    package.propagate = False
            except BaseException:
                try:
                    package.handlers[:] = previous_handlers
                except BaseException:
                    pass
                try:
                    package.setLevel(previous_level)
                    package.propagate = previous_propagate
                except BaseException:
                    pass
                if candidate is not None:
                    try:
                        candidate.close()
                    except BaseException:
                        pass
            else:
                for obsolete in ([replaced] if replaced is not None else []) + duplicates:
                    try:
                        obsolete.close()
                    except BaseException:
                        pass
    except BaseException:
        pass


def configure_host_observability(*, service_name: str, log: str | None = None) -> int | None:
    """Configure logging/tracing only when called by an explicit process host."""
    level = resolve_log_level(log)
    try:
        configure_archetype_logging(level)
    except BaseException:
        pass
    try:
        _obs.configure_tracing(
            service_name=service_name,
            debug_console=level == logging.DEBUG,
        )
    except BaseException:
        pass
    return level
