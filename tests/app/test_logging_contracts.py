# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Structured stdlib logging and explicit-host ownership contracts."""

from __future__ import annotations

import io
import logging
import sys
import types
from typing import cast

import pytest
from fastapi.testclient import TestClient
from opentelemetry import context, trace
from uuid_utils import uuid7

from archetype import _logging, _obs

pytestmark = pytest.mark.contract("observability.logging.correlated")


def _record() -> logging.LogRecord:
    return logging.LogRecord(
        name="archetype.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="fixed diagnostic",
        args=(),
        exc_info=None,
    )


def test_log_record_vocabulary_is_immutable_and_exact() -> None:
    assert _logging.LOG_RECORD_FIELDS == frozenset(
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
            "span_id",
            "trace_id",
        }
    )


def test_logging_configuration_rejects_hostile_or_unsupported_inputs(monkeypatch) -> None:
    assert _logging.resolve_log_level(cast(str, object())) is None

    class FailingLevels(dict[str, int]):
        def get(self, key: str, default: int | None = None) -> int | None:
            raise KeyboardInterrupt("level lookup must not escape")

    monkeypatch.setattr(_logging, "_LOG_LEVELS", FailingLevels())
    assert _logging.resolve_log_level("info") is None
    assert _logging._is_owned_handler(object()) is False

    package = logging.getLogger("archetype")
    before = (tuple(package.handlers), package.level, package.propagate)
    _logging.configure_archetype_logging(cast(int, True))
    _logging.configure_archetype_logging(1_000_000)
    assert (tuple(package.handlers), package.level, package.propagate) == before


def test_filter_replaces_forged_fields_from_trusted_current_context() -> None:
    record = _record()
    for field in _logging.LOG_RECORD_FIELDS:
        record.__dict__[field] = "forged"

    world_id = str(uuid7())
    run_id = str(uuid7())
    correlation_digest = "a" * 64

    class Secret:
        def __str__(self) -> str:
            raise AssertionError("logging correlation must not stringify payloads")

        def __repr__(self) -> str:
            raise AssertionError("logging correlation must not render payloads")

    span_context = trace.SpanContext(
        trace_id=0x1234,
        span_id=0x5678,
        is_remote=False,
        trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
    )
    token = context.attach(trace.set_span_in_context(trace.NonRecordingSpan(span_context)))
    try:
        with _obs.bind_context(
            {
                "archetype.world.id": world_id,
                "archetype.run.id": run_id,
                "archetype.correlation.digest": correlation_digest,
                "archetype.operation": "artifact.publish",
                "archetype.outcome": Secret(),
                "payload": Secret(),
            }
        ):
            assert _logging._CorrelationFilter().filter(record) is True
    finally:
        context.detach(token)

    assert record.trace_id == f"{span_context.trace_id:032x}"
    assert record.span_id == f"{span_context.span_id:016x}"
    assert record.__dict__["archetype.world.id"] == world_id
    assert record.__dict__["archetype.run.id"] == run_id
    assert record.__dict__["archetype.correlation.digest"] == correlation_digest
    assert record.__dict__["archetype.operation"] == "artifact.publish"
    assert "archetype.outcome" not in record.__dict__
    assert "archetype.actor.id" not in record.__dict__


def test_filter_removes_forged_fields_without_valid_context() -> None:
    record = _record()
    for field in _logging.LOG_RECORD_FIELDS:
        record.__dict__[field] = "forged"

    assert _logging._CorrelationFilter().filter(record) is True
    assert not (_logging.LOG_RECORD_FIELDS & record.__dict__.keys())


def test_filter_is_fail_open_when_context_providers_fail(monkeypatch) -> None:
    record = _record()
    record.__dict__["trace_id"] = "forged"
    record.__dict__["archetype.world.id"] = "forged"

    def fail_span():
        raise KeyboardInterrupt("Bearer should-not-be-exported")

    def fail_context():
        raise SystemExit("Bearer should-not-be-exported")

    monkeypatch.setattr(_logging.trace, "get_current_span", fail_span)
    monkeypatch.setattr(_logging._obs, "capture_context", fail_context)

    assert _logging._CorrelationFilter().filter(record) is True
    assert record.getMessage() == "fixed diagnostic"
    assert "trace_id" not in record.__dict__
    assert "archetype.world.id" not in record.__dict__


def test_owned_handler_omits_exception_text_and_arbitrary_object_strings() -> None:
    stream = io.StringIO()
    handler = _logging._ArchetypeHandler()
    handler.setStream(stream)

    class Secret:
        def __str__(self) -> str:
            raise AssertionError("owned logging must not render arbitrary objects")

        def __repr__(self) -> str:
            raise AssertionError("owned logging must not represent arbitrary objects")

    try:
        raise RuntimeError("Bearer raw-exception-must-not-be-exported")
    except RuntimeError:
        exception_record = logging.LogRecord(
            name="archetype.test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="fixed failure diagnostic",
            args=(),
            exc_info=sys.exc_info(),
        )
    object_record = logging.LogRecord(
        name="archetype.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="unsafe argument %s",
        args=(Secret(),),
        exc_info=None,
    )
    mapping_record = logging.LogRecord(
        name="archetype.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="mapping %(safe)s %(unsafe)s",
        args={"safe": 7, "unsafe": Secret(), 1: Secret()},
        exc_info=None,
    )

    handler.handle(exception_record)
    handler.handle(object_record)
    handler.handle(mapping_record)
    handler.close()

    assert stream.getvalue() == (
        "E archetype.test: fixed failure diagnostic\n"
        "I archetype.test: unsafe argument <value omitted>\n"
        "I archetype.test: mapping 7 <value omitted>\n"
    )


def test_owned_handler_failure_does_not_escape() -> None:
    class FailingStream:
        def write(self, value: str) -> None:
            raise KeyboardInterrupt("handler output failure must not escape")

        def flush(self) -> None:
            raise SystemExit("handler flush failure must not escape")

    handler = _logging._ArchetypeHandler()
    handler.stream = FailingStream()
    handler.handle(_record())
    handler.close()


def test_partial_handler_install_rolls_back_under_the_configuration_lock(
    monkeypatch,
) -> None:
    package = logging.getLogger("archetype")
    package_state = (tuple(package.handlers), package.level, package.propagate)
    for handler in list(package.handlers):
        package.removeHandler(handler)

    def add_then_fail(handler: logging.Handler) -> None:
        package.handlers.append(handler)
        raise KeyboardInterrupt("partial install")

    monkeypatch.setattr(package, "addHandler", add_then_fail)
    try:
        _logging.configure_archetype_logging(logging.INFO)
        assert package.handlers == []
        assert package.level == package_state[1]
        assert package.propagate is package_state[2]
    finally:
        package.handlers.clear()
        package.handlers.extend(package_state[0])
        package.setLevel(package_state[1])
        package.propagate = package_state[2]


def test_stale_and_duplicate_owned_handlers_are_replaced_atomically() -> None:
    package = logging.getLogger("archetype")
    package_state = (tuple(package.handlers), package.level, package.propagate)
    for handler in list(package.handlers):
        package.removeHandler(handler)

    stale_type = type(
        "_ArchetypeHandler",
        (logging.StreamHandler,),
        {"__module__": _logging.__name__},
    )
    stale = stale_type(io.StringIO())
    duplicate = stale_type(io.StringIO())
    stale._archetype_handler_marker = _logging._HANDLER_MARKER
    duplicate._archetype_handler_marker = _logging._HANDLER_MARKER
    foreign = logging.NullHandler()
    package.handlers.extend([foreign, stale, duplicate])
    try:
        _logging.configure_archetype_logging(logging.INFO)
        owned = [handler for handler in package.handlers if _logging._is_owned_handler(handler)]
        assert len(owned) == 1
        assert type(owned[0]) is _logging._ArchetypeHandler
        assert package.handlers == [owned[0], foreign]
        assert stale._closed is True
        assert duplicate._closed is True
    finally:
        for handler in list(package.handlers):
            package.removeHandler(handler)
            if handler not in package_state[0]:
                handler.close()
        package.handlers.extend(package_state[0])
        package.setLevel(package_state[1])
        package.propagate = package_state[2]


def test_configuration_owns_only_its_package_handler() -> None:
    root = logging.getLogger()
    package = logging.getLogger("archetype")
    root_state = (tuple(root.handlers), tuple(root.filters), root.level)
    factory = logging.getLogRecordFactory()
    package_state = (
        tuple(package.handlers),
        tuple(package.filters),
        package.level,
        package.propagate,
    )
    for handler in list(package.handlers):
        package.removeHandler(handler)
    for existing_filter in list(package.filters):
        package.removeFilter(existing_filter)

    foreign = logging.StreamHandler(io.StringIO())
    foreign_filter = logging.Filter("foreign")
    foreign.addFilter(foreign_filter)
    package.addHandler(foreign)
    try:
        _logging.configure_archetype_logging(logging.INFO)
        _logging.configure_archetype_logging(logging.DEBUG)

        owned = [handler for handler in package.handlers if _logging._is_owned_handler(handler)]
        assert len(owned) == 1
        assert package.handlers[0] is owned[0]
        assert package.level == logging.DEBUG
        assert package.propagate is False
        assert foreign.filters == [foreign_filter]
        assert len(owned[0].filters) == 1
        assert type(owned[0].filters[0]) is _logging._CorrelationFilter
        assert (tuple(root.handlers), tuple(root.filters), root.level) == root_state
        assert logging.getLogRecordFactory() is factory
    finally:
        for handler in list(package.handlers):
            package.removeHandler(handler)
            if handler not in package_state[0]:
                handler.close()
        for handler in package_state[0]:
            package.addHandler(handler)
        for existing_filter in package_state[1]:
            package.addFilter(existing_filter)
        package.setLevel(package_state[2])
        package.propagate = package_state[3]


def test_quiet_host_owns_only_a_null_handler() -> None:
    root = logging.getLogger()
    package = logging.getLogger("archetype")
    root_state = (tuple(root.handlers), tuple(root.filters), root.level)
    factory = logging.getLogRecordFactory()
    package_state = (tuple(package.handlers), package.level, package.propagate)
    for handler in list(package.handlers):
        package.removeHandler(handler)
    package.setLevel(logging.ERROR)
    package.propagate = False

    try:
        _logging.configure_archetype_logging(None)
        owned = [handler for handler in package.handlers if _logging._is_owned_handler(handler)]
        assert len(owned) == 1
        assert type(owned[0]) is _logging._ArchetypeNullHandler
        assert package.handlers == owned
        assert package.level == logging.ERROR
        assert package.propagate is False
        assert (tuple(root.handlers), tuple(root.filters), root.level) == root_state
        assert logging.getLogRecordFactory() is factory
    finally:
        for handler in list(package.handlers):
            package.removeHandler(handler)
            if handler not in package_state[0]:
                handler.close()
        package.handlers.extend(package_state[0])
        package.setLevel(package_state[1])
        package.propagate = package_state[2]


def test_quiet_host_preserves_explicit_root_capture() -> None:
    root = logging.getLogger()
    package = logging.getLogger("archetype")
    root_state = (tuple(root.handlers), root.level)
    package_state = (tuple(package.handlers), package.level, package.propagate)
    for handler in list(package.handlers):
        package.removeHandler(handler)

    captured: list[logging.LogRecord] = []

    class CaptureHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    capture = CaptureHandler()
    root.addHandler(capture)
    root.setLevel(logging.WARNING)
    package.setLevel(logging.NOTSET)
    package.propagate = True
    try:
        _logging.configure_archetype_logging(None)
        logging.getLogger("archetype.test").warning("host-owned capture")
    finally:
        root.removeHandler(capture)
        root.handlers[:] = root_state[0]
        root.setLevel(root_state[1])
        for handler in list(package.handlers):
            package.removeHandler(handler)
            if handler not in package_state[0]:
                handler.close()
        package.handlers.extend(package_state[0])
        package.setLevel(package_state[1])
        package.propagate = package_state[2]

    assert [record.getMessage() for record in captured] == ["host-owned capture"]


def test_quiet_setup_enables_but_never_downgrades_owned_stderr() -> None:
    package = logging.getLogger("archetype")
    package_state = (tuple(package.handlers), package.level, package.propagate)
    for handler in list(package.handlers):
        package.removeHandler(handler)

    try:
        _logging.configure_archetype_logging(None)
        quiet = next(handler for handler in package.handlers if _logging._is_owned_handler(handler))
        assert type(quiet) is _logging._ArchetypeNullHandler

        _logging.configure_archetype_logging(logging.INFO)
        enabled = next(
            handler for handler in package.handlers if _logging._is_owned_handler(handler)
        )
        assert type(enabled) is _logging._ArchetypeHandler
        assert enabled is not quiet
        assert quiet._closed is True

        _logging.configure_archetype_logging(None)
        assert package.handlers == [enabled]
        assert package.level == logging.INFO
        assert package.propagate is False
    finally:
        for handler in list(package.handlers):
            package.removeHandler(handler)
            if handler not in package_state[0]:
                handler.close()
        package.handlers.extend(package_state[0])
        package.setLevel(package_state[1])
        package.propagate = package_state[2]


def test_host_package_handler_receives_trusted_enriched_record() -> None:
    package = logging.getLogger("archetype")
    package_state = (tuple(package.handlers), package.level, package.propagate)
    for handler in list(package.handlers):
        package.removeHandler(handler)

    captured: list[dict[str, object]] = []

    class CaptureHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(dict(vars(record)))

    foreign = CaptureHandler()
    package.addHandler(foreign)
    world_id = str(uuid7())
    span_context = trace.SpanContext(
        trace_id=0x1234,
        span_id=0x5678,
        is_remote=False,
        trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
    )
    token = context.attach(trace.set_span_in_context(trace.NonRecordingSpan(span_context)))
    try:
        _logging.configure_archetype_logging(logging.INFO)
        owned = next(handler for handler in package.handlers if _logging._is_owned_handler(handler))
        owned.setStream(io.StringIO())
        assert package.handlers == [owned, foreign]
        with _obs.bind_context({"archetype.world.id": world_id}):
            logging.getLogger("archetype.test").info("fixed diagnostic")
    finally:
        context.detach(token)
        for handler in list(package.handlers):
            package.removeHandler(handler)
            if handler not in package_state[0]:
                handler.close()
        package.handlers.extend(package_state[0])
        package.setLevel(package_state[1])
        package.propagate = package_state[2]

    assert len(captured) == 1
    assert captured[0]["trace_id"] == f"{span_context.trace_id:032x}"
    assert captured[0]["span_id"] == f"{span_context.span_id:016x}"
    assert captured[0]["archetype.world.id"] == world_id


def test_host_configuration_is_a_semantic_noop_when_setup_fails(monkeypatch) -> None:
    def fail_logging(level: int) -> None:
        raise KeyboardInterrupt("logging setup should-not-escape")

    def fail_tracing(**kwargs: object) -> None:
        raise SystemExit("tracing setup should-not-escape")

    monkeypatch.setattr(_logging, "configure_archetype_logging", fail_logging)
    monkeypatch.setattr(_logging._obs, "configure_tracing", fail_tracing)

    assert (
        _logging.configure_host_observability(
            service_name="archetype-test",
            log="info",
        )
        == logging.INFO
    )


def test_api_factory_defers_host_configuration_to_each_lifespan(monkeypatch) -> None:
    from archetype.api import app as api_app

    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        api_app,
        "configure_host_observability",
        lambda **kwargs: calls.append(kwargs),
    )

    first = api_app.create_app()
    second = api_app.create_app()
    assert calls == []
    with TestClient(first):
        pass
    with TestClient(second):
        pass

    assert calls == [
        {"service_name": "archetype-api"},
        {"service_name": "archetype-api"},
    ]


def test_cli_serve_configures_the_server_host_before_uvicorn(monkeypatch) -> None:
    from archetype.cli import main as cli

    events: list[tuple[str, object]] = []
    monkeypatch.setattr(
        cli,
        "configure_host_observability",
        lambda **kwargs: events.append(("configure", kwargs)),
    )
    monkeypatch.setitem(
        sys.modules,
        "uvicorn",
        types.SimpleNamespace(run=lambda *args, **kwargs: events.append(("run", (args, kwargs)))),
    )

    cli.serve(host="127.0.0.1", port=8123, reload=False)

    assert events == [
        ("configure", {"service_name": "archetype-api"}),
        (
            "run",
            (
                ("archetype.api.app:create_app",),
                {"host": "127.0.0.1", "port": 8123, "reload": False, "factory": True},
            ),
        ),
    ]
