# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fresh-process quiet-import and stdout/stderr logging contracts."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest

pytestmark = [
    pytest.mark.contract("observability.logging.correlated"),
    pytest.mark.process,
]


def _run(source: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    for key in tuple(env):
        if key.startswith(("LOGFIRE_", "OTEL_")) or key == "ARCHETYPE_LOG":
            env.pop(key)
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def test_imports_do_not_mutate_logging_or_telemetry() -> None:
    result = _run(
        """
        import logging
        import sys
        from opentelemetry import _logs, metrics, trace

        root = logging.getLogger()
        package = logging.getLogger("archetype")
        root_state = (tuple(root.handlers), tuple(root.filters), root.level, root.disabled)
        package_state = (
            tuple(package.handlers),
            tuple(package.filters),
            package.level,
            package.propagate,
            package.disabled,
        )
        manager_disabled = logging.root.manager.disable
        factory = logging.getLogRecordFactory()
        trace_provider = trace.get_tracer_provider()
        meter_provider = metrics.get_meter_provider()
        logger_provider = _logs.get_logger_provider()

        import archetype
        import archetype.runtime.runtime
        import archetype.api.app

        assert (
            tuple(root.handlers),
            tuple(root.filters),
            root.level,
            root.disabled,
        ) == root_state
        assert (
            tuple(package.handlers),
            tuple(package.filters),
            package.level,
            package.propagate,
            package.disabled,
        ) == package_state
        assert logging.root.manager.disable == manager_disabled
        assert logging.getLogRecordFactory() is factory
        assert trace.get_tracer_provider() is trace_provider
        assert metrics.get_meter_provider() is meter_provider
        assert _logs.get_logger_provider() is logger_provider
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_api_factory_does_not_instrument_or_warn_about_logfire() -> None:
    result = _run(
        """
        import warnings

        from archetype.api.app import create_app
        import logfire

        calls = []
        logfire.instrument_fastapi = lambda *args, **kwargs: calls.append((args, kwargs))

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            create_app()
        assert calls == []
        assert not [warning for warning in caught if "logfire" in str(warning.message).lower()]
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_private_observability_boundary_import_is_quiet_and_vendor_free() -> None:
    result = _run(
        """
        import sys

        import archetype._obs

        assert "logfire" not in sys.modules
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_explicit_host_configuration_is_reload_safe() -> None:
    result = _run(
        """
        import importlib
        import logging

        import archetype._logging as host_logging

        package = logging.getLogger("archetype")
        host_logging.configure_archetype_logging(logging.INFO)
        before = [
            handler for handler in package.handlers
            if host_logging._is_owned_handler(handler)
        ]
        assert len(before) == 1

        host_logging = importlib.reload(host_logging)
        host_logging.configure_archetype_logging(logging.DEBUG)
        after = [
            handler for handler in package.handlers
            if host_logging._is_owned_handler(handler)
        ]
        assert len(after) == 1
        assert after[0] is not before[0]
        assert type(after[0]) is host_logging._ArchetypeHandler
        assert type(after[0].formatter) is host_logging._SafeFormatter
        assert len(after[0].filters) == 1
        assert type(after[0].filters[0]) is host_logging._CorrelationFilter
        assert package.level == logging.DEBUG
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_quiet_host_configuration_is_reload_safe() -> None:
    result = _run(
        """
        import importlib
        import logging

        import archetype._logging as host_logging

        package = logging.getLogger("archetype")
        host_logging.configure_archetype_logging(None)
        before = [
            handler for handler in package.handlers
            if host_logging._is_owned_handler(handler)
        ]
        assert len(before) == 1
        assert type(before[0]) is host_logging._ArchetypeNullHandler

        host_logging = importlib.reload(host_logging)
        host_logging.configure_archetype_logging(None)
        after = [
            handler for handler in package.handlers
            if host_logging._is_owned_handler(handler)
        ]
        assert len(after) == 1
        assert after[0] is not before[0]
        assert type(after[0]) is host_logging._ArchetypeNullHandler
        assert before[0]._closed is True
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_runtime_without_log_suppresses_last_resort_output() -> None:
    result = _run(
        """
        import asyncio
        import logging

        from archetype import ArchetypeRuntime

        async def main():
            async with ArchetypeRuntime():
                logging.getLogger("archetype.test").warning("must stay quiet")

        asyncio.run(main())
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_enabled_host_logs_use_stderr_and_leave_stdout_machine_readable() -> None:
    result = _run(
        """
        import logging
        from archetype._logging import configure_host_observability

        configure_host_observability(service_name="archetype-test", log="info")
        logging.getLogger("archetype.test").info("diagnostic %s", 7)
        print('{"result":7}')
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == '{"result":7}\n'
    assert result.stderr == "I archetype.test: diagnostic 7\n"
