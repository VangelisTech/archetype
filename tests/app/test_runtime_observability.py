# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Runtime observability contract (runtime.md R13).

Scripts own stdout: the explicit runtime host keeps package diagnostics quiet
unless ARCHETYPE_LOG / ArchetypeRuntime(log=...) enables its stderr handler.
"""

import logging

import pytest

from archetype._logging import configure_archetype_logging, resolve_log_level


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("debug", logging.DEBUG),
        ("INFO", logging.INFO),
        (" warning ", logging.WARNING),
        ("error", logging.ERROR),
        ("", None),
        ("verbose", None),
    ],
)
def test_resolve_log_level(value, expected):
    assert resolve_log_level(value) == expected


def test_env_var_is_the_default_source(monkeypatch):
    monkeypatch.setenv("ARCHETYPE_LOG", "info")
    assert resolve_log_level() == logging.INFO
    monkeypatch.delenv("ARCHETYPE_LOG")
    assert resolve_log_level() is None


def test_configure_wires_package_logger_idempotently():
    pkg = logging.getLogger("archetype")
    before_handlers = list(pkg.handlers)
    before_level = pkg.level
    before_propagate = pkg.propagate
    try:
        configure_archetype_logging(logging.INFO)
        configure_archetype_logging(logging.DEBUG)
        added = [h for h in pkg.handlers if h not in before_handlers]
        assert len(added) <= 1, "repeat configuration must not stack handlers"
        assert pkg.level == logging.DEBUG
        if added:
            # Our handler owns the records: without stopping propagation, a
            # host with root logging configured would double-emit every line.
            assert pkg.propagate is False
        # Root logging stays untouched: libraries and the runtime never
        # reconfigure logging that isn't theirs.
        assert logging.getLogger().level == logging.WARNING or not logging.getLogger().handlers
    finally:
        for h in [h for h in pkg.handlers if h not in before_handlers]:
            pkg.removeHandler(h)
        pkg.setLevel(before_level)
        pkg.propagate = before_propagate
