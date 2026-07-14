# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Runtime observability contract (runtime.md R16).

Scripts own their stdout: the runtime configures nothing unless asked, and
one flag — ARCHETYPE_LOG / ArchetypeRuntime(log=...) — wires the stdlib
``archetype`` hierarchy at the script boundary.
"""

import logging

import pytest

from archetype.runtime.runtime import _configure_archetype_logging, _resolve_log_level


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
    assert _resolve_log_level(value) == expected


def test_env_var_is_the_default_source(monkeypatch):
    monkeypatch.setenv("ARCHETYPE_LOG", "info")
    assert _resolve_log_level() == logging.INFO
    monkeypatch.delenv("ARCHETYPE_LOG")
    assert _resolve_log_level() is None


def test_configure_wires_package_logger_idempotently():
    pkg = logging.getLogger("archetype")
    before_handlers = list(pkg.handlers)
    before_level = pkg.level
    before_propagate = pkg.propagate
    try:
        _configure_archetype_logging(logging.INFO)
        _configure_archetype_logging(logging.DEBUG)
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
