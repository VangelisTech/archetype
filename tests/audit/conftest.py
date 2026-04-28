# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Audit-suite fixtures and markers.

The audit suite encodes the pre-merge checklist as executable tests. Each test
asserts one bullet from `docs/audits/categorization.md` and is registered in
`_manifest.AUDIT_ITEMS` by its checklist id (e.g. ``"B.1.1"``).
"""

from __future__ import annotations

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "audit: pre-merge audit invariant",
    )
    config.addinivalue_line(
        "markers",
        "audit_critical: append-only or single-gate invariant; merge blocker",
    )
    config.addinivalue_line(
        "markers",
        "audit_integration: requires services; not a fast unit test",
    )
    config.addinivalue_line(
        "markers",
        "audit_e2e: full-stack via httpx or CliRunner",
    )
    config.addinivalue_line(
        "markers",
        "audit_slow: subprocess-based (examples, doc build); skipped by default",
    )
