# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Audit Section A.5 — Append-only invariant (protocol-level).

These tests are the cheapest possible enforcement of the load-bearing
append-only contract: the storage and audit-log Protocols simply must
not declare any method whose name suggests deletion. If a future PR adds
one, this file goes red before runtime ever sees the call.

`A.5.3` (no DELETE statements in the storage implementation) is a
separate scan — kept out of this file so the protocol-only check stays
zero-cost and import-only.
"""

from __future__ import annotations

import pytest

from archetype.app.interfaces import iAuditLog
from archetype.core.interfaces import iAsyncStore

pytestmark = [pytest.mark.audit, pytest.mark.audit_critical]


def _public_method_names(proto: type) -> set[str]:
    """Names of public methods declared on a Protocol class.

    Skips dunders and `__init__` (which is structural/typing-only on Protocols).
    """
    return {
        name
        for name in dir(proto)
        if not name.startswith("_") and callable(getattr(proto, name))
    }


# ── A.5.1 — iAsyncStore ─────────────────────────────────────────────────


def test_a_5_1_async_store_no_destructive_methods() -> None:
    """iAsyncStore declares no method whose name contains delete/drop/remove_data.

    The store is append-only; any destructive surface would compromise the
    audit-trail invariant relied on by replay, fork, and counterfactual
    analysis.
    """
    forbidden_substrings = ("delete", "drop", "remove_data")
    methods = _public_method_names(iAsyncStore)
    offenders = sorted(
        m for m in methods if any(s in m.lower() for s in forbidden_substrings)
    )
    assert not offenders, (
        f"iAsyncStore declares destructive methods: {offenders}. "
        "Append-only stores must not expose deletion."
    )


# ── A.5.2 — iAuditLog ───────────────────────────────────────────────────


def test_a_5_2_audit_log_no_destructive_methods() -> None:
    """iAuditLog declares no method whose name contains delete or drop.

    Audit rows are immutable. A destructive surface here would let an
    actor erase evidence of their own past actions.
    """
    forbidden_substrings = ("delete", "drop")
    methods = _public_method_names(iAuditLog)
    offenders = sorted(
        m for m in methods if any(s in m.lower() for s in forbidden_substrings)
    )
    assert not offenders, (
        f"iAuditLog declares destructive methods: {offenders}. "
        "Audit logs are append-only."
    )
