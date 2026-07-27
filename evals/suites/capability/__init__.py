# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Blocking architectural capability-evaluation package."""

from __future__ import annotations

from evals.harness import EvalHarness
from evals.suites.capability import (
    redaction,
    tasks,
)


def register(harness: EvalHarness) -> None:
    """Register the stable capability suite in diagnostic order."""
    tasks.register(harness)
    redaction.register(harness)


__all__ = [
    "redaction",
    "register",
    "tasks",
]
