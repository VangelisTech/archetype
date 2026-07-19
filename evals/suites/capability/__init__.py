# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Blocking architectural capability-evaluation package."""

from __future__ import annotations

from evals.harness import EvalHarness
from evals.suites.capability import (
    agent_missions,
    indexed_finalization,
    mission_attempt_claims,
    redaction,
    sandbox_kernel,
    tasks,
)


def register(harness: EvalHarness) -> None:
    """Register the stable capability suite in diagnostic order."""
    tasks.register(harness)
    agent_missions.register(harness)
    mission_attempt_claims.register(harness)
    indexed_finalization.register(harness)
    redaction.register(harness)
    sandbox_kernel.register(harness)


__all__ = [
    "agent_missions",
    "indexed_finalization",
    "mission_attempt_claims",
    "redaction",
    "register",
    "sandbox_kernel",
    "tasks",
]
