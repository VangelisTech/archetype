# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Blocking architectural capability-evaluation package."""

from __future__ import annotations

from evals.harness import EvalHarness
from evals.suites.capability import agent_missions, tasks


def register(harness: EvalHarness) -> None:
    """Register the stable capability suite in diagnostic order."""
    tasks.register(harness)
    agent_missions.register(harness)

__all__ = ["agent_missions", "register", "tasks"]
