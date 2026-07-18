# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Structural specification-conformance evaluation package."""

from evals.suites.spec_contracts import tasks
from evals.suites.spec_contracts.tasks import SUITE, register, task_runtime_gate_only_boundary

__all__ = ["SUITE", "register", "task_runtime_gate_only_boundary", "tasks"]
