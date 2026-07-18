# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Stable registration catalog for repository evaluation suites."""

from __future__ import annotations

from evals.harness import EvalHarness
from evals.suites import capability, idempotency, regression, spec_contracts


def register_all(harness: EvalHarness) -> None:
    """Register every suite in stable diagnostic order."""
    regression.register(harness)
    spec_contracts.register(harness)
    idempotency.register(harness)
    capability.register(harness)
