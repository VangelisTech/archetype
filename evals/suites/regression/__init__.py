# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Blocking regression-evaluation package."""

from __future__ import annotations

from evals.harness import EvalHarness
from evals.suites.regression.poison import register as register_poison
from evals.suites.regression.tasks import register as register_contracts


def register(harness: EvalHarness) -> None:
    """Register stable contracts before adversarial poison-command cases."""
    register_contracts(harness)
    register_poison(harness)


__all__ = ["register"]
