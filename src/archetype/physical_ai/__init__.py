# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Physical-AI state, execution boundaries, and evaluation contracts.

This family owns reusable simulation and policy state, typed workflow values,
and pure optimization. Application workflows that create worlds, run episodes,
query ledgers, or grade outcomes live in :mod:`archetype.app.physical_ai`.
"""

from archetype.physical_ai.contracts import (
    InstructionSweepConfig,
    InstructionSweepReport,
    PhysicalTaskEvalConfig,
    PhysicalTaskEvalReport,
    TrialOutcome,
    VariantOutcome,
)
from archetype.physical_ai.optimization import (
    OptimizationResult,
    PerturbationStrategy,
    RoundRecord,
    TemplatePerturbation,
    optimize_instruction,
)

__all__ = [
    "InstructionSweepConfig",
    "InstructionSweepReport",
    "OptimizationResult",
    "PerturbationStrategy",
    "PhysicalTaskEvalConfig",
    "PhysicalTaskEvalReport",
    "RoundRecord",
    "TemplatePerturbation",
    "TrialOutcome",
    "VariantOutcome",
    "optimize_instruction",
]
