# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Supported physical-AI values and direct operation models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict

from archetype.core.config import StorageConfig

if TYPE_CHECKING:
    from archetype.physical_ai.interfaces import EnvClient, PolicyClient


@dataclass(frozen=True)
class PhysicalTaskEvalConfig:
    """One instruction evaluated across multiple deterministic trial seeds."""

    suite: str
    task_id: int
    trials: int
    max_steps: int
    storage: StorageConfig = field(default_factory=StorageConfig)
    with_frames: bool = False
    instruction: str = "reach"

    def __post_init__(self) -> None:
        if not self.suite.strip():
            raise ValueError("suite must not be empty")
        if self.trials < 1:
            raise ValueError("trials must be at least 1")
        if self.max_steps < 1:
            raise ValueError("max_steps must be at least 1")


@dataclass(frozen=True)
class InstructionSweepConfig:
    """Instruction variants evaluated on paired initial-state seeds."""

    suite: str
    task_id: int
    variants: tuple[str, ...]
    seeds_per_variant: int
    max_steps: int
    storage: StorageConfig = field(default_factory=StorageConfig)
    with_frames: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "variants", tuple(self.variants))
        if not self.suite.strip():
            raise ValueError("suite must not be empty")
        if not self.variants:
            raise ValueError("variants must contain at least one instruction")
        if self.seeds_per_variant < 1:
            raise ValueError("seeds_per_variant must be at least 1")
        if self.max_steps < 1:
            raise ValueError("max_steps must be at least 1")


@dataclass(frozen=True)
class TrialOutcome:
    """Ledger-derived terminal outcome for one physical trial entity."""

    trial_idx: int
    env_key: int
    seed: int
    success: bool
    episode_length: int


@dataclass(frozen=True)
class PhysicalTaskEvalReport:
    """Ledger-derived result for one batched physical task evaluation."""

    suite: str
    task_id: int
    instruction: str
    world_id: str
    run_id: str
    trials: tuple[TrialOutcome, ...] = ()

    @property
    def success_rate(self) -> float:
        """Fraction of trial entities whose terminal status latched success."""

        return (
            sum(trial.success for trial in self.trials) / len(self.trials) if self.trials else 0.0
        )

    @property
    def mean_length(self) -> float:
        """Mean terminal environment-step count across all trials."""

        return (
            sum(trial.episode_length for trial in self.trials) / len(self.trials)
            if self.trials
            else 0.0
        )


@dataclass(frozen=True)
class VariantOutcome:
    """Ledger-derived aggregate for one instruction variant."""

    instruction: str
    n_trials: int
    n_success: int
    success_rate: float
    mean_length: float


@dataclass(frozen=True)
class InstructionSweepReport:
    """All variants graded from one addressable physical evaluation run."""

    suite: str
    task_id: int
    world_id: str
    run_id: str
    variants: tuple[VariantOutcome, ...] = ()

    @property
    def scores(self) -> dict[str, float]:
        """Map each instruction to its success-rate objective."""

        return {variant.instruction: variant.success_rate for variant in self.variants}

    @property
    def best(self) -> VariantOutcome | None:
        """Best variant with deterministic shorter-then-lexical tie breaking."""

        if not self.variants:
            return None
        return min(
            self.variants,
            key=lambda variant: (
                -variant.success_rate,
                len(variant.instruction),
                variant.instruction,
            ),
        )


if TYPE_CHECKING:
    _EnvOperationClient = EnvClient
    _PolicyOperationClient = PolicyClient
else:
    # Direct-only clients are validated at the registered handler boundary.
    # Keeping Pydantic from introspecting structural protocols also prevents
    # provider properties from becoming an accidental dispatch-time effect.
    _EnvOperationClient = Any
    _PolicyOperationClient = Any


class _PhysicalAIOperation(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    direct_only: ClassVar[bool] = True
    operation: str


class EvaluatePhysicalTask(_PhysicalAIOperation):
    """Evaluate one typed physical task configuration."""

    operation: Literal["evaluate_physical_task"] = "evaluate_physical_task"
    config: PhysicalTaskEvalConfig
    env_client: _EnvOperationClient
    policy_client: _PolicyOperationClient | None = None


class SweepPhysicalInstructions(_PhysicalAIOperation):
    """Evaluate instruction variants against paired initial-state seeds."""

    operation: Literal["sweep_physical_instructions"] = "sweep_physical_instructions"
    config: InstructionSweepConfig
    env_client: _EnvOperationClient
    policy_client: _PolicyOperationClient


def summarize_physical_ai_operation(
    operation: _PhysicalAIOperation,
) -> Mapping[str, Any]:
    """Return the discriminator without clients, instructions, or provider state."""

    return {"operation": operation.operation}


__all__ = [
    "EvaluatePhysicalTask",
    "InstructionSweepConfig",
    "InstructionSweepReport",
    "PhysicalTaskEvalConfig",
    "PhysicalTaskEvalReport",
    "SweepPhysicalInstructions",
    "TrialOutcome",
    "VariantOutcome",
    "summarize_physical_ai_operation",
]
