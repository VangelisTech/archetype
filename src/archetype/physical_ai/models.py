# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact direct operation models owned by the physical-AI family."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict

from archetype.physical_ai.contracts import (
    InstructionSweepConfig,
    PhysicalTaskEvalConfig,
)

if TYPE_CHECKING:
    from archetype.physical_ai.manipulation import EnvClient
    from archetype.physical_ai.policy import PolicyClient
else:
    # These direct-only provider protocols are intentionally not runtime
    # validated: neither protocol is runtime-checkable, and the exact handler
    # remains the capability boundary.
    EnvClient = Any
    PolicyClient = Any


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
    env_client: EnvClient
    policy_client: PolicyClient | None = None


class SweepPhysicalInstructions(_PhysicalAIOperation):
    """Evaluate instruction variants against paired initial-state seeds."""

    operation: Literal["sweep_physical_instructions"] = "sweep_physical_instructions"
    config: InstructionSweepConfig
    env_client: EnvClient
    policy_client: PolicyClient


def summarize_physical_ai_operation(
    operation: _PhysicalAIOperation,
) -> Mapping[str, Any]:
    """Return the discriminator without clients, instructions, or provider state."""

    return {"operation": operation.operation}


__all__ = [
    "EvaluatePhysicalTask",
    "SweepPhysicalInstructions",
    "summarize_physical_ai_operation",
]
