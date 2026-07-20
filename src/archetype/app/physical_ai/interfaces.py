# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Application port owned by the physical-AI workflow family."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from archetype.physical_ai.contracts import (
    InstructionSweepConfig,
    InstructionSweepReport,
    PhysicalTaskEvalConfig,
    PhysicalTaskEvalReport,
)
from archetype.physical_ai.manipulation import EnvClient
from archetype.physical_ai.policy import PolicyClient


@runtime_checkable
class iPhysicalAIService(Protocol):
    """Create, execute, and grade ledger-backed physical evaluations."""

    async def evaluate_task(
        self,
        config: PhysicalTaskEvalConfig,
        *,
        env_client: EnvClient,
        policy_client: PolicyClient | None = None,
    ) -> PhysicalTaskEvalReport: ...

    async def sweep_instructions(
        self,
        config: InstructionSweepConfig,
        *,
        env_client: EnvClient,
        policy_client: PolicyClient,
    ) -> InstructionSweepReport: ...
