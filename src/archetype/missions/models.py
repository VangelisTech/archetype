# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact direct operation models owned by the missions family."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

from archetype.core.config import StorageConfig
from archetype.missions.contracts import (
    AgentMissionConfig,
    MissionSubmission,
    SubmittedMission,
)
from archetype.missions.sandboxes import CheckpointRef


class _MissionOperation(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    direct_only: ClassVar[bool] = True
    operation: str
    owner_id: str


class SubmitMission(_MissionOperation):
    """Construct and submit one mission through a reserved runtime owner."""

    operation: Literal["submit_mission"] = "submit_mission"
    name: str
    config: AgentMissionConfig
    storage: str | Path | StorageConfig | None = None
    submission: MissionSubmission


class RunMission(_MissionOperation):
    """Run one previously submitted mission through its reserved owner."""

    operation: Literal["run_mission"] = "run_mission"
    mission: SubmittedMission
    max_ticks: int | None = Field(default=None, ge=1)


class RestoreMissionSandbox(_MissionOperation):
    """Restore one submitted mission's sandbox from a checkpoint."""

    operation: Literal["restore_mission_sandbox"] = "restore_mission_sandbox"
    mission: SubmittedMission
    checkpoint: CheckpointRef


def summarize_mission_operation(operation: _MissionOperation) -> Mapping[str, Any]:
    """Return only the discriminator; all mission evidence stays private."""

    return {"operation": operation.operation}


__all__ = [
    "RestoreMissionSandbox",
    "RunMission",
    "SubmitMission",
    "summarize_mission_operation",
]
