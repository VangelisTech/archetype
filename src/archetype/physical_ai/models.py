# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Supported hosted Physical-AI values and operation model."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict

from archetype.core.config import StorageConfig
from archetype.physical_ai.hosted_activity_contracts import HostedEpisodeObservation
from archetype.physical_ai.hosted_episode import canonical_hosted_episode_config
from archetype.physical_ai.hosted_modal import ModalHostedEpisodeConfig


@dataclass(frozen=True, slots=True)
class HostedEpisodeRequest:
    """One deterministic episode in a hosted provider request batch."""

    trial_id: int
    suite: str
    task_id: int
    seed: int
    instruction: str
    max_transitions: int
    environment_id: str
    policy_id: str
    config_json: str = "{}"

    def __post_init__(self) -> None:
        if isinstance(self.trial_id, bool) or self.trial_id < 0:
            raise ValueError("trial_id must be an integer >= 0")
        if isinstance(self.task_id, bool) or self.task_id < 0:
            raise ValueError("task_id must be an integer >= 0")
        if isinstance(self.seed, bool) or self.seed < 0:
            raise ValueError("seed must be an integer >= 0")
        if isinstance(self.max_transitions, bool) or self.max_transitions < 0:
            raise ValueError("max_transitions must be an integer >= 0")
        for field in ("suite", "instruction", "environment_id", "policy_id"):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} must be a non-empty string")
        object.__setattr__(
            self,
            "config_json",
            canonical_hosted_episode_config(self.config_json),
        )


class RunHostedEpisode(BaseModel):
    """Execute or recover one whole-episode Modal Activity."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    direct_only: ClassVar[bool] = True
    operation: Literal["run_hosted_episode"] = "run_hosted_episode"
    world_id: str
    storage_config: StorageConfig
    activity_id: str
    requests: tuple[HostedEpisodeRequest, ...]
    provider: ModalHostedEpisodeConfig


def summarize_physical_ai_operation(operation: RunHostedEpisode) -> Mapping[str, Any]:
    """Return bounded operation identity without provider or request payloads."""

    return {
        "operation": operation.operation,
        "world_id": str(operation.world_id),
    }


__all__ = [
    "HostedEpisodeObservation",
    "HostedEpisodeRequest",
    "ModalHostedEpisodeConfig",
    "RunHostedEpisode",
    "summarize_physical_ai_operation",
]
