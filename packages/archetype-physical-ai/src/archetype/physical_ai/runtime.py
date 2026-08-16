# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed Physical-AI operations over one Archetype world handle."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from uuid_utils import uuid7

from archetype.physical_ai.models import (
    HostedEpisodeObservation,
    HostedEpisodeRequest,
    ModalHostedEpisodeConfig,
    RunHostedEpisode,
)


class PhysicalAI:
    """Expose Physical-AI workflows for one world.

    Construct this adapter directly or through ``world.library("physical-ai")``.
    The wrapped world remains the owner of lazy activation and operation
    admission; this adapter contributes only Physical-AI behavior.
    """

    def __init__(self, world: Any) -> None:
        self._world = world

    async def run_hosted_episode(
        self,
        requests: Sequence[HostedEpisodeRequest],
        *,
        provider: ModalHostedEpisodeConfig,
        activity_id: str | None = None,
    ) -> HostedEpisodeObservation:
        """Run or recover one complete Modal-hosted episode batch."""

        async def dispatch(world_id: object, storage_config: Any, dispatcher: Any) -> Any:
            assert storage_config is not None
            return await dispatcher.apply(
                RunHostedEpisode(
                    world_id=str(world_id),
                    storage_config=storage_config,
                    activity_id=activity_id or f"hosted-{uuid7()}",
                    requests=tuple(requests),
                    provider=provider,
                )
            )

        return await self._world._call_library(
            dispatch,
            capability="run_hosted_episode",
            require_storage=True,
        )


__all__ = ["PhysicalAI"]
