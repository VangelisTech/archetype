# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ECS bridge for one-attempt-per-tick coding-agent missions."""

from __future__ import annotations

import asyncio
from typing import Any

import daft
from daft import DataFrame

from archetype.app.coding_agents.models import CodingAgentEpisode
from archetype.app.coding_agents.service import CodingAgentService
from archetype.app.missions.models import MISSION_COMPONENTS
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.resources import Resources


class CodingAgentProcessor(AsyncProcessor):
    """Record one submission per tick and delegate transition authority."""

    components = (*MISSION_COMPONENTS, CodingAgentEpisode)
    priority = 10

    async def process(
        self,
        df: DataFrame,
        resources: Resources | None = None,
        tick: int = 0,
        **kwargs: Any,
    ) -> DataFrame:
        if resources is None:
            raise KeyError("CodingAgentProcessor requires world resources")
        service = resources.get(CodingAgentService)
        if service is None:
            raise KeyError("no CodingAgentService in world resources")
        rows = df.to_pylist()

        async def run_row(row: dict[str, Any]) -> dict[str, Any]:
            mission_id = str(row.get("codingagentepisode__mission_id") or "")
            if not mission_id:
                mission_id = (
                    f"{row['world_id']}:{row['entity_id']}:{row.get('mission__name') or 'mission'}"
                )
            return await service.run_tick(mission_id, row, tick=tick)

        # Every mission owns a distinct sandbox session, so submissions from
        # the same archetype are independent and can fan out concurrently.
        updated = await asyncio.gather(*(run_row(row) for row in rows))
        return daft.from_pylist(updated).select(*df.column_names)
