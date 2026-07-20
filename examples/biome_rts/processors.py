# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure DataFrame processors activated by prefab capability composition."""

from __future__ import annotations

from daft import DataFrame, col
from daft.functions import when

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.graph import DepthProcessor

from .components import Cargo, Harvester, Heading, Mobility, Position


class MovementProcessor(AsyncProcessor):
    """Move live mobile entities; static prefabs have no Position/Heading."""

    components = (Position, Heading, Mobility)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column(
            "position__x",
            col("position__x") + col("heading__x") * col("mobility__speed"),
        ).with_column(
            "position__y",
            col("position__y") + col("heading__y") * col("mobility__speed"),
        )


class HarvestProcessor(AsyncProcessor):
    """Gather into instance-owned cargo, capped by the prefab capability."""

    components = (Cargo, Harvester)
    priority = 20

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        gathered = col("cargo__amount") + col("harvester__rate")
        over_capacity = gathered > col("harvester__capacity")
        return df.with_column(
            "cargo__amount",
            when(over_capacity, then=col("harvester__capacity")).otherwise(gathered),
        )


def biome_rts_processors() -> list[AsyncProcessor]:
    """Create the processor composition used by the Biome RTS example."""

    return [MovementProcessor(), HarvestProcessor(), DepthProcessor()]
