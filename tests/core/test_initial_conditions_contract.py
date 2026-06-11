# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Initial-conditions contract.

An entity's first persisted row is its raw spawn values at the tick it
materializes; processors first apply on the following tick. Formally:
x_0 is given, x_{t+1} = f(x_t). The ledger therefore contains the full
sequence x_0, f(x_0), f^2(x_0), ... — initial conditions included.

Before this contract, spawns were processed inside their materialization
tick, so the first row was f(x_0) and x_0 was unrecoverable.
"""

import pytest

from archetype.core.aio import AsyncProcessor, AsyncSystem
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from tests.conftest import make_world_service


class Level(Component):
    n: float = 0.0


class Step(AsyncProcessor):
    components = (Level,)
    priority = 10

    async def process(self, df, **kwargs):
        from daft import col

        return df.with_column("level__n", col("level__n") + 1.0)


@pytest.mark.asyncio
async def test_initial_conditions_persist_at_spawn_tick(tmp_path):
    """The first persisted row is x_0, not f(x_0)."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(Step())
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)
        await world.create_entity([Level(n=5.0)])

        await world.run(RunConfig(num_steps=3))

        sig = (Level,)
        by_tick = {}
        for t in range(3):
            df = await world.query_archetype(sig=sig, ticks=[t])
            rows = df.to_pylist()
            assert len(rows) == 1
            by_tick[t] = rows[0]["level__n"]

        assert by_tick == {0: 5.0, 1: 6.0, 2: 7.0}, (
            f"ledger must contain x_0, f(x_0), f^2(x_0); got {by_tick}"
        )
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_mid_run_spawn_persists_initial_conditions(tmp_path):
    """An entity spawned mid-simulation lands raw at its spawn tick and is
    first transformed on the next tick — while older entities keep
    advancing uninterrupted."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(Step())
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)
        first = await world.create_entity([Level(n=0.0)])

        await world.run(RunConfig(num_steps=2))  # first: 0.0 @ t0, 1.0 @ t1

        second = await world.create_entity([Level(n=100.0)])
        await world.run(RunConfig(num_steps=2))  # t2: first 2.0, second 100.0 (raw)
        #                                          t3: first 3.0, second 101.0

        sig = (Level,)
        t2 = {
            r["entity_id"]: r["level__n"]
            for r in (await world.query_archetype(sig=sig, ticks=[2])).to_pylist()
        }
        t3 = {
            r["entity_id"]: r["level__n"]
            for r in (await world.query_archetype(sig=sig, ticks=[3])).to_pylist()
        }
        assert t2[second] == 100.0, f"mid-run spawn must persist raw initial conditions: {t2}"
        assert t2[first] == 2.0
        assert t3[second] == 101.0, f"the transition applies on the tick after spawn: {t3}"
        assert t3[first] == 3.0
    finally:
        await ws.shutdown()
