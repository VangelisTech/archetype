from __future__ import annotations

from typing import Tuple, Optional

from archetype.core.component import Component
from archetype.core.aio.async_processor import AsyncProcessor

from .common import BenchResult, RunConfig, make_world, Timer
from archetype.core.config import StorageConfig, CacheConfig
from archetype.core.orchestrator import WorldOrchestrator


class A(Component):
    value: int


class B(Component):
    value: int


class AddBToA(AsyncProcessor):
    components = (A,)
    priority = 1

    def __init__(self, world):
        self.world = world

    async def process(self, df, **kwargs):
        mat = df.select("entity_id", "a__value").collect()
        arrow_tbl = mat.to_arrow()
        ents = arrow_tbl.column("entity_id").to_pylist()
        avals = arrow_tbl.column("a__value").to_pylist()
        for ent, aval in zip(ents, avals):
            await self.world.add_components(ent, [B(value=avals[0] if isinstance(aval, list) else aval)])
        return df


class RemoveB(AsyncProcessor):
    components = (A, B)
    priority = 2

    def __init__(self, world):
        self.world = world

    async def process(self, df, **kwargs):
        mat = df.select("entity_id").collect()
        arrow_tbl = mat.to_arrow()
        for ent in arrow_tbl.column("entity_id").to_pylist():
            await self.world.remove_components(ent, [B])
        return df


async def run(
    entities: int = 1000,
    steps: int = 1,
    *,
    orchestrator: Optional[WorldOrchestrator] = None,
    storage: Optional[StorageConfig] = None,
    cache_config: Optional[CacheConfig] = None,
    instrumented: Optional[bool] = None,
) -> Tuple[BenchResult, Tuple]:
    world, orch = await make_world(
        "add-remove",
        storage=storage,
        cache_config=cache_config,
        instrumented=instrumented,
        orchestrator=orchestrator,
    )
    try:
        for i in range(entities):
            await world.create_entity([A(value=i)])

        await world.add_processor(AddBToA(world))
        await world.add_processor(RemoveB(world))

        rc = RunConfig.benchmark(steps=steps)
        with Timer() as t:
            await world.run(rc)

        result = BenchResult(
            name="add_remove",
            entities=entities,
            steps=steps,
            elapsed_s=t.elapsed,
            extras={},
        )
        return result, (world.world_id, rc.run_id)
    finally:
        await orch.shutdown()


