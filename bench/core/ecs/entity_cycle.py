from __future__ import annotations

from archetype.app.services.world_service import WorldService
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import CacheConfig, StorageConfig

from .common import BenchResult, RunConfig, Timer, make_world


class A(Component):
    value: int


class B(Component):
    value: int


class SpawnBPerA(AsyncProcessor):
    components = (A,)
    priority = 1

    def __init__(self, world):
        self.world = world

    async def process(self, df, **kwargs):
        # For each A entity, create one B entity (avoid pydict; use counts)
        mat = df.collect()
        count = mat.count_rows()
        for _ in range(count):
            await self.world.create_entity([B(value=1)])
        return df


class KillAllB(AsyncProcessor):
    components = (B,)
    priority = 2

    def __init__(self, world):
        self.world = world

    async def process(self, df, **kwargs):
        # Despawn all entities with B (avoid pydict)
        mat = df.select("entity_id").collect()
        arrow_tbl = mat.to_arrow()
        entity_ids = arrow_tbl.column("entity_id").to_pylist()
        for ent_id in entity_ids:
            await self.world.remove_entity(ent_id)
        return df


async def run(
    entities: int = 1000,
    steps: int = 1,
    *,
    orchestrator: WorldService | None = None,
    storage: StorageConfig | None = None,
    cache_config: CacheConfig | None = None,
    instrumented: bool | None = None,
) -> tuple[BenchResult, tuple]:
    world, orch = await make_world(
        "entity-cycle",
        storage=storage,
        cache_config=cache_config,
        instrumented=instrumented,
        orchestrator=orchestrator,
    )
    try:
        for i in range(entities):
            await world.create_entity([A(value=i)])

        await world.add_processor(SpawnBPerA(world))
        await world.add_processor(KillAllB(world))

        rc = RunConfig.benchmark(steps=steps)
        with Timer() as t:
            await world.run(rc)

        result = BenchResult(
            name="entity_cycle",
            entities=entities,
            steps=steps,
            elapsed_s=t.elapsed,
            extras={},
        )
        return result, (world.world_id, rc.run_id)
    finally:
        await orch.shutdown()
