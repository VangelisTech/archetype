from __future__ import annotations

from daft import col, lit

from archetype.app.world.service import WorldService
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import CacheConfig, StorageConfig

from .common import BenchResult, RunConfig, Timer, make_world


class A(Component):
    value: int


class B(Component):
    value: int


class C(Component):
    value: int


class D(Component):
    value: int


class E(Component):
    value: int


class DoubleA(AsyncProcessor):
    components = (A,)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("a__value", col("a__value") * lit(2))


class DoubleB(AsyncProcessor):
    components = (B,)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("b__value", col("b__value") * lit(2))


class DoubleC(AsyncProcessor):
    components = (C,)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("c__value", col("c__value") * lit(2))


class DoubleD(AsyncProcessor):
    components = (D,)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("d__value", col("d__value") * lit(2))


class DoubleE(AsyncProcessor):
    components = (E,)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("e__value", col("e__value") * lit(2))


async def run(
    entities: int = 1000,
    steps: int = 1,
    *,
    orchestrator: WorldService | None = None,
    storage: StorageConfig | None = None,
    cache_config: CacheConfig | None = None,
) -> tuple[BenchResult, tuple]:
    world, orch = await make_world(
        "packed-iteration",
        storage=storage,
        cache_config=cache_config,
        orchestrator=orchestrator,
    )
    try:
        # Create dataset: 1,000 entities, each with (A,B,C,D,E)
        for i in range(entities):
            await world.create_entity([A(value=i), B(value=i), C(value=i), D(value=i), E(value=i)])

        # Register 5 independent systems/queries
        await world.add_processor(DoubleA())
        await world.add_processor(DoubleB())
        await world.add_processor(DoubleC())
        await world.add_processor(DoubleD())
        await world.add_processor(DoubleE())

        rc = RunConfig.benchmark(steps=steps)
        with Timer() as t:
            await world.run(rc)

        result = BenchResult(
            name="packed_iteration",
            entities=entities,
            steps=steps,
            elapsed_s=t.elapsed,
            extras={},
        )
        return result, (world.world_id, rc.run_id)
    finally:
        await orch.shutdown()
