from __future__ import annotations

from typing import Tuple, Optional
from daft import col

from archetype.core.component import Component
from archetype.core.aio.async_processor import AsyncProcessor

from .common import BenchResult, RunConfig, make_world, Timer
from archetype.core.config import StorageConfig, CacheConfig
from archetype.core.orchestrator import WorldOrchestrator


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


class SwapAB(AsyncProcessor):
    components = (A, B)
    priority = 1

    async def process(self, df, **kwargs):
        # swap a.value and b.value
        return df.with_columns({
            "a__value": col("b__value"),
            "b__value": col("a__value"),
        })


class SwapCD(AsyncProcessor):
    components = (C, D)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_columns({
            "c__value": col("d__value"),
            "d__value": col("c__value"),
        })


class SwapCE(AsyncProcessor):
    components = (C, E)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_columns({
            "c__value": col("e__value"),
            "e__value": col("c__value"),
        })


async def run(
    entities_per_group: int = 1000,
    steps: int = 1,
    *,
    orchestrator: Optional[WorldOrchestrator] = None,
    storage: Optional[StorageConfig] = None,
    cache_config: Optional[CacheConfig] = None,
    instrumented: Optional[bool] = None,
) -> Tuple[BenchResult, Tuple]:
    world, orch = await make_world(
        "simple-iteration",
        storage=storage,
        cache_config=cache_config,
        instrumented=instrumented,
        orchestrator=orchestrator,
    )
    try:
        # Datasets
        for i in range(entities_per_group):
            await world.create_entity([A(value=i), B(value=i)])
        for i in range(entities_per_group):
            await world.create_entity([A(value=i), B(value=i), C(value=i)])
        for i in range(entities_per_group):
            await world.create_entity([A(value=i), B(value=i), C(value=i), D(value=i)])
        for i in range(entities_per_group):
            await world.create_entity([A(value=i), B(value=i), C(value=i), E(value=i)])

        await world.add_processor(SwapAB())
        await world.add_processor(SwapCD())
        await world.add_processor(SwapCE())

        rc = RunConfig.benchmark(steps=steps)
        with Timer() as t:
            await world.run(rc)

        total_entities = entities_per_group * 4
        result = BenchResult(
            name="simple_iteration",
            entities=total_entities,
            steps=steps,
            elapsed_s=t.elapsed,
            extras={},
        )
        return result, (world.world_id, rc.run_id)
    finally:
        await orch.shutdown()


