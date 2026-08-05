from __future__ import annotations

import string

from daft import col, lit

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import CacheConfig, StorageConfig

from .common import BenchResult, BenchWorldHarness, RunConfig, Timer, make_world


class Data(Component):
    value: int


def make_letter_component(name: str) -> type[Component]:
    # Dynamically create Pydantic/Lance Component subclasses
    return type(name, (Component,), {"__annotations__": {"value": int}})


class DoubleData(AsyncProcessor):
    components = (Data,)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("data__value", col("data__value") * lit(2))


def make_double_letter_proc(letter_cls: type[Component]):
    class _P(AsyncProcessor):
        components = (letter_cls,)
        priority = 1

        async def process(self, df, **kwargs):
            return df.with_column(
                f"{letter_cls.__name__.lower()}__value",
                col(f"{letter_cls.__name__.lower()}__value") * lit(2),
            )

    _P.__name__ = f"Double{letter_cls.__name__}"
    return _P


async def run(
    entities_per_component: int = 100,
    steps: int = 1,
    *,
    harness: BenchWorldHarness | None = None,
    storage: StorageConfig | None = None,
    cache_config: CacheConfig | None = None,
) -> tuple[BenchResult, tuple]:
    world, worlds = await make_world(
        "fragmented-iteration",
        storage=storage,
        cache_config=cache_config,
        harness=harness,
    )
    try:
        letters: list[type[Component]] = [
            make_letter_component(ch) for ch in string.ascii_uppercase
        ]

        # Create 26 component types (A..Z), each with N entities plus Data component
        for letter_cls in letters:
            for i in range(entities_per_component):
                await world.create_entity([letter_cls.model_validate({"value": i}), Data(value=i)])

        # Two passes: double Data values, then double Z values
        await world.add_processor(DoubleData())
        DoubleZ = make_double_letter_proc(next(cls for cls in letters if cls.__name__ == "Z"))
        await world.add_processor(DoubleZ())

        rc = RunConfig.benchmark(steps=steps)
        with Timer() as t:
            await world.run(rc)

        total_entities = entities_per_component * len(letters)
        result = BenchResult(
            name="fragmented_iteration",
            entities=total_entities,
            steps=steps,
            elapsed_s=t.elapsed,
            extras={"components": len(letters)},
        )
        return result, (world.world_id, world.run_id)
    finally:
        await worlds.shutdown()
