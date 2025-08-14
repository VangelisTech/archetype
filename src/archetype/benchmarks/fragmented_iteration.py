from __future__ import annotations

import string
from typing import Tuple, List, Type
from daft import col, lit

from archetype.core.component import Component
from archetype.core.aio.async_processor import AsyncProcessor

from .common import BenchResult, RunConfig, make_world, Timer


class Data(Component):
    value: int


def make_letter_component(name: str) -> Type[Component]:
    # Dynamically create Pydantic/Lance Component subclasses
    return type(name, (Component,), {"__annotations__": {"value": int}})


class DoubleData(AsyncProcessor):
    components = (Data,)
    priority = 1

    async def process(self, df, **kwargs):
        return df.with_column("data__value", col("data__value") * lit(2))


def make_double_letter_proc(letter_cls: Type[Component]):
    class _P(AsyncProcessor):
        components = (letter_cls,)
        priority = 1

        async def process(self, df, **kwargs):
            return df.with_column(f"{letter_cls.__name__.lower()}__value", col(f"{letter_cls.__name__.lower()}__value") * lit(2))

    _P.__name__ = f"Double{letter_cls.__name__}"
    return _P


async def run(entities_per_component: int = 100, steps: int = 1) -> Tuple[BenchResult, Tuple]:
    world, orch = await make_world("fragmented-iteration")
    try:
        letters: List[Type[Component]] = [make_letter_component(ch) for ch in string.ascii_uppercase]

        # Create 26 component types (A..Z), each with N entities plus Data component
        for letter_cls in letters:
            for i in range(entities_per_component):
                await world.create_entity([letter_cls(value=i), Data(value=i)])

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
        return result, (world.world_id, rc.run_id)
    finally:
        await orch.shutdown()


