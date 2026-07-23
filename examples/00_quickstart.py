# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The smallest complete Archetype simulation."""

import asyncio

from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component, StorageConfig


class Counter(Component):
    value: int = 0


class Increment(AsyncProcessor):
    components = (Counter,)

    async def process(self, df: DataFrame, **_) -> DataFrame:
        return df.with_column("counter__value", col("counter__value") + 1)


async def run_demo(storage_uri: str) -> dict[str, object]:
    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "quickstart",
            storage=StorageConfig(uri=storage_uri, namespace="quickstart"),
            processors=[Increment()],
        )
        await world.spawn(Counter())
        await world.step()  # Persist the initial state before processors run.
        result = await world.run(steps=3)
        history = await world.query(Counter)
        current = history.where(col("tick") == result.final_tick - 1)
        return {"value": current.collect().to_pylist()[0]["counter__value"]}


async def main() -> None:
    print((await run_demo("./archetype_data"))["value"])


if __name__ == "__main__":
    asyncio.run(main())
