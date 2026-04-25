# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Fork for Counterfactuals
=========================

Fork a world three times with different physics parameters,
run each fork, and compare the results.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/02_fork_counterfactual.py
"""

import asyncio
from dataclasses import dataclass

from archetype import ArchetypeRuntime
from archetype.core.component import Component
from archetype.core.config import StorageConfig


@dataclass
class PhysicsConfig:
    gravity: float = 9.8
    drag: float = 0.1


class Probe(Component):
    label: str = ""


async def main():
    storage = StorageConfig(uri="./archetype_data", namespace="counterfactuals")

    async with ArchetypeRuntime() as runtime:
        base = runtime.world("base", storage=storage)

        await base.spawn(Probe(label="seed"))
        await base.run(steps=1)

        print(f"Base world: {base.world_id}")
        print(f"Base state: tick={base.tick}\n")

        branches_run = 0
        for gravity in [1.0, 9.8, 25.0]:
            fork = await base.fork(f"gravity-{gravity}", storage=storage)
            fork.resources.insert(PhysicsConfig(gravity=gravity))

            result = await fork.run(steps=10)
            rows = (await fork.query(Probe)).collect().to_pylist()
            branches_run += 1
            print(f"gravity={gravity:>5.1f}: tick={result.final_tick}, entities={len(rows)}")

        print(f"\nRan {branches_run} counterfactual branches from the same base state.")


if __name__ == "__main__":
    asyncio.run(main())
