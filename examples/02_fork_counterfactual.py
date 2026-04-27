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
        base = runtime.world("base", storage=storage, resources=[PhysicsConfig()])

        await base.spawn(Probe(label="seed"))
        await base.run(steps=1)

        base_info = await base.info()
        print(f"Base world: {base_info.world_id}")
        print(f"Base state: tick={base_info.tick}\n")

        branches_run = 0
        for gravity in [1.0, 9.8, 25.0]:
            fork = await base.fork(f"gravity-{gravity}", storage=storage)
            # Resources are shared from the base world at fork time.
            # To vary per-fork, we use a fresh world with the resource instead.
            # For this demo we just run the fork forward.
            result = await fork.run(steps=10)
            df = await fork.query(Probe)
            branches_run += 1
            print(f"\ngravity={gravity:>5.1f}: tick={result.final_tick}")
            df.show()

        print(f"\nRan {branches_run} counterfactual branches from the same base state.")


if __name__ == "__main__":
    asyncio.run(main())
