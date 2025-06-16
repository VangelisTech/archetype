#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import sys
import os
import asyncio
import time

notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_dir, "..", "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from archetype.core import processor, Processor, Component, make_simple_world
from archetype.core.aio import make_async_world, AsyncProcessor

from daft import DataFrame, col


# Define Components (reuse from simple_simulation)
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float


# Sync processor for comparison
@processor(Position, Velocity, priority=1)
class MovementProcessor(Processor):
    def process(self, df: DataFrame, dt: float) -> DataFrame:
        time.sleep(0.01)
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })


# Async processor with simulated I/O
@processor(Position, Velocity, priority=1)
class AsyncMovementProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, semaphore: asyncio.Semaphore, dt: float) -> DataFrame:
        # Simulate async I/O (external physics API, etc.)
        await asyncio.sleep(0.01)  # 10ms simulated I/O per archetype

        # Same logic as sync version
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })


async def benchmark_async_vs_sync():
    """Compare async vs sync performance with identical workloads."""

    uri = "/Users/everett-founder/git/vangelis/internal/work/libs/archetype/data"

    print("Setting up simulation worlds...")

    # Create sync world (reuse existing data structure)
    sync_world = make_simple_world(uri, debug=False)
    sync_world.add_processor(MovementProcessor())

    # Create async world (wrapping sync infrastructure)
    async_world = make_async_world(uri=uri, max_concurrent_archetypes=10)
    async_world.add_processor(AsyncMovementProcessor())

    # Spawn entities (shared setup)
    num_entities = 50
    print(f"📦 Spawning {num_entities} entities...")

    for i in range(num_entities):
        sync_world.spawn(Position(x=0, y=0), Velocity(vx=i+1, vy=i+1))
        async_world.spawn(Position(x=0, y=0), Velocity(vx=i+1, vy=i+1))

    # Benchmark sync execution (using sync processors sequentially)
    print("\n⏱️  Running sync simulation...")
    sync_start = time.time()

    num_steps = 100
    for step in range(num_steps):
        sync_world.step(dt=0.1)  # Use sync_step for fair comparison

    sync_time = time.time() - sync_start
    print(f"✅ Sync simulation: {num_steps} steps in {sync_time:.3f}s ({sync_time/num_steps:.3f}s per step)")

    # Benchmark async execution
    print("\n⚡ Running async simulation...")
    async_start = time.time()

    for step in range(num_steps):
        await async_world.step(dt=0.1)

    async_time = time.time() - async_start
    print(f"✅ Async simulation: {num_steps} steps in {async_time:.3f}s ({async_time/num_steps:.3f}s per step)")

    # Results
    print("\n Performance Comparison:")
    print(f"   Sync:  {sync_time:.3f}s total")
    print(f"   Async: {async_time:.3f}s total")

    if async_time < sync_time:
        speedup = sync_time / async_time
        print(f"   🎉 Async is {speedup:.2f}x faster!")
    else:
        slowdown = async_time / sync_time
        print(f"   ⚠️  Async is {slowdown:.2f}x slower (overhead > I/O benefit)")





if __name__ == "__main__":
    print("🧪 Async Archetype System Benchmark")

    asyncio.run(benchmark_async_vs_sync())
