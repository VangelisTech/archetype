
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

import time
import os
import shutil
import sys
import asyncio

# Add the project root to the python path
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_dir, "..", "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from archetype.core.aio import make_async_world, AsyncProcessor
from archetype.core import Component, processor
from daft import DataFrame

# Define Components
class A(Component): value: int
class B(Component): value: int

# Define Processors
@processor(A)
class SystemA(AsyncProcessor):
    async def process(self, df: DataFrame, semaphore: asyncio.Semaphore) -> DataFrame:
        # In a real scenario, we would need a way to spawn entities from a processor.
        # For this benchmark, we'll just return the dataframe as is.
        return df

@processor(B)
class SystemB(AsyncProcessor):
    async def process(self, df: DataFrame, semaphore: asyncio.Semaphore) -> DataFrame:
        # In a real scenario, we would need a way to despawn entities from a processor.
        # For this benchmark, we'll just return the dataframe as is.
        return df

async def run_benchmark(world):
    # Entity Cycle
    # Dataset: 1,000 entities with a single A component.
    for i in range(1000):
        world.spawn(A(value=i))

    world.add_processor(SystemA())
    world.add_processor(SystemB())

    start_time = time.time()
    
    # Iterate through all entities, and create 1 entity with a B component.
    await world.step()
    for i in range(1000):
        world.spawn(B(value=i))
    
    # Then iterate through all entities with a B component and destroy them.
    await world.step()
    for i in range(1, 1001):
        await world.despawn(i + 1000) # Despawn the B entities

    end_time = time.time()

    return end_time - start_time

async def main():
    temp_dir = ".archetype_benchmarks/entity_cycle_async"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    world = make_async_world(uri=temp_dir)
    
    print("Running Entity Cycle Benchmark (Async)...")
    duration = await run_benchmark(world)
    print(f"Entity Cycle Benchmark (Async) duration: {duration:.3f}s")

    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    asyncio.run(main())
