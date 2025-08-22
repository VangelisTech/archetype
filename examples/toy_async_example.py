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

import os
import asyncio
import sys
# Dev convenience: enable only when explicitly set by the user
if os.environ.get("ARCT_DEV_EXAMPLE", "0") == "1":
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
    os.environ.setdefault("ARCT_VIZTRACER", "1")
    os.environ.setdefault("ARCT_TRACY", "1")

from archetype import Component, AsyncProcessor, WorldOrchestrator, AsyncSystem, StorageConfig, RunConfig, WorldConfig, CacheConfig
from daft import DataFrame, col

# Define Components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float
class MovementProcessor(AsyncProcessor):
    components = [Position, Velocity]
    priority = 1
    
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        df = df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })
        return df

async def single_world_simulation(
    storage_config: StorageConfig, 
    world_config: WorldConfig, 
    run_config: RunConfig,
    cache_config=None,
    num_entities: int = 10000,
    ):
    # Set up orchestrator and system
    orchestrator = WorldOrchestrator()

    # Create the world via the orchestrator
    world = await orchestrator.create_world(
        config=world_config,
        system=AsyncSystem(),
        storage_config=storage_config,
        cache_config=cache_config,
        instrumented=True,
    )
    # Register the movement processor
    await world.add_processor(MovementProcessor())

    
    for i in range(num_entities):
        await world.create_entity([Position(x=0, y=0), Velocity(vx=i, vy=i)])

    # Run simulation steps
    await world.run(run_config, dt=0.1)

    return world

if __name__ == "__main__":
    pwd = os.getcwd()
    # Set up configuration objects
    storage_config = StorageConfig(
        uri=f"{pwd}/archetype_data",
        namespace="examples",
    )
    world_config = WorldConfig(name="toy_async_world")

    run_config = RunConfig(num_steps=50, debug=False, show_rows=0, explain=False)
    cache_config = CacheConfig()  # Explicit cache configuration
    num_entities = 100000

    asyncio.run(single_world_simulation(storage_config, world_config, run_config, cache_config=cache_config, num_entities=num_entities))