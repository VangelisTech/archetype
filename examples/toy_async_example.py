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
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))


# Enable VizTracer-based instrumentation before importing archetype modules
# (shim treats ARCT_TRACY=1 or ARCT_VIZTRACER=1 as "enabled")
os.environ.setdefault("ARCT_VIZTRACER", "1")
os.environ.setdefault("ARCT_TRACY", "1")

from archetype import Component, AsyncProcessor, WorldOrchestrator, AsyncSystem, StorageConfig, RunConfig, WorldConfig
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

    # Spawn initial entities (list of components as required by the API)
    # Allow override via env var; use a smaller default when viztracer is enabled to keep traces manageable
    default_n = 1000000
    if os.getenv("ARCT_VIZTRACER", "0") == "1":
        default_n = 50000
    num_entities = int(os.getenv("ARCT_ENTITIES", str(default_n)))
    for i in range(num_entities):
        await world.create_entity([Position(x=0, y=0), Velocity(vx=i, vy=i)])

    # Run simulation steps
    await world.run(run_config, dt=0.1)

    return world

if __name__ == "__main__":
    # Set up configuration objects
    storage_config = StorageConfig(
        uri=".archetype_data/",
        namespace="examples",
        has_cache=True,
        use_lancedb=True,
        backend="lancedb",
    )
    world_config = WorldConfig(name="toy_async_world")
    run_config = RunConfig(num_steps=10, debug=True, show_rows=0, explain=False)

    # If VizTracer is available, emit an HTML timeline for inspection
    try:
        from viztracer import VizTracer  # type: ignore
        tracer_entries = int(os.getenv("ARCT_VIZ_ENTRIES", "3000000"))
        with VizTracer(output_file="toy_trace.html", tracer_entries=tracer_entries):
            asyncio.run(single_world_simulation(storage_config, world_config, run_config))
        print("wrote toy_trace.html")
    except Exception:
        asyncio.run(single_world_simulation(storage_config, world_config, run_config))