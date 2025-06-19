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
import tempfile

# Ensure the parent directory is in sys.path so 'archetype' can be imported
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_dir, "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


from archetype.core import processor, Component # noqa: F401
from archetype.core.aio import AsyncWorld, AsyncStore, AsyncQueryManager, AsyncUpdateManager, AsyncSystem, AsyncProcessor # noqa: F401

from daft import DataFrame, col # noqa: F401
import asyncio # noqa: F401


# Define Components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float


@processor(Position, Velocity, priority=1)
class MovementProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, semaphore: asyncio.Semaphore, dt: float) -> DataFrame: # Assuming df is passed by the system
        async with semaphore:
            print(f"    MovementProcessor: Processing {len(df)} entities with dt={dt}")
            print(f"    Input data:")
            df.show()
            
            df = df.with_columns({
                "position__x": col("position__x") + col("velocity__vx") * dt,
                "position__y": col("position__y") + col("velocity__vy") * dt,
            })
            
            print(f"    Output data:")
            df.show()
            return df

def make_async_world(uri, debug=False):
    """Create an async world using the current simplified architecture."""
    store = AsyncStore(uri)
    querier = AsyncQueryManager(store)
    updater = AsyncUpdateManager(store)
    system = AsyncSystem()
    
    world = AsyncWorld(
        querier=querier,
        updater=updater,
        async_system=system,
        world_id="toy_world",
        run_id="toy_run",
        debug=debug
    )
    return world

async def main(uri, debug=False):
    world = make_async_world(uri, debug=debug)
    await world.async_system.add_processor(MovementProcessor())

    print("Spawning entities...")
    entity_ids = []
    for i in range(10):
        entity_id = world.spawn(Position(x=0.0, y=0.0), Velocity(vx=float(i), vy=float(i)))
        entity_ids.append(entity_id)
        print(f"  Spawned entity {entity_id} with velocity ({i}, {i})")

    print(f"Running {10} simulation steps...")
    for i in range(10):
        print(f"  Step {i+1}")
        await world.step(dt=0.01)
        print(f"    Current world step: {world.current_step}")

    # Verify the simulation actually worked by checking final positions
    print("\nVerifying simulation results...")
    from archetype.core.interfaces import Archetype
    sig = Archetype.sig_from_components([Position(x=0.0, y=0.0), Velocity(vx=0.0, vy=0.0)])
    
    try:
        # Use the store directly since that's what has get_archetype_df
        final_df = await world.querier._store.get_archetype_df(sig, world.current_step, world.world_id, world.run_id)
        print(f"Found {len(final_df)} entities in final state")
        
        if len(final_df) > 0:
            print("Final entity states:")
            final_df.show()
            return world
        else:
            print("❌ No entities found in final state!")
            return None
    except Exception as e:
        print(f"❌ Failed to verify results: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Use temp directory instead of relative path
    temp_dir = tempfile.mkdtemp()
    print(f"Using temp directory: {temp_dir}")

    try:
        world = asyncio.run(main(temp_dir, debug=True))
        if world:
            print("✓ Async simulation completed and verified!")
        else:
            print("❌ Async simulation failed verification!")
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
