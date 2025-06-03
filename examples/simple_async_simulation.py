#!/usr/bin/env python3

import sys
import os
import asyncio
import time

# Ensure the parent directory is in sys.path so 'archetype' can be imported
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_dir, "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from daft import DataFrame, col 

from archetype.core import processor, Component
from archetype.core.aio import make_async_world, AsyncProcessor, async_processor


# Define Components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float


# Async processor with simulated I/O
@processor(Position, Velocity, priority=1)
class AsyncMovementProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, semaphore: asyncio.Semaphore, dt: float) -> DataFrame:
        await asyncio.sleep(0.01)
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })


async def main():
    """
    Simple demo showing async performance benefit.
    
    We'll create multiple archetypes (each entity gets its own archetype table
    since they all have different Position+Velocity values) to show true
    concurrent processing.
    """
    
    print("🚀 Async Archetype System Demo")
    print("=" * 40)
    
    uri = "/Users/everett-founder/git/vangelis/internal/work/libs/archetype/data"

    # Create async world
    async_world = make_async_world(uri, debug=True, max_concurrent_archetypes=10)
    async_world.add_processor(AsyncMovementProcessor())
    
    # Spawn multiple entities (this creates multiple archetype tables)
    print("> Spawning 5 entities (each will create its own archetype table)...")
    async_world.spawn(Position(x=1, y=1), Velocity(vx=1, vy=1))
    async_world.spawn(Position(x=2, y=2), Velocity(vx=2, vy=2))
    async_world.spawn(Position(x=3, y=3), Velocity(vx=3, vy=3))
    async_world.spawn(Position(x=4, y=4), Velocity(vx=4, vy=4))
    async_world.spawn(Position(x=5, y=5), Velocity(vx=5, vy=5))
    
        
    start = time.time()
    for i in range(10):
        await async_world.step(dt=0.1)
    elapsed = time.time() - start
    
    print(f"✅ Async step completed in {elapsed:.3f}s")
    
    if elapsed < 0.1:  # Much less than 5 × 50ms = 250ms
        print("🎉 SUCCESS: Concurrent processing is working!")
        print(f"   If processed sequentially: would take ~250ms")
        print(f"   Actual concurrent time: {elapsed*1000:.0f}ms")
        print(f"   Speedup achieved: ~{250/1000/elapsed:.1f}x")
    else:
        print("⚠️  Something's not quite right with concurrency")
        
    print("\n🔍 How it works:")
    print("   1. Each entity has different Position+Velocity values")
    print("   2. This creates separate archetype tables per entity")  
    print("   3. AsyncSystem processes each archetype table concurrently")
    print("   4. 50ms I/O per archetype happens in parallel, not sequentially")


if __name__ == "__main__":
    asyncio.run(main())