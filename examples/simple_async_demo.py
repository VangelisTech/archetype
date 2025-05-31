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

from archetype.core import processor, Processor, Component
from archetype import make_simple_world
from archetype.core.aio.async_system import AsyncProcessor, async_processor
from archetype.core.aio.async_world import AsyncWorld

from daft import DataFrame, col 


# Define Components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float


# Async processor with simulated I/O
@async_processor(Position, Velocity, priority=1)
class AsyncMovementProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        # Simulate meaningful async I/O delay
        await asyncio.sleep(0.05)  # 50ms per archetype
        
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })


async def demonstrate_async_benefit():
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
    sync_world = make_simple_world(uri, debug=False)
    async_world = AsyncWorld(sync_world, max_concurrent_archetypes=5)
    async_world.add_processor(AsyncMovementProcessor())
    
    # Spawn multiple entities (this creates multiple archetype tables)
    print("📦 Spawning 5 entities (each will create its own archetype table)...")
    async_world.spawn(Position(x=1, y=1), Velocity(vx=1, vy=1))
    async_world.spawn(Position(x=2, y=2), Velocity(vx=2, vy=2))
    async_world.spawn(Position(x=3, y=3), Velocity(vx=3, vy=3))
    async_world.spawn(Position(x=4, y=4), Velocity(vx=4, vy=4))
    async_world.spawn(Position(x=5, y=5), Velocity(vx=5, vy=5))
    
    async_world.materialize_spawns()
    
    print("✅ Entities spawned - each creates its own archetype table")
    print("   This means 5 separate archetype tables to process concurrently")
    
    # Run async simulation
    print("\n⚡ Running async simulation step...")
    print("   Expected: ~50ms total (5 archetypes × 50ms each, processed concurrently)")
    
    start = time.time()
    await async_world.async_step(dt=0.1)
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
    asyncio.run(demonstrate_async_benefit())