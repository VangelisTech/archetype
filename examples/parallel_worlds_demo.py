"""
Demonstration of running multiple worlds in parallel using Archetype.

This example shows:
1. Creating multiple worlds programmatically
2. Running them all in parallel
3. Basic performance comparison vs sequential execution
"""

import asyncio
import time
from archetype.app import Archetype
from archetype.core import Component
from archetype.core.aio import AsyncProcessor, AsyncSystem
import daft


# Define a simple component
class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


# Define a simple processor
class MovementProcessor(AsyncProcessor):
    """Updates positions based on velocities."""
    
    components = [Position, Velocity]
    priority = 0
    
    async def process(self, df: daft.DataFrame, dt: float = 1.0) -> daft.DataFrame:
        """Apply velocity to position."""
        df = df.with_column("position__x", 
                           daft.col("position__x") + daft.col("velocity__vx") * dt)
        df = df.with_column("position__y",
                           daft.col("position__y") + daft.col("velocity__vy") * dt)
        return df


async def create_populated_system(world_index: int) -> AsyncSystem:
    """Create a system with processors for a world."""
    system = AsyncSystem()
    system.add_processor(MovementProcessor())
    return system


async def main():
    print("🚀 Archetype Parallel Worlds Demo")
    print("=" * 50)
    
    # Initialize Archetype
    app = await Archetype.create(storage_uri=".demo_data")
    
    # Configuration
    NUM_WORLDS = 10
    NUM_STEPS = 100
    
    print(f"\n📊 Configuration:")
    print(f"  - Number of worlds: {NUM_WORLDS}")
    print(f"  - Steps per world: {NUM_STEPS}")
    
    # ========================================
    # PARALLEL EXECUTION
    # ========================================
    print(f"\n⚡ Running {NUM_WORLDS} worlds in PARALLEL...")
    start_time = time.time()
    
    # Create and run worlds in parallel
    world_ids = await app.run_parallel_worlds(
        num_worlds=NUM_WORLDS,
        steps=NUM_STEPS,
        system_factory=create_populated_system
    )
    
    parallel_time = time.time() - start_time
    print(f"✅ Parallel execution completed in {parallel_time:.2f} seconds")
    
    # Clean up parallel worlds
    for world_id in world_ids:
        app.orchestrator.remove_world(world_id)
    
    # ========================================
    # SEQUENTIAL EXECUTION (for comparison)
    # ========================================
    print(f"\n🐌 Running {NUM_WORLDS} worlds SEQUENTIALLY...")
    start_time = time.time()
    
    for i in range(NUM_WORLDS):
        # Create world
        world = await app.create_world(
            name=f"sequential_world_{i}",
            system=await create_populated_system(i)
        )
        
        # Run it
        await app.run_world(world.world_id, steps=NUM_STEPS)
        
        # Clean up
        app.orchestrator.remove_world(world.world_id)
    
    sequential_time = time.time() - start_time
    print(f"✅ Sequential execution completed in {sequential_time:.2f} seconds")
    
    # ========================================
    # RESULTS
    # ========================================
    print("\n📈 Performance Results:")
    print(f"  - Sequential: {sequential_time:.2f}s")
    print(f"  - Parallel:   {parallel_time:.2f}s")
    speedup = sequential_time / parallel_time
    print(f"  - Speedup:    {speedup:.2f}x faster")
    
    # Cleanup
    await app.shutdown()
    print("\n✨ Demo completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())