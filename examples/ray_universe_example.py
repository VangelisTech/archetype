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

"""
Ray Universe Example

This example demonstrates the Ray-enhanced universe with distributed world actors
and shared system pools. It shows how to:

1. Create a Ray universe with episode caching
2. Create multiple worlds with different configurations
3. Run distributed simulations across Ray actors
4. Monitor performance and statistics

Requirements:
    pip install ray

Usage:
    python ray_universe_example.py
"""

import asyncio
import sys
import os
from pathlib import Path

# Add the src directory to the path so we can import archetype
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    import ray
except ImportError:
    print("Ray is required for this example. Install with: pip install ray")
    sys.exit(1)

import archetype as arch
from archetype.core.interfaces import Component
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.processor import Processor as SyncProcessor
from pydantic import Field
import daft
import random
import time


# Example Components
class Position(Component):
    x: float = Field(default=0.0)
    y: float = Field(default=0.0)


class Velocity(Component):
    dx: float = Field(default=0.0)
    dy: float = Field(default=0.0)


class Health(Component):
    current: int = Field(default=100)
    max: int = Field(default=100)


# Example Processors
class MovementProcessor(AsyncProcessor):
    """Async processor that updates entity positions based on velocity."""
    
    def __init__(self):
        super().__init__()
        self.components = [Position, Velocity]
        self.priority = 1
    
    async def process(self, df: daft.DataFrame, semaphore, *args, **kwargs) -> daft.DataFrame:
        """Update positions based on velocity."""
        async with semaphore:
            # Simulate some processing time
            await asyncio.sleep(0.001)
            
            # Update positions
            updated_df = df.with_column(
                "position_x", df["position_x"] + df["velocity_dx"]
            ).with_column(
                "position_y", df["position_y"] + df["velocity_dy"]
            )
            
            return updated_df


class HealthDecayProcessor(SyncProcessor):
    """Sync processor that decreases health over time."""
    
    def __init__(self):
        super().__init__()
        self.components = [Health]
        self.priority = 2
    
    def process(self, df: daft.DataFrame, *args, **kwargs) -> daft.DataFrame:
        """Decrease health by 1 each step."""
        return df.with_column(
            "health_current", 
            (df["health_current"] - 1).clip(0, df["health_max"])
        )


class CollisionProcessor(AsyncProcessor):
    """Async processor that handles entity collisions."""
    
    def __init__(self):
        super().__init__()
        self.components = [Position, Health]
        self.priority = 3
    
    async def process(self, df: daft.DataFrame, semaphore, *args, **kwargs) -> daft.DataFrame:
        """Simple collision detection - entities at same position lose health."""
        async with semaphore:
            # Simulate collision detection computation
            await asyncio.sleep(0.002)
            
            # For simplicity, just return the dataframe unchanged
            # In a real implementation, you'd detect collisions and update health
            return df


async def create_test_entities(universe, world_id: str, count: int = 100):
    """Create test entities in the specified world."""
    print(f"Creating {count} test entities in world {world_id}")
    
    # Get the world actor reference
    world_actor = universe._impl.world_actors[world_id]
    
    # Create entities with random positions and velocities
    for i in range(count):
        components = [
            Position(x=random.uniform(-100, 100), y=random.uniform(-100, 100)),
            Velocity(dx=random.uniform(-1, 1), dy=random.uniform(-1, 1)),
            Health(current=random.randint(50, 100), max=100)
        ]
        
        # Spawn entity in the world actor
        entity_id = ray.get(world_actor.spawn.remote(*components))
        
        if i % 20 == 0:
            print(f"  Created entity {i+1}/{count}")


async def run_ray_universe_demo():
    """Run the Ray universe demonstration."""
    print("=== Ray Universe Example ===\n")
    
    # Initialize Ray
    print("1. Initializing Ray...")
    arch.init_ray()
    print(f"   Ray cluster resources: {ray.cluster_resources()}")
    
    # Create Ray universe with episode caching
    print("\n2. Creating Ray universe with episode caching...")
    universe = await arch.create_ray_universe(
        store_uri="memory://",
        system_pool_size=4  # 4 system actors in the pool
    )
    
    print(f"   Universe created with {len(universe._impl.system_actor_pool)} system actors")
    
    # Create multiple worlds with different configurations
    print("\n3. Creating multiple worlds...")
    
    # World 1: Movement simulation
    movement_world = await universe.create_world(
        processors=[MovementProcessor(), HealthDecayProcessor()],
        world_id="movement_world",
        num_cpus=1.0,
        memory=256  # MB
    )
    print(f"   Created movement world: {movement_world}")
    
    # World 2: Combat simulation
    combat_world = await universe.create_world(
        processors=[MovementProcessor(), CollisionProcessor(), HealthDecayProcessor()],
        world_id="combat_world", 
        num_cpus=2.0,
        memory=512  # MB
    )
    print(f"   Created combat world: {combat_world}")
    
    # Create test entities in both worlds
    print("\n4. Populating worlds with entities...")
    await create_test_entities(universe, movement_world, count=50)
    await create_test_entities(universe, combat_world, count=75)
    
    # Get initial statistics
    print("\n5. Initial universe statistics:")
    stats = universe.get_stats()
    print(f"   Active worlds: {stats['active_worlds']}")
    print(f"   System pool size: {stats['system_pool_size']}")
    print(f"   Ray cluster resources: {stats.get('ray_cluster_resources', {})}")
    
    # Run simulation steps
    print("\n6. Running distributed simulation...")
    
    step_times = []
    for step in range(10):
        start_time = time.time()
        
        # Step all worlds concurrently
        result = await universe.step_all_worlds()
        
        end_time = time.time()
        step_time = end_time - start_time
        step_times.append(step_time)
        
        print(f"   Step {step + 1}: {step_time:.3f}s - {result['worlds_stepped']} worlds")
        
        # Show world statistics every 5 steps
        if (step + 1) % 5 == 0:
            for world_id in [movement_world, combat_world]:
                world_stats = await universe.get_world_stats(world_id)
                print(f"     {world_id}: step {world_stats['current_step']}, "
                      f"{world_stats['active_entities']} entities")
    
    # Performance analysis
    avg_step_time = sum(step_times) / len(step_times)
    print(f"\n   Average step time: {avg_step_time:.3f}s")
    print(f"   Total simulation time: {sum(step_times):.3f}s")
    
    # Run a longer simulation on just one world
    print("\n7. Running focused simulation on movement world...")
    focused_result = await universe.run_simulation(
        world_id=movement_world,
        steps=20,
        flush_every=5
    )
    
    print(f"   Completed {focused_result['steps_executed']} steps in {focused_result['total_time']:.3f}s")
    print(f"   Average time per step: {focused_result['avg_time_per_step']:.3f}s")
    
    # Final statistics
    print("\n8. Final universe statistics:")
    final_stats = universe.get_stats()
    print(f"   Total worlds created: {final_stats['total_worlds_created']}")
    print(f"   Total steps executed: {final_stats['total_steps_executed']}")
    
    cache_stats = final_stats['store_cache_stats']
    print(f"   Episode cache: {cache_stats['cached_records']} records, "
          f"{cache_stats['episode_count']} flushes")
    
    # Cleanup
    print("\n9. Cleaning up...")
    await universe.shutdown()
    arch.shutdown_ray()
    
    print("Ray universe demo completed successfully!")


async def run_performance_comparison():
    """Compare Ray universe performance with regular async universe."""
    print("\n=== Performance Comparison ===\n")
    
    processors = [MovementProcessor(), HealthDecayProcessor()]
    
    # Test with regular async universe
    print("Testing regular async universe...")
    async_universe = await arch.create_universe("memory://", "episode")
    async_world = await async_universe.create_world(processors)
    
    # Create entities
    world_impl = async_universe._impl.worlds[async_world]
    for i in range(100):
        world_impl.spawn(
            Position(x=random.uniform(-100, 100), y=random.uniform(-100, 100)),
            Velocity(dx=random.uniform(-1, 1), dy=random.uniform(-1, 1)),
            Health(current=100, max=100)
        )
    
    # Time async universe
    start_time = time.time()
    for _ in range(10):
        await async_universe.step_world(async_world)
    async_time = time.time() - start_time
    
    await async_universe.shutdown()
    
    # Test with Ray universe
    print("Testing Ray universe...")
    arch.init_ray()
    ray_universe = await arch.create_ray_universe("memory://", system_pool_size=2)
    ray_world = await ray_universe.create_world(processors)
    
    # Create entities
    await create_test_entities(ray_universe, ray_world, count=100)
    
    # Time Ray universe
    start_time = time.time()
    for _ in range(10):
        await ray_universe.step_world(ray_world)
    ray_time = time.time() - start_time
    
    await ray_universe.shutdown()
    arch.shutdown_ray()
    
    # Results
    print(f"\nPerformance Results (10 steps, 100 entities):")
    print(f"  Async Universe: {async_time:.3f}s")
    print(f"  Ray Universe:   {ray_time:.3f}s")
    print(f"  Speedup:        {async_time/ray_time:.2f}x" if ray_time < async_time else f"  Slowdown:       {ray_time/async_time:.2f}x")


if __name__ == "__main__":
    try:
        # Run the main demo
        asyncio.run(run_ray_universe_demo())
        
        # Run performance comparison
        asyncio.run(run_performance_comparison())
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"Error running demo: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Ensure Ray is shut down
        try:
            arch.shutdown_ray()
        except:
            pass 