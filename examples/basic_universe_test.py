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
Basic Universe Test

This script tests the consolidated Universe architecture to ensure:
1. Regular episode universe works
2. Ray universe works (if Ray is available)
3. Basic processor functionality
4. Episode caching and flushing

Usage:
    python basic_universe_test.py
"""

import asyncio
import sys
import os
from pathlib import Path

# Add the src directory to the path so we can import archetype
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    import archetype as arch
    from archetype.core.interfaces import Component
    from archetype.core.aio.async_processor import AsyncProcessor
    from pydantic import Field
    import daft
    print("✓ Archetype imports successful")
except ImportError as e:
    print(f"✗ Import error: {e}")
    sys.exit(1)


# Test Components
class Position(Component):
    x: float = Field(default=0.0)
    y: float = Field(default=0.0)


class Velocity(Component):
    dx: float = Field(default=0.0)
    dy: float = Field(default=0.0)


# Test Processor
class SimpleMovementProcessor(AsyncProcessor):
    """Simple processor that moves entities based on velocity."""
    
    def __init__(self):
        super().__init__()
        self.components = [Position, Velocity]
        self.priority = 1
    
    async def process(self, df: daft.DataFrame, semaphore, *args, **kwargs) -> daft.DataFrame:
        """Update positions based on velocity."""
        async with semaphore:
            # Simple movement: position += velocity
            updated_df = df.with_column(
                "position_x", df["position_x"] + df["velocity_dx"]
            ).with_column(
                "position_y", df["position_y"] + df["velocity_dy"]
            )
            
            return updated_df


async def test_episode_universe():
    """Test basic episode universe functionality."""
    print("\n=== Testing Episode Universe ===")
    
    try:
        # Create episode universe
        universe = await arch.create_universe("memory://", "episode")
        print("✓ Episode universe created")
        
        # Create world with processor
        world_id = await universe.create_world([SimpleMovementProcessor()])
        print(f"✓ World created: {world_id}")
        
        # Get world stats
        stats = await universe.get_world_stats(world_id)
        print(f"✓ World stats: step={stats['current_step']}, entities={stats['active_entities']}")
        
        # Step the world (should work even with no entities)
        result = await universe.step_world(world_id)
        print(f"✓ World stepped: {result}")
        
        # Test run_simulation
        sim_result = await universe.run_simulation(world_id, steps=3, flush_every=2)
        print(f"✓ Simulation completed: {sim_result['steps_executed']} steps in {sim_result['total_time']:.3f}s")
        
        # Get final universe stats
        universe_stats = universe.get_stats()
        print(f"✓ Universe stats: {universe_stats['active_worlds']} worlds, {universe_stats['total_steps_executed']} total steps")
        
        # Test episode cache stats
        cache_stats = universe_stats['store_cache_stats']
        print(f"✓ Cache stats: {cache_stats['cached_records']} records, {cache_stats['episode_count']} episodes")
        
        # Cleanup
        await universe.shutdown()
        print("✓ Episode universe test completed successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Episode universe test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_ray_universe():
    """Test Ray universe functionality (if Ray is available)."""
    print("\n=== Testing Ray Universe ===")
    
    try:
        # Check if Ray is available
        try:
            import ray
        except ImportError:
            print("⚠ Ray not available, skipping Ray universe test")
            return True
        
        # Initialize Ray
        arch.init_ray()
        print("✓ Ray initialized")
        
        # Create Ray universe
        universe = await arch.create_ray_universe("memory://", system_pool_size=2)
        print("✓ Ray universe created")
        
        # Create world with processor
        world_id = await universe.create_world([SimpleMovementProcessor()])
        print(f"✓ Ray world created: {world_id}")
        
        # Get world stats
        stats = await universe.get_world_stats(world_id)
        print(f"✓ Ray world stats: step={stats['current_step']}, entities={stats['active_entities']}")
        
        # Step the world
        result = await universe.step_world(world_id)
        print(f"✓ Ray world stepped: {result}")
        
        # Test run_simulation
        sim_result = await universe.run_simulation(world_id, steps=3, flush_every=2)
        print(f"✓ Ray simulation completed: {sim_result['steps_executed']} steps in {sim_result['total_time']:.3f}s")
        
        # Get final universe stats
        universe_stats = universe.get_stats()
        print(f"✓ Ray universe stats: {universe_stats['active_worlds']} worlds, {universe_stats['system_pool_size']} system actors")
        
        # Cleanup
        await universe.shutdown()
        arch.shutdown_ray()
        print("✓ Ray universe test completed successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Ray universe test failed: {e}")
        import traceback
        traceback.print_exc()
        try:
            arch.shutdown_ray()
        except:
            pass
        return False


async def test_quick_simulation():
    """Test the quick_simulation convenience function."""
    print("\n=== Testing Quick Simulation ===")
    
    try:
        # Test quick simulation
        result = await arch.quick_simulation(
            processors=[SimpleMovementProcessor()],
            steps=5,
            store_uri="memory://",
            universe_type="episode"
        )
        
        print(f"✓ Quick simulation completed: {result['steps_executed']} steps in {result['total_time']:.3f}s")
        return True
        
    except Exception as e:
        print(f"✗ Quick simulation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run all tests."""
    print("Starting Archetype Universe Tests...")
    
    results = []
    
    # Test episode universe
    results.append(await test_episode_universe())
    
    # Test Ray universe
    results.append(await test_ray_universe())
    
    # Test quick simulation
    results.append(await test_quick_simulation())
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    print(f"\n=== Test Summary ===")
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! Architecture is working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nTests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Test runner error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 