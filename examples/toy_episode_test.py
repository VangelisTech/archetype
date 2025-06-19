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
Toy Episode Simulation Test

This script tests the episode caching functionality with a simple simulation:
1. Creates an AsyncEpisodeStore with a base AsyncStore
2. Spawns entities with Position and Velocity components
3. Runs a simple movement processor
4. Tests episode caching and flushing
5. Verifies data persistence

Usage:
    python toy_episode_test.py
"""

import asyncio
import sys
import os
import tempfile
from pathlib import Path

# Add the src directory to the path so we can import archetype
# Current structure: /home/ray/default/archetype/examples/toy_episode_test.py
# Package location: /home/ray/default/archetype/src/archetype/
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import daft
from pydantic import Field

# Import archetype components
from archetype.core.interfaces import Component, Archetype
from archetype.core.processor import processor
from archetype.core.aio.async_store import AsyncStore, AsyncEpisodeStore
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.aio.async_updater import AsyncUpdateManager

print("✓ All imports successful")


# Test Components
class Position(Component):
    x: float = Field(default=0.0)
    y: float = Field(default=0.0)


class Velocity(Component):
    dx: float = Field(default=1.0)
    dy: float = Field(default=0.5)


# Test Processor
@processor(Position, Velocity, priority=1)
class MovementProcessor(AsyncProcessor):
    """Simple processor that moves entities based on velocity."""
    async def process(self, df: daft.DataFrame, semaphore, *args, **kwargs) -> daft.DataFrame:
        """Update positions based on velocity."""
        async with semaphore:
            # Simple movement: position += velocity
            updated_df = df.with_column(
                "position_x", df["position_x"] + df["velocity_dx"]
            ).with_column(
                "position_y", df["position_y"] + df["velocity_dy"]
            )
            
            print(f"  Processed {len(updated_df)} entities")
            return updated_df


async def test_episode_store_basic():
    """Test basic episode store functionality."""
    print("\n=== Testing Episode Store Basics ===")
    
    # Create temporary directory for testing
    temp_dir = tempfile.mkdtemp()
    print(f"Using temp directory: {temp_dir}")
    
    try:
        # Create base store and episode store
        base_store = AsyncStore(temp_dir)
        episode_store = AsyncEpisodeStore(base_store, max_cache_size_mb=100)
        print("✓ Episode store created")
        
        # Create test data
        sig = Archetype.sig_from_components([Position(), Velocity()])
        print(f"✓ Archetype signature: {sig}")
        
        # Create some test entities
        entities = []
        for i in range(5):
            entity = {
                "world_id": "test_world",
                "run_id": "test_run",
                "entity_id": i,
                "step": 0,
                "is_active": True,
                "position_x": float(i),
                "position_y": float(i * 2),
                "velocity_dx": 1.0,
                "velocity_dy": 0.5
            }
            entities.append(entity)
        
        # Create dataframe and append to episode store
        df = daft.from_pylist(entities)
        print(f"✓ Created dataframe with {len(df)} entities")
        
        # Test append to episode cache
        await episode_store.append(sig, df, 0, "test_world", "test_run")
        print("✓ Appended to episode cache")
        
        # Check cache stats
        cache_stats = episode_store.get_cache_stats()
        print(f"✓ Cache stats: {cache_stats}")
        
        # Test reading from cache
        read_df = await episode_store.get_archetype_df(sig, 0, "test_world", "test_run")
        print(f"✓ Read {len(read_df)} entities from cache")
        
        # Test flushing episodes
        flush_stats = await episode_store.flush_episodes()
        print(f"✓ Flushed episodes: {flush_stats}")
        
        # Verify data persisted to base store
        base_df = await base_store.get_archetype_df(sig, 0, "test_world", "test_run")
        print(f"✓ Verified {len(base_df)} entities persisted to base store")
        
        return True
        
    except Exception as e:
        print(f"✗ Episode store test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


async def test_episode_simulation():
    """Test a complete episode simulation with processing."""
    print("\n=== Testing Episode Simulation ===")
    
    # Create temporary directory for testing
    temp_dir = tempfile.mkdtemp()
    print(f"Using temp directory: {temp_dir}")
    
    try:
        # Create infrastructure
        base_store = AsyncStore(temp_dir)
        episode_store = AsyncEpisodeStore(base_store, max_cache_size_mb=100)
        querier = AsyncQueryManager(episode_store)
        updater = AsyncUpdateManager(episode_store)
        system = AsyncSystem()
        
        # Add processor to system
        processor = MovementProcessor()
        await system.add_processor(processor)
        print("✓ Created simulation infrastructure")
        
        # Create world
        world = AsyncWorld(
            querier=querier,
            updater=updater,
            async_system=system,
            world_id="episode_world",
            run_id="episode_run"
        )
        print("✓ Created world")
        
        # Spawn some entities
        print("Spawning entities...")
        entity_ids = []
        for i in range(3):
            entity_id = world.spawn(
                Position(x=float(i), y=float(i * 2)),
                Velocity(dx=1.0, dy=0.5)
            )
            entity_ids.append(entity_id)
            print(f"  Spawned entity {entity_id}")
        
        print(f"✓ Spawned {len(entity_ids)} entities")
        
        # Run simulation steps
        print("Running simulation steps...")
        for step in range(3):
            print(f"\n--- Step {step + 1} ---")
            
            # Check cache stats before step
            cache_stats = episode_store.get_cache_stats()
            print(f"  Cache before step: {cache_stats['cached_records']} records")
            
            # Execute step
            await world.step()
            print(f"  Completed step {world.current_step}")
            
            # Check cache stats after step
            cache_stats = episode_store.get_cache_stats()
            print(f"  Cache after step: {cache_stats['cached_records']} records")
        
        # Final flush
        print("\nFlushing final episode...")
        flush_stats = await episode_store.flush_episodes()
        print(f"✓ Final flush: {flush_stats}")
        
        # Verify final cache state
        final_cache_stats = episode_store.get_cache_stats()
        print(f"✓ Final cache stats: {final_cache_stats}")
        
        # Verify data in base store
        sig = Archetype.sig_from_components([Position(), Velocity()])
        final_df = await base_store.get_archetype_df(sig, world.current_step, "episode_world", "episode_run")
        print(f"✓ Final data in base store: {len(final_df)} entities")
        
        # Show final positions
        if len(final_df) > 0:
            positions = final_df.select("entity_id", "position_x", "position_y").to_pylist()
            print("Final entity positions:")
            for pos in positions:
                print(f"  Entity {pos['entity_id']}: ({pos['position_x']}, {pos['position_y']})")
        
        return True
        
    except Exception as e:
        print(f"✗ Episode simulation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


async def test_episode_cache_flushing():
    """Test episode cache auto-flushing behavior."""
    print("\n=== Testing Episode Cache Auto-Flushing ===")
    
    # Create temporary directory for testing
    temp_dir = tempfile.mkdtemp()
    print(f"Using temp directory: {temp_dir}")
    
    try:
        # Create episode store with small auto-flush threshold
        base_store = AsyncStore(temp_dir)
        episode_store = AsyncEpisodeStore(base_store, max_cache_size_mb=1)
        
        # Override the auto-flush threshold for testing
        original_threshold = episode_store.total_cached_operations
        episode_store.total_cached_operations = 0  # Reset counter
        print("✓ Created episode store with small flush threshold")
        
        # Create test signature
        sig = Archetype.sig_from_components([Position()])
        
        # Add many small operations to trigger auto-flush
        print("Adding operations to trigger auto-flush...")
        for i in range(5):
            entity = {
                "world_id": "test_world",
                "run_id": "test_run", 
                "entity_id": i,
                "step": 0,
                "is_active": True,
                "position_x": float(i),
                "position_y": float(i)
            }
            df = daft.from_pylist([entity])
            await episode_store.append(sig, df, 0, "test_world", "test_run")
            
            cache_stats = episode_store.get_cache_stats()
            print(f"  Operation {i+1}: {cache_stats['total_operations']} total ops, "
                  f"{cache_stats['cached_records']} cached records")
        
        # Check if auto-flush occurred
        final_cache_stats = episode_store.get_cache_stats()
        print(f"✓ Final cache state: {final_cache_stats}")
        
        return True
        
    except Exception as e:
        print(f"✗ Episode cache flushing test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


async def main():
    """Run all episode tests."""
    print("Starting Episode Caching Tests...")
    
    results = []
    
    # Test basic episode store functionality
    results.append(await test_episode_store_basic())
    
    # Test complete episode simulation
    results.append(await test_episode_simulation())
    
    # Test cache auto-flushing
    results.append(await test_episode_cache_flushing())
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    print(f"\n=== Episode Test Summary ===")
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All episode tests passed! Episode caching is working correctly.")
        print("Ready to proceed with universe testing.")
        return 0
    else:
        print("❌ Some episode tests failed. Please fix before proceeding.")
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