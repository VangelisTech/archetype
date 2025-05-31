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

from archetype.core import Component
from archetype import make_simple_world
from archetype.core.aio.async_system import AsyncProcessor, async_processor
from archetype.core.aio.episode import EpisodeProcessor, Episode
from archetype.core.aio.episode_world import EpisodeWorld

from daft import DataFrame, col 


# Define Components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float

class Health(Component):
    hp: int
    max_hp: int


# Fast processor (minimal I/O)
@async_processor(Position, Velocity, priority=1)
class FastMovementProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        # Fast processing - minimal delay
        await asyncio.sleep(0.01)  # 10ms
        
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })


# Slow processor (heavy I/O)
@async_processor(Health, priority=2)
class SlowHealthProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        # Slow processing - simulates complex health calculations
        await asyncio.sleep(0.1)  # 100ms
        
        # Simulate health regeneration
        return df.with_columns({
            "health__hp": col("health__hp") + 1
        })


# Episode-aware processor that handles temporal boundaries
class EpisodeAwareProcessor(EpisodeProcessor, AsyncProcessor):
    components = (Position, Velocity)
    priority = 3
    
    def can_process_archetype(self, archetype_signature):
        return set(self.components).issubset(set(archetype_signature))
    
    async def process_episode(
        self,
        archetype_name: str,
        df: DataFrame,
        episode: Episode,
        dt: float
    ) -> DataFrame:
        # Episode-aware processing
        await asyncio.sleep(0.02)  # 20ms
        
        # Example: Apply different logic based on episode
        if episode.start_step % 20 == 0:  # Every other episode
            # Special processing for milestone episodes
            return df.with_columns({
                "position__x": col("position__x") * 1.1,  # 10% boost
                "position__y": col("position__y") * 1.1,
            })
        else:
            # Normal processing
            return df
    
    async def on_episode_boundary(
        self,
        archetype_name: str,
        old_episode: Episode,
        new_episode: Episode
    ):
        """Handle episode transitions."""
        print(f"    🔄 {archetype_name}: Episode {old_episode.id} → {new_episode.id}")


async def demonstrate_episode_coordination():
    """
    Demonstrate episode-based coordination with different processing speeds.
    
    Key concepts shown:
    1. Different archetypes progress at different rates
    2. Fast processors don't wait for slow ones
    3. Synchronization points provide coordination when needed
    4. Episode boundaries enable checkpointing and special logic
    """
    
    print("🎬 Episode Coordination System Demo")
    print("=" * 50)
    
    uri = "/Users/everett-founder/git/vangelis/internal/work/libs/archetype/data"
    
    # Create episode world with small episodes for demo
    sync_world = make_simple_world(uri, debug=False)
    episode_world = EpisodeWorld(
        sync_world, 
        episode_size=5,  # Small episodes for demo
        max_concurrent_archetypes=3
    )
    
    # Add processors with different speeds
    episode_world.add_processor(FastMovementProcessor())
    episode_world.add_processor(SlowHealthProcessor()) 
    episode_world.add_processor(EpisodeAwareProcessor())
    
    print("📦 Spawning entities with different component combinations...")
    
    # Entities with Position+Velocity (fast processing)
    episode_world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
    episode_world.spawn(Position(x=10, y=10), Velocity(vx=2, vy=2))
    
    # Entities with Health (slow processing)  
    episode_world.spawn(Health(hp=80, max_hp=100))
    episode_world.spawn(Health(hp=90, max_hp=100))
    
    # Entity with all components (both fast and slow processing)
    episode_world.spawn(
        Position(x=5, y=5), 
        Velocity(vx=1.5, vy=1.5),
        Health(hp=75, max_hp=100)
    )
    
    episode_world.materialize_spawns()
    print("✅ Entities spawned with different archetype signatures")
    
    # Demonstrate independent episode progression
    print("\n🏃‍♂️ Running episodes with independent progression...")
    print("   Fast archetypes (Position+Velocity) will complete episodes quickly")
    print("   Slow archetypes (Health) will take longer per episode")
    print("   Mixed archetypes will be limited by their slowest processor")
    
    # Run multiple episodes
    episode_stats = await episode_world.run_with_sync_points(
        num_episodes=3,
        dt=0.1,
        sync_every=2  # Sync every 2 episodes
    )
    
    # Show results
    print("\n📊 Episode Statistics:")
    for stats in episode_stats:
        sync_marker = " (SYNC)" if stats["was_sync_point"] else ""
        print(f"   Episode {stats['episode_number']}{sync_marker}: "
              f"{stats['episode_duration']:.3f}s, "
              f"{stats['archetypes_processed']} archetypes processed")
    
    # Show final episode status
    print("\n📋 Final Episode Status:")
    status = episode_world.get_episode_status()
    print(f"   Current step: {status['current_step']}")
    print(f"   Episode size: {status['episode_size']}")
    print(f"   Active episodes: {status['active_episodes']}")
    print(f"   Completed episodes: {status['completed_episodes']}")
    
    if status['episode_details']:
        print("   Episode details:")
        for episode_id, details in status['episode_details'].items():
            print(f"     {episode_id}: {details['completed_archetypes']} archetypes completed")
    
    print("\n🎯 Key Episode Coordination Benefits Demonstrated:")
    print("   ✅ Fast archetypes don't wait for slow ones")
    print("   ✅ Episode boundaries provide natural checkpointing")
    print("   ✅ Synchronization points enable coordination when needed")
    print("   ✅ Different processing speeds handled naturally")
    print("   ✅ Episode-aware processors can apply temporal logic")


if __name__ == "__main__":
    asyncio.run(demonstrate_episode_coordination())