#!/usr/bin/env python3
"""
Example demonstrating the Archetype app layer with command broker integration.

This example shows:
1. Using the high-level ArchetypeApp interface
2. Creating and managing worlds through the app layer
3. Command broker integration for queuing operations
4. Running parallel simulations
"""

import asyncio
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from archetype.app import ArchetypeApp
from archetype.core import Component, AsyncProcessor, RunConfig
from archetype.app.models import Command, CommandType
from archetype.app.auth.models import ActorCtx
from daft import DataFrame, col
import uuid_utils as uuid


# Define Components
class Position(Component):
    x: float
    y: float
    z: float = 0.0


class Velocity(Component):
    vx: float
    vy: float
    vz: float = 0.0


class Health(Component):
    current: int
    max: int


# Define Processors
class MovementProcessor(AsyncProcessor):
    """Processes entity movement based on velocity."""
    components = [Position, Velocity]
    priority = 1
    
    async def process(self, df: DataFrame, dt: float = 0.016) -> DataFrame:
        df = df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
            "position__z": col("position__z") + col("velocity__vz") * dt,
        })
        return df


class BoundaryProcessor(AsyncProcessor):
    """Keeps entities within boundaries."""
    components = [Position]
    priority = 2
    
    async def process(self, df: DataFrame, boundary: float = 100.0) -> DataFrame:
        # Clamp positions to boundary
        df = df.with_columns({
            "position__x": col("position__x").clip(-boundary, boundary),
            "position__y": col("position__y").clip(-boundary, boundary),
            "position__z": col("position__z").clip(0, boundary),
        })
        return df


async def demo_app_layer(num_entities: int = 100):
    """Demonstrate using the app layer."""
    # Create the app with default configuration
    app = await ArchetypeApp.create(
        storage_uri=".archetype_demo",
    )
    
    world = await app.create_world("demo_world")
    
    # Add processors to the world
    await world.add_processor(MovementProcessor())
    await world.add_processor(BoundaryProcessor())

    for i in range(num_entities):
        # Create entities with random positions and velocities
        await world.create_entity([
            Position(x=i * 0.5, y=i * 0.3, z=0),
            Velocity(vx=10 * (i % 3 - 1), vy=10 * (i % 5 - 2), vz=0),
            Health(current=100, max=100)
        ])
    
    print(f"✅ Spawned {num_entities} entities")
    
    # Demonstrate command broker
    print("\n📨 Testing command broker...")
    
    # Create some commands
    broker = app.container.broker
    actor = ActorCtx(id=uuid.uuid7(), roles={"admin"})
    
    # Queue some commands
    test_commands = [
        Command(
            id=uuid.uuid7(),
            type=CommandType.SPAWN,
            priority=1,
            payload={"entity_type": "player", "position": [0, 0, 0]}
        ),
        Command(
            id=uuid.uuid7(),
            type=CommandType.UPDATE,
            priority=2,
            payload={"entity_id": 1, "health": 50}
        ),
        Command(
            id=uuid.uuid7(),
            type=CommandType.DESPAWN,
            priority=3,
            payload={"entity_id": 99}
        ),
    ]
    
    for cmd in test_commands:
        await broker.enqueue(str(world.world_id), cmd, actor)
    
    pending = await broker.get_pending_count(str(world.world_id))
    print(f"✅ Queued {pending} commands")
    
    # Process commands
    commands = await broker.dequeue(str(world.world_id), max_items=10)
    print(f"✅ Dequeued {len(commands)} commands")
    
    # Run simulation
    print("\n🎯 Running simulation...")
    
    # Run for 10 steps with debug output
    run_config = RunConfig.dev(steps=10, show_rows=3)
    await app.run_world("demo_world", steps=10, debug=True)
    
    print("✅ Simulation complete!")
    
    # Query final state
    print("\n📊 Querying final state...")
    
    # Get all entities with Position component
    positions_df = await world.get_components([Position])
    if positions_df is not None:
        count = positions_df.count_rows()
        print(f"✅ Found {count} entities with Position component")
        
        # Show a sample (collect first to materialize)
        sample = positions_df.limit(5).collect()
        if len(sample) > 0:
            print("\nSample positions:")
            print(sample)
    
    # Demonstrate parallel worlds
    print("\n🌍 Creating parallel worlds...")
    
    world_ids = await app.run_parallel_worlds(
        num_worlds=3,
        steps=5,
        system_factory=lambda i: create_custom_system(i)
    )
    
    print(f"✅ Created and ran {len(world_ids)} parallel worlds")
    
    # Cleanup
    print("\n🧹 Shutting down...")
    await app.shutdown()
    print("✅ App shutdown complete")


def create_custom_system(world_index: int):
    """Factory function to create customized systems for parallel worlds."""
    from archetype.core.aio import AsyncSystem
    
    system = AsyncSystem()
    # Could customize based on world_index
    # For example, different processor configurations
    return system


async def demo_command_service():
    """Demonstrate the command service integration."""
    
    print("\n🎮 Command Service Demo")
    print("=" * 50)
    
    app = await ArchetypeApp.create(storage_uri=".archetype_commands")
    
    # Get the command service
    cmd_service = app.container.command_service
    
    # Create a world to send commands to
    world = await app.create_world("command_world")
    
    # Create and dispatch commands
    actor = ActorCtx(id=uuid.uuid7(), roles={"player"})
    
    # Spawn command
    spawn_cmd = Command(
        id=uuid.uuid7(),
        type=CommandType.SPAWN,
        priority=1,
        payload={
            "components": [
                {"type": "Position", "x": 10, "y": 20},
                {"type": "Velocity", "vx": 5, "vy": -3}
            ]
        }
    )
    
    result = await cmd_service.dispatch(spawn_cmd, actor)
    print(f"Spawn command result: {result}")
    
    # Batch commands
    batch_cmds = [
        Command(
            id=uuid.uuid7(),
            type=CommandType.UPDATE,
            priority=2,
            payload={"entity_id": i, "position": {"x": i * 10, "y": i * 5}}
        )
        for i in range(5)
    ]
    
    results = await cmd_service.dispatch_batch(batch_cmds, actor)
    print(f"Dispatched {len(results)} batch commands")
    
    await app.shutdown()
    print("✅ Command service demo complete")


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════╗
    ║     Archetype App Layer Demonstration            ║
    ╠══════════════════════════════════════════════════╣
    ║  This example shows the high-level app layer     ║
    ║  with command broker and service integration.    ║
    ╚══════════════════════════════════════════════════╝
    """)
    
    # Run the main demo
    asyncio.run(demo_app_layer())
    
    # Optionally run command service demo
    # asyncio.run(demo_command_service())
