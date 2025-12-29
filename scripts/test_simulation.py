#!/usr/bin/env python3
"""
End-to-end simulation test script.

This script validates the full Archetype stack:
1. Storage initialization
2. World creation
3. Running simulations
4. Command broker integration
"""

import asyncio

# Add src to path
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


async def test_core_world():
    """Test the core AsyncWorld directly."""
    print("\n" + "=" * 60)
    print("TEST 1: Core AsyncWorld")
    print("=" * 60)

    from archetype.core.aio.async_querier import AsyncQueryManager
    from archetype.core.aio.async_store import AsyncStore
    from archetype.core.aio.async_system import AsyncSystem
    from archetype.core.aio.async_updater import AsyncUpdateManager
    from archetype.core.aio.async_world import AsyncWorld
    from archetype.core.component import Component
    from archetype.core.config import RunConfig, StorageConfig, WorldConfig
    from archetype.core.runtime.storage import StorageContextFactory

    # Create temp storage
    with tempfile.TemporaryDirectory() as tmpdir:
        storage_config = StorageConfig(uri=tmpdir, namespace="test_ns")
        context = StorageContextFactory.build(storage_config)

        print(f"  Storage: {tmpdir}")
        print(f"  Namespace: {storage_config.namespace}")

        # Create store and world components
        store = AsyncStore(context)
        querier = AsyncQueryManager(store)
        updater = AsyncUpdateManager(store)
        system = AsyncSystem()
        world_config = WorldConfig(name="test_world")

        # Create world
        world = AsyncWorld(world_config, querier, updater, system)
        print(f"  ✓ World created: {world.name} (ID: {world.world_id})")

        # Define a simple component
        class Position(Component):
            x: float = 0.0
            y: float = 0.0
            z: float = 0.0

        # Spawn entity
        entity_id = await world.create_entity([Position(x=0, y=0, z=100)])
        print(f"  ✓ Entity spawned: {entity_id}")

        # Run one step
        run_config = RunConfig(num_steps=1)
        await world.run(run_config)
        print(f"  ✓ Ran 1 step, tick now: {world.tick}")

        # Query entities
        df = await world.get_components([Position])
        count = df.collect().count_rows()
        print(f"  ✓ Query returned {count} entities")

        await store.shutdown()
        return True


async def test_world_with_processor():
    """Test world with a processor (physics update)."""
    print("\n" + "=" * 60)
    print("TEST 2: World with Processor")
    print("=" * 60)

    import daft

    from archetype.core.aio.async_processor import AsyncProcessor
    from archetype.core.aio.async_querier import AsyncQueryManager
    from archetype.core.aio.async_store import AsyncStore
    from archetype.core.aio.async_system import AsyncSystem
    from archetype.core.aio.async_updater import AsyncUpdateManager
    from archetype.core.aio.async_world import AsyncWorld
    from archetype.core.component import Component
    from archetype.core.config import RunConfig, StorageConfig, WorldConfig
    from archetype.core.runtime.storage import StorageContextFactory

    with tempfile.TemporaryDirectory() as tmpdir:
        storage_config = StorageConfig(uri=tmpdir, namespace="physics_ns")
        context = StorageContextFactory.build(storage_config)

        class Position(Component):
            x: float = 0.0
            y: float = 0.0
            z: float = 0.0

        class Velocity(Component):
            vx: float = 0.0
            vy: float = 0.0
            vz: float = 0.0

        class PhysicsProcessor(AsyncProcessor):
            """Simple physics: position += velocity * dt"""

            components = (Position, Velocity)
            priority = 1

            async def process(self, df: daft.DataFrame, **kwargs) -> daft.DataFrame:
                dt = 0.1  # 100ms timestep
                return (
                    df.with_column(
                        "position__x", daft.col("position__x") + daft.col("velocity__vx") * dt
                    )
                    .with_column(
                        "position__y", daft.col("position__y") + daft.col("velocity__vy") * dt
                    )
                    .with_column(
                        "position__z", daft.col("position__z") + daft.col("velocity__vz") * dt
                    )
                )

        # Create world with processor
        store = AsyncStore(context)
        querier = AsyncQueryManager(store)
        updater = AsyncUpdateManager(store)
        system = AsyncSystem()
        await system.add_processor(PhysicsProcessor())

        world_config = WorldConfig(name="physics_world")
        world = AsyncWorld(world_config, querier, updater, system)
        print("  ✓ World created with PhysicsProcessor")

        # Spawn entity at origin with velocity
        entity_id = await world.create_entity(
            [
                Position(x=0, y=0, z=1000),  # Start at 1000m altitude
                Velocity(vx=100, vy=0, vz=-50),  # Moving forward and falling
            ]
        )
        print("  ✓ Entity spawned at z=1000")

        # Run simulation for 10 steps
        print("  Running 10 simulation steps...")
        run_config = RunConfig(num_steps=10)
        await world.run(run_config)

        # Query final state
        df = await world.get_components([Position])
        collected = df.collect()

        # Get final position (columns are lowercase with __ separator)
        data = collected.to_pydict()
        final_x = data["position__x"][0]
        final_z = data["position__z"][0]

        print("  ✓ After 10 steps (1 second):")
        print(f"    x: 0 → {final_x:.1f} (expected: ~100)")
        print(f"    z: 1000 → {final_z:.1f} (expected: ~950)")

        await store.shutdown()
        return True


async def test_orchestrator():
    """Test the WorldOrchestrator for multi-world management."""
    print("\n" + "=" * 60)
    print("TEST 3: World Orchestrator")
    print("=" * 60)

    from archetype.app.orchestrator import WorldOrchestrator
    from archetype.core.aio.async_system import AsyncSystem
    from archetype.core.component import Component
    from archetype.core.config import RunConfig, StorageConfig, WorldConfig

    with tempfile.TemporaryDirectory() as tmpdir:
        storage_config = StorageConfig(uri=tmpdir, namespace="orch_ns")

        orchestrator = WorldOrchestrator()
        print("  ✓ Orchestrator created")

        # Create a world
        world_config = WorldConfig(name="world_alpha")
        system = AsyncSystem()

        world = await orchestrator.create_world(
            config=world_config,
            system=system,
            storage_config=storage_config,
        )
        print(f"  ✓ World created: {world.name} (ID: {world.world_id})")

        # Create second world
        world_config2 = WorldConfig(name="world_beta")
        system2 = AsyncSystem()

        world2 = await orchestrator.create_world(
            config=world_config2,
            system=system2,
            storage_config=storage_config,
        )
        print(f"  ✓ World created: {world2.name} (ID: {world2.world_id})")

        # List worlds
        world_ids = orchestrator.list_worlds()
        print(f"  ✓ Listed {len(world_ids)} worlds")

        # Run a world
        class Position(Component):
            x: float = 0.0
            y: float = 0.0

        entity_id = await world.create_entity([Position(x=10, y=20)])
        run_config = RunConfig(num_steps=3)
        await orchestrator.run_world(world.world_id, run_config)
        print(f"  ✓ World ran for 3 steps, tick: {world.tick}")

        await orchestrator.shutdown()
        return True


async def test_service_layer():
    """Test the service layer (SimulationService, WorldService)."""
    print("\n" + "=" * 60)
    print("TEST 4: Service Layer")
    print("=" * 60)

    from archetype.app.container import ServiceContainer
    from archetype.core.aio.async_system import AsyncSystem
    from archetype.core.config import StorageConfig, WorldConfig

    with tempfile.TemporaryDirectory() as tmpdir:
        storage_config = StorageConfig(uri=tmpdir, namespace="service_ns")

        # Create service container
        container = ServiceContainer(storage_config=storage_config)
        print("  ✓ ServiceContainer created")

        # Get services
        world_service = container.world_service
        simulation_service = container.simulation_service
        command_service = container.command_service

        print("  ✓ Services initialized")

        # Create a world via orchestrator (service layer uses orchestrator)
        world_config = WorldConfig(name="service_world")
        system = AsyncSystem()

        world = await container.orchestrator.create_world(
            config=world_config,
            system=system,
            storage_config=storage_config,
        )
        print(f"  ✓ World created via orchestrator: {world.world_id}")

        # Use world service to get status
        status = await world_service.get_world_status(world.world_id)
        print(f"  ✓ World status: tick={status['tick']}, entities={status['entity_count']}")

        # List worlds via orchestrator
        worlds = container.orchestrator.list_worlds()
        print(f"  ✓ Listed {len(worlds)} world(s)")

        await container.shutdown()
        return True


async def test_command_broker():
    """Test command broker functionality."""
    print("\n" + "=" * 60)
    print("TEST 5: Command Broker")
    print("=" * 60)

    from archetype.app.container import ServiceContainer
    from archetype.core.aio.async_system import AsyncSystem
    from archetype.core.config import StorageConfig, WorldConfig

    with tempfile.TemporaryDirectory() as tmpdir:
        storage_config = StorageConfig(uri=tmpdir, namespace="broker_ns")
        container = ServiceContainer(storage_config=storage_config)

        # Create world
        world_config = WorldConfig(name="broker_world")
        system = AsyncSystem()
        world = await container.orchestrator.create_world(
            config=world_config,
            system=system,
            storage_config=storage_config,
        )
        print(f"  ✓ World created: {world.world_id}")

        # Submit a command via broker (op must be one of the legacy literals)
        cmd_id = await container.command_service.enqueue_command(
            world_id=world.world_id,
            op="create_entity",  # Legacy op values: create_entity, delete_entity, etc.
            payload={"entity_id": str(uuid4()), "components": []},
            priority=0,
        )
        print(f"  ✓ Command enqueued: {cmd_id}")

        # Check pending commands
        pending = await container.command_service.peek_commands(world.world_id)
        print(f"  ✓ Pending commands: {len(pending)}")

        # Get command history
        history = await container.command_service.get_command_history(world.world_id, limit=10)
        print(f"  ✓ Command history: {len(history)} entries")

        await container.shutdown()
        return True


async def test_full_simulation():
    """Full end-to-end simulation with multiple entities."""
    print("\n" + "=" * 60)
    print("TEST 6: Full Flight Simulation")
    print("=" * 60)

    import daft

    from archetype.core.aio.async_processor import AsyncProcessor
    from archetype.core.aio.async_querier import AsyncQueryManager
    from archetype.core.aio.async_store import AsyncStore
    from archetype.core.aio.async_system import AsyncSystem
    from archetype.core.aio.async_updater import AsyncUpdateManager
    from archetype.core.aio.async_world import AsyncWorld
    from archetype.core.component import Component
    from archetype.core.config import RunConfig, StorageConfig, WorldConfig
    from archetype.core.runtime.storage import StorageContextFactory

    with tempfile.TemporaryDirectory() as tmpdir:
        storage_config = StorageConfig(uri=tmpdir, namespace="flight_sim_ns")
        context = StorageContextFactory.build(storage_config)

        # Components for a simple flight sim
        class Aircraft(Component):
            callsign: str = "UNKNOWN"
            aircraft_type: str = "generic"

        class Position(Component):
            lat: float = 0.0
            lon: float = 0.0
            alt: float = 0.0  # meters

        class Velocity(Component):
            north: float = 0.0  # m/s
            east: float = 0.0
            down: float = 0.0

        class FlightPhysics(AsyncProcessor):
            """Simple flight physics processor."""

            components = (Position, Velocity)
            priority = 1

            async def process(self, df: daft.DataFrame, **kwargs) -> daft.DataFrame:
                dt = 1.0  # 1 second timestep
                # Convert velocity to position delta (simplified, no Earth curvature)
                # 1 degree lat ≈ 111km
                m_per_deg = 111000.0

                return (
                    df.with_column(
                        "position__lat",
                        daft.col("position__lat") + daft.col("velocity__north") / m_per_deg * dt,
                    )
                    .with_column(
                        "position__lon",
                        daft.col("position__lon") + daft.col("velocity__east") / m_per_deg * dt,
                    )
                    .with_column(
                        "position__alt", daft.col("position__alt") - daft.col("velocity__down") * dt
                    )
                )

        # Create world
        store = AsyncStore(context)
        querier = AsyncQueryManager(store)
        updater = AsyncUpdateManager(store)
        system = AsyncSystem()
        await system.add_processor(FlightPhysics())

        world_config = WorldConfig(name="flight_sim")
        world = AsyncWorld(world_config, querier, updater, system)
        print("  ✓ World created with FlightPhysics processor")

        # Spawn multiple aircraft
        aircraft_data = [
            ("UAL123", "B737", 37.7749, -122.4194, 10000, 250, 0, 0),  # SF heading north
            ("DAL456", "A320", 34.0522, -118.2437, 12000, 0, 200, 0),  # LA heading east
            ("AAL789", "B777", 40.7128, -74.0060, 35000, -100, 100, -5),  # NYC descending SW
        ]

        for callsign, atype, lat, lon, alt, vn, ve, vd in aircraft_data:
            await world.create_entity(
                [
                    Aircraft(callsign=callsign, aircraft_type=atype),
                    Position(lat=lat, lon=lon, alt=alt),
                    Velocity(north=vn, east=ve, down=vd),
                ]
            )
            print(f"  ✓ Spawned {callsign} ({atype}) at ({lat:.2f}, {lon:.2f}) alt={alt}m")

        # Run simulation for 60 steps (1 minute)
        print("\n  Simulating 60 seconds...")
        run_config = RunConfig(num_steps=60)
        await world.run(run_config)

        # Query final state
        df = await world.get_components([Aircraft, Position])
        collected = df.collect()
        data = collected.to_pydict()

        print("\n  Final positions after 60 seconds:")
        # Columns are lowercase with __ separator
        for i in range(len(data["aircraft__callsign"])):
            cs = data["aircraft__callsign"][i]
            lat = data["position__lat"][i]
            lon = data["position__lon"][i]
            alt = data["position__alt"][i]
            print(f"    {cs}: ({lat:.4f}, {lon:.4f}) alt={alt:.0f}m")

        await store.shutdown()
        return True


async def main():
    """Run all tests."""
    print("\n" + "#" * 60)
    print("# ARCHETYPE SIMULATION TEST SUITE")
    print("#" * 60)

    results = []

    tests = [
        ("Core AsyncWorld", test_core_world),
        ("World with Processor", test_world_with_processor),
        ("World Orchestrator", test_orchestrator),
        ("Service Layer", test_service_layer),
        ("Command Broker", test_command_broker),
        ("Full Flight Simulation", test_full_simulation),
    ]

    for name, test_fn in tests:
        try:
            success = await test_fn()
            results.append((name, success))
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            import traceback

            traceback.print_exc()
            results.append((name, False))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, ok in results if ok)
    total = len(results)

    for name, ok in results:
        status = "✓ PASS" if ok else "✗ FAIL"
        print(f"  {status}: {name}")

    print(f"\n  {passed}/{total} tests passed")

    if passed == total:
        print("\n  🎉 All simulations running successfully!")

    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
