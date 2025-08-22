import asyncio
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))


from archetype.core.config import StorageConfig, CacheConfig, RunConfig, WorldConfig
from archetype.core.orchestrator import WorldOrchestrator
from archetype.app.world_service import WorldService
from archetype.app.simulation_service import SimulationService
from archetype.core.aio import AsyncSystem
from archetype.core.component import Component

# 1) Define simple components
class Position(Component):
    x: float
    y: float
    is_active: bool = True

class Velocity(Component):
    dx: float
    dy: float
    is_active: bool = True


async def main():
    # 2) Config: UC + GCS
    # Expect UC bearer token in env UC_TOKEN or pass directly
    uc_endpoint = os.getenv("UC_ENDPOINT", "http://localhost:8080/api/2.1/unity-catalog")
    uc_token = os.getenv("UC_TOKEN")
    if not uc_token:
        raise RuntimeError("Set UC_TOKEN to your UC access token")
    uc_catalog = os.getenv("UC_CATALOG", "unity")
    uc_schema = os.getenv("UC_SCHEMA", "default")

    from archetype.core.config import UnityCatalogConfig
    storage = StorageConfig(
        uri="./archetype_data",                  # local workspace for metadata
        namespace="simulations",
        uc_config=UnityCatalogConfig(
            enabled=True,
            enforce_permissions=True,
            endpoint=uc_endpoint,
            token=uc_token,
            catalog=uc_catalog,
            schema=uc_schema,
            default_region=None,
        ),
    )
    cache = CacheConfig()
    run = RunConfig.dev(steps=2)

    # 3) Minimal system (no-op)
    system = AsyncSystem()

    # 4) Bootstrap services
    orchestrator = WorldOrchestrator()
    world_service = WorldService(orchestrator=orchestrator)
    sim_service = SimulationService(
        orchestrator=orchestrator,
        world_service=world_service,
        storage_config=storage,
        cache_config=cache,
    )

    # 5) Create 1 world and run
    world_ids = await sim_service.run_parallel_worlds(
        num_worlds=1,
        run_config=run,
        system_factory=lambda i: system,
    )
    world_id = world_ids[0]
    world = orchestrator.get_world(world_id)

    # 6) Issue a write via command service-like pattern:
    # spawn an entity with Position + Velocity components
    pos = Position(x=1.0, y=2.0)
    vel = Velocity(dx=0.1, dy=0.2)

    # The world API supports adding via caches and stepping; here we call world methods directly:
    entity_id = await world.create_entity([pos, vel])

    # Step to write out initial state (wrappers enforce UC MODIFY, request temp GCS creds)
    await orchestrator.run_world(world_id, run)

    # 7) Query (wrappers enforce UC SELECT, request temp GCS creds if needed)
    df = await world.query_archetype(
        (Position, Velocity),
        ticks=None,
        entity_ids=[entity_id],
        components=None,
        run_config=run,
    )
    # Materialize and print a few rows for demo purposes
    df.collect()
    print("Row count:", df.count_rows())
    df.show(5)

    # 8) Cleanup
    await orchestrator.shutdown()


if __name__ == "__main__":
    asyncio.run(main())