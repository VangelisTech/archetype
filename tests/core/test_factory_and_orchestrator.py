import pytest

from archetype.app.factory import WorldFactory
from archetype.app.orchestrator import WorldOrchestrator
from archetype.app.storage_manager import StorageBackendManager
from archetype.core.aio import AsyncSystem
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


@pytest.mark.asyncio
async def test_factory_creates_async_world_and_default_system(tmp_path):
    """Creating a world with no explicit system yields a default async world and runs 1 step without error."""
    mgr = StorageBackendManager()
    try:
        factory = WorldFactory(mgr)
        world = await factory.create_world(
            world_config=WorldConfig(name="w"),
            storage_config=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
            system=None,  # no system provided, so default is used
        )
        # World should expose async API and have world_id/name set
        assert world.world_id is not None
        assert world.name == "w"
        # Run 1 step to ensure the pipeline is wired
        await world.run(RunConfig(num_steps=1))
    finally:
        await mgr.shutdown()


@pytest.mark.asyncio
async def test_orchestrator_world_lifecycle_and_name_lookup(tmp_path):
    """Idempotent create by world_id, name lookup, run_all, and remove_by_name behavior are correct."""
    orch = WorldOrchestrator()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        w1 = await orch.create_world(
            WorldConfig(name="alpha"), system=AsyncSystem(), storage_config=storage
        )
        # idempotent create with same world_id returns same instance
        w1_again = await orch.create_world(
            WorldConfig(world_id=w1.world_id, name="alpha"),
            system=AsyncSystem(),
            storage_config=storage,
        )
        assert w1 is w1_again

        # lookup by name should work
        got = orch.get_world_by_name("alpha")
        assert got.world_id == w1.world_id

        # run all should not error
        await orch.run_all_worlds(RunConfig(num_steps=1))

        # remove by name
        orch.remove_world_by_name("alpha")
        assert len(orch.list_worlds()) == 0
    finally:
        await orch.shutdown()
