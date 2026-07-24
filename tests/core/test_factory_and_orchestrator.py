import pytest

from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from tests.conftest import make_world_harness


@pytest.mark.asyncio
async def test_factory_creates_async_world_and_default_system(tmp_path):
    """Creating a world with no explicit system yields a default async world and runs 1 step without error."""
    ws = make_world_harness()
    try:
        world = await ws.lifecycle.create_world(
            WorldConfig(name="w"),
            StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
        )
        assert world.world_id is not None
        assert world.name == "w"
        await world.run(RunConfig(num_steps=1))
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_world_lifecycle_and_registry_name_lookup(tmp_path):
    """Idempotent create by world_id, name lookup, and remove behavior are correct."""
    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        w1 = await ws.lifecycle.create_world(WorldConfig(name="alpha"), storage_config=storage)
        # idempotent create with same world_id returns same instance
        w1_again = await ws.lifecycle.create_world(
            WorldConfig(world_id=w1.world_id, name="alpha"),
            storage_config=storage,
        )
        assert w1 is w1_again

        # lookup by name should work
        got = await ws.registry.live_world(await ws.registry.world_id_for_name("alpha"))
        assert got is not None
        assert got.world_id == w1.world_id

        # The registry returns the strongly owned live worlds.
        worlds = await ws.registry.list_worlds()
        assert len(worlds) == 1

        # destroy by ID
        await ws.lifecycle.destroy_world(w1.world_id)
        assert len(await ws.registry.list_worlds()) == 0
    finally:
        await ws.close()
