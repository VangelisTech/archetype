import pytest

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.aio import AsyncWorld
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


@pytest.mark.asyncio
async def test_world_service_run_world(tmp_path):
    """Running a world through WorldService should work via direct world.run()."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        w = await ws.create_world(WorldConfig(name="w"), storage_config=storage)

        if isinstance(w, AsyncWorld):
            await w.run(RunConfig(num_steps=1))
    finally:
        await ws.shutdown()
