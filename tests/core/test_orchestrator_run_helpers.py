import pytest

from archetype.core.aio import AsyncWorld
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from tests.conftest import make_world_harness


@pytest.mark.asyncio
async def test_world_service_run_world(tmp_path):
    """Running a world through WorldService should work via direct world.run()."""
    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        w = await ws.lifecycle.create_world(WorldConfig(name="w"), storage_config=storage)

        if isinstance(w, AsyncWorld):
            await w.run(RunConfig(num_steps=1))
    finally:
        await ws.close()
