import pytest

from archetype.core.aio import AsyncWorld
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from tests.conftest import make_world_harness


@pytest.mark.asyncio
async def test_managed_world_run_directly(tmp_path):
    """Running a managed world should work via direct world.run()."""
    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        w = await ws.lifecycle.create_world(WorldConfig(name="w"), storage_config=storage)

        if isinstance(w, AsyncWorld):
            await w.run(RunConfig(num_steps=1))
    finally:
        await ws.close()
