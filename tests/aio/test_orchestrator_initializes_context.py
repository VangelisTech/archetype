import pytest

from archetype.core.config import StorageConfig, WorldConfig
from tests.conftest import make_world_harness


@pytest.mark.asyncio
async def test_world_lifecycle_builds_context_once(tmp_path):
    ws = make_world_harness()
    try:
        cfg = WorldConfig(name="w1")
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        _w1 = await ws.lifecycle.create_world(cfg, storage_config=storage)
        _w2 = await ws.lifecycle.create_world(WorldConfig(name="w2"), storage_config=storage)

        assert len(await ws.registry.list_worlds()) == 2
    finally:
        await ws.close()
