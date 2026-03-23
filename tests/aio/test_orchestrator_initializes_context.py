import pytest

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.config import StorageConfig, WorldConfig


@pytest.mark.asyncio
async def test_world_service_builds_context_once(tmp_path):
    ws = WorldService(StorageService())
    try:
        cfg = WorldConfig(name="w1")
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        _w1 = await ws.create_world(cfg, storage_config=storage)
        _w2 = await ws.create_world(
            WorldConfig(name="w2"), storage_config=storage
        )

        assert len(ws.list_worlds()) == 2
    finally:
        await ws.shutdown()
