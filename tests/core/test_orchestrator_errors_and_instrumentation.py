import pytest
import uuid_utils as uuid

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.config import StorageConfig, WorldConfig


@pytest.mark.asyncio
async def test_world_service_duplicate_name_raises(tmp_path):
    """Creating two worlds with the same name should raise to prevent ambiguous name lookups."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        await ws.create_world(WorldConfig(name="dup"), storage_config=storage)
        with pytest.raises(ValueError):
            await ws.create_world(WorldConfig(name="dup"), storage_config=storage)
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_world_service_getters_missing_keys_raise(tmp_path):
    """Accessing non-existent worlds by name and id should raise KeyError with a clear message."""
    ws = WorldService(StorageService())
    try:
        with pytest.raises(KeyError):
            ws.get_world_by_name("missing")
        with pytest.raises(KeyError):
            ws.get_world(uuid.uuid7())
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_world_service_removal_clears_name_mapping_and_allows_reuse(tmp_path):
    """Removing a world by id should remove its name mapping so the same name can be reused."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store3"), namespace="ns")
        w = await ws.create_world(WorldConfig(name="cycle"), storage_config=storage)
        wid = w.world_id
        ws.remove_world(wid)
        with pytest.raises(KeyError):
            ws.get_world(wid)
        # name should be free again
        w2 = await ws.create_world(WorldConfig(name="cycle"), storage_config=storage)
        assert w2.world_id != wid
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_world_service_remove_nonexistent_is_noop(tmp_path):
    """Removing a world that does not exist should be a no-op (no exception)."""
    ws = WorldService(StorageService())
    try:
        ws.remove_world(uuid.uuid7())
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_world_service_allows_multiple_worlds_without_names(tmp_path):
    """Creating worlds with name=None should not clash and both should be listed."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store4"), namespace="ns")
        w1 = await ws.create_world(WorldConfig(), storage_config=storage)
        w2 = await ws.create_world(WorldConfig(), storage_config=storage)
        assert w1.world_id != w2.world_id
        assert len(ws.list_worlds()) == 2
    finally:
        await ws.shutdown()
