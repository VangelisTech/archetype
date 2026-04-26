import pytest
import uuid_utils as uuid

from archetype.core.config import StorageConfig, WorldConfig
from tests.conftest import make_world_service


@pytest.mark.asyncio
async def test_world_service_duplicate_name_raises(tmp_path):
    """Creating two worlds with the same name should raise to prevent ambiguous name lookups."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        await ws.create_world(WorldConfig(name="dup"), storage_config=storage)
        with pytest.raises(ValueError):
            await ws.create_world(WorldConfig(name="dup"), storage_config=storage)
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_world_service_duplicate_name_create_does_not_leak_orphan_world(tmp_path):
    """A failed duplicate-name create_world does not leave a
    half-built world in _worlds (previously inserted before the name check)."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        await ws.create_world(WorldConfig(name="dup"), storage_config=storage)
        baseline_worlds = len(ws._orchestrator._registry._worlds)
        baseline_names = len(ws._orchestrator._registry._names)

        with pytest.raises(ValueError):
            await ws.create_world(WorldConfig(name="dup"), storage_config=storage)

        assert len(ws._orchestrator._registry._worlds) == baseline_worlds, (
            f"create_world leaked an orphan world into _worlds: "
            f"baseline={baseline_worlds}, after={len(ws._orchestrator._registry._worlds)}"
        )
        assert len(ws._orchestrator._registry._names) == baseline_names
        # Every world in _worlds must be reachable via _world_names.
        unreachable = [
            wid
            for wid in ws._orchestrator._registry._worlds
            if wid not in ws._orchestrator._registry._names.values()
        ]
        assert unreachable == [], f"orphaned worlds: {unreachable}"
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_world_service_repeated_duplicate_name_creates_do_not_grow_worlds(tmp_path):
    """Repeated failing duplicate-name create_world calls
    not accumulate worlds in the in-memory registry."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        await ws.create_world(WorldConfig(name="dup"), storage_config=storage)

        for _ in range(10):
            with pytest.raises(ValueError):
                await ws.create_world(WorldConfig(name="dup"), storage_config=storage)

        assert len(ws._orchestrator._registry._worlds) == 1, (
            f"10 failed retries leaked {len(ws._orchestrator._registry._worlds) - 1} orphan worlds"
        )
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_world_service_getters_missing_keys_raise(tmp_path):
    """Accessing non-existent worlds by name and id should raise KeyError with a clear message."""
    ws = make_world_service()
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
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store3"), namespace="ns")
        w = await ws.create_world(WorldConfig(name="cycle"), storage_config=storage)
        wid = w.world_id
        await ws.destroy_world(wid)
        with pytest.raises(KeyError):
            ws.get_world(wid)
        # name should be free again
        w2 = await ws.create_world(WorldConfig(name="cycle"), storage_config=storage)
        assert w2.world_id != wid
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_world_service_destroy_nonexistent_is_noop(tmp_path):
    """Destroying a world that does not exist should be a no-op (no exception)."""
    ws = make_world_service()
    try:
        await ws.destroy_world(uuid.uuid7())
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_world_service_allows_multiple_worlds_without_names(tmp_path):
    """Creating worlds with name=None should not clash and both should be listed."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store4"), namespace="ns")
        w1 = await ws.create_world(WorldConfig(), storage_config=storage)
        w2 = await ws.create_world(WorldConfig(), storage_config=storage)
        assert w1.world_id != w2.world_id
        assert len(ws.list_worlds()) == 2
    finally:
        await ws.shutdown()
