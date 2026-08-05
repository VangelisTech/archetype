import pytest
import uuid_utils as uuid

from archetype.core.config import StorageConfig, WorldConfig
from tests.conftest import make_world_harness


@pytest.mark.asyncio
async def test_world_lifecycle_duplicate_name_raises(tmp_path):
    """Creating two worlds with the same name should raise to prevent ambiguous name lookups."""
    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        await ws.lifecycle.create_world(WorldConfig(name="dup"), storage_config=storage)
        with pytest.raises(ValueError):
            await ws.lifecycle.create_world(WorldConfig(name="dup"), storage_config=storage)
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_world_lifecycle_duplicate_name_create_does_not_leak_orphan_world(tmp_path):
    """A failed duplicate-name create_world does not leave a
    half-built world in _worlds (previously inserted before the name check)."""
    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        await ws.lifecycle.create_world(WorldConfig(name="dup"), storage_config=storage)
        baseline_worlds = len(await ws.registry.list_worlds())

        with pytest.raises(ValueError):
            await ws.lifecycle.create_world(WorldConfig(name="dup"), storage_config=storage)

        worlds = await ws.registry.list_worlds()
        assert len(worlds) == baseline_worlds, (
            f"create_world leaked an orphan world into _worlds: "
            f"baseline={baseline_worlds}, after={len(worlds)}"
        )
        assert await ws.registry.world_id_for_name("dup") == str(worlds[0].world_id)
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_world_lifecycle_repeated_duplicate_name_creates_do_not_grow_worlds(tmp_path):
    """Repeated failing duplicate-name create_world calls
    not accumulate worlds in the in-memory registry."""
    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        await ws.lifecycle.create_world(WorldConfig(name="dup"), storage_config=storage)

        for _ in range(10):
            with pytest.raises(ValueError):
                await ws.lifecycle.create_world(WorldConfig(name="dup"), storage_config=storage)

        worlds = await ws.registry.list_worlds()
        assert len(worlds) == 1, f"10 failed retries leaked {len(worlds) - 1} orphan worlds"
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_world_registry_missing_keys_are_explicit(tmp_path):
    """Accessing non-existent worlds by name and id should raise KeyError with a clear message."""
    ws = make_world_harness()
    try:
        with pytest.raises(KeyError):
            await ws.registry.world_id_for_name("missing")
        with pytest.raises(KeyError):
            async with ws.registry.operation(str(uuid.uuid7())):
                pass
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_world_lifecycle_removal_clears_name_mapping_and_allows_reuse(tmp_path):
    """Removing a world by id should remove its name mapping so the same name can be reused."""
    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store3"), namespace="ns")
        w = await ws.lifecycle.create_world(WorldConfig(name="cycle"), storage_config=storage)
        wid = w.world_id
        await ws.lifecycle.destroy_world(wid)
        assert await ws.registry.live_world(str(wid)) is None
        # name should be free again
        w2 = await ws.lifecycle.create_world(WorldConfig(name="cycle"), storage_config=storage)
        assert w2.world_id != wid
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_world_lifecycle_destroy_nonexistent_is_noop(tmp_path):
    """Destroying a world that does not exist should be a no-op (no exception)."""
    ws = make_world_harness()
    try:
        await ws.lifecycle.destroy_world(uuid.uuid7())
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_world_lifecycle_allows_multiple_worlds_without_names(tmp_path):
    """Creating worlds with name=None should not clash and both should be listed."""
    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store4"), namespace="ns")
        w1 = await ws.lifecycle.create_world(WorldConfig(), storage_config=storage)
        w2 = await ws.lifecycle.create_world(WorldConfig(), storage_config=storage)
        assert w1.world_id != w2.world_id
        assert len(await ws.registry.list_worlds()) == 2
    finally:
        await ws.close()
