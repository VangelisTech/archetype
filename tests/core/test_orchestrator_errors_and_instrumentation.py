import pytest

from archetype.app.orchestrator import WorldOrchestrator
from archetype.core.aio import AsyncSystem
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


@pytest.mark.asyncio
async def test_orchestrator_duplicate_name_raises(tmp_path):
    """Creating two worlds with the same name should raise to prevent ambiguous name lookups."""
    orch = WorldOrchestrator()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        await orch.create_world(
            WorldConfig(name="dup"), system=AsyncSystem(), storage_config=storage
        )
        with pytest.raises(ValueError):
            await orch.create_world(
                WorldConfig(name="dup"), system=AsyncSystem(), storage_config=storage
            )
    finally:
        await orch.shutdown()


@pytest.mark.asyncio
async def test_orchestrator_getters_missing_keys_raise(tmp_path):
    """Accessing non-existent worlds by name and id should raise KeyError with a clear message."""
    orch = WorldOrchestrator()
    try:
        with pytest.raises(KeyError):
            orch.get_world_by_name("missing")
        import uuid_utils as uuid

        with pytest.raises(KeyError):
            orch.get_world(uuid.uuid7())
    finally:
        await orch.shutdown()


@pytest.mark.asyncio
async def test_orchestrator_removal_clears_name_mapping_and_allows_reuse(tmp_path):
    """Removing a world by id should remove its name mapping so the same name can be reused for a new world."""
    orch = WorldOrchestrator()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store3"), namespace="ns")
        w = await orch.create_world(
            WorldConfig(name="cycle"), system=AsyncSystem(), storage_config=storage
        )
        wid = w.world_id
        orch.remove_world(wid)
        with pytest.raises(KeyError):
            orch.get_world(wid)
        # name should be free again
        w2 = await orch.create_world(
            WorldConfig(name="cycle"), system=AsyncSystem(), storage_config=storage
        )
        assert w2.world_id != wid
    finally:
        await orch.shutdown()


@pytest.mark.asyncio
async def test_orchestrator_remove_nonexistent_is_noop(tmp_path):
    """Removing a world that does not exist should be a no-op (no exception)."""
    orch = WorldOrchestrator()
    try:
        import uuid_utils as uuid

        orch.remove_world(uuid.uuid7())
    finally:
        await orch.shutdown()


@pytest.mark.asyncio
async def test_orchestrator_allows_multiple_worlds_without_names(tmp_path):
    """Creating worlds with name=None should not clash and both should be runnable and listed."""
    orch = WorldOrchestrator()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store4"), namespace="ns")
        w1 = await orch.create_world(WorldConfig(), system=AsyncSystem(), storage_config=storage)
        w2 = await orch.create_world(WorldConfig(), system=AsyncSystem(), storage_config=storage)
        assert w1.world_id != w2.world_id
        await orch.run_all_worlds(RunConfig(num_steps=1))
        assert len(orch.list_worlds()) == 2
    finally:
        await orch.shutdown()
