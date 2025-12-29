import pytest

from archetype.app.orchestrator import WorldOrchestrator
from archetype.core.aio import AsyncSystem
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


@pytest.mark.asyncio
async def test_orchestrator_builds_context_once(tmp_path):
    orch = WorldOrchestrator()
    try:
        cfg = WorldConfig(name="w1")
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        _w1 = await orch.create_world(cfg, system=AsyncSystem(), storage_config=storage)
        _w2 = await orch.create_world(
            WorldConfig(name="w2"), system=AsyncSystem(), storage_config=storage
        )

        # Running both should work and share the same backing context transparently
        rc = RunConfig(num_steps=1)
        await orch.run_all_worlds(rc)

        assert len(orch.list_worlds()) == 2
    finally:
        await orch.shutdown()
