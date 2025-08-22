import pytest
import pytest_asyncio

from archetype.core.config import StorageConfig, WorldConfig, RunConfig
from archetype.core.orchestrator import WorldOrchestrator
from archetype.core.aio import AsyncSystem


@pytest.mark.asyncio
async def test_orchestrator_run_world_and_run_world_by_name(tmp_path):
    """run_world(world_id, rc) and run_world_by_name(name, rc) should delegate to the async world's run and not raise."""
    orch = WorldOrchestrator()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        w = await orch.create_world(WorldConfig(name="w"), system=AsyncSystem(), storage_config=storage)

        await orch.run_world(w.world_id, RunConfig(num_steps=1))
        await orch.run_world_by_name("w", RunConfig(num_steps=1))
    finally:
        await orch.shutdown()


