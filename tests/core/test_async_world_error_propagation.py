import pytest
import pytest_asyncio

import daft

from archetype.core.config import StorageConfig, WorldConfig, RunConfig
from archetype.core.aio import AsyncSystem, AsyncProcessor
from archetype.core.component import Component
from archetype.core.orchestrator import WorldOrchestrator


class Foo(Component):
    x: int


class OKProc(AsyncProcessor):
    components = (Foo,)
    priority = 0

    async def process(self, df, **kwargs):
        return df


class BadProc(AsyncProcessor):
    components = (Foo,)
    priority = 1

    async def process(self, df, **kwargs):
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_async_world_processor_error_is_logged_not_raised(tmp_path, caplog):
    """If processors raise, system logs error and world continues (current design)."""
    orch = WorldOrchestrator()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(OKProc())
        await system.add_processor(BadProc())
        world = await orch.create_world(WorldConfig(name="w"), system=system, storage_config=storage)
        await world.create_entity([Foo(x=1)])

        with caplog.at_level("ERROR"):
            await world.run(RunConfig(num_steps=1))
        assert any("Error processing archetype" in rec.message for rec in caplog.records)
    finally:
        await orch.shutdown()


