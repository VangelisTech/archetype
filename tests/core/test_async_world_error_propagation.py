import pytest

from archetype.core.aio import AsyncProcessor, AsyncSystem
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from tests.conftest import make_world_service


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
async def test_async_world_processor_error_fails_the_step(tmp_path, caplog):
    """A processor failure fails the tick: the step raises and the world
    does not advance.

    The old contract (log and continue with the pre-failure frame) would
    append rows the failed processor never transformed — a silent hole in
    the per-tick history.
    """
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(OKProc())
        await system.add_processor(BadProc())
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)
        await world.create_entity([Foo(x=1)])

        with caplog.at_level("ERROR"):
            with pytest.raises(RuntimeError, match="boom"):
                await world.run(RunConfig(num_steps=1))
        assert any("Error processing archetype" in rec.message for rec in caplog.records)
        # The failed tick did not happen: the counter never advanced.
        assert world.tick == 0
    finally:
        await ws.shutdown()
