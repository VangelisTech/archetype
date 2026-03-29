import pytest
from daft import col

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.aio import AsyncProcessor, AsyncSystem
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Pos(Component):
    x: int


class MoveRight(AsyncProcessor):
    components = (Pos,)
    priority = 0

    async def process(self, df, **kwargs):
        # increment x by 1 each step
        return df.with_column("pos__x", col("pos__x") + 1)


@pytest.mark.asyncio
async def test_prefer_live_reads_uses_live_snapshot_when_true(tmp_path):
    """With prefer_live_reads=True, world should reuse live snapshot instead of querier for previous state."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(MoveRight())
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)

        # spawn entity with Pos(x=0)
        await world.create_entity([Pos(x=0)])

        # Step 1: writes x=1; live holds active row
        await world.run(RunConfig(num_steps=1, prefer_live_reads=True))
        # Step 2 with prefer_live_reads=True should start from x=1 and go to x=2
        await world.run(RunConfig(num_steps=1, prefer_live_reads=True))

        # After two steps, active row should have x=2
        # Use public get_components API instead of accessing internals
        df = await world.get_components([Pos])
        assert df.where(col("is_active")).select("pos__x").to_pylist()[-1]["pos__x"] == 2
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_prefer_live_reads_false_queries_previous_tick(tmp_path):
    """With prefer_live_reads=False, world queries prior tick from store rather than using live snapshot."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store2"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(MoveRight())
        world = await ws.create_world(WorldConfig(name="w2"), storage_config=storage, system=system)

        await world.create_entity([Pos(x=5)])

        # Two steps without live reads; correctness should still be x+1 each step
        await world.run(RunConfig(num_steps=2, prefer_live_reads=False))
        df = await world.get_components([Pos])
        assert df.where(col("is_active")).select("pos__x").to_pylist()[-1]["pos__x"] == 7
    finally:
        await ws.shutdown()
