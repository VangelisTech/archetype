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

        # N+1: tick 0 applies spawn, tick 1+ processors run
        # Step 1 (tick 0): spawn applied post-processors
        # Step 2 (tick 1): MoveRight runs → x=1
        # Step 3 (tick 2): MoveRight runs → x=2
        await world.run(RunConfig(num_steps=3, prefer_live_reads=True))

        # After three steps, active row should have x=2 (processors ran at tick 1 and 2)
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

        # N+1: tick 0 applies spawn, ticks 1-2 processors run → x = 5 + 2 = 7
        await world.run(RunConfig(num_steps=3, prefer_live_reads=False))
        df = await world.get_components([Pos])
        assert df.where(col("is_active")).select("pos__x").to_pylist()[-1]["pos__x"] == 7
    finally:
        await ws.shutdown()
