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


@pytest.mark.asyncio
async def test_failed_tick_commits_nothing_and_is_retryable(tmp_path):
    """A failed tick is a no-op: nothing is appended, staged mutations
    survive, and the same tick can be retried after the failure is fixed.

    The compute phase runs before any append, so a processor failure
    cannot half-commit a tick or consume the spawn it never persisted.
    """
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(OKProc())
        await system.add_processor(BadProc())
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)
        eid = await world.create_entity([Foo(x=1)])
        sig = (Foo,)

        with pytest.raises(RuntimeError, match="boom"):
            await world.run(RunConfig(num_steps=1))

        # Nothing committed; the pending spawn survived the failure.
        assert (await world.query_archetype(sig=sig, ticks=[0])).count_rows() == 0
        assert any(row["entity_id"] == eid for row in world.spawn_cache.get(sig, []))

        # Heal and retry the SAME tick: the spawn materializes.
        world.system.processors = [p for p in world.system.processors if not isinstance(p, BadProc)]
        await world.run(RunConfig(num_steps=1))
        rows = (await world.query_archetype(sig=sig, ticks=[0])).to_pylist()
        assert [row["entity_id"] for row in rows] == [eid]
        assert world.tick == 1
    finally:
        await ws.shutdown()


class Bar(Component):
    y: int


@pytest.mark.asyncio
async def test_one_failing_archetype_blocks_all_appends(tmp_path):
    """The compute phase is a barrier: when any archetype's processor
    fails, NO archetype appends rows for that tick — a tick either
    commits or it didn't happen, across the whole world."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(BadProc())  # matches Foo archetypes only
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)
        await world.create_entity([Foo(x=1)])
        bar_eid = await world.create_entity([Bar(y=2)])

        with pytest.raises(RuntimeError, match="boom"):
            await world.run(RunConfig(num_steps=1))

        # The healthy archetype appended nothing either, and keeps its spawn.
        bar_sig = (Bar,)
        assert (await world.query_archetype(sig=bar_sig, ticks=[0])).count_rows() == 0
        assert any(row["entity_id"] == bar_eid for row in world.spawn_cache.get(bar_sig, []))
        assert world.tick == 0
    finally:
        await ws.shutdown()
