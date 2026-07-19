import asyncio
import pickle

import pytest

from archetype.core.aio import AsyncProcessor, AsyncSystem
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.errors import TickExecutionError, TickFailure
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
            # `except RuntimeError` remains a valid caller contract (#444):
            # the structured TickExecutionError subclasses it. Its message
            # names the failed table, not the processor's exception text.
            with pytest.raises(RuntimeError) as raised:
                await world.run(RunConfig(num_steps=1))
        assert isinstance(raised.value, TickExecutionError)
        assert Archetype.get_name((Foo,)) in str(raised.value)
        assert "boom" not in str(raised.value)
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

        with pytest.raises(TickExecutionError):
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

        with pytest.raises(TickExecutionError):
            await world.run(RunConfig(num_steps=1))

        # The healthy archetype appended nothing either, and keeps its spawn.
        bar_sig = (Bar,)
        assert (await world.query_archetype(sig=bar_sig, ticks=[0])).count_rows() == 0
        assert any(row["entity_id"] == bar_eid for row in world.spawn_cache.get(bar_sig, []))
        assert world.tick == 0
    finally:
        await ws.shutdown()


class RateLimitError(RuntimeError):
    """Provider-shaped test error without importing a vendor client."""


class FailFooWith(AsyncProcessor):
    components = (Foo,)
    priority = 1

    def __init__(self, error: Exception) -> None:
        self.error = error

    async def process(self, df, **kwargs):
        raise self.error


class FailBarWith(AsyncProcessor):
    components = (Bar,)
    priority = 1

    def __init__(self, error: Exception) -> None:
        self.error = error

    async def process(self, df, **kwargs):
        raise self.error


@pytest.mark.asyncio
async def test_step_preserves_ordered_structured_compute_failures(tmp_path):
    """#444: the aggregate step error preserves every failed table identity
    and the ORIGINAL exception objects in ascending table-id order, chained
    as an ExceptionGroup cause — and its own message never leaks the
    originals' text."""
    timeout = TimeoutError("private provider timeout detail")
    rate_limit = RateLimitError("private provider quota detail")
    errors_by_table = {
        Archetype.get_name((Foo,)): timeout,
        Archetype.get_name((Bar,)): rate_limit,
    }
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(FailFooWith(timeout))
        await system.add_processor(FailBarWith(rate_limit))
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)
        await world.create_entity([Foo(x=1)])
        await world.create_entity([Bar(y=2)])

        with pytest.raises(TickExecutionError) as raised:
            await world.step(RunConfig(num_steps=1))

        error = raised.value
        expected_tables = tuple(sorted(errors_by_table))
        assert isinstance(error, RuntimeError)
        assert error.phase == "compute"
        assert tuple(failure.table_id for failure in error.failures) == expected_tables
        assert tuple(failure.error for failure in error.failures) == tuple(
            errors_by_table[table_id] for table_id in expected_tables
        )
        assert isinstance(error.__cause__, ExceptionGroup)
        assert error.__cause__.exceptions == tuple(
            errors_by_table[table_id] for table_id in expected_tables
        )
        assert "private provider" not in str(error)
        assert world.tick == 0
        assert set(world.spawn_cache) == {(Foo,), (Bar,)}
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_step_preserves_ordered_structured_commit_failures(tmp_path, monkeypatch):
    """#444: commit-phase aggregation carries the same structured contract,
    with phase="commit" and every staged mutation preserved for retry."""
    foo_error = OSError("private foo commit detail")
    bar_error = RuntimeError("private bar execution detail")
    errors_by_table = {
        Archetype.get_name((Foo,)): foo_error,
        Archetype.get_name((Bar,)): bar_error,
    }
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage)
        await world.create_entity([Foo(x=1)])
        await world.create_entity([Bar(y=2)])
        world.commit_coordinator = None

        async def fail_commit(sig, df, run_config):
            raise errors_by_table[Archetype.get_name(sig)]

        monkeypatch.setattr(world, "_commit_archetype", fail_commit)

        with pytest.raises(TickExecutionError) as raised:
            await world.step(RunConfig(num_steps=1))

        error = raised.value
        expected_tables = tuple(sorted(errors_by_table))
        assert error.phase == "commit"
        assert tuple(failure.table_id for failure in error.failures) == expected_tables
        assert tuple(failure.error for failure in error.failures) == tuple(
            errors_by_table[table_id] for table_id in expected_tables
        )
        assert isinstance(error.__cause__, ExceptionGroup)
        assert error.__cause__.exceptions == tuple(
            errors_by_table[table_id] for table_id in expected_tables
        )
        assert "private" not in str(error)
        assert world.tick == 0
        assert set(world.spawn_cache) == {(Foo,), (Bar,)}
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_step_does_not_wrap_task_cancellation(tmp_path):
    """Cancellation is not a table failure: it propagates raw so task
    teardown is never masked behind the aggregate (#444 keeps the existing
    cancellation semantics)."""

    class CancelFoo(AsyncProcessor):
        components = (Foo,)
        priority = 1

        async def process(self, df, **kwargs):
            raise asyncio.CancelledError("cancel step")

    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(CancelFoo())
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)
        await world.create_entity([Foo(x=1)])

        # asyncio surfaces a task's cancellation as a fresh CancelledError
        # (the original message does not survive gather); the contract under
        # test is the TYPE: cancellation is never wrapped in the aggregate.
        with pytest.raises(asyncio.CancelledError):
            await world.step(RunConfig(num_steps=1))

        assert world.tick == 0
        assert (Foo,) in world.spawn_cache
    finally:
        await ws.shutdown()


def test_tick_execution_error_survives_pickle_round_trip():
    """The keyword-only constructor needs an explicit __reduce__; without it
    any process boundary (logging queues, worker pools) drops the error."""
    error = TickExecutionError(
        phase="compute",
        failures=(TickFailure(table_id="a_1c_table", error=ValueError("original detail")),),
    )

    clone = pickle.loads(pickle.dumps(error))

    assert isinstance(clone, TickExecutionError)
    assert clone.phase == "compute"
    assert clone.failures[0].table_id == "a_1c_table"
    assert isinstance(clone.failures[0].error, ValueError)
    assert str(clone) == str(error)
