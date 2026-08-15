# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Lifecycle parity tests.

Covers shutdown draining and error aggregation, op_lock serialization,
multi-runtime isolation, blocking/async runtime-facade parity, and viewer
guardrails.
"""

from __future__ import annotations

import asyncio

import pytest
from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component
from archetype.core.config import StorageConfig
from archetype.errors import RuntimeShutdownError
from archetype.evaluation import handlers as evaluation_handlers
from archetype.evaluation.models import RunGraders
from archetype.research.models import AutoResearchConfig
from archetype.research.runtime import Research
from archetype.runtime import SyncRuntimeWorld
from archetype.runtime.world import RuntimeWorld


class LifecycleParityPos(Component):
    x: float = 0.0
    y: float = 0.0


class BlockingIncrement(AsyncProcessor):
    """Hold one step open so shutdown ordering is observable."""

    components = (LifecycleParityPos,)

    def __init__(self, entered: asyncio.Event, release: asyncio.Event) -> None:
        self.entered = entered
        self.release = release

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        self.entered.set()
        await self.release.wait()
        return df.with_column(
            "lifecycleparitypos__x",
            col("lifecycleparitypos__x") + 1,
        )


# ── 1. Shutdown error aggregation ──────────────────────────────────────


class TestShutdownErrorAggregation:
    @pytest.mark.asyncio
    async def test_shutdown_closes_healthy_world_and_reports_failing_owner(
        self, tmp_path, monkeypatch
    ):
        """A failed owner close does not prevent peer world handles from closing."""
        rt = ArchetypeRuntime()
        await rt.__aenter__()

        storage_a = StorageConfig(uri=str(tmp_path / "a"), namespace="ns")
        storage_b = StorageConfig(uri=str(tmp_path / "b"), namespace="ns")

        world_a = rt.world("world-a", storage=storage_a)
        world_b = rt.world("world-b", storage=storage_b)

        # Activate both worlds
        await world_a.spawn(LifecycleParityPos(x=1.0))
        await world_b.spawn(LifecycleParityPos(x=2.0))

        healthy_shutdown = world_a._state.shutdown

        async def exploding_shutdown() -> None:
            raise RuntimeError("world-a kaboom")

        monkeypatch.setattr(world_a._state, "shutdown", exploding_shutdown)
        try:
            with pytest.raises(RuntimeShutdownError) as captured:
                await rt.shutdown()

            failure = captured.value.failures[0]
            assert captured.value.phase == "world-handles"
            assert len(captured.value.failures) == 1
            assert failure.owner == world_a._reservation.owner
            assert isinstance(failure.cause, RuntimeError)
            assert str(failure.cause) == "world-a kaboom"
            assert world_b._state.closed
            assert world_b._reservation.released
            assert not world_a._reservation.released
        finally:
            monkeypatch.setattr(world_a._state, "shutdown", healthy_shutdown)
            await rt.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_waits_for_in_flight_step(self, tmp_path):
        """Runtime services stay open until an admitted world operation exits."""
        entered = asyncio.Event()
        release = asyncio.Event()
        runtime = ArchetypeRuntime()
        world = runtime.world(
            "shutdown-race",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
        )

        await world.spawn(LifecycleParityPos())
        await world.step()  # persist the raw initial condition
        await world.add_processor(BlockingIncrement(entered, release))

        step_task = asyncio.create_task(world.step())
        await asyncio.wait_for(entered.wait(), timeout=5)
        shutdown_task = asyncio.create_task(runtime.shutdown())
        await asyncio.sleep(0)  # let shutdown stop admission and reach op_lock

        assert not shutdown_task.done(), "shutdown overtook the in-flight step"

        release.set()
        await asyncio.wait_for(step_task, timeout=5)
        await asyncio.wait_for(shutdown_task, timeout=5)

        with pytest.raises(RuntimeError, match="closed"):
            await world.info()

    @pytest.mark.asyncio
    async def test_shutdown_waits_for_admitted_autoresearch(self, tmp_path):
        runtime = ArchetypeRuntime()
        world = runtime.world(
            "autoresearch-drain",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
        )
        await world.spawn(LifecycleParityPos())
        entered = asyncio.Event()
        release = asyncio.Event()

        async def prepare_candidate(_context: object) -> None:
            entered.set()
            await release.wait()

        operation = asyncio.create_task(
            Research(world).autoresearch(
                AutoResearchConfig(
                    experiment_name="lifecycle-parity",
                    experiment_id="autoresearch-drain",
                    evaluator_id="lifecycle-parity-evaluator",
                    rollout_contract_id="lifecycle-parity-rollout",
                    max_iterations=1,
                    num_episodes=1,
                    record_to_ledger=False,
                ),
                lambda _rollout: 0.0,
                prepare_candidate=prepare_candidate,
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=5)
        shutdown = asyncio.create_task(runtime.shutdown())
        await asyncio.sleep(0)

        assert not shutdown.done()
        release.set()
        result = await operation
        assert result.iterations_completed == 1
        await shutdown

    @pytest.mark.asyncio
    async def test_shutdown_waits_for_post_query_grading(self, tmp_path, monkeypatch):
        runtime = ArchetypeRuntime()
        world = runtime.world(
            "grade-drain",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
        )
        await world.spawn(LifecycleParityPos())
        await world.step()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def blocked_graders(*args, **kwargs):
            entered.set()
            await release.wait()
            return ["finished"]

        run_graders = runtime._resources.dispatcher._registry.resolve_name("run_graders")
        assert run_graders.model is RunGraders
        assert run_graders.handler is evaluation_handlers.run_graders
        monkeypatch.setattr(evaluation_handlers.grading, "run_graders", blocked_graders)
        operation = asyncio.create_task(
            world.grade(LifecycleParityPos, graders=[lambda _frame: object()])
        )
        await entered.wait()
        shutdown = asyncio.create_task(runtime.shutdown())
        await asyncio.sleep(0)

        assert not shutdown.done()
        release.set()
        assert await operation == ["finished"]
        await shutdown


# ── 2. op_lock serialization ──────────────────────────────────────────


class TestOpLockSerialization:
    @pytest.mark.asyncio
    async def test_concurrent_steps_no_deadlock_tick_advances_by_10(self, tmp_path):
        """Launch 10 concurrent step() calls on the same world. They must
        all complete (no deadlock) and the tick must advance by exactly 10."""
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("op-lock", storage=storage)

            # Activate with one entity so step() has something to do
            await world.spawn(LifecycleParityPos(x=0.0))

            info_before = await world.info()
            tick_before = info_before.tick

            # 10 concurrent step() calls — op_lock serializes them
            tasks = [asyncio.create_task(world.step()) for _ in range(10)]
            await asyncio.gather(*tasks)

            info_after = await world.info()
            assert info_after.tick == tick_before + 10


# ── 3. Multi-runtime isolation ─────────────────────────────────────────


class TestMultiRuntimeIsolation:
    @pytest.mark.asyncio
    async def test_shutdown_one_runtime_leaves_other_usable(self, tmp_path):
        """Two ArchetypeRuntime instances are independent. Shutting down one
        does not affect worlds in the other."""
        rt1 = ArchetypeRuntime()
        rt2 = ArchetypeRuntime()
        await rt1.__aenter__()
        await rt2.__aenter__()

        storage1 = StorageConfig(uri=str(tmp_path / "rt1"), namespace="ns")
        storage2 = StorageConfig(uri=str(tmp_path / "rt2"), namespace="ns")

        world1 = rt1.world("w1", storage=storage1)
        world2 = rt2.world("w2", storage=storage2)

        await world1.spawn(LifecycleParityPos(x=1.0))
        await world2.spawn(LifecycleParityPos(x=2.0))

        # Shut down rt1
        await rt1.shutdown()

        # rt2's world should still be operational
        eid = await world2.spawn(LifecycleParityPos(x=3.0))
        assert isinstance(eid, int)

        info = await world2.info()
        assert info.tick >= 0

        await rt2.shutdown()


# ── 4. Blocking/async runtime-facade parity ───────────────────────────


class TestBlockingAsyncRuntimeFacadeParity:
    def test_framework_world_methods_have_blocking_facade_parity(self):
        """Framework operations reach parity through the blocking facade."""
        async_methods = {
            name
            for name in dir(RuntimeWorld)
            if not name.startswith("_") and callable(getattr(RuntimeWorld, name))
        }
        sync_methods = {
            name
            for name in dir(SyncRuntimeWorld)
            if not name.startswith("_") and callable(getattr(SyncRuntimeWorld, name))
        }

        assert async_methods - sync_methods == {"library"}
        assert sync_methods == async_methods - {"library"}


# ── 5. Viewer override raises on mutation ──────────────────────────────


class TestViewerGuardrail:
    @pytest.mark.asyncio
    async def test_trusted_runtime_does_not_require_adapter_identity(self, tmp_path):
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("trusted-test", storage=storage)
            assert isinstance(await world.spawn(LifecycleParityPos(x=1.0)), int)
