# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Lifecycle parity tests.

Covers shutdown draining and error aggregation, op_lock serialization,
multi-runtime isolation, sync/async surface parity, and viewer guardrails.
"""

from __future__ import annotations

import asyncio

import pytest
from daft import DataFrame, col
from uuid_utils import uuid7

from archetype import ArchetypeRuntime, AsyncProcessor, Component
from archetype.app.auth.errors import GuardrailError
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.core.config import StorageConfig
from archetype.runtime import SyncRuntimeWorld
from archetype.runtime.world import RuntimeWorld


class Pos(Component):
    x: float = 0.0
    y: float = 0.0


class BlockingIncrement(AsyncProcessor):
    """Hold one step open so shutdown ordering is observable."""

    components = (Pos,)

    def __init__(self, entered: asyncio.Event, release: asyncio.Event) -> None:
        self.entered = entered
        self.release = release

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        self.entered.set()
        await self.release.wait()
        return df.with_column("pos__x", col("pos__x") + 1)


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


# ── 1. Shutdown error aggregation ──────────────────────────────────────


class TestShutdownErrorAggregation:
    @pytest.mark.asyncio
    async def test_shutdown_destroys_healthy_world_and_raises_composite(self, tmp_path):
        """When one world's shutdown raises, the runtime still destroys the
        other world and then raises a composite RuntimeError."""
        rt = ArchetypeRuntime()
        await rt.__aenter__()

        storage_a = StorageConfig(uri=str(tmp_path / "a"), namespace="ns")
        storage_b = StorageConfig(uri=str(tmp_path / "b"), namespace="ns")

        world_a = rt.world("world-a", storage=storage_a)
        world_b = rt.world("world-b", storage=storage_b)

        # Activate both worlds
        await world_a.spawn(Pos(x=1.0))
        await world_b.spawn(Pos(x=2.0))

        # Monkeypatch world_a's _shutdown_internal to raise

        async def exploding_shutdown(*, from_runtime: bool) -> None:
            raise RuntimeError("world-a kaboom")

        world_a._shutdown_internal = exploding_shutdown

        with pytest.raises(RuntimeError, match="1 error"):
            await rt.shutdown()

        # world_b should still have been shut down despite world_a's failure
        assert world_b._state.closed

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

        await world.spawn(Pos())
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
            await world.spawn(Pos(x=0.0))

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

        await world1.spawn(Pos(x=1.0))
        await world2.spawn(Pos(x=2.0))

        # Shut down rt1
        await rt1.shutdown()

        # rt2's world should still be operational
        eid = await world2.spawn(Pos(x=3.0))
        assert isinstance(eid, int)

        info = await world2.info()
        assert info.tick >= 0

        await rt2.shutdown()


# ── 4. Sync/async surface parity ──────────────────────────────────────


class TestSyncAsyncSurfaceParity:
    def test_every_public_method_on_runtime_world_exists_on_sync(self):
        """Every public (non-dunder) method on RuntimeWorld must have a
        matching method on SyncRuntimeWorld."""
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

        missing = async_methods - sync_methods
        assert not missing, (
            f"SyncRuntimeWorld is missing public methods present on RuntimeWorld: {sorted(missing)}"
        )


# ── 5. Viewer override raises on mutation ──────────────────────────────


class TestViewerGuardrail:
    @pytest.mark.asyncio
    async def test_viewer_cannot_create_world(self, tmp_path):
        """ArchetypeRuntime(actor_ctx=ActorCtx(roles={'viewer'})) should
        raise GuardrailError on the first operation that needs admin
        (world creation via spawn)."""
        viewer_ctx = ActorCtx(id=uuid7(), roles={"viewer"})

        async with ArchetypeRuntime(actor_ctx=viewer_ctx) as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("viewer-test", storage=storage)

            with pytest.raises(GuardrailError):
                await world.spawn(Pos(x=1.0))
