# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Runtime contract tests.

Exercises ArchetypeRuntime and RuntimeWorld invariants that the
specification guarantees: single-flight activation, actor binding,
default admin identity, shutdown idempotency, fork handles,
and pre-activation hook rejection.
"""

from __future__ import annotations

import asyncio

import pytest
from uuid_utils import uuid7

from archetype import ArchetypeRuntime, Component
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.core.config import StorageConfig
from archetype.core.hooks import PreTick


class Pos(Component):
    x: float = 0.0
    y: float = 0.0


class Vel(Component):
    dx: float = 0.0
    dy: float = 0.0


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


# ── 1. Single-flight activation ─────────────────────────────────────────


class TestSingleFlightActivation:
    @pytest.mark.asyncio
    async def test_concurrent_spawns_produce_one_create_world(self, tmp_path):
        """50 concurrent spawn() calls on a never-activated world produce
        exactly ONE create_world call."""
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("single-flight", storage=storage)

            gate = rt._container.command_service
            original_create = gate.create_world
            call_count = 0

            async def counting_create(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                return await original_create(*args, **kwargs)

            gate.create_world = counting_create

            # Launch 50 concurrent spawns. Each triggers ensure_init if
            # the world is not yet activated.  The init_lock should
            # collapse them into a single create_world.
            #
            # spawn() acquires op_lock serially, but the first call
            # activates the world; subsequent calls see initialized=True.
            # We verify the activation path, not concurrent entity creation.
            tasks = [asyncio.create_task(world.spawn(Pos(x=float(i)))) for i in range(50)]
            results = await asyncio.gather(*tasks)

            assert call_count == 1, f"Expected 1 create_world call, got {call_count}"
            # All 50 entities should have been created
            assert len(results) == 50


# ── 2. Actor binding ────────────────────────────────────────────────────


class TestActorBinding:
    @pytest.mark.asyncio
    async def test_as_actor_produces_sibling_with_different_ctx(self, tmp_path):
        """world.as_actor(ctx) returns a sibling sharing the same underlying
        world state but bound to a different ActorCtx."""
        admin_ctx = ActorCtx(id=uuid7(), roles={"admin"})
        player_ctx = ActorCtx(id=uuid7(), roles={"admin", "player"})

        async with ArchetypeRuntime(actor_ctx=admin_ctx) as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("actor-test", storage=storage)
            sibling = world.as_actor(player_ctx)

            # Both share the same _state (same logical world)
            assert world._state is sibling._state
            # But different contexts
            assert world._ctx is not sibling._ctx
            assert world._ctx.id == admin_ctx.id
            assert sibling._ctx.id == player_ctx.id

    @pytest.mark.asyncio
    async def test_sibling_operations_use_sibling_ctx(self, tmp_path):
        """Operations through the sibling handle use the sibling's ActorCtx,
        verified via audit row actor_id."""
        admin_ctx = ActorCtx(id=uuid7(), roles={"admin"})
        player_ctx = ActorCtx(id=uuid7(), roles={"admin", "player"})

        async with ArchetypeRuntime(actor_ctx=admin_ctx) as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("actor-audit", storage=storage)

            # Activate via the admin handle
            await world.spawn(Pos(x=1.0))

            sibling = world.as_actor(player_ctx)
            await sibling.spawn(Pos(x=2.0))

            # Query audit log — filter by the sibling's actor_id
            audit = rt._container.audit_log
            sibling_rows = (await audit.query(actor_id=player_ctx.id)).to_pylist()
            assert len(sibling_rows) >= 1, "Sibling operation should appear in audit log"
            assert sibling_rows[0]["command_type"] == "spawn"


# ── 3. Default admin identity ───────────────────────────────────────────


class TestDefaultAdminIdentity:
    @pytest.mark.asyncio
    async def test_default_ctx_has_admin_role(self, tmp_path):
        """ArchetypeRuntime() without explicit ctx defaults to {admin} roles,
        which is required for create_world."""
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("admin-test", storage=storage)

            # create_world is admin-only. If the default ctx lacks admin,
            # this will raise GuardrailError.
            eid = await world.spawn(Pos(x=1.0))
            assert isinstance(eid, int)

            # Double-check the runtime's actor ctx
            assert "admin" in rt._actor_ctx.roles


# ── 3.5. Batch spawn sugar ─────────────────────────────────────────────


class TestSpawnBatchSugar:
    @pytest.mark.asyncio
    async def test_spawn_batch_repeats_component_template(self, tmp_path):
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("spawn-batch", storage=storage)

            entity_ids = await world.spawn_batch(Pos(x=1.0, y=2.0), 25)
            await world.step()
            rows = (await world.query(Pos)).collect().to_pylist()

            assert len(entity_ids) == 25
            assert len(set(entity_ids)) == 25
            assert len(rows) == 25
            assert {row["pos__x"] for row in rows} == {1.0}
            assert {row["pos__y"] for row in rows} == {2.0}

    @pytest.mark.asyncio
    async def test_spawn_batch_supports_multi_component_template(self, tmp_path):
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("spawn-batch-multi", storage=storage)

            entity_ids = await world.spawn_batch(Pos(x=3.0), Vel(dx=4.0), count=10)
            await world.step()
            rows = (await world.query(Pos, Vel)).collect().to_pylist()

            assert len(entity_ids) == 10
            assert len(rows) == 10
            assert {row["pos__x"] for row in rows} == {3.0}
            assert {row["vel__dx"] for row in rows} == {4.0}

    @pytest.mark.asyncio
    async def test_spawn_batch_rejects_missing_or_invalid_count(self, tmp_path):
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("spawn-batch-invalid", storage=storage)

            with pytest.raises(TypeError, match="requires a count"):
                await world.spawn_batch(Pos())

            with pytest.raises(ValueError, match="count must be >= 1"):
                await world.spawn_batch(Pos(), 0)


# ── 4. Shutdown idempotency ─────────────────────────────────────────────


class TestShutdownIdempotency:
    @pytest.mark.asyncio
    async def test_double_shutdown_is_noop(self, tmp_path):
        """Calling shutdown() twice does not raise."""
        rt = ArchetypeRuntime()
        await rt.__aenter__()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = rt.world("shutdown-test", storage=storage)
        await world.spawn(Pos())

        await rt.shutdown()
        await rt.shutdown()  # second call is a no-op

    @pytest.mark.asyncio
    async def test_operations_after_shutdown_raise(self, tmp_path):
        """Operations after shutdown raise RuntimeError."""
        rt = ArchetypeRuntime()
        await rt.__aenter__()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = rt.world("post-shutdown", storage=storage)
        await world.spawn(Pos())

        await rt.shutdown()

        # Creating a new world handle should fail
        with pytest.raises(RuntimeError):
            rt.world("should-fail")

        # Spawning on existing handle should fail (runtime is closed)
        with pytest.raises(RuntimeError):
            await world.spawn(Pos())


# ── 5. Fork handles ─────────────────────────────────────────────────────


class TestForkHandles:
    @pytest.mark.asyncio
    async def test_fork_returns_new_handle_with_different_world_id(self, tmp_path):
        """world.fork("branch") returns a new handle with a distinct world_id."""
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("fork-source", storage=storage)
            await world.spawn(Pos(x=1.0))

            fork = await world.fork(
                "branch",
                storage=StorageConfig(uri=str(tmp_path / "fork_store"), namespace="ns"),
            )

            # Fork has a different world_id
            assert fork.world_id != world.world_id
            # Fork is a RuntimeWorld
            assert type(fork).__name__ == "RuntimeWorld"

    @pytest.mark.asyncio
    async def test_fork_registered_with_runtime(self, tmp_path):
        """The fork handle is registered with the runtime. Shutdown destroys it."""
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("fork-reg", storage=storage)
            await world.spawn(Pos())

            fork = await world.fork(
                "branch",
                storage=StorageConfig(uri=str(tmp_path / "fork_store"), namespace="ns"),
            )

            # Fork should be tracked
            assert fork in rt._handles

        # After exiting the context manager (shutdown), the fork state is closed
        assert fork._state.closed


# ── 6. Pre-activation add_hook raises ───────────────────────────────────


class TestPreActivationHookRaises:
    @pytest.mark.asyncio
    async def test_add_hook_before_activation_raises(self, tmp_path):
        """Calling world.add_hook(PreTick, handler) before any activation
        raises RuntimeError."""
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("hook-test", storage=storage)

            async def handler(event):
                pass

            with pytest.raises(RuntimeError, match="Cannot add_hook before activation"):
                await world.add_hook(PreTick, handler)


# ── Observability vendor neutrality ──────────────────────────────────────


class TestVendorNeutralObservability:
    """ArchetypeRuntime must construct with no telemetry vendor configured.

    Without LOGFIRE_*/OTEL_* opt-in the runtime never touches logfire (it
    may not even be installed for package consumers); spans ride the no-op
    OTel API.
    """

    def test_runtime_never_calls_logfire_without_opt_in(self, monkeypatch):
        logfire = pytest.importorskip("logfire")
        for var in (
            "LOGFIRE_TOKEN",
            "LOGFIRE_API_KEY",
            "LOGFIRE_SEND_TO_LOGFIRE",
            "ARCHETYPE_LOG",
        ):
            monkeypatch.delenv(var, raising=False)

        calls: list[dict] = []
        monkeypatch.setattr(logfire, "configure", lambda **kw: calls.append(kw))

        runtime = ArchetypeRuntime()
        asyncio.run(runtime.shutdown())

        assert calls == [], "no vendor SDK configuration without explicit opt-in"

    def test_runtime_uses_logfire_when_opted_in(self, monkeypatch):
        logfire = pytest.importorskip("logfire")
        from archetype import _obs

        monkeypatch.setattr(_obs, "_configured", False)
        monkeypatch.setenv("LOGFIRE_SEND_TO_LOGFIRE", "1")
        monkeypatch.delenv("ARCHETYPE_LOG", raising=False)

        calls: list[dict] = []
        monkeypatch.setattr(logfire, "configure", lambda **kw: calls.append(kw))

        runtime = ArchetypeRuntime()
        asyncio.run(runtime.shutdown())

        assert calls and calls[-1]["send_to_logfire"] is True
        assert calls[-1]["console"] is False, "console verbosity belongs to ARCHETYPE_LOG"
