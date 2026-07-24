# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Runtime contract tests.

Exercises ArchetypeRuntime and RuntimeWorld invariants that the
specification guarantees: actor-free trust, single-flight activation,
shutdown idempotency, fork handles, and pre-activation hook rejection.
"""

from __future__ import annotations

import asyncio
import gc
import weakref

import pytest

from archetype import ArchetypeRuntime, Component
from archetype.core.aio import AsyncProcessor
from archetype.core.config import StorageConfig
from archetype.core.errors import TickExecutionError
from archetype.core.hooks import PreTick
from archetype.runtime.world import _runtime_cleanup_scope


class Pos(Component):
    x: float = 0.0
    y: float = 0.0


class Vel(Component):
    dx: float = 0.0
    dy: float = 0.0


class FailPosWith(AsyncProcessor):
    components = (Pos,)
    priority = 1

    def __init__(self, error: Exception) -> None:
        self.error = error

    async def process(self, df, **kwargs):
        raise self.error


# ── 1. Single-flight activation ─────────────────────────────────────────


class TestSingleFlightActivation:
    @pytest.mark.asyncio
    async def test_concurrent_spawns_produce_one_create_world(self, tmp_path):
        """50 concurrent spawn() calls on a never-activated world produce
        exactly ONE create_world call."""
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("single-flight", storage=storage)

            application = rt._container.application
            original_create = application.create_world
            call_count = 0

            async def counting_create(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                return await original_create(*args, **kwargs)

            application.create_world = counting_create

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

    @pytest.mark.asyncio
    async def test_failed_activation_rolls_back_and_can_retry(self, tmp_path, monkeypatch):
        runtime = ArchetypeRuntime()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = runtime.world("activation-retry", storage=storage, processors=[object()])
        application = runtime._container.application
        original_add = application.add_processor
        calls = 0

        async def fail_once(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("injected wiring failure")
            return await original_add(*args, **kwargs)

        monkeypatch.setattr(application, "add_processor", fail_once)
        try:
            with pytest.raises(RuntimeError, match="injected wiring failure"):
                await world.spawn(Pos())

            assert not world._state.initialized
            assert await runtime._container.world_registry.list_worlds() == []

            # Replace the invalid processor as well as the transient failure;
            # retry must perform one clean activation rather than collide with
            # the rolled-back name registration.
            world._state.init_processors = []
            entity_id = await world.spawn(Pos())
            assert isinstance(entity_id, int)
            assert world._state.initialized
        finally:
            await runtime.shutdown()


# ── 2. Actor binding ────────────────────────────────────────────────────


class TestActorBinding:
    @pytest.mark.asyncio
    async def test_runtime_bypasses_untrusted_command_gateway(self, tmp_path, monkeypatch):
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "trusted-runtime",
                storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
            )

            async def forbidden_gateway_call(*args, **kwargs):
                raise AssertionError("trusted runtime must not call CommandGateway")

            monkeypatch.setattr(
                runtime._container.command_gateway,
                "create_world",
                forbidden_gateway_call,
            )
            assert isinstance(await world.spawn(Pos()), int)

    @pytest.mark.asyncio
    async def test_runtime_has_no_access_audit_identity(self, tmp_path):
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "actor-free",
                storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
            )
            await world.spawn(Pos())
            rows = await runtime._container.audit_log.query(world_id=world.world_id)
            assert rows.count_rows() == 0


# ── 3. Default admin identity ───────────────────────────────────────────


class TestDefaultAdminIdentity:
    def test_runtime_constructor_has_no_actor_context(self):
        import inspect

        assert "actor_ctx" not in inspect.signature(ArchetypeRuntime).parameters


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

    @pytest.mark.asyncio
    async def test_failed_world_destroy_remains_retryable(self, tmp_path, monkeypatch):
        runtime = ArchetypeRuntime()
        world = runtime.world(
            "retry-destroy",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
        )
        await world.spawn(Pos())
        application = runtime._application
        original_destroy = application.destroy_world
        attempts = 0

        async def fail_once(world_id):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("destroy failed")
            await original_destroy(world_id)

        monkeypatch.setattr(application, "destroy_world", fail_once)

        with pytest.raises(RuntimeError, match="destroy failed"):
            await world.shutdown()

        assert not world._state.closed
        assert world in runtime._handles
        with pytest.raises(RuntimeError, match="closed"):
            await world.spawn(Pos())

        await world.shutdown()

        assert attempts == 2
        assert world._state.closed
        assert world not in runtime._handles
        await runtime.shutdown()

    @pytest.mark.asyncio
    async def test_container_cancellation_group_uses_retryable_runtime_error_contract(
        self, monkeypatch
    ):
        runtime = ArchetypeRuntime()
        attempts = 0

        async def fail_container_shutdown():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise BaseExceptionGroup(
                    "container shutdown failed",
                    [asyncio.CancelledError("sandbox close cancelled")],
                )

        monkeypatch.setattr(runtime._container, "shutdown", fail_container_shutdown)

        with pytest.raises(RuntimeError, match="encountered 1 error") as captured:
            await runtime.shutdown()

        assert isinstance(captured.value.__cause__, BaseExceptionGroup)
        assert isinstance(captured.value.__cause__.exceptions[0], asyncio.CancelledError)

        with pytest.raises(RuntimeError, match="closed"):
            runtime.world("rejected-after-container-failure")

        await runtime.shutdown()

        assert attempts == 2

    @pytest.mark.asyncio
    async def test_runtime_shutdown_retries_mission_handles_before_container(self, monkeypatch):
        runtime = ArchetypeRuntime()
        calls: list[str] = []

        class FailingMissionHandle:
            attempts = 0

            async def _shutdown_internal(self, *, from_runtime: bool) -> None:
                assert from_runtime
                calls.append("mission")
                self.attempts += 1
                if self.attempts == 1:
                    raise RuntimeError("mission close failed")
                runtime._mission_handles.discard(self)  # type: ignore[arg-type]

        mission = FailingMissionHandle()
        runtime._mission_handles.add(mission)  # type: ignore[arg-type]

        async def shutdown_container() -> None:
            calls.append("container")

        monkeypatch.setattr(runtime._container, "shutdown", shutdown_container)

        with pytest.raises(RuntimeError, match="encountered 1 error") as captured:
            await runtime.shutdown()

        assert calls == ["mission"]
        assert isinstance(captured.value.__cause__, RuntimeError)
        assert "mission close failed" in str(captured.value.__cause__)

        mission_ref = weakref.ref(mission)
        del mission
        gc.collect()
        assert mission_ref() is not None
        with pytest.raises(RuntimeError, match="closed"):
            runtime.world("rejected-after-failed-shutdown")

        await runtime.shutdown()
        await runtime.shutdown()

        assert calls == ["mission", "mission", "container"]
        assert not runtime._mission_handles

    @pytest.mark.asyncio
    async def test_failed_mission_cleanup_still_drains_admitted_world_work(
        self, tmp_path, monkeypatch
    ):
        runtime = ArchetypeRuntime()
        world = runtime.world(
            "drain-before-mission-failure",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
        )
        await world.spawn(Pos())

        run_started = asyncio.Event()
        run_release = asyncio.Event()
        mission_attempted = asyncio.Event()

        async def blocking_run(*args, **kwargs):
            run_started.set()
            await run_release.wait()
            return object()

        monkeypatch.setattr(runtime._application, "run", blocking_run)

        class FailingOnceMissionHandle:
            attempts = 0

            async def _shutdown_internal(self, *, from_runtime: bool) -> None:
                assert from_runtime
                self.attempts += 1
                mission_attempted.set()
                if self.attempts == 1:
                    raise RuntimeError("mission close failed")
                runtime._mission_handles.discard(self)  # type: ignore[arg-type]

        mission = FailingOnceMissionHandle()
        runtime._mission_handles.add(mission)  # type: ignore[arg-type]

        admitted_run = asyncio.create_task(world.run())
        await run_started.wait()
        shutdown = asyncio.create_task(runtime.shutdown())
        await mission_attempted.wait()
        await asyncio.sleep(0)

        assert not shutdown.done()

        run_release.set()
        await admitted_run
        with pytest.raises(RuntimeError, match="mission close failed"):
            await shutdown

        assert not world._state.closed
        with pytest.raises(RuntimeError, match="closed"):
            await world.spawn(Pos())

        await runtime.shutdown()
        assert world._state.closed

    @pytest.mark.asyncio
    async def test_cleanup_capability_does_not_admit_a_sibling_world(self, tmp_path):
        runtime = ArchetypeRuntime()
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        mission_world = runtime.world("mission", storage=storage)
        sibling_world = runtime.world("sibling", storage=storage)
        await mission_world.spawn(Pos())
        await sibling_world.spawn(Pos())

        runtime._shutdown_started = True
        mission_world._state.closing = True
        try:
            with _runtime_cleanup_scope(mission_world._state):
                await mission_world.spawn(Pos())
                with pytest.raises(RuntimeError, match="closed"):
                    await sibling_world.spawn(Pos())
        finally:
            runtime._shutdown_started = False
            mission_world._state.closing = False
            await runtime.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_runtime_shutdown_is_single_flight(self, monkeypatch):
        runtime = ArchetypeRuntime()
        close_started = asyncio.Event()
        close_release = asyncio.Event()
        second_started = asyncio.Event()
        calls: list[str] = []

        class BlockingMissionHandle:
            async def _shutdown_internal(self, *, from_runtime: bool) -> None:
                assert from_runtime
                calls.append("mission")
                close_started.set()
                await close_release.wait()
                runtime._mission_handles.discard(self)  # type: ignore[arg-type]

        mission = BlockingMissionHandle()
        runtime._mission_handles.add(mission)  # type: ignore[arg-type]

        async def shutdown_container() -> None:
            calls.append("container")

        async def second_shutdown() -> None:
            second_started.set()
            await runtime.shutdown()

        monkeypatch.setattr(runtime._container, "shutdown", shutdown_container)
        first = asyncio.create_task(runtime.shutdown())
        await close_started.wait()
        second = asyncio.create_task(second_shutdown())
        await second_started.wait()
        assert not second.done()

        close_release.set()
        await asyncio.gather(first, second)

        assert calls == ["mission", "container"]

    @pytest.mark.asyncio
    async def test_cancelled_runtime_shutdown_retains_cleanup_for_retry(self, monkeypatch):
        runtime = ArchetypeRuntime()
        close_started = asyncio.Event()
        never_release = asyncio.Event()
        calls: list[str] = []

        class CancelledOnceMissionHandle:
            attempts = 0

            async def _shutdown_internal(self, *, from_runtime: bool) -> None:
                assert from_runtime
                calls.append("mission")
                self.attempts += 1
                if self.attempts == 1:
                    close_started.set()
                    await never_release.wait()
                runtime._mission_handles.discard(self)  # type: ignore[arg-type]

        mission = CancelledOnceMissionHandle()
        runtime._mission_handles.add(mission)  # type: ignore[arg-type]

        async def shutdown_container() -> None:
            calls.append("container")

        monkeypatch.setattr(runtime._container, "shutdown", shutdown_container)
        interrupted = asyncio.create_task(runtime.shutdown())
        await close_started.wait()
        interrupted.cancel()
        with pytest.raises(RuntimeError, match="encountered 1 error") as captured:
            await interrupted

        assert isinstance(captured.value.__cause__, asyncio.CancelledError)
        assert calls == ["mission"]
        with pytest.raises(RuntimeError, match="closed"):
            runtime.world("rejected-after-cancelled-shutdown")

        await runtime.shutdown()

        assert calls == ["mission", "mission", "container"]

    @pytest.mark.asyncio
    async def test_world_shutdown_cancellation_does_not_skip_sibling_cleanup(self, monkeypatch):
        runtime = ArchetypeRuntime()
        calls: list[str] = []

        class CancelledOnceWorldHandle:
            attempts = 0

            async def _shutdown_internal(self, *, from_runtime: bool) -> None:
                assert from_runtime
                calls.append("cancelled-world")
                self.attempts += 1
                if self.attempts == 1:
                    raise asyncio.CancelledError("world close cancelled")

        class SiblingWorldHandle:
            async def _shutdown_internal(self, *, from_runtime: bool) -> None:
                assert from_runtime
                calls.append("sibling-world")

        first = CancelledOnceWorldHandle()
        second = SiblingWorldHandle()
        runtime._handles = (first, second)  # type: ignore[assignment]

        async def shutdown_container() -> None:
            calls.append("container")

        monkeypatch.setattr(runtime._container, "shutdown", shutdown_container)

        with pytest.raises(RuntimeError, match="encountered 1 error") as captured:
            await runtime.shutdown()

        assert isinstance(captured.value.__cause__, asyncio.CancelledError)
        assert calls == ["cancelled-world", "sibling-world"]

        await runtime.shutdown()

        assert calls == [
            "cancelled-world",
            "sibling-world",
            "cancelled-world",
            "sibling-world",
            "container",
        ]


class TestStructuredStepFailures:
    """#444: TickExecutionError crosses the runtime boundary unchanged, so
    scripts classify provider failures by isinstance on failure.error."""

    @pytest.mark.asyncio
    async def test_runtime_propagates_structured_failure_unchanged(self, tmp_path):
        processor_error = TimeoutError("private provider detail")
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "structured-failure",
                storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
                processors=[FailPosWith(processor_error)],
            )
            await world.spawn(Pos())

            with pytest.raises(TickExecutionError) as raised:
                await world.step()

            assert raised.value.phase == "compute"
            assert len(raised.value.failures) == 1
            assert raised.value.failures[0].error is processor_error

    def test_sync_runtime_preserves_the_same_failure_contract(self, tmp_path):
        processor_error = TimeoutError("private provider detail")
        with ArchetypeRuntime.sync() as runtime:
            world = runtime.world(
                "sync-structured-failure",
                storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
                processors=[FailPosWith(processor_error)],
            )
            world.spawn(Pos())

            with pytest.raises(TickExecutionError) as raised:
                world.step()

            assert raised.value.phase == "compute"
            assert len(raised.value.failures) == 1
            assert raised.value.failures[0].error is processor_error


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
