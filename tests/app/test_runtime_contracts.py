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
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

import pytest

from archetype import ArchetypeRuntime, Component
from archetype.commands.models import GetAuditHistory
from archetype.core.aio import AsyncProcessor
from archetype.core.config import StorageConfig
from archetype.core.errors import TickExecutionError
from archetype.core.hooks import PreTick
from archetype.errors import RuntimeShutdownError
from archetype.runtime import runtime as runtime_module
from archetype.runtime_resources import (
    OwnerReservation,
    RuntimeCloseState,
    RuntimeResources,
    RuntimeShutdownPhase,
)
from archetype.world.models import (
    AddProcessor,
    CreateWorld,
    DestroyWorld,
    ListWorlds,
    Run,
)


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


class _DrainDispatcher:
    """Minimal canonical admission seam for isolated runtime-owner tests."""

    def __init__(
        self,
        events: list[str],
        *,
        stop_failures: list[BaseException] | None = None,
    ) -> None:
        self.events = events
        self.stop_failures = list(stop_failures or [])
        self.stop_calls = 0
        self.wait_calls = 0

    async def stop_admission(self) -> None:
        self.stop_calls += 1
        self.events.append("admission:stop")
        if self.stop_failures:
            raise self.stop_failures.pop(0)

    async def wait_drained(self) -> None:
        self.wait_calls += 1
        self.events.append("admission:drain")


class _OwnedHandle:
    def __init__(
        self,
        label: str,
        events: list[str],
        *,
        failures: list[BaseException] | None = None,
        started: asyncio.Event | None = None,
        release: asyncio.Event | None = None,
    ) -> None:
        self.label = label
        self.events = events
        self.failures = list(failures or [])
        self.started = started
        self.release = release
        self.close_calls = 0

    async def aclose(self) -> None:
        self.close_calls += 1
        self.events.append(f"close:{self.label}:{self.close_calls}")
        if self.started is not None:
            self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.failures:
            raise self.failures.pop(0)


class _Dependency:
    def __init__(self, label: str, events: list[str]) -> None:
        self.label = label
        self.events = events
        self.close_calls = 0

    async def shutdown(self) -> None:
        self.close_calls += 1
        self.events.append(f"shutdown:{self.label}:{self.close_calls}")


async def _reserve_handle(
    resources: RuntimeResources,
    handle: Any,
    *,
    owner: str,
    phase: RuntimeShutdownPhase,
) -> OwnerReservation:
    reservation = resources.reserve_owner(owner, phase=phase)

    async def construct() -> Any:
        return handle

    assert await reservation.construct(construct) is handle
    return reservation


def _runtime_with_resources(
    monkeypatch: pytest.MonkeyPatch,
    resources: RuntimeResources,
) -> ArchetypeRuntime:
    monkeypatch.setattr(
        runtime_module,
        "build_runtime_resources",
        lambda _config: resources,
    )
    runtime = ArchetypeRuntime()
    assert runtime._resources is resources
    return runtime


def _replace_operation_handler(
    monkeypatch: pytest.MonkeyPatch,
    dispatcher: Any,
    *,
    operation_name: str,
    operation_type: type,
    handler: Callable[[Any], Awaitable[Any]],
) -> Callable[[Any], Awaitable[Any]]:
    """Install one fault/counting handler at the exact operation registry seam."""

    registry = dispatcher._registry
    original = registry.resolve_name(operation_name)
    assert original.model is operation_type
    replacement = replace(original, handler=handler)
    monkeypatch.setitem(registry._by_name, operation_name, replacement)
    monkeypatch.setitem(registry._by_model, operation_type, replacement)
    return original.handler


# ── 1. Single-flight activation ─────────────────────────────────────────


class TestSingleFlightActivation:
    @pytest.mark.asyncio
    async def test_concurrent_spawns_produce_one_create_world(self, tmp_path, monkeypatch):
        """50 concurrent spawn() calls on a never-activated world produce
        exactly ONE create_world call."""
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("single-flight", storage=storage)

            call_count = 0

            async def counting_create(operation):
                nonlocal call_count
                assert type(operation) is CreateWorld
                call_count += 1
                return await original_create(operation)

            original_create = _replace_operation_handler(
                monkeypatch,
                rt._resources.dispatcher,
                operation_name="create_world",
                operation_type=CreateWorld,
                handler=counting_create,
            )

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
        processor = FailPosWith(RuntimeError("unused"))
        world = runtime.world("activation-retry", storage=storage, processors=[processor])
        calls = 0

        async def fail_once(operation):
            nonlocal calls
            assert type(operation) is AddProcessor
            calls += 1
            if calls == 1:
                raise RuntimeError("injected wiring failure")
            return await original_add(operation)

        original_add = _replace_operation_handler(
            monkeypatch,
            runtime._resources.dispatcher,
            operation_name="add_processor",
            operation_type=AddProcessor,
            handler=fail_once,
        )
        try:
            with pytest.raises(RuntimeError, match="injected wiring failure"):
                await world.spawn(Pos())

            assert not world._state.initialized
            assert await runtime._resources.dispatcher.apply(ListWorlds()) == []

            # Retry must perform one clean activation rather than collide with
            # the rolled-back name registration.
            entity_id = await world.spawn(Pos())
            assert isinstance(entity_id, int)
            assert world._state.initialized
            assert calls == 2
        finally:
            await runtime.shutdown()


# ── 2. Actor binding ────────────────────────────────────────────────────


class TestActorBinding:
    @pytest.mark.asyncio
    async def test_runtime_never_uses_actor_aware_dispatch(self, tmp_path, monkeypatch):
        async with ArchetypeRuntime() as runtime:
            world = runtime.world(
                "trusted-runtime",
                storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
            )

            async def forbidden_actor_aware_call(*args, **kwargs):
                raise AssertionError("trusted runtime must not call dispatcher.apply_as")

            monkeypatch.setattr(
                runtime._resources.dispatcher,
                "apply_as",
                forbidden_actor_aware_call,
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
            rows = await runtime._resources.dispatcher.apply(
                GetAuditHistory(world_id=world.world_id)
            )
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
        attempts = 0

        async def fail_once(operation):
            nonlocal attempts
            assert type(operation) is DestroyWorld
            attempts += 1
            if attempts == 1:
                raise RuntimeError("destroy failed")
            await original_destroy(operation)

        original_destroy = _replace_operation_handler(
            monkeypatch,
            runtime._resources.dispatcher,
            operation_name="destroy_world",
            operation_type=DestroyWorld,
            handler=fail_once,
        )
        reservation = world._reservation
        assert reservation is not None

        with pytest.raises(RuntimeError, match="destroy failed"):
            await world.destroy()

        assert not world._state.closed
        assert not reservation.released
        assert isinstance(await world.spawn(Pos()), int)

        await world.destroy()

        assert attempts == 2
        assert world._state.closed
        assert reservation.released
        with pytest.raises(RuntimeError, match="closed"):
            await world.spawn(Pos())
        await runtime.shutdown()

    @pytest.mark.asyncio
    async def test_admission_cancellation_group_uses_retryable_runtime_error_contract(
        self, monkeypatch
    ):
        events: list[str] = []
        cancellation_group = BaseExceptionGroup(
            "admission shutdown failed",
            [asyncio.CancelledError("sandbox close cancelled")],
        )
        dispatcher = _DrainDispatcher(events, stop_failures=[cancellation_group])
        resources = RuntimeResources(dispatcher=dispatcher)
        runtime = _runtime_with_resources(monkeypatch, resources)

        with pytest.raises(RuntimeShutdownError) as captured:
            await runtime.shutdown()

        assert captured.value.phase == "admission"
        assert len(captured.value.failures) == 1
        assert captured.value.failures[0].owner == "dispatcher"
        assert captured.value.failures[0].cause is cancellation_group
        assert isinstance(cancellation_group.exceptions[0], asyncio.CancelledError)

        with pytest.raises(RuntimeError, match="closed"):
            runtime.world("rejected-after-admission-failure")

        await runtime.shutdown()

        assert dispatcher.stop_calls == 2
        assert dispatcher.wait_calls == 1
        assert resources.close_state is RuntimeCloseState.CLOSED

    @pytest.mark.asyncio
    async def test_runtime_shutdown_retries_workflow_owners_before_dependencies(self, monkeypatch):
        events: list[str] = []
        dispatcher = _DrainDispatcher(events)
        audit = _Dependency("audit", events)
        resources = RuntimeResources(dispatcher=dispatcher, audit=audit)
        runtime = _runtime_with_resources(monkeypatch, resources)
        close_failure = RuntimeError("mission close failed")
        mission = _OwnedHandle(
            "mission:retry",
            events,
            failures=[close_failure],
        )
        reservation = await _reserve_handle(
            resources,
            mission,
            owner="mission:retry",
            phase="workflow-handles",
        )

        with pytest.raises(RuntimeShutdownError) as captured:
            await runtime.shutdown()

        assert captured.value.phase == "workflow-handles"
        assert captured.value.failures[0].owner == "mission:retry"
        assert captured.value.failures[0].cause is close_failure
        assert events == [
            "admission:stop",
            "admission:drain",
            "close:mission:retry:1",
        ]
        assert audit.close_calls == 0
        assert not reservation.released

        mission_ref = weakref.ref(mission)
        del mission
        gc.collect()
        assert mission_ref() is not None
        with pytest.raises(RuntimeError, match="closed"):
            runtime.world("rejected-after-failed-shutdown")

        await runtime.shutdown()
        await runtime.shutdown()

        assert events == [
            "admission:stop",
            "admission:drain",
            "close:mission:retry:1",
            "close:mission:retry:2",
            "shutdown:audit:1",
        ]
        assert reservation.released
        assert resources.close_state is RuntimeCloseState.CLOSED
        del captured
        del close_failure
        gc.collect()
        assert mission_ref() is None

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
        admission_stopped = asyncio.Event()

        dispatcher = runtime._resources.dispatcher
        original_stop_admission = dispatcher.stop_admission

        async def stop_admission():
            await original_stop_admission()
            admission_stopped.set()

        monkeypatch.setattr(dispatcher, "stop_admission", stop_admission)

        async def blocking_run(operation):
            assert type(operation) is Run
            run_started.set()
            await run_release.wait()
            return await original_run(operation)

        original_run = _replace_operation_handler(
            monkeypatch,
            dispatcher,
            operation_name="run",
            operation_type=Run,
            handler=blocking_run,
        )
        mission = _OwnedHandle(
            "mission:drain-order",
            [],
            failures=[RuntimeError("mission close failed")],
            started=mission_attempted,
        )
        await _reserve_handle(
            runtime._resources,
            mission,
            owner="mission:drain-order",
            phase="workflow-handles",
        )

        admitted_run = asyncio.create_task(world.run())
        await run_started.wait()
        shutdown = asyncio.create_task(runtime.shutdown())
        await admission_stopped.wait()

        assert not shutdown.done()
        assert not mission_attempted.is_set()

        run_release.set()
        await admitted_run
        with pytest.raises(RuntimeShutdownError) as captured:
            await shutdown

        assert captured.value.phase == "workflow-handles"
        assert "mission close failed" in str(captured.value.failures[0].cause)
        assert mission_attempted.is_set()
        assert not world._state.closed
        with pytest.raises(RuntimeError, match="closed"):
            await world.spawn(Pos())

        await runtime.shutdown()
        assert world._state.closed

    @pytest.mark.asyncio
    async def test_workflow_cleanup_cannot_admit_a_sibling_world(self, monkeypatch):
        events: list[str] = []
        resources = RuntimeResources(dispatcher=_DrainDispatcher(events))
        runtime = _runtime_with_resources(monkeypatch, resources)

        class CleanupOwner:
            async def aclose(self) -> None:
                events.append("cleanup:mission")
                with pytest.raises(RuntimeError, match="closed"):
                    runtime.world("sibling")

        cleanup = CleanupOwner()
        await _reserve_handle(
            resources,
            cleanup,
            owner="mission:exact-cleanup",
            phase="workflow-handles",
        )

        await runtime.shutdown()

        assert events == [
            "admission:stop",
            "admission:drain",
            "cleanup:mission",
        ]

    @pytest.mark.asyncio
    async def test_concurrent_runtime_shutdown_is_single_flight(self, monkeypatch):
        events: list[str] = []
        dispatcher = _DrainDispatcher(events)
        audit = _Dependency("audit", events)
        resources = RuntimeResources(dispatcher=dispatcher, audit=audit)
        runtime = _runtime_with_resources(monkeypatch, resources)
        close_started = asyncio.Event()
        close_release = asyncio.Event()
        second_started = asyncio.Event()
        mission = _OwnedHandle(
            "mission:single-flight",
            events,
            started=close_started,
            release=close_release,
        )
        await _reserve_handle(
            resources,
            mission,
            owner="mission:single-flight",
            phase="workflow-handles",
        )

        async def second_shutdown() -> None:
            second_started.set()
            await runtime.shutdown()

        first = asyncio.create_task(runtime.shutdown())
        await close_started.wait()
        second = asyncio.create_task(second_shutdown())
        await second_started.wait()
        assert not second.done()

        close_release.set()
        await asyncio.gather(first, second)

        assert events == [
            "admission:stop",
            "admission:drain",
            "close:mission:single-flight:1",
            "shutdown:audit:1",
        ]
        assert mission.close_calls == 1
        assert audit.close_calls == 1

    @pytest.mark.asyncio
    async def test_cancelled_runtime_shutdown_retains_cleanup_for_retry(self, monkeypatch):
        events: list[str] = []
        resources = RuntimeResources(dispatcher=_DrainDispatcher(events))
        runtime = _runtime_with_resources(monkeypatch, resources)
        close_started = asyncio.Event()
        close_release = asyncio.Event()
        mission = _OwnedHandle(
            "mission:shielded",
            events,
            started=close_started,
            release=close_release,
        )
        reservation = await _reserve_handle(
            resources,
            mission,
            owner="mission:shielded",
            phase="workflow-handles",
        )
        interrupted = asyncio.create_task(runtime.shutdown())
        await close_started.wait()
        interrupted.cancel()
        with pytest.raises(RuntimeShutdownError) as captured:
            await interrupted

        assert captured.value.phase == "workflow-handles"
        assert isinstance(captured.value.failures[0].cause, asyncio.CancelledError)
        assert mission.close_calls == 1
        assert not reservation.released
        with pytest.raises(RuntimeError, match="closed"):
            runtime.world("rejected-after-cancelled-shutdown")

        retry = asyncio.create_task(runtime.shutdown())
        close_release.set()
        await retry

        assert mission.close_calls == 1
        assert reservation.released
        assert resources.close_state is RuntimeCloseState.CLOSED

    @pytest.mark.asyncio
    async def test_world_shutdown_cancellation_does_not_skip_sibling_cleanup(self, monkeypatch):
        events: list[str] = []
        audit = _Dependency("audit", events)
        resources = RuntimeResources(
            dispatcher=_DrainDispatcher(events),
            audit=audit,
        )
        runtime = _runtime_with_resources(monkeypatch, resources)
        cancellation = asyncio.CancelledError("world close cancelled")
        first = _OwnedHandle(
            "world:cancelled",
            events,
            failures=[cancellation],
        )
        second = _OwnedHandle("world:sibling", events)
        first_reservation = await _reserve_handle(
            resources,
            first,
            owner="world:cancelled",
            phase="world-handles",
        )
        second_reservation = await _reserve_handle(
            resources,
            second,
            owner="world:sibling",
            phase="world-handles",
        )

        with pytest.raises(RuntimeShutdownError) as captured:
            await runtime.shutdown()

        assert captured.value.phase == "world-handles"
        assert captured.value.failures[0].cause is cancellation
        assert events == [
            "admission:stop",
            "admission:drain",
            "close:world:cancelled:1",
            "close:world:sibling:1",
        ]
        assert not first_reservation.released
        assert second_reservation.released
        assert audit.close_calls == 0

        await runtime.shutdown()

        assert events == [
            "admission:stop",
            "admission:drain",
            "close:world:cancelled:1",
            "close:world:sibling:1",
            "close:world:cancelled:2",
            "shutdown:audit:1",
        ]
        assert first_reservation.released
        assert second.close_calls == 1


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
        """The fork handle has one strong process-owner reservation."""
        async with ArchetypeRuntime() as rt:
            storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
            world = rt.world("fork-reg", storage=storage)
            await world.spawn(Pos())

            fork = await world.fork(
                "branch",
                storage=StorageConfig(uri=str(tmp_path / "fork_store"), namespace="ns"),
            )

            reservation = fork._reservation
            assert reservation is not None
            owner = reservation.owner
            fork_state = fork._state
            fork_ref = weakref.ref(fork)
            assert rt._resources.owner(owner) is reservation
            assert not reservation.released
            del fork
            gc.collect()
            assert fork_ref() is not None

        # After exiting the context manager (shutdown), the fork state is closed
        assert fork_state.closed
        assert reservation.released
        with pytest.raises(KeyError, match=owner):
            rt._resources.owner(owner)
        gc.collect()
        assert fork_ref() is None


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
