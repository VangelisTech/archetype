# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused ownership contracts for the inert RuntimeMissions facade."""

from __future__ import annotations

import asyncio
import gc
import weakref
from typing import Any

import pytest

from archetype.core.config import StorageConfig
from archetype.missions.contracts import (
    AgentMissionConfig,
    AgentTask,
    CommandValidator,
    SubmittedMission,
)
from archetype.missions.models import RestoreMissionSandbox, RunMission, SubmitMission
from archetype.missions.sandboxes import CheckpointRef
from archetype.runtime.runtime import ArchetypeRuntime
from archetype.runtime_resources import OperationAdmission


class _Backend:
    name = "owner-contract"

    async def create(self, spec: Any) -> Any:
        del spec
        raise NotImplementedError

    async def restore(self, spec: Any, checkpoint: Any) -> Any:
        del spec, checkpoint
        raise NotImplementedError


class _Dispatcher:
    def __init__(self) -> None:
        self.effects = 0

    async def apply(self, operation: object) -> object:
        del operation
        self.effects += 1
        return object()


class _Service:
    world_id = "world-1"

    async def query(self, *components: object) -> object:
        del components
        return object()


class _Reservation:
    def __init__(self, *, fail_once: bool) -> None:
        self._fail_once = fail_once
        self._released = False
        self.close_calls = 0
        self.anchors: list[object] = []
        self.bound: object = _Service()
        self.operation_admission = OperationAdmission(
            closed_message="Agent Missions handle is closed"
        )

    @property
    def released(self) -> bool:
        return self._released

    def retain_anchor[T](self, anchor: T) -> T:
        self.anchors.append(anchor)
        return anchor

    def ensure_close_allowed(self) -> None:
        if self.operation_admitted():
            raise RuntimeError("runtime owner cannot close from its admitted operation")
        return None

    def admit_operation(self):
        return self.operation_admission.admit()

    def operation_admitted(self) -> bool:
        return self.operation_admission.admitted_by_current_task()

    def request_operation_stop(self) -> None:
        self.operation_admission.request_stop()

    def require_bound(self) -> object:
        return self.bound

    async def aclose(self) -> None:
        self.close_calls += 1
        self.request_operation_stop()
        await self.operation_admission.stop_admission()
        await self.operation_admission.wait_drained()
        if self._fail_once:
            self._fail_once = False
            raise RuntimeError("cleanup retry required")
        self.release()

    def release(self) -> None:
        self._released = True
        self.anchors.clear()


class _Resources:
    def __init__(self, reservation: _Reservation) -> None:
        self.dispatcher = _Dispatcher()
        self.reservation = reservation
        self.reservations: list[tuple[str, str]] = []
        self.operation_admission = OperationAdmission(closed_message="runtime is closed")

    def reserve_owner(
        self,
        owner: str,
        *,
        phase: str,
        closed_message: str | None = None,
    ) -> _Reservation:
        del closed_message
        self.reservations.append((owner, phase))
        return self.reservation

    def admit_operation(self):
        return self.operation_admission.admit()

    def admit_owner_operation(self, reservation: _Reservation):
        assert reservation is self.reservation
        return reservation.admit_operation()

    def operation_admitted(self) -> bool:
        return self.operation_admission.admitted_by_current_task()

    def owner(self, owner: str) -> _Reservation:
        assert owner.startswith("mission:")
        return self.reservation


def _runtime(reservation: _Reservation) -> ArchetypeRuntime:
    runtime = object.__new__(ArchetypeRuntime)
    runtime._resources = _Resources(reservation)  # type: ignore[attr-defined]
    runtime._shutdown_started = False  # type: ignore[attr-defined]
    runtime._closed = False  # type: ignore[attr-defined]
    return runtime


def _config() -> AgentMissionConfig:
    return AgentMissionConfig(
        sandbox_backend=_Backend(),
        sandbox_environment="owner-contract",
    )


def _mission() -> SubmittedMission:
    return SubmittedMission(
        mission_id=1,
        task_ids=(),
        repository="repo",
        branch="branch",
    )


async def _invoke(handle: Any, operation: str) -> object:
    if operation == "submit":
        return await handle.submit(
            repository="repo",
            branch="branch",
            tasks=(
                AgentTask(
                    name="task",
                    prompt="Implement the task.",
                    validators=(CommandValidator(name="test", command=("pytest", "-q")),),
                ),
            ),
        )
    if operation == "run":
        return await handle.run(_mission())
    if operation == "restore":
        return await handle.restore_sandbox(
            _mission(),
            CheckpointRef(
                provider="provider",
                checkpoint_id="checkpoint",
                uri="provider://checkpoint",
                created_at_ms=1,
            ),
        )
    if operation == "query":
        return await handle.query()
    raise AssertionError(f"unknown test operation {operation!r}")


@pytest.mark.asyncio
async def test_mission_facade_anchor_survives_failed_close_until_retry_release() -> None:
    reservation = _Reservation(fail_once=True)
    runtime = _runtime(reservation)
    handle = runtime.missions("anchored", config=_config())

    assert reservation.anchors == [handle]
    assert runtime._resources.reservations[0][1] == "workflow-handles"  # type: ignore[attr-defined]
    handle_ref = weakref.ref(handle)
    del handle
    gc.collect()
    assert handle_ref() is not None

    owned = handle_ref()
    assert owned is not None
    try:
        await owned.close()
    except RuntimeError as error:
        assert str(error) == "cleanup retry required"
        error.__traceback__ = None
    else:
        pytest.fail("the first cleanup attempt must fail")
    effects_before_late_work = runtime._resources.dispatcher.effects  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError, match="closed"):
        await owned.run(
            SubmittedMission(
                mission_id=1,
                task_ids=(),
                repository="repo",
                branch="branch",
            )
        )
    assert runtime._resources.dispatcher.effects == effects_before_late_work  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError, match="closed"):
        _ = owned.world_id
    del owned
    gc.collect()

    assert handle_ref() is not None
    assert not reservation.released

    owned = handle_ref()
    assert owned is not None
    await owned.close()
    del owned
    gc.collect()

    assert reservation.close_calls == 2
    assert reservation.released
    assert handle_ref() is None


@pytest.mark.asyncio
async def test_runtime_owned_release_closes_facade_before_dispatch_effect() -> None:
    reservation = _Reservation(fail_once=False)
    runtime = _runtime(reservation)
    handle = runtime.missions("released", config=_config())
    resources = runtime._resources  # type: ignore[attr-defined]

    reservation.release()

    with pytest.raises(RuntimeError, match="closed"):
        await handle.submit(
            repository="unused",
            branch="unused",
            tasks=(),
        )
    assert resources.dispatcher.effects == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["submit", "run", "restore", "query"])
async def test_mission_close_drains_each_admitted_public_operation(operation: str) -> None:
    operation_started = asyncio.Event()
    operation_release = asyncio.Event()

    class BlockingDispatcher(_Dispatcher):
        async def apply(self, operation: object) -> object:
            self.effects += 1
            expected_type = {
                "submit": SubmitMission,
                "run": RunMission,
                "restore": RestoreMissionSandbox,
            }.get(operation_name)
            if expected_type is not None and type(operation) is expected_type:
                operation_started.set()
                await operation_release.wait()
            return object()

    reservation = _Reservation(fail_once=False)
    runtime = _runtime(reservation)
    resources = runtime._resources  # type: ignore[attr-defined]
    resources.dispatcher = BlockingDispatcher()
    operation_name = operation

    class QueryService(_Service):
        async def query(self, *components: object) -> object:
            del components
            operation_started.set()
            await operation_release.wait()
            async with resources.admit_operation():
                return object()

    if operation == "query":
        reservation.bound = QueryService()
    handle = runtime.missions("drained", config=_config())

    admitted = asyncio.create_task(_invoke(handle, operation))
    await asyncio.wait_for(operation_started.wait(), timeout=5)
    close = asyncio.create_task(handle.close())
    for _ in range(100):
        if handle._public_closing:
            break
        await asyncio.sleep(0)
    assert handle._public_closing
    released_before_operation_exited = reservation.released

    with pytest.raises(RuntimeError, match="closed"):
        await _invoke(handle, operation)
    with pytest.raises(RuntimeError, match="closed"):
        _ = handle.world_id

    operation_release.set()
    await asyncio.wait_for(admitted, timeout=5)
    await asyncio.wait_for(close, timeout=5)

    assert not released_before_operation_exited
    assert reservation.released


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["submit", "run", "restore", "query"])
async def test_process_admission_drains_each_mission_operation(operation: str) -> None:
    operation_started = asyncio.Event()
    operation_release = asyncio.Event()

    class BlockingDispatcher(_Dispatcher):
        async def apply(self, dispatched: object) -> object:
            self.effects += 1
            expected_type = {
                "submit": SubmitMission,
                "run": RunMission,
                "restore": RestoreMissionSandbox,
            }.get(operation)
            if expected_type is not None and type(dispatched) is expected_type:
                operation_started.set()
                await operation_release.wait()
            return object()

    reservation = _Reservation(fail_once=False)
    runtime = _runtime(reservation)
    resources = runtime._resources  # type: ignore[attr-defined]
    resources.dispatcher = BlockingDispatcher()

    class QueryService(_Service):
        async def query(self, *components: object) -> object:
            del components
            operation_started.set()
            await operation_release.wait()
            async with resources.admit_operation():
                return object()

    if operation == "query":
        reservation.bound = QueryService()
    handle = runtime.missions("process-drained", config=_config())

    admitted = asyncio.create_task(_invoke(handle, operation))
    await asyncio.wait_for(operation_started.wait(), timeout=5)
    runtime._shutdown_started = True
    await resources.operation_admission.stop_admission()
    drain = asyncio.create_task(resources.operation_admission.wait_drained())
    await asyncio.sleep(0)
    assert not drain.done()

    with pytest.raises(RuntimeError, match="closed"):
        await _invoke(handle, operation)
    with pytest.raises(RuntimeError, match="closed"):
        _ = handle.world_id

    operation_release.set()
    await asyncio.wait_for(admitted, timeout=5)
    await asyncio.wait_for(drain, timeout=5)
    await handle.close()

    assert reservation.released


@pytest.mark.asyncio
async def test_mission_close_allows_same_task_nested_public_continuation() -> None:
    outer_reached_nested_call = asyncio.Event()
    nested_call_release = asyncio.Event()
    reservation = _Reservation(fail_once=False)
    runtime = _runtime(reservation)
    handle = runtime.missions("nested-continuation", config=_config())

    async def admitted_compound_operation() -> object:
        async with runtime._resources.admit_operation():  # type: ignore[attr-defined]
            async with handle._operation_admission.admit():
                outer_reached_nested_call.set()
                await nested_call_release.wait()
                return await handle.run(_mission())

    admitted = asyncio.create_task(admitted_compound_operation())
    await asyncio.wait_for(outer_reached_nested_call.wait(), timeout=5)
    close = asyncio.create_task(handle.close())
    for _ in range(100):
        if handle._public_closing:
            break
        await asyncio.sleep(0)
    assert handle._public_closing

    nested_call_release.set()
    await asyncio.wait_for(admitted, timeout=5)
    await asyncio.wait_for(close, timeout=5)

    assert reservation.released


@pytest.mark.asyncio
async def test_mission_close_from_current_admitted_operation_rejects_without_deadlock() -> None:
    reservation = _Reservation(fail_once=False)
    runtime = _runtime(reservation)
    handle = runtime.missions("self-close-rejection", config=_config())

    async with handle._operation_admission.admit():
        with pytest.raises(RuntimeError, match="cannot close from an admitted operation"):
            await asyncio.wait_for(handle.close(), timeout=1)

    assert not handle._public_closing
    assert not reservation.released
    await handle.close()
    assert reservation.released


@pytest.mark.asyncio
async def test_mission_supervised_task_cannot_partially_close_its_owner(tmp_path) -> None:
    runtime = ArchetypeRuntime()
    handle = runtime.missions(
        "supervised-mission-self-close",
        config=_config(),
        storage=StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="supervised_mission_self_close",
        ),
    )
    reservation = handle._reservation

    async def supervised_self_close() -> None:
        with pytest.raises(
            RuntimeError,
            match="cannot close from its supervised task",
        ):
            await asyncio.wait_for(handle.close(), timeout=1)
        assert not handle._public_closing
        assert not handle._public_closed
        assert not reservation.released
        with pytest.raises(ValueError, match="requires the Modal sandbox backend"):
            await _invoke(handle, "submit")
        assert not reservation.released

    task = reservation.spawn(supervised_self_close, label="self-close")
    await asyncio.wait_for(task, timeout=5)

    assert not handle._public_closing
    await handle.close()
    assert reservation.released
    await runtime.shutdown()
