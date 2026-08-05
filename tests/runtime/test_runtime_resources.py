# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""RED contracts for the process-lifetime resource owner.

The PR-4 modules are imported inside each test.  Until the interface seed
lands, one missing module therefore fails one selected node without collapsing
collection of the other contracts.
"""

from __future__ import annotations

import ast
import asyncio
import gc
import inspect
import weakref
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("runtime.lifecycle.retryable_teardown"),
]


class _Dispatcher:
    """Deterministic admission/drain double for the landed PR-3 seam."""

    def __init__(self, events: list[str], *, drained: bool = True) -> None:
        self.events = events
        self.stop_calls = 0
        self.wait_calls = 0
        self.stopped = asyncio.Event()
        self.drain_release = asyncio.Event()
        if drained:
            self.drain_release.set()

    @asynccontextmanager
    async def _admit_runtime_operation(
        self,
        continuation: Callable[[], bool],
    ) -> AsyncIterator[None]:
        del continuation
        yield

    def request_stop(self) -> None:
        pass

    async def stop_admission(self) -> None:
        self.stop_calls += 1
        self.events.append("admission:stop")
        self.stopped.set()

    async def wait_drained(self) -> None:
        self.wait_calls += 1
        self.events.append("admission:drain")
        await self.drain_release.wait()


class _Closeable:
    def __init__(
        self,
        owner: str,
        events: list[str],
        *,
        failures: list[BaseException] | None = None,
        started: asyncio.Event | None = None,
        release: asyncio.Event | None = None,
    ) -> None:
        self.owner = owner
        self.events = events
        self.failures = list(failures or [])
        self.started = started
        self.release = release
        self.close_calls = 0
        self.cancelled = False

    async def aclose(self) -> None:
        self.close_calls += 1
        self.events.append(f"close:{self.owner}:{self.close_calls}")
        if self.started is not None:
            self.started.set()
        if self.release is not None:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        if self.failures:
            raise self.failures.pop(0)


class _Dependency:
    def __init__(self, owner: str, events: list[str]) -> None:
        self.owner = owner
        self.events = events
        self.shutdown_calls = 0

    async def shutdown(self) -> None:
        self.shutdown_calls += 1
        self.events.append(f"shutdown:{self.owner}:{self.shutdown_calls}")


class _Anchor:
    pass


def _runtime_api() -> tuple[Any, Any, Any]:
    """Load only the selected node's absent PR-4 seams."""

    runtime_module = import_module("archetype.runtime_resources")
    errors_module = import_module("archetype.errors")
    error_exports = vars(errors_module)
    return (
        runtime_module,
        error_exports["RuntimeShutdownError"],
        error_exports["RuntimeShutdownFailure"],
    )


def _new_resources(
    runtime_module: Any,
    dispatcher: _Dispatcher,
    *,
    audit: _Dependency | None = None,
    storage: _Dependency | None = None,
    owns_storage: bool = False,
) -> Any:
    """Name the minimal interface seed without constructing app services."""

    return runtime_module.RuntimeResources(
        dispatcher=dispatcher,
        audit=audit,
        storage=storage,
        owns_storage=owns_storage,
    )


async def _construct(reservation: Any, resource: _Closeable) -> _Closeable:
    async def factory() -> _Closeable:
        return resource

    constructed = await reservation.construct(factory)
    assert constructed is resource
    return constructed


async def _wait(event: asyncio.Event) -> None:
    """Bound a broken synchronization implementation without sleeping."""

    await asyncio.wait_for(event.wait(), timeout=0.5)


async def test_operation_admission_rejects_self_drain_without_deadlock() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    admission = runtime_module.OperationAdmission(closed_message="closed")

    async with admission.admit():
        await admission.stop_admission()
        with pytest.raises(RuntimeError, match="cannot drain its current task"):
            await asyncio.wait_for(admission.wait_drained(), timeout=0.5)

    await admission.wait_drained()


@pytest.mark.parametrize("first", ["admit", "stop"])
async def test_operation_admission_contention_is_counted_or_rejected(first: str) -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    admission = runtime_module.OperationAdmission(closed_message="closed")
    entered = asyncio.Event()
    release = asyncio.Event()
    outcome: list[str] = []

    async def operation() -> None:
        try:
            async with admission.admit():
                outcome.append("accepted")
                entered.set()
                await release.wait()
        except RuntimeError as exc:
            assert str(exc) == "closed"
            outcome.append("rejected")

    if first == "admit":
        operation_task = asyncio.create_task(operation())
        await _wait(entered)
        await admission._lock.acquire()
        stop_task = asyncio.create_task(admission.stop_admission())
    else:
        await admission._lock.acquire()
        stop_task = asyncio.create_task(admission.stop_admission())
        await asyncio.sleep(0)
        operation_task = asyncio.create_task(operation())
    await asyncio.sleep(0)
    admission._lock.release()
    await asyncio.wait_for(stop_task, timeout=0.5)
    drain = asyncio.create_task(admission.wait_drained())

    if first == "admit":
        await asyncio.sleep(0)
        assert not drain.done()
        release.set()
        await asyncio.wait_for(operation_task, timeout=0.5)
        await asyncio.wait_for(drain, timeout=0.5)
        assert outcome == ["accepted"]
    else:
        await asyncio.wait_for(operation_task, timeout=0.5)
        await asyncio.wait_for(drain, timeout=0.5)
        assert outcome == ["rejected"]


async def test_operation_admission_repeated_cancellation_cannot_strand_depth() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    admission = runtime_module.OperationAdmission(closed_message="closed")
    entered = asyncio.Event()

    async def operation() -> None:
        async with admission.admit():
            entered.set()
            await asyncio.Event().wait()

    admitted = asyncio.create_task(operation())
    await _wait(entered)
    await admission._lock.acquire()
    admitted.cancel()
    await asyncio.sleep(0)
    admitted.cancel()

    await asyncio.sleep(0)
    assert not admitted.done()
    assert admission._depths.get(admitted) == 1
    drain = asyncio.create_task(admission.wait_drained())
    await asyncio.sleep(0)
    assert not drain.done()

    admission._lock.release()
    with pytest.raises(asyncio.CancelledError):
        await admitted
    await asyncio.wait_for(drain, timeout=0.5)

    assert admitted not in admission._depths


async def test_reservation_precedes_factory_and_eager_task_execution() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    dispatcher = _Dispatcher(events)
    resources = _new_resources(runtime_module, dispatcher)
    reservation = resources.reserve_owner(
        "mission:alpha",
        phase="workflow-handles",
    )

    def assert_already_reserved(origin: str) -> None:
        with pytest.raises(RuntimeError, match="mission:alpha"):
            resources.reserve_owner(
                "mission:alpha",
                phase="workflow-handles",
            )
        events.append(origin)

    handle = _Closeable("mission:alpha", events)

    async def factory() -> _Closeable:
        assert_already_reserved("factory:owned")
        return handle

    assert await reservation.construct(factory) is handle

    async def eager_coro() -> None:
        assert_already_reserved("task:owned")

    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    try:
        loop.set_task_factory(asyncio.eager_task_factory)
        task = reservation.spawn(eager_coro, label="critic-prewarm")
        await task
    finally:
        loop.set_task_factory(previous_factory)

    assert events[:2] == ["factory:owned", "task:owned"]
    await resources.aclose()
    assert handle.close_calls == 1


async def test_eager_supervised_task_cannot_close_its_own_owner() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner(
        "mission:eager-self-close",
        phase="workflow-handles",
    )
    handle = await _construct(
        reservation,
        _Closeable("mission:eager-self-close", events),
    )

    async def eager_self_close() -> None:
        with pytest.raises(RuntimeError, match="cannot close from its supervised task"):
            await reservation.aclose()

    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    try:
        loop.set_task_factory(asyncio.eager_task_factory)
        task = reservation.spawn(eager_self_close, label="eager-self-close")
        await task
    finally:
        loop.set_task_factory(previous_factory)

    assert handle.close_calls == 0
    assert not reservation.released

    await reservation.aclose()

    assert handle.close_calls == 1
    assert reservation.released


async def test_construct_return_cancellation_retains_and_closes_active_resource() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner(
        "mission:construct-return-cancel",
        phase="workflow-handles",
    )
    factory_started = asyncio.Event()
    returned: asyncio.Future[_Closeable] = asyncio.get_running_loop().create_future()
    handle = _Closeable("mission:construct-return-cancel", events)

    async def factory() -> _Closeable:
        factory_started.set()
        return await returned

    constructing = asyncio.create_task(reservation.construct(factory))
    await _wait(factory_started)
    returned.set_result(handle)
    constructing.cancel("cancelled after active resource return")

    with pytest.raises(asyncio.CancelledError):
        await constructing

    assert reservation.require_bound() is handle
    assert handle.close_calls == 0

    await resources.aclose()

    assert handle.close_calls == 1
    assert reservation.released


async def test_cancelled_blocked_factory_is_joined_before_owner_release() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner(
        "mission:construct-blocked-cancel",
        phase="workflow-handles",
    )
    factory_started = asyncio.Event()
    factory_cancelled = asyncio.Event()

    async def factory() -> _Closeable:
        factory_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            factory_cancelled.set()
            raise
        raise AssertionError("unreachable")

    constructing = asyncio.create_task(reservation.construct(factory))
    await _wait(factory_started)
    constructing.cancel("cancel blocked construction")
    closing = asyncio.create_task(resources.aclose())

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(constructing, timeout=0.5)
    await _wait(factory_cancelled)
    await asyncio.wait_for(closing, timeout=0.5)

    assert reservation.released


async def test_close_allows_exact_constructor_to_bind_queued_partial_after_seal() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner(
        "mission:construct-sealed-continuation",
        phase="workflow-handles",
    )
    factory_started = asyncio.Event()
    provider_ready: asyncio.Future[_Closeable] = asyncio.get_running_loop().create_future()
    provider = _Closeable("mission:construct-sealed-continuation", events)

    async def factory() -> _Closeable:
        factory_started.set()
        partial = await provider_ready
        reservation.bind(partial, close=partial.aclose)
        return partial

    constructing = asyncio.create_task(reservation.construct(factory))
    await _wait(factory_started)
    loop = asyncio.get_running_loop()
    loop.call_soon(provider_ready.set_result, provider)
    closing = asyncio.create_task(resources.aclose())

    assert await asyncio.wait_for(constructing, timeout=0.5) is provider
    await asyncio.wait_for(closing, timeout=0.5)

    assert provider.close_calls == 1
    assert reservation.released


async def test_eager_constructor_cannot_close_its_owner_or_process() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner(
        "mission:eager-construction-self-close",
        phase="workflow-handles",
    )
    handle = _Closeable("mission:eager-construction-self-close", events)

    async def factory() -> _Closeable:
        with pytest.raises(RuntimeError, match="construction task"):
            await reservation.aclose()
        with pytest.raises(RuntimeError, match="construction task"):
            await resources.aclose()
        assert not reservation._sealed
        assert resources.close_state.value == "OPEN"
        return handle

    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    try:
        loop.set_task_factory(asyncio.eager_task_factory)
        assert await reservation.construct(factory) is handle
    finally:
        loop.set_task_factory(previous_factory)

    assert handle.close_calls == 0
    assert not reservation.released
    await resources.aclose()
    assert handle.close_calls == 1
    assert reservation.released


@pytest.mark.parametrize("target", ["resource", "audit", "storage"])
async def test_eager_owned_cleanup_cannot_reenter_process_close(target: str) -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources: Any

    class ReentrantCleanup:
        def __init__(self) -> None:
            self.close_calls = 0

        async def aclose(self) -> None:
            self.close_calls += 1
            with pytest.raises(RuntimeError, match="cleanup task"):
                await resources.aclose()

    cleanup = ReentrantCleanup()
    if target == "audit":
        resources = _new_resources(
            runtime_module,
            _Dispatcher(events),
            audit=cleanup,
        )
    elif target == "storage":
        resources = _new_resources(
            runtime_module,
            _Dispatcher(events),
            storage=cleanup,
            owns_storage=True,
        )
    else:
        resources = _new_resources(runtime_module, _Dispatcher(events))
        reservation = resources.reserve_owner(
            "mission:cleanup-reentry",
            phase="workflow-handles",
        )
        reservation.bind(cleanup, close=cleanup.aclose)

    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    try:
        loop.set_task_factory(asyncio.eager_task_factory)
        await asyncio.wait_for(resources.aclose(), timeout=0.5)
    finally:
        loop.set_task_factory(previous_factory)

    assert cleanup.close_calls == 1
    assert resources.close_state.value == "CLOSED"


async def test_exact_owner_lookup_resolves_only_a_completely_bound_resource() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner(
        "mission:lookup",
        phase="workflow-handles",
    )

    assert resources.owner("mission:lookup") is reservation
    with pytest.raises(RuntimeError, match="not bound"):
        reservation.require_bound()
    with pytest.raises(KeyError, match="mission:missing"):
        resources.owner("mission:missing")

    handle = _Closeable("mission:lookup", events)
    await _construct(reservation, handle)

    assert resources.owner("mission:lookup").require_bound() is handle
    await resources.aclose()
    with pytest.raises(KeyError, match="mission:lookup"):
        resources.owner("mission:lookup")


async def test_unsafe_owner_and_task_labels_reject_before_user_code() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))

    with pytest.raises(ValueError, match="bounded safe identifier") as owner_error:
        resources.reserve_owner(
            "mission:\nRAW_PROVIDER_EVIDENCE",
            phase="workflow-handles",
        )
    assert "RAW_PROVIDER_EVIDENCE" not in str(owner_error.value)

    reservation = resources.reserve_owner(
        "mission:safe",
        phase="workflow-handles",
    )
    factory_called = False

    async def must_not_run() -> None:
        nonlocal factory_called
        factory_called = True

    with pytest.raises(ValueError, match="bounded safe identifier") as label_error:
        reservation.spawn(
            must_not_run,
            label="critic prewarm RAW_PROVIDER_EVIDENCE",
        )
    assert "RAW_PROVIDER_EVIDENCE" not in str(label_error.value)
    assert not factory_called

    await _construct(reservation, _Closeable("mission:safe", events))
    await resources.aclose()


async def test_direct_owner_close_seals_and_joins_supervised_work() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner(
        "mission:direct-close",
        phase="workflow-handles",
    )
    handle = await _construct(
        reservation,
        _Closeable("mission:direct-close", events),
    )
    task_started = asyncio.Event()
    task_cancelled = asyncio.Event()
    task_release = asyncio.Event()

    async def supervised() -> None:
        task_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append("task:cancelled")
            task_cancelled.set()
            await task_release.wait()

    task = reservation.spawn(supervised, label="critic-prewarm")
    await _wait(task_started)
    closing = asyncio.create_task(reservation.aclose())
    await _wait(task_cancelled)

    assert handle.close_calls == 0
    task_release.set()
    await asyncio.wait_for(closing, timeout=0.5)

    assert task.done() and not task.cancelled()
    assert events[-2:] == ["task:cancelled", "close:mission:direct-close:1"]
    with pytest.raises(KeyError, match="mission:direct-close"):
        resources.owner("mission:direct-close")
    await reservation.aclose()
    with pytest.raises(KeyError, match="mission:direct-close"):
        resources.owner("mission:direct-close")
    with pytest.raises(RuntimeError, match="sealed"):
        reservation.spawn(supervised, label="critic-prewarm")

    await resources.aclose()
    assert handle.close_calls == 1


async def test_supervised_phase_cancels_every_owner_before_waiting_for_any_peer() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    first = resources.reserve_owner("mission:first", phase="workflow-handles")
    second = resources.reserve_owner("mission:second", phase="workflow-handles")
    await _construct(first, _Closeable("mission:first", events))
    await _construct(second, _Closeable("mission:second", events))
    tasks_started = [asyncio.Event(), asyncio.Event()]
    tasks_cancelled = [asyncio.Event(), asyncio.Event()]
    resistant_release = asyncio.Event()

    async def first_task() -> None:
        tasks_started[0].set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append("cancel:mission:first")
            tasks_cancelled[0].set()
            await resistant_release.wait()

    async def second_task() -> None:
        tasks_started[1].set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append("cancel:mission:second")
            tasks_cancelled[1].set()
            raise

    first.spawn(first_task, label="resistant-prewarm")
    second.spawn(second_task, label="peer-prewarm")
    await asyncio.gather(*(_wait(started) for started in tasks_started))
    closing = asyncio.create_task(resources.aclose())
    await asyncio.gather(*(_wait(cancelled) for cancelled in tasks_cancelled))

    assert not closing.done()
    assert events[-2:] == ["cancel:mission:first", "cancel:mission:second"]
    resistant_release.set()
    await asyncio.wait_for(closing, timeout=0.5)

    assert events[-2:] == ["close:mission:first:1", "close:mission:second:1"]


async def test_supervised_task_cannot_close_its_own_process_owner() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner("mission:self-close", phase="workflow-handles")
    handle = await _construct(
        reservation,
        _Closeable("mission:self-close", events),
    )

    async def self_closing_task() -> None:
        with pytest.raises(RuntimeError, match="cannot close from its supervised task"):
            await asyncio.wait_for(resources.aclose(), timeout=0.5)

    task = reservation.spawn(self_closing_task, label="self-close")
    await asyncio.wait_for(task, timeout=0.5)

    assert handle.close_calls == 0
    assert resources.close_state.value == "OPEN"
    await resources.aclose()
    assert handle.close_calls == 1


async def test_cancelled_prewarm_leaves_provider_cleanup_with_sandbox_owner() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner("mission:provider-owner", phase="workflow-handles")
    provider = _Closeable("provider:session", events)

    class SandboxOwner:
        async def aclose(self) -> None:
            events.append("close:sandbox-owner")
            await provider.aclose()

    sandbox_owner = SandboxOwner()
    reservation.bind(sandbox_owner, close=sandbox_owner.aclose)
    task_started = asyncio.Event()
    task_cancelled = asyncio.Event()

    async def prewarm() -> None:
        task_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append("task:prewarm-cancelled")
            task_cancelled.set()
            raise
        events.append("provider:late-acquire")

    reservation.spawn(prewarm, label="critic-prewarm")
    await _wait(task_started)
    closing = asyncio.create_task(resources.aclose())
    await _wait(task_cancelled)
    await asyncio.wait_for(closing, timeout=0.5)

    assert "provider:late-acquire" not in events
    assert events[-3:] == [
        "task:prewarm-cancelled",
        "close:sandbox-owner",
        "close:provider:session:1",
    ]


async def test_partial_constructor_failure_remains_cleanup_owned() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner(
        "mission:partial",
        phase="workflow-handles",
    )
    acquired = _Closeable("provider:partial", events)
    acquired_ref = weakref.ref(acquired)
    factory_scope = [acquired]

    async def failing_factory() -> _Closeable:
        partial = factory_scope[0]
        reservation.bind(partial, close=partial.aclose)
        raise LookupError("constructor stopped after provider acquisition")

    with pytest.raises(LookupError, match="constructor stopped"):
        await reservation.construct(failing_factory)

    factory_scope.clear()
    del acquired
    gc.collect()
    assert acquired_ref() is not None, "the failed reservation must retain partial ownership"

    await resources.aclose()
    assert events[-1] == "close:provider:partial:1"


async def test_close_is_sticky_while_preclose_reservation_may_bind_until_drain() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    dispatcher = _Dispatcher(events, drained=False)
    resources = _new_resources(runtime_module, dispatcher)
    admitted = resources.reserve_owner(
        "mission:admitted",
        phase="workflow-handles",
    )

    close_task = asyncio.create_task(resources.aclose())
    await _wait(dispatcher.stopped)

    with pytest.raises(RuntimeError):
        resources.reserve_owner("mission:late", phase="workflow-handles")

    retained = _Closeable("mission:admitted", events)
    await _construct(admitted, retained)
    assert retained.close_calls == 0

    dispatcher.drain_release.set()
    await asyncio.wait_for(close_task, timeout=0.5)

    assert retained.close_calls == 1
    with pytest.raises(RuntimeError):
        resources.reserve_owner("mission:after", phase="workflow-handles")


async def test_process_close_drains_owner_only_admission_before_resource_cleanup() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    cleanup_started = asyncio.Event()
    reservation = resources.reserve_owner(
        "mission:owner-only-admission",
        phase="workflow-handles",
    )
    handle = await _construct(
        reservation,
        _Closeable(
            "mission:owner-only-admission",
            events,
            started=cleanup_started,
        ),
    )
    operation_entered = asyncio.Event()
    operation_release = asyncio.Event()
    close_task: asyncio.Task[None] | None = None

    async def owner_only_operation() -> None:
        async with reservation.admit_operation():
            operation_entered.set()
            await operation_release.wait()
            assert handle.close_calls == 0

    operation = asyncio.create_task(owner_only_operation())
    try:
        await _wait(operation_entered)
        close_task = asyncio.create_task(resources.aclose())
        for _ in range(100):
            if reservation.operation_admission._stop_requested:
                break
            await asyncio.sleep(0)

        assert reservation.operation_admission._stop_requested
        assert not cleanup_started.is_set()
        assert not close_task.done()
        with pytest.raises(RuntimeError, match="closed"):
            async with reservation.admit_operation():
                pytest.fail("late owner-only work must not be admitted")

        operation_release.set()
        await asyncio.wait_for(operation, timeout=0.5)
        await asyncio.wait_for(close_task, timeout=0.5)
        assert handle.close_calls == 1
        assert reservation.released
    finally:
        operation_release.set()
        await asyncio.gather(operation, return_exceptions=True)
        if close_task is not None:
            await asyncio.gather(close_task, return_exceptions=True)


async def test_process_close_rejects_owner_only_work_but_preserves_exact_continuation() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner(
        "mission:preclose-continuation",
        phase="workflow-handles",
    )
    handle = await _construct(
        reservation,
        _Closeable("mission:preclose-continuation", events),
    )
    process_entered = asyncio.Event()
    allow_owner_crossing = asyncio.Event()
    owner_crossed = asyncio.Event()

    async def compound_operation() -> None:
        async with resources.admit_operation():
            process_entered.set()
            await allow_owner_crossing.wait()
            async with resources.admit_owner_operation(reservation):
                owner_crossed.set()
                assert handle.close_calls == 0

    compound = asyncio.create_task(compound_operation())
    await _wait(process_entered)
    close_task = asyncio.create_task(resources.aclose())
    for _ in range(100):
        if reservation.operation_admission._stop_requested:
            break
        await asyncio.sleep(0)

    assert resources.close_state is runtime_module.RuntimeCloseState.CLOSING_RETRYABLE
    assert reservation.operation_admission._stop_requested
    assert not close_task.done()
    with pytest.raises(RuntimeError, match="closed"):
        async with reservation.admit_operation():
            pytest.fail("fresh owner-only work must reject at process close-start")

    allow_owner_crossing.set()
    await asyncio.wait_for(owner_crossed.wait(), timeout=0.5)
    await asyncio.wait_for(compound, timeout=0.5)
    await asyncio.wait_for(close_task, timeout=0.5)

    assert handle.close_calls == 1
    assert reservation.released


async def test_owner_created_by_preclose_continuation_is_born_process_stopped() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    process_entered = asyncio.Event()
    allow_owner_creation = asyncio.Event()
    owner_bound = asyncio.Event()
    created: list[Any] = []
    handles: list[_Closeable] = []

    async def compound_operation() -> None:
        async with resources.admit_operation():
            process_entered.set()
            await allow_owner_creation.wait()
            reservation = resources.reserve_owner(
                "mission:created-during-close",
                phase="workflow-handles",
            )
            created.append(reservation)
            assert reservation.operation_admission._stop_requested

            async def rejected_child() -> None:
                with pytest.raises(RuntimeError, match="closed"):
                    async with resources.admit_owner_operation(reservation):
                        pytest.fail("child task must not inherit process continuation")

            await asyncio.create_task(rejected_child())
            handle = _Closeable("mission:created-during-close", events)
            handles.append(handle)
            async with resources.admit_owner_operation(reservation):
                await _construct(reservation, handle)
                owner_bound.set()

    compound = asyncio.create_task(compound_operation())
    await _wait(process_entered)
    close_task = asyncio.create_task(resources.aclose())
    for _ in range(100):
        if resources.close_state is runtime_module.RuntimeCloseState.CLOSING_RETRYABLE:
            break
        await asyncio.sleep(0)
    assert resources.close_state is runtime_module.RuntimeCloseState.CLOSING_RETRYABLE

    allow_owner_creation.set()
    await asyncio.wait_for(owner_bound.wait(), timeout=0.5)
    await asyncio.wait_for(compound, timeout=0.5)
    await asyncio.wait_for(close_task, timeout=0.5)

    assert len(created) == 1
    assert created[0].released
    assert len(handles) == 1
    assert handles[0].close_calls == 1


async def test_process_close_broadcasts_all_owner_stop_intents_before_gate_wait() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    first = resources.reserve_owner("mission:first-gate", phase="workflow-handles")
    second = resources.reserve_owner("mission:second-gate", phase="workflow-handles")
    first_handle = await _construct(
        first,
        _Closeable("mission:first-gate", events),
    )
    second_handle = await _construct(
        second,
        _Closeable("mission:second-gate", events),
    )
    await first.operation_admission._lock.acquire()
    close_task = asyncio.create_task(resources.aclose())
    try:
        for _ in range(100):
            if (
                first.operation_admission._stop_requested
                and second.operation_admission._stop_requested
            ):
                break
            await asyncio.sleep(0)

        assert first.operation_admission._stop_requested
        assert second.operation_admission._stop_requested
        assert first_handle.close_calls == 0
        assert second_handle.close_calls == 0
        assert not close_task.done()
        with pytest.raises(RuntimeError, match="closed"):
            async with second.admit_operation():
                pytest.fail("peer stop intent must reject before the first gate unlocks")
    finally:
        first.operation_admission._lock.release()

    await asyncio.wait_for(close_task, timeout=0.5)
    assert first_handle.close_calls == 1
    assert second_handle.close_calls == 1


async def test_owner_admission_stop_failure_is_labelled_sticky_and_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_module, RuntimeShutdownError, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    first = resources.reserve_owner("mission:first-stop", phase="workflow-handles")
    second = resources.reserve_owner("mission:second-stop", phase="workflow-handles")
    first_handle = await _construct(
        first,
        _Closeable("mission:first-stop", events),
    )
    second_handle = await _construct(
        second,
        _Closeable("mission:second-stop", events),
    )
    failure = RuntimeError("owner gate stop unavailable")
    original_stop = first.operation_admission.stop_admission
    attempts = 0

    async def fail_once() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise failure
        await original_stop()

    monkeypatch.setattr(first.operation_admission, "stop_admission", fail_once)

    with pytest.raises(RuntimeShutdownError) as captured:
        await resources.aclose()

    assert captured.value.phase == "admission"
    assert len(captured.value.failures) == 1
    assert captured.value.failures[0].owner == first.owner
    assert captured.value.failures[0].cause is failure
    assert first.operation_admission._stop_requested
    assert second.operation_admission._stop_requested
    assert second._operation_admission_stopped
    assert first_handle.close_calls == 0
    assert second_handle.close_calls == 0

    await resources.aclose()
    assert attempts == 2
    assert first_handle.close_calls == 1
    assert second_handle.close_calls == 1


async def test_owner_admission_drain_failure_is_labelled_and_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_module, RuntimeShutdownError, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    first = resources.reserve_owner("mission:first-drain", phase="workflow-handles")
    second = resources.reserve_owner("mission:second-drain", phase="workflow-handles")
    first_handle = await _construct(
        first,
        _Closeable("mission:first-drain", events),
    )
    second_handle = await _construct(
        second,
        _Closeable("mission:second-drain", events),
    )
    failure = RuntimeError("owner gate drain unavailable")
    original_drain = first._drain_operation_admission
    attempts = 0

    async def fail_once() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise failure
        await original_drain()

    monkeypatch.setattr(first, "_drain_operation_admission", fail_once)

    with pytest.raises(RuntimeShutdownError) as captured:
        await resources.aclose()

    assert captured.value.phase == "admission"
    assert len(captured.value.failures) == 1
    assert captured.value.failures[0].owner == first.owner
    assert captured.value.failures[0].cause is failure
    assert second._operation_admission_drained
    assert first_handle.close_calls == 0
    assert second_handle.close_calls == 0

    await resources.aclose()
    assert attempts == 2
    assert first_handle.close_calls == 1
    assert second_handle.close_calls == 1


async def test_phase_attempts_every_peer_and_preserves_ordered_original_causes() -> None:
    runtime_module, RuntimeShutdownError, RuntimeShutdownFailure = _runtime_api()
    events: list[str] = []
    dispatcher = _Dispatcher(events)
    audit = _Dependency("audit", events)
    storage = _Dependency("storage", events)
    resources = _new_resources(
        runtime_module,
        dispatcher,
        audit=audit,
        storage=storage,
        owns_storage=True,
    )
    first_cause = RuntimeError("RAW_FIRST_SECRET")
    third_cause = asyncio.CancelledError("RAW_THIRD_SECRET")
    peers = (
        _Closeable("workflow:first", events, failures=[first_cause]),
        _Closeable("workflow:second", events),
        _Closeable("workflow:third", events, failures=[third_cause]),
    )
    for peer in peers:
        reservation = resources.reserve_owner(
            peer.owner,
            phase="workflow-handles",
        )
        await _construct(reservation, peer)

    world = _Closeable("world:dependent", events)
    await _construct(
        resources.reserve_owner(world.owner, phase="world-handles"),
        world,
    )

    with pytest.raises(RuntimeShutdownError) as raised:
        await resources.aclose()

    error = raised.value
    assert error.phase == "workflow-handles"
    assert all(isinstance(failure, RuntimeShutdownFailure) for failure in error.failures)
    assert [(failure.phase, failure.owner) for failure in error.failures] == [
        ("workflow-handles", "workflow:first"),
        ("workflow-handles", "workflow:third"),
    ]
    assert tuple(failure.cause for failure in error.failures) == (
        first_cause,
        third_cause,
    )
    assert isinstance(error.__cause__, BaseExceptionGroup)
    assert error.__cause__.exceptions == (first_cause, third_cause)
    assert "workflow-handles" in str(error)
    assert "workflow:first" in str(error)
    assert "workflow:third" in str(error)
    assert "RAW_FIRST_SECRET" not in str(error)
    assert "RAW_THIRD_SECRET" not in str(error)
    assert [event for event in events if event.startswith("close:")] == [
        "close:workflow:first:1",
        "close:workflow:second:1",
        "close:workflow:third:1",
    ]
    assert world.close_calls == audit.shutdown_calls == storage.shutdown_calls == 0


async def test_phase_starts_every_peer_before_waiting_for_blocked_first_owner() -> None:
    runtime_module, RuntimeShutdownError, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    first_release = asyncio.Event()
    second_release = asyncio.Event()
    first_cause = RuntimeError("first blocked close failed")
    second_cause = LookupError("second blocked close failed")
    first = _Closeable(
        "workflow:blocking-first",
        events,
        failures=[first_cause],
        started=first_started,
        release=first_release,
    )
    second = _Closeable(
        "workflow:blocking-second",
        events,
        failures=[second_cause],
        started=second_started,
        release=second_release,
    )
    await _construct(
        resources.reserve_owner(first.owner, phase="workflow-handles"),
        first,
    )
    await _construct(
        resources.reserve_owner(second.owner, phase="workflow-handles"),
        second,
    )

    closing = asyncio.create_task(resources.aclose())
    await _wait(first_started)
    await _wait(second_started)

    assert not closing.done()
    first_release.set()
    second_release.set()
    with pytest.raises(RuntimeShutdownError) as captured:
        await closing

    assert [(failure.owner, failure.cause) for failure in captured.value.failures] == [
        (first.owner, first_cause),
        (second.owner, second_cause),
    ]

    await resources.aclose()

    assert first.close_calls == 2
    assert second.close_calls == 2


async def test_failed_phase_retains_owner_and_dependencies_until_retry() -> None:
    runtime_module, RuntimeShutdownError, _shutdown_failure = _runtime_api()
    events: list[str] = []
    dispatcher = _Dispatcher(events)
    audit = _Dependency("audit", events)
    storage = _Dependency("storage", events)
    resources = _new_resources(
        runtime_module,
        dispatcher,
        audit=audit,
        storage=storage,
        owns_storage=True,
    )
    retry_cause = RuntimeError("workflow retry required")
    retrying = _Closeable(
        "workflow:retry",
        events,
        failures=[retry_cause],
    )
    successful_peer = _Closeable("workflow:peer", events)
    world = _Closeable("world:one", events)
    for resource, phase in (
        (retrying, "workflow-handles"),
        (successful_peer, "workflow-handles"),
        (world, "world-handles"),
    ):
        await _construct(
            resources.reserve_owner(resource.owner, phase=phase),
            resource,
        )

    with pytest.raises(RuntimeShutdownError):
        await resources.aclose()

    assert retrying.close_calls == 1
    assert successful_peer.close_calls == 1
    assert world.close_calls == audit.shutdown_calls == storage.shutdown_calls == 0

    await resources.aclose()
    assert retrying.close_calls == 2
    assert successful_peer.close_calls == 1, "successful peers are released after phase failure"
    assert world.close_calls == audit.shutdown_calls == storage.shutdown_calls == 1
    assert events[-4:] == [
        "close:workflow:retry:2",
        "close:world:one:1",
        "shutdown:audit:1",
        "shutdown:storage:1",
    ]

    await resources.aclose()
    assert retrying.close_calls == 2
    assert world.close_calls == audit.shutdown_calls == storage.shutdown_calls == 1
    assert dispatcher.stop_calls == dispatcher.wait_calls == 1


async def test_owner_anchor_is_strong_until_failed_cleanup_retry_releases() -> None:
    runtime_module, RuntimeShutdownError, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    reservation = resources.reserve_owner(
        "mission:anchored",
        phase="workflow-handles",
    )
    handle = _Closeable(
        "mission:anchored",
        events,
        failures=[RuntimeError("retry cleanup")],
    )
    await _construct(reservation, handle)
    anchor = _Anchor()
    anchor_ref = weakref.ref(anchor)

    assert reservation.retain_anchor(anchor) is anchor
    assert not reservation.released
    del anchor
    gc.collect()

    with pytest.raises(RuntimeShutdownError):
        await resources.aclose()

    gc.collect()
    assert anchor_ref() is not None
    assert not reservation.released
    assert resources.owner("mission:anchored") is reservation

    await resources.aclose()
    gc.collect()

    assert reservation.released
    assert anchor_ref() is None
    with pytest.raises(KeyError, match="mission:anchored"):
        resources.owner("mission:anchored")


async def test_cancelled_waiter_does_not_cancel_owned_cleanup() -> None:
    runtime_module, RuntimeShutdownError, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()
    handle = _Closeable(
        "workflow:shielded",
        events,
        started=cleanup_started,
        release=cleanup_release,
    )
    reservation = resources.reserve_owner(handle.owner, phase="workflow-handles")
    await _construct(reservation, handle)
    anchor = _Anchor()
    anchor_ref = weakref.ref(anchor)
    reservation.retain_anchor(anchor)
    del anchor

    cancelled_waiter = asyncio.create_task(resources.aclose())
    await _wait(cleanup_started)
    cancelled_waiter.cancel()
    with pytest.raises(RuntimeShutdownError) as raised:
        await cancelled_waiter

    failure = raised.value.failures[0]
    assert failure.phase == "workflow-handles"
    assert failure.owner == handle.owner
    assert isinstance(failure.cause, asyncio.CancelledError)
    assert isinstance(raised.value.__cause__, BaseExceptionGroup)
    assert raised.value.__cause__.exceptions == (failure.cause,)
    assert not handle.cancelled
    gc.collect()
    assert anchor_ref() is not None
    assert not reservation.released

    retry = asyncio.create_task(resources.aclose())
    cleanup_release.set()
    await asyncio.wait_for(retry, timeout=0.5)

    assert handle.close_calls == 1, "retry joins the still-owned cleanup operation"
    assert not handle.cancelled
    gc.collect()
    assert reservation.released
    assert anchor_ref() is None


async def test_waiter_cancellation_after_provider_success_does_not_repeat_close() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    close_started = asyncio.Event()
    close_release = asyncio.Event()
    provider = _Closeable(
        "provider:done-cancel-race",
        events,
        started=close_started,
        release=close_release,
    )
    reservation = resources.reserve_owner(
        "mission:done-cancel-race",
        phase="workflow-handles",
    )
    await _construct(reservation, provider)
    waiter = asyncio.create_task(reservation.aclose())
    await _wait(close_started)
    owned_close = reservation._resource_close
    assert owned_close is not None
    inner = owned_close._task
    assert inner is not None
    inner.add_done_callback(lambda _task: waiter.cancel())

    close_release.set()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    assert provider.close_calls == 1
    assert owned_close.complete
    assert not reservation.released

    await reservation.aclose()

    assert provider.close_calls == 1
    assert reservation.released
    await resources.aclose()


async def test_waiter_cancellation_after_provider_failure_preserves_retry() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    close_started = asyncio.Event()
    close_release = asyncio.Event()
    failure = RuntimeError("provider close failed")
    provider = _Closeable(
        "provider:failed-cancel-race",
        events,
        failures=[failure],
        started=close_started,
        release=close_release,
    )
    reservation = resources.reserve_owner(
        "mission:failed-cancel-race",
        phase="workflow-handles",
    )
    await _construct(reservation, provider)
    waiter = asyncio.create_task(reservation.aclose())
    await _wait(close_started)
    owned_close = reservation._resource_close
    assert owned_close is not None
    inner = owned_close._task
    assert inner is not None
    inner.add_done_callback(lambda _task: waiter.cancel())

    close_release.set()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    assert provider.close_calls == 1
    assert not owned_close.complete
    assert not reservation.released

    await reservation.aclose()

    assert provider.close_calls == 2
    assert reservation.released
    await resources.aclose()


async def test_late_failed_waiter_cannot_clobber_an_active_retry() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    events: list[str] = []
    resources = _new_resources(runtime_module, _Dispatcher(events))
    first_started = asyncio.Event()
    first_release = asyncio.Event()
    retry_started = asyncio.Event()
    retry_release = asyncio.Event()

    class FailOnceThenBlock:
        def __init__(self) -> None:
            self.close_calls = 0

        async def aclose(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                first_started.set()
                await first_release.wait()
                raise RuntimeError("first close failed")
            if self.close_calls == 2:
                retry_started.set()
                await retry_release.wait()
                return
            raise AssertionError("provider cleanup ran more than twice")

    provider = FailOnceThenBlock()
    reservation = resources.reserve_owner(
        "mission:failed-waiter-race",
        phase="workflow-handles",
    )
    reservation.bind(provider, close=provider.aclose)
    first_failure_seen = asyncio.Event()
    second_failure_seen = asyncio.Event()

    async def retrying_waiter() -> None:
        with pytest.raises(RuntimeError, match="first close failed"):
            await reservation.aclose()
        first_failure_seen.set()
        await reservation.aclose()

    async def late_old_waiter() -> None:
        with pytest.raises(RuntimeError, match="first close failed"):
            await reservation.aclose()
        second_failure_seen.set()

    retrying = asyncio.create_task(retrying_waiter())
    await _wait(first_started)
    late = asyncio.create_task(late_old_waiter())
    await asyncio.sleep(0)
    first_release.set()
    await _wait(first_failure_seen)
    await _wait(retry_started)
    await _wait(second_failure_seen)
    await asyncio.wait_for(late, timeout=0.5)

    owned_close = reservation._resource_close
    assert owned_close is not None
    active_retry = owned_close._task
    assert active_retry is not None
    third = asyncio.create_task(reservation.aclose())
    await asyncio.sleep(0)

    assert provider.close_calls == 2
    assert owned_close._task is active_retry

    retry_release.set()
    await asyncio.wait_for(asyncio.gather(retrying, third), timeout=0.5)

    assert provider.close_calls == 2
    assert reservation.released
    await resources.aclose()


async def test_concurrent_close_serializes_failure_retry_and_success() -> None:
    runtime_module, RuntimeShutdownError, _shutdown_failure = _runtime_api()
    events: list[str] = []
    dispatcher = _Dispatcher(events)
    resources = _new_resources(runtime_module, dispatcher)
    first_attempt_started = asyncio.Event()
    first_attempt_release = asyncio.Event()
    handle = _Closeable(
        "workflow:concurrent",
        events,
        failures=[RuntimeError("first attempt fails")],
        started=first_attempt_started,
        release=first_attempt_release,
    )
    await _construct(
        resources.reserve_owner(handle.owner, phase="workflow-handles"),
        handle,
    )

    first = asyncio.create_task(resources.aclose())
    await _wait(first_attempt_started)

    queued = [asyncio.Event(), asyncio.Event()]

    async def close_after_started(started: asyncio.Event) -> None:
        started.set()
        await resources.aclose()

    second = asyncio.create_task(close_after_started(queued[0]))
    third = asyncio.create_task(close_after_started(queued[1]))
    await asyncio.gather(*(_wait(started) for started in queued))
    first_attempt_release.set()

    results = await asyncio.wait_for(
        asyncio.gather(first, second, third, return_exceptions=True),
        timeout=0.5,
    )
    assert sum(isinstance(result, RuntimeShutdownError) for result in results) == 1
    assert sum(result is None for result in results) == 2
    assert handle.close_calls == 2
    assert dispatcher.stop_calls == dispatcher.wait_calls == 1


async def test_injected_storage_detaches_and_owned_storage_closes() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()

    injected_events: list[str] = []
    injected = _Dependency("storage:injected", injected_events)
    injected_ref = weakref.ref(injected)
    injected_resources = _new_resources(
        runtime_module,
        _Dispatcher(injected_events),
        storage=injected,
        owns_storage=False,
    )
    del injected
    await injected_resources.aclose()
    gc.collect()

    assert injected_events == ["admission:stop", "admission:drain"]
    assert injected_ref() is None, "successful close must detach injected storage ownership"

    owned_events: list[str] = []
    owned = _Dependency("storage:owned", owned_events)
    owned_resources = _new_resources(
        runtime_module,
        _Dispatcher(owned_events),
        storage=owned,
        owns_storage=True,
    )
    await owned_resources.aclose()

    assert owned.shutdown_calls == 1
    assert owned_events == [
        "admission:stop",
        "admission:drain",
        "shutdown:storage:owned:1",
    ]


async def test_real_composition_detaches_dispatch_graph_only_after_successful_close(
    tmp_path: Path,
) -> None:
    runtime_module, RuntimeShutdownError, _shutdown_failure = _runtime_api()
    storage_module = import_module("archetype.storage")
    wiring = import_module("archetype.wiring")
    control_config = storage_module.ControlCatalogConfig(catalog_dir=tmp_path / "catalogs")
    borrowed_storage = storage_module.StorageService(control_catalog_config=control_config)
    borrowed_ref = weakref.ref(borrowed_storage)
    resources = wiring.build_runtime_resources(
        wiring.RuntimeBootstrapConfig(
            control_catalog_config=control_config,
            storage_service=borrowed_storage,
        )
    )
    dispatcher = resources.dispatcher
    audit_events: list[str] = []
    resources._audit = _Closeable(  # noqa: SLF001 - retry boundary oracle
        "audit",
        audit_events,
        failures=[RuntimeError("audit close failed")],
    )
    resources._audit_close = None  # noqa: SLF001 - replace before first close
    del borrowed_storage

    with pytest.raises(RuntimeShutdownError) as captured:
        await resources.aclose()

    assert captured.value.failures[0].cause.args == ("audit close failed",)
    gc.collect()
    assert borrowed_ref() is not None
    assert resources.dispatcher is dispatcher
    assert dispatcher._registry is not None  # noqa: SLF001 - retained retry graph

    await resources.aclose()
    gc.collect()

    assert borrowed_ref() is None
    assert resources.dispatcher is dispatcher
    assert not hasattr(dispatcher, "_registry")
    with pytest.raises(RuntimeError, match="command admission is not accepting work"):
        await dispatcher.apply(object())


async def test_runtime_resources_has_no_product_behavior_surface() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
    dispatcher_type = import_module("archetype.commands.dispatch").CommandDispatcher
    source_path = Path(inspect.getsourcefile(runtime_module.RuntimeResources) or "")
    assert source_path.is_file()
    tree = ast.parse(source_path.read_text())
    runtime_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "RuntimeResources"
    )
    public_methods = {
        node.name
        for node in runtime_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and not node.name.startswith("_")
    }
    product_behavior = {
        "add_hook",
        "add_processor",
        "create_world",
        "defer",
        "despawn",
        "discover",
        "evaluate",
        "fork",
        "ingest",
        "query",
        "research",
        "restore_sandbox",
        "run",
        "spawn",
        "step",
        "submit",
        "update",
    }

    assert {"reserve_owner", "aclose"} <= public_methods
    assert public_methods.isdisjoint(product_behavior)
    assert all(
        not (
            isinstance(node, ast.ImportFrom)
            and (node.module or "").startswith(
                (
                    "archetype.api",
                    "archetype.app",
                    "archetype.cli",
                    "archetype.runtime",
                )
            )
        )
        for node in ast.walk(tree)
    )
    assert not any(
        isinstance(node, ast.Name) and node.id == "ContextVar" for node in ast.walk(tree)
    )
    assert "_bind_runtime_operation_admission" not in vars(dispatcher_type)
    assert "_runtime_continuation" not in inspect.getsource(dispatcher_type.__init__)
