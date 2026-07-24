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
    await _construct(
        resources.reserve_owner(handle.owner, phase="workflow-handles"),
        handle,
    )

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

    retry = asyncio.create_task(resources.aclose())
    cleanup_release.set()
    await asyncio.wait_for(retry, timeout=0.5)

    assert handle.close_calls == 1, "retry joins the still-owned cleanup operation"
    assert not handle.cancelled


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


async def test_runtime_resources_has_no_product_behavior_surface() -> None:
    runtime_module, _shutdown_error, _shutdown_failure = _runtime_api()
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
