# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Explicit process-lifetime resource ownership.

The owner is deliberately generic: it knows how to reserve, supervise, and
close resources, but it has no product operations or family-specific state.
"""

from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Iterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Literal, Protocol, cast, runtime_checkable

from archetype.errors import RuntimeShutdownError, RuntimeShutdownFailure

type RuntimeShutdownPhase = Literal[
    "admission",
    "supervised-tasks",
    "workflow-handles",
    "world-handles",
    "audit",
    "storage",
]

RUNTIME_SHUTDOWN_PHASES: tuple[RuntimeShutdownPhase, ...] = (
    "admission",
    "supervised-tasks",
    "workflow-handles",
    "world-handles",
    "audit",
    "storage",
)

_OWNER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._-]{0,127}\Z")
_LABEL_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
_MISSING = object()


class RuntimeCloseState(StrEnum):
    """Stable process-owner close-state vocabulary."""

    OPEN = "OPEN"
    CLOSING_RETRYABLE = "CLOSING_RETRYABLE"
    CLOSED = "CLOSED"


class _OwnerAdmissionStop(StrEnum):
    """Monotonic owner-gate stop authority."""

    OPEN = "OPEN"
    PROCESS = "PROCESS"
    STRICT = "STRICT"


type AsyncResourceFactory[T] = Callable[[], Awaitable[T]]
type SupervisedCoroutineFactory[T] = Callable[[], Coroutine[Any, Any, T]]
type AsyncClose = Callable[[], Awaitable[None]]


class OperationAdmission:
    """Task-reentrant admission and drain state for one explicit lifetime owner."""

    def __init__(self, *, closed_message: str) -> None:
        self._closed_message = closed_message
        self._accepting = True
        self._stop_requested = False
        self._lock = asyncio.Lock()
        self._depths: dict[asyncio.Task[Any], int] = {}
        self._drained = asyncio.Event()
        self._drained.set()

    @asynccontextmanager
    async def admit(self, *, continuation: bool = False) -> AsyncIterator[None]:
        """Admit one outer operation while allowing same-task nested calls."""

        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("runtime operation admission requires an asyncio task")
        observed_depth = self._depths.get(task, 0)
        if observed_depth == 0 and self._stop_requested and not continuation:
            raise RuntimeError(self._closed_message)
        async with self._lock:
            depth = self._depths.get(task, 0)
            if depth == 0:
                if (self._stop_requested or not self._accepting) and not continuation:
                    raise RuntimeError(self._closed_message)
                self._drained.clear()
            self._depths[task] = depth + 1
        try:
            yield
        finally:
            release = asyncio.create_task(
                self._release_admission(task),
                name="archetype-runtime-admission-release",
            )
            interrupted = False
            while True:
                try:
                    await asyncio.shield(release)
                    break
                except asyncio.CancelledError:
                    interrupted = True
                    if release.done():
                        break
            release.result()
            if interrupted:
                raise asyncio.CancelledError

    async def stop_admission(self) -> None:
        """Atomically reject new outer operations while retaining nested work."""

        self.request_stop()
        async with self._lock:
            self._accepting = False

    def request_stop(self) -> None:
        """Synchronously publish sticky stop intent before lock acquisition."""

        self._stop_requested = True

    async def wait_drained(self) -> None:
        """Wait until every task admitted before the stop boundary has exited."""

        if self.admitted_by_current_task():
            raise RuntimeError("operation admission cannot drain its current task")
        await self._drained.wait()

    def admitted_by_current_task(self) -> bool:
        """Whether the current task already owns an outer admission."""

        try:
            task = asyncio.current_task()
        except RuntimeError:
            return False
        return task is not None and task in self._depths

    async def _release_admission(self, task: asyncio.Task[Any]) -> None:
        """Finish counted exit even if the admitted waiter is cancelled again."""

        async with self._lock:
            remaining = self._depths[task] - 1
            if remaining:
                self._depths[task] = remaining
            else:
                del self._depths[task]
                if not self._depths:
                    self._drained.set()


@runtime_checkable
class OwnerReservation(Protocol):
    """A strongly registered owner slot created before resource execution."""

    @property
    def owner(self) -> str: ...

    @property
    def phase(self) -> RuntimeShutdownPhase: ...

    @property
    def released(self) -> bool:
        """Whether this exact owner completed cleanup and released its anchors."""

        ...

    @property
    def operation_admission(self) -> OperationAdmission:
        """Return the exact-task gate shared by this owner's ingress paths."""

        ...

    def admit_operation(self) -> AbstractAsyncContextManager[None]:
        """Admit one owner-scoped operation, including same-task continuations."""

        ...

    def operation_admitted(self) -> bool:
        """Whether the current task already owns this reservation's admission."""

        ...

    def request_operation_stop(self) -> None:
        """Synchronously publish a strict local-owner stop boundary."""

        ...

    def retain_anchor[T](self, anchor: T) -> T:
        """Keep an inert handle reachable until this owner releases."""

        ...

    def ensure_close_allowed(self) -> None:
        """Reject close from one of this reservation's supervised tasks."""

        ...

    async def construct[T](self, factory: AsyncResourceFactory[T]) -> T:
        """Construct and bind one resource inside this reservation."""

        ...

    def bind[T](
        self,
        resource: T,
        *,
        close: AsyncClose,
    ) -> T:
        """Bind an already acquired partial resource before another failure point."""

        ...

    def require_bound(self) -> object:
        """Return the completely constructed resource without side effects."""

        ...

    def spawn[T](
        self,
        factory: SupervisedCoroutineFactory[T],
        *,
        label: str,
    ) -> asyncio.Task[T]:
        """Create a supervised task from a not-yet-executed coroutine factory."""

        ...

    async def aclose(self) -> None:
        """Join the reservation's retryable close operation."""

        ...


class _OwnedClose:
    """One shielded, retryable close operation."""

    def __init__(self, close: AsyncClose) -> None:
        self._close = close
        self._task: asyncio.Task[None] | None = None
        self.complete = False

    def active_in_current_task(self) -> bool:
        """Whether cleanup re-entered its own close authority."""

        try:
            current = asyncio.current_task()
        except RuntimeError:
            return False
        return current is not None and current is self._task

    async def run(self) -> None:
        if self.complete:
            return
        task = self._task
        if task is None:
            start = asyncio.Event()

            async def invoke() -> None:
                # Suspend eager execution until recursive-close preflight can
                # identify this exact cleanup task.
                await start.wait()
                await self._invoke()

            task = asyncio.create_task(invoke(), name="archetype-runtime-cleanup")
            self._task = task
            start.set()
        try:
            await asyncio.shield(task)
        except _OwnedFailure as failure:
            if self._task is task:
                self._task = None
            raise failure.cause from None
        except BaseException:
            if task.done():
                if self._task is task:
                    self._task = None
                try:
                    task.result()
                except _OwnedFailure:
                    # The caller's interruption still surfaces. The failed
                    # owned operation remains retryable on the next waiter.
                    pass
                except BaseException:
                    pass
                else:
                    # The owned effect committed before cancellation reached
                    # this waiter. Record that fact without swallowing the
                    # caller's cancellation; a retry only releases ownership.
                    self.complete = True
            raise
        if self._task is task:
            self._task = None
        self.complete = True

    async def _invoke(self) -> None:
        try:
            result = self._close()
            if not inspect.isawaitable(result):
                raise TypeError("runtime resource cleanup must return an awaitable")
            await result
        except BaseException as exc:
            # ``asyncio.Task`` translates an escaping CancelledError into task
            # cancellation and loses the original exception object. Carry all
            # resource failures as ordinary task failures until the shielded
            # owner can re-raise the exact cause.
            raise _OwnedFailure(exc) from None


class _OwnedFailure(Exception):
    """Transport an exact BaseException through an asyncio task."""

    def __init__(self, cause: BaseException) -> None:
        self.cause = cause
        super().__init__("owned cleanup failed")


class _SupervisedSlot:
    """Registration inserted before an eager task can execute."""

    __slots__ = ("label", "task")

    def __init__(self, label: str) -> None:
        self.label = label
        self.task: asyncio.Task[Any] | None = None


class _OwnerReservation:
    """Concrete reservation state kept private to the process owner."""

    def __init__(
        self,
        *,
        owner: str,
        phase: RuntimeShutdownPhase,
        closed_message: str,
        on_release: Callable[[str, _OwnerReservation], None],
    ) -> None:
        self._owner = owner
        self._phase = phase
        self._on_release: Callable[[str, _OwnerReservation], None] | None = on_release
        self._resource: object = _MISSING
        self._resource_close: _OwnedClose | None = None
        self._construct_complete = False
        self._construct_lock = asyncio.Lock()
        self._construct_task: asyncio.Task[object] | None = None
        self._anchors: list[object] = []
        self._supervised: list[_SupervisedSlot] = []
        self._supervised_join: _OwnedClose | None = None
        self._operation_admission = OperationAdmission(closed_message=closed_message)
        self._operation_stop = _OwnerAdmissionStop.OPEN
        self._operation_admission_stopped = False
        self._operation_admission_drained = False
        self._sealed = False
        self._released = False

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def phase(self) -> RuntimeShutdownPhase:
        return self._phase

    @property
    def released(self) -> bool:
        return self._released

    @property
    def operation_admission(self) -> OperationAdmission:
        return self._operation_admission

    @asynccontextmanager
    async def admit_operation(self) -> AsyncIterator[None]:
        """Join strict owner-only admission without process continuation."""

        async with self._operation_admission.admit():
            yield

    def operation_admitted(self) -> bool:
        return self._operation_admission.admitted_by_current_task()

    def request_operation_stop(self) -> None:
        self._request_strict_operation_stop()

    def retain_anchor[T](self, anchor: T) -> T:
        self._require_bindable()
        if anchor is None:
            raise TypeError("runtime owner anchor must not be None")
        if all(retained is not anchor for retained in self._anchors):
            self._anchors.append(anchor)
        return anchor

    def ensure_close_allowed(self) -> None:
        """Reject close from work that would have to await or cancel itself."""

        self._require_not_current_supervised_task()

    async def construct[T](self, factory: AsyncResourceFactory[T]) -> T:
        if not callable(factory):
            raise TypeError("runtime resource factory must be callable")
        async with self._construct_lock:
            self._require_bindable()
            if self._construct_complete:
                return cast(T, self.require_bound())
            if self._resource is not _MISSING:
                raise RuntimeError(
                    f"runtime owner {self._owner!r} retains an incomplete construction"
                )
            start = asyncio.Event()

            async def invoke() -> T:
                # Suspend eager execution until exact task identity is visible
                # to owner-close preflight.
                await start.wait()
                return await self._invoke_construct(factory)

            task = asyncio.create_task(invoke(), name="archetype-runtime-construction")
            self._construct_task = task
            start.set()
            try:
                try:
                    resource = await asyncio.shield(task)
                except _OwnedFailure as failure:
                    raise failure.cause from None
                except BaseException:
                    # A factory may have returned an active resource at the
                    # same instant its caller was cancelled. Schedule owned
                    # cancellation behind any already-queued factory wakeup,
                    # then reconcile terminal binding before cancellation can
                    # escape.
                    asyncio.get_running_loop().call_soon(task.cancel)
                    while not task.done():
                        try:
                            await asyncio.shield(task)
                        except asyncio.CancelledError:
                            continue
                        except _OwnedFailure:
                            break
                    if task.done():
                        try:
                            task.result()
                        except _OwnedFailure:
                            pass
                    raise
            finally:
                if self._construct_task is task:
                    self._construct_task = None
            return resource

    def bind[T](
        self,
        resource: T,
        *,
        close: AsyncClose,
    ) -> T:
        self._require_bind_allowed()
        if self._construct_complete:
            if self._resource is resource:
                return resource
            raise RuntimeError(f"runtime owner {self._owner!r} is already bound")
        if not callable(close):
            raise TypeError("runtime resource cleanup must be callable")
        self._set_bound(resource, close=close)
        return resource

    def require_bound(self) -> object:
        if not self._construct_complete or self._resource is _MISSING:
            raise RuntimeError(f"runtime owner {self._owner!r} is not bound")
        return self._resource

    def spawn[T](
        self,
        factory: SupervisedCoroutineFactory[T],
        *,
        label: str,
    ) -> asyncio.Task[T]:
        self._require_bindable()
        _validate_label(label)
        if not callable(factory):
            raise TypeError("supervised coroutine factory must be callable")

        slot = _SupervisedSlot(label)
        self._supervised.append(slot)
        start = asyncio.Event()

        async def invoke() -> T:
            # An eager task factory can execute ``invoke`` inside
            # ``create_task``.  Suspend before user code until the returned
            # task has been bound to its exact inventory slot.
            await start.wait()
            coroutine = factory()
            if not inspect.iscoroutine(coroutine):
                raise TypeError("supervised factory must return a coroutine")
            return await coroutine

        try:
            task = asyncio.create_task(invoke(), name=f"archetype-runtime-{label}")
        except BaseException:
            self._supervised.remove(slot)
            raise
        slot.task = task
        start.set()
        return task

    async def aclose(self) -> None:
        self._require_not_current_supervised_task()
        self._request_strict_operation_stop()
        await self._stop_operation_admission()
        await self._drain_operation_admission()
        self._seal()
        self._cancel_construction()
        self._cancel_supervised_tasks()
        await self._join_construction()
        await self._join_supervised_tasks()
        await self._close_resource()

    def _request_process_operation_stop(self) -> None:
        """Reject owner-only work while preserving exact process continuations."""

        if self._operation_stop is _OwnerAdmissionStop.OPEN:
            self._operation_stop = _OwnerAdmissionStop.PROCESS
        if not self._operation_admission_stopped:
            self._operation_admission.request_stop()

    def _request_strict_operation_stop(self) -> None:
        """Monotonically reject every new outer owner operation."""

        self._operation_stop = _OwnerAdmissionStop.STRICT
        if not self._operation_admission_stopped:
            self._operation_admission.request_stop()

    @asynccontextmanager
    async def _admit_process_operation(
        self,
        *,
        continuation: bool,
    ) -> AsyncIterator[None]:
        """Admit only lifecycle-authorized exact work through PROCESS stop."""

        async with self._operation_admission.admit(
            continuation=(continuation and self._operation_stop is _OwnerAdmissionStop.PROCESS)
        ):
            yield

    async def _stop_operation_admission(self) -> None:
        if self._operation_admission_stopped:
            return
        await self._operation_admission.stop_admission()
        self._operation_admission_stopped = True

    async def _drain_operation_admission(self) -> None:
        if self._operation_admission_drained:
            return
        await self._operation_admission.wait_drained()
        self._operation_admission_drained = True

    def _seal(self) -> None:
        self._sealed = True

    async def _join_supervised_tasks(self) -> None:
        if self._supervised_join is None:

            async def join() -> None:
                tasks = self._supervised_tasks()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            self._supervised_join = _OwnedClose(join)
        await self._supervised_join.run()
        self._supervised.clear()

    def _cancel_supervised_tasks(self) -> None:
        """Signal every owned task before any reservation waits for a peer."""

        self._require_not_current_supervised_task()
        for task in self._supervised_tasks():
            if not task.done():
                task.cancel()

    def _cancel_construction(self) -> None:
        """Signal an in-flight factory before waiting for owner cleanup."""

        self._require_not_current_supervised_task()
        task = self._construct_task
        if task is not None and not task.done():
            # Preserve a result wakeup already queued ahead of close while
            # still cancelling a factory that remains suspended.
            asyncio.get_running_loop().call_soon(task.cancel)

    async def _join_construction(self) -> None:
        """Wait until an owned factory bound its result or became terminal."""

        task = self._construct_task
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)

    def _require_not_current_supervised_task(self) -> None:
        if self._operation_admission.admitted_by_current_task():
            raise RuntimeError("runtime owner cannot close from its admitted operation")
        current = asyncio.current_task()
        if current is not None and current is self._construct_task:
            raise RuntimeError("runtime owner cannot close from its construction task")
        if current is not None and current in self._supervised_tasks():
            raise RuntimeError("runtime owner cannot close from its supervised task")
        close = self._resource_close
        if close is not None and close.active_in_current_task():
            raise RuntimeError("runtime owner cannot close from its cleanup task")

    def _supervised_tasks(self) -> tuple[asyncio.Task[Any], ...]:
        return tuple(slot.task for slot in self._supervised if slot.task is not None)

    async def _close_resource(self) -> None:
        async with self._construct_lock:
            close = self._resource_close
            if close is None:
                self._release()
                return
        await close.run()
        self._release()

    async def _invoke_construct[T](self, factory: AsyncResourceFactory[T]) -> T:
        """Bind a returned active resource inside an owner-retained task."""

        try:
            resource = await factory()
            close = _infer_close(resource)
            self._set_bound(resource, close=close)
            self._construct_complete = True
            return resource
        except BaseException as exc:
            raise _OwnedFailure(exc) from None

    def _set_bound(self, resource: object, *, close: AsyncClose) -> None:
        if not callable(close):
            raise TypeError("runtime resource cleanup must be callable")
        self._resource = resource
        self._resource_close = _OwnedClose(close)

    def _release(self) -> None:
        self._request_strict_operation_stop()
        self._resource = _MISSING
        self._resource_close = None
        self._anchors.clear()
        self._released = True
        on_release = self._on_release
        self._on_release = None
        if on_release is not None:
            on_release(self._owner, self)

    def _require_bindable(self) -> None:
        if self._sealed or self._released:
            raise RuntimeError(f"runtime owner {self._owner!r} is sealed")

    def _require_bind_allowed(self) -> None:
        """Permit only the exact pre-seal constructor to finish ownership."""

        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        construction_continuation = current is not None and current is self._construct_task
        if self._released or (self._sealed and not construction_continuation):
            raise RuntimeError(f"runtime owner {self._owner!r} is sealed")


class RuntimeResources:
    """Own process resources through deterministic, retryable close phases."""

    def __init__(
        self,
        *,
        dispatcher: Any,
        audit: Any | None = None,
        storage: Any | None = None,
        owns_storage: bool = False,
        world_library_manifests: tuple[Any, ...] = (),
    ) -> None:
        self._dispatcher = dispatcher
        self._audit = audit
        self._storage = storage
        self._owns_storage = owns_storage
        self._world_library_manifests = tuple(world_library_manifests)
        self._world_libraries: MappingProxyType[str, Any] = MappingProxyType({})
        self._world_libraries_retained = False
        self._installing_world_library: str | None = None
        self._owners: dict[str, _OwnerReservation] = {}
        self._close_state = RuntimeCloseState.OPEN
        self._close_lock = asyncio.Lock()
        self._operation_admission = OperationAdmission(
            closed_message="runtime resource admission is closed"
        )
        runtime_admission = getattr(
            self._dispatcher,
            "_admit_runtime_operation",
            None,
        )
        request_dispatcher_stop = getattr(self._dispatcher, "request_stop", None)
        if not all(
            callable(value)
            for value in (
                runtime_admission,
                request_dispatcher_stop,
            )
        ):
            raise TypeError("runtime dispatcher must expose coordinated admission and stop intent")
        self._operation_admission_stopped = False
        self._operation_admission_drained = False
        self._admission_stopped = False
        self._admission_drained = False
        self._sealed = False
        self._audit_close: _OwnedClose | None = None
        self._storage_close: _OwnedClose | None = None

    @property
    def dispatcher(self) -> Any:
        """Return the shared command dispatcher owned by this process state."""

        return self._dispatcher

    @property
    def world_library_manifests(self) -> tuple[Any, ...]:
        """Return the exact deterministic manifest set used for composition."""

        return self._world_library_manifests

    @property
    def world_libraries(self) -> MappingProxyType[str, Any]:
        """Return installed adapters by canonical library name."""

        return self._world_libraries

    def retain_world_libraries(self, installed: tuple[Any, ...]) -> None:
        """Bind the complete installed adapter inventory exactly once."""

        if self._world_libraries_retained:
            raise RuntimeError("runtime world libraries are already retained")
        expected = tuple(manifest.name for manifest in self._world_library_manifests)
        actual = tuple(library.name for library in installed)
        if actual != expected:
            raise ValueError(
                f"installed world-library order {actual!r} does not match manifests {expected!r}"
            )
        self._world_libraries = MappingProxyType({library.name: library for library in installed})
        self._world_libraries_retained = True

    @contextmanager
    def _world_library_installation(self, name: str) -> Iterator[None]:
        """Keep synchronous installers declarative until composition commits."""

        if self._installing_world_library is not None:
            raise RuntimeError("world-library installation cannot be nested")
        self._installing_world_library = name
        try:
            yield
        finally:
            self._installing_world_library = None

    def world_library(self, name: str) -> Any:
        """Resolve one installed library or raise a stable lookup error."""

        if not isinstance(name, str) or not name:
            raise ValueError("world-library name must be a non-empty string")
        try:
            return self._world_libraries[name]
        except KeyError:
            raise KeyError(f"world library {name!r} is not installed") from None

    @property
    def close_state(self) -> RuntimeCloseState:
        """Return the current stable close-state value."""

        return self._close_state

    @asynccontextmanager
    async def admit_operation(self) -> AsyncIterator[None]:
        """Admit a whole public call into both coordinated ingress gates."""

        async with self._operation_admission.admit(
            continuation=self._dispatcher_admitted_by_current_task()
        ):
            async with self._dispatcher._admit_runtime_operation(
                self._operation_admission.admitted_by_current_task
            ):
                yield

    def operation_admitted(self) -> bool:
        """Whether the current task may finish already-admitted compound work."""

        return (
            self._operation_admission.admitted_by_current_task()
            or self._dispatcher_admitted_by_current_task()
        )

    @asynccontextmanager
    async def admit_owner_operation(
        self,
        reservation: OwnerReservation,
    ) -> AsyncIterator[None]:
        """Admit owner ingress with process-close continuation authority."""

        concrete = cast(_OwnerReservation, reservation)
        if self._owners.get(concrete.owner) is not concrete:
            raise RuntimeError(f"runtime owner {concrete.owner!r} is not registered")
        continuation = self._close_state is not RuntimeCloseState.OPEN and self.operation_admitted()
        async with concrete._admit_process_operation(continuation=continuation):
            yield

    def ensure_close_allowed(self) -> None:
        """Reject a caller that would have to cancel and await itself."""

        for reservation in self._owners.values():
            reservation.ensure_close_allowed()
        for close in (self._audit_close, self._storage_close):
            if close is not None and close.active_in_current_task():
                raise RuntimeError("RuntimeResources cannot close from an owned cleanup task")

    def reserve_owner(
        self,
        owner: str,
        *,
        phase: RuntimeShutdownPhase,
        closed_message: str | None = None,
    ) -> OwnerReservation:
        """Synchronously reserve ownership before a factory or task may execute."""

        if self._installing_world_library is not None:
            raise RuntimeError(
                f"world-library installer {self._installing_world_library!r} cannot reserve "
                "runtime resources; acquire them lazily from an async operation"
            )
        _validate_owner(owner)
        _validate_owner_phase(phase)
        if self._close_state is not RuntimeCloseState.OPEN and not self.operation_admitted():
            raise RuntimeError("runtime resource admission is closed")
        if owner in self._owners:
            raise RuntimeError(f"runtime owner {owner!r} is already reserved")
        reservation = _OwnerReservation(
            owner=owner,
            phase=phase,
            closed_message=closed_message or f"runtime owner {owner!r} is closed",
            on_release=self._release_owner,
        )
        if self._close_state is not RuntimeCloseState.OPEN:
            reservation._request_process_operation_stop()
        self._owners[owner] = reservation
        return reservation

    def owner(self, owner: str) -> OwnerReservation:
        """Resolve one exact existing owner without creating or running work."""

        _validate_owner(owner)
        try:
            return self._owners[owner]
        except KeyError:
            raise KeyError(f"runtime owner {owner!r} is not reserved") from None

    async def aclose(self) -> None:
        """Serialize dependency-phased, retryable process shutdown."""

        if self.operation_admitted():
            raise RuntimeError("RuntimeResources cannot close from an admitted operation")
        self.ensure_close_allowed()
        async with self._close_lock:
            if self._close_state is RuntimeCloseState.CLOSED:
                return
            if self.operation_admitted():
                raise RuntimeError("RuntimeResources cannot close from an admitted operation")
            self.ensure_close_allowed()
            self._close_state = RuntimeCloseState.CLOSING_RETRYABLE
            for reservation in tuple(self._owners.values()):
                reservation._request_process_operation_stop()

            await self._close_admission()
            await self._close_owner_admissions()
            self._seal()
            await self._close_supervised()
            await self._close_owner_phase("workflow-handles")
            await self._close_owner_phase("world-handles")
            await self._close_dependency("audit")
            await self._close_dependency("storage")
            self._detach_closed_dispatcher_graph()
            self._close_state = RuntimeCloseState.CLOSED

    async def _close_owner_admissions(self) -> None:
        """Stop and drain every post-global owner gate before owned teardown."""

        reservations = tuple(self._owners.values())
        for reservation in reservations:
            reservation._request_strict_operation_stop()

        failures: list[RuntimeShutdownFailure] = []
        stop_attempts = tuple(
            (
                reservation.owner,
                asyncio.create_task(
                    reservation._stop_operation_admission(),
                    name="archetype-runtime-stop-owner-admission",
                ),
            )
            for reservation in reservations
        )
        for owner, attempt in stop_attempts:
            try:
                await attempt
            except BaseException as exc:
                failures.append(RuntimeShutdownFailure("admission", owner, exc))
        if failures:
            _raise_shutdown_error("admission", failures)

        drain_attempts = tuple(
            (
                reservation.owner,
                asyncio.create_task(
                    reservation._drain_operation_admission(),
                    name="archetype-runtime-drain-owner-admission",
                ),
            )
            for reservation in reservations
        )
        for owner, attempt in drain_attempts:
            try:
                await attempt
            except BaseException as exc:
                failures.append(RuntimeShutdownFailure("admission", owner, exc))
        if failures:
            _raise_shutdown_error("admission", failures)

    async def _close_admission(self) -> None:
        failures: list[RuntimeShutdownFailure] = []
        if not self._operation_admission_stopped:
            self._operation_admission.request_stop()
        if not self._admission_stopped:
            self._dispatcher.request_stop()
        if not self._operation_admission_stopped:
            try:
                await self._operation_admission.stop_admission()
            except BaseException as exc:
                failures.append(RuntimeShutdownFailure("admission", "runtime-operations", exc))
            else:
                self._operation_admission_stopped = True
        if failures:
            _raise_shutdown_error("admission", failures)

        if not self._operation_admission_drained:
            try:
                await self._operation_admission.wait_drained()
            except BaseException as exc:
                failures.append(RuntimeShutdownFailure("admission", "runtime-operations", exc))
            else:
                self._operation_admission_drained = True
        if failures:
            _raise_shutdown_error("admission", failures)

        await self._try_stop_dispatcher(failures)
        if self._admission_stopped and not self._admission_drained:
            try:
                await self._dispatcher.wait_drained()
            except BaseException as exc:
                failures.append(RuntimeShutdownFailure("admission", "dispatcher", exc))
            else:
                self._admission_drained = True
        if failures:
            _raise_shutdown_error("admission", failures)

    async def _try_stop_dispatcher(
        self,
        failures: list[RuntimeShutdownFailure],
    ) -> None:
        """Finalize dispatcher stop after the outer process gate is drained."""

        if self._admission_stopped:
            return
        try:
            await self._dispatcher.stop_admission()
        except BaseException as exc:
            failures.append(RuntimeShutdownFailure("admission", "dispatcher", exc))
        else:
            self._admission_stopped = True

    def _seal(self) -> None:
        if self._sealed:
            return
        self._sealed = True
        for reservation in self._owners.values():
            reservation._seal()

    async def _close_supervised(self) -> None:
        failures: list[RuntimeShutdownFailure] = []
        reservations = tuple(self._owners.values())
        for reservation in reservations:
            try:
                reservation._cancel_construction()
                reservation._cancel_supervised_tasks()
            except BaseException as exc:
                failures.append(
                    RuntimeShutdownFailure(
                        "supervised-tasks",
                        reservation.owner,
                        exc,
                    )
                )
        for reservation in reservations:
            try:
                await reservation._join_construction()
                await reservation._join_supervised_tasks()
            except BaseException as exc:
                failures.append(
                    RuntimeShutdownFailure(
                        "supervised-tasks",
                        reservation.owner,
                        exc,
                    )
                )
        if failures:
            _raise_shutdown_error("supervised-tasks", failures)

    async def _close_owner_phase(self, phase: RuntimeShutdownPhase) -> None:
        failures: list[RuntimeShutdownFailure] = []
        attempts: list[tuple[str, asyncio.Task[None]]] = []
        for owner, reservation in tuple(self._owners.items()):
            if reservation.phase != phase:
                continue
            attempts.append(
                (
                    owner,
                    asyncio.create_task(
                        reservation._close_resource(),
                        name=f"archetype-runtime-close-{phase}",
                    ),
                )
            )
        for owner, attempt in attempts:
            try:
                await attempt
            except BaseException as exc:
                failures.append(RuntimeShutdownFailure(phase, owner, exc))
        if failures:
            _raise_shutdown_error(phase, failures)

    def _release_owner(self, owner: str, reservation: _OwnerReservation) -> None:
        """Remove exactly the successfully released reservation from inventory."""

        if self._owners.get(owner) is reservation:
            del self._owners[owner]

    def _dispatcher_admitted_by_current_task(self) -> bool:
        admitted = getattr(self._dispatcher, "operation_admitted", None)
        return bool(admitted()) if callable(admitted) else False

    def _detach_closed_dispatcher_graph(self) -> None:
        """Release real-composition handlers only after every close phase succeeds."""

        detach = getattr(self._dispatcher, "_detach_closed_graph", None)
        if callable(detach):
            detach()

    async def _close_dependency(self, phase: Literal["audit", "storage"]) -> None:
        dependency = self._audit if phase == "audit" else self._storage
        if dependency is None:
            return
        if phase == "storage" and not self._owns_storage:
            self._storage = None
            return

        operation_name = "_audit_close" if phase == "audit" else "_storage_close"
        operation = cast(_OwnedClose | None, getattr(self, operation_name))
        if operation is None:
            operation = _OwnedClose(_infer_close(dependency))
            setattr(self, operation_name, operation)
        try:
            await operation.run()
        except BaseException as exc:
            _raise_shutdown_error(
                phase,
                [RuntimeShutdownFailure(phase, phase, exc)],
            )
        if phase == "audit":
            self._audit = None
            self._audit_close = None
        else:
            self._storage = None
            self._storage_close = None


def _infer_close(resource: object) -> AsyncClose:
    for name in ("aclose", "close", "shutdown"):
        close = getattr(resource, name, None)
        if callable(close):
            return cast(AsyncClose, close)
    raise TypeError("runtime-owned resource must expose async close or shutdown")


def _validate_owner(owner: object) -> None:
    if not isinstance(owner, str) or _OWNER_PATTERN.fullmatch(owner) is None:
        raise ValueError("runtime owner must be a bounded safe identifier")


def _validate_label(label: object) -> None:
    if not isinstance(label, str) or _LABEL_PATTERN.fullmatch(label) is None:
        raise ValueError("runtime task label must be a bounded safe identifier")


def _validate_owner_phase(phase: object) -> None:
    if phase not in {"workflow-handles", "world-handles"}:
        raise ValueError("runtime owners require a resource-handle shutdown phase")


def _raise_shutdown_error(
    phase: RuntimeShutdownPhase,
    failures: list[RuntimeShutdownFailure],
) -> None:
    causes = [failure.cause for failure in failures]
    group = BaseExceptionGroup(
        f"runtime shutdown phase {phase!r} failed",
        causes,
    )
    raise RuntimeShutdownError(phase, tuple(failures)) from group


__all__ = [
    "AsyncClose",
    "AsyncResourceFactory",
    "OperationAdmission",
    "OwnerReservation",
    "RUNTIME_SHUTDOWN_PHASES",
    "RuntimeCloseState",
    "RuntimeResources",
    "RuntimeShutdownPhase",
    "SupervisedCoroutineFactory",
]
