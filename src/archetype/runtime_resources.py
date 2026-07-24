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
from collections.abc import Awaitable, Callable, Coroutine
from enum import StrEnum
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


type AsyncResourceFactory[T] = Callable[[], Awaitable[T]]
type SupervisedCoroutineFactory[T] = Callable[[], Coroutine[Any, Any, T]]
type AsyncClose = Callable[[], Awaitable[None]]


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

    def retain_anchor[T](self, anchor: T) -> T:
        """Keep an inert handle reachable until this owner releases."""

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

    async def run(self) -> None:
        if self.complete:
            return
        task = self._task
        if task is None:
            task = asyncio.create_task(self._invoke(), name="archetype-runtime-cleanup")
            self._task = task
        try:
            await asyncio.shield(task)
        except _OwnedFailure as failure:
            self._task = None
            raise failure.cause from None
        except BaseException:
            if task.done():
                self._task = None
            raise
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
    ) -> None:
        self._owner = owner
        self._phase = phase
        self._resource: object = _MISSING
        self._resource_close: _OwnedClose | None = None
        self._construct_complete = False
        self._construct_lock = asyncio.Lock()
        self._anchors: list[object] = []
        self._supervised: list[_SupervisedSlot] = []
        self._supervised_join: _OwnedClose | None = None
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

    def retain_anchor[T](self, anchor: T) -> T:
        self._require_bindable()
        if anchor is None:
            raise TypeError("runtime owner anchor must not be None")
        if all(retained is not anchor for retained in self._anchors):
            self._anchors.append(anchor)
        return anchor

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
            resource = await factory()
            close = _infer_close(resource)
            self._set_bound(resource, close=close)
            self._construct_complete = True
            return resource

    def bind[T](
        self,
        resource: T,
        *,
        close: AsyncClose,
    ) -> T:
        self._require_bindable()
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

        async def invoke() -> T:
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
        return task

    async def aclose(self) -> None:
        self._seal()
        await self._join_supervised_tasks()
        await self._close_resource()

    def _seal(self) -> None:
        self._sealed = True

    async def _join_supervised_tasks(self) -> None:
        if self._supervised_join is None:

            async def join() -> None:
                tasks = tuple(slot.task for slot in self._supervised if slot.task is not None)
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            self._supervised_join = _OwnedClose(join)
        await self._supervised_join.run()
        self._supervised.clear()

    async def _close_resource(self) -> None:
        close = self._resource_close
        if close is None:
            self._release()
            return
        await close.run()
        self._release()

    def _set_bound(self, resource: object, *, close: AsyncClose) -> None:
        if not callable(close):
            raise TypeError("runtime resource cleanup must be callable")
        self._resource = resource
        self._resource_close = _OwnedClose(close)

    def _release(self) -> None:
        self._resource = _MISSING
        self._resource_close = None
        self._anchors.clear()
        self._released = True

    def _require_bindable(self) -> None:
        if self._sealed or self._released:
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
    ) -> None:
        self._dispatcher = dispatcher
        self._audit = audit
        self._storage = storage
        self._owns_storage = owns_storage
        self._owners: dict[str, _OwnerReservation] = {}
        self._close_state = RuntimeCloseState.OPEN
        self._close_lock = asyncio.Lock()
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
    def close_state(self) -> RuntimeCloseState:
        """Return the current stable close-state value."""

        return self._close_state

    def reserve_owner(
        self,
        owner: str,
        *,
        phase: RuntimeShutdownPhase,
    ) -> OwnerReservation:
        """Synchronously reserve ownership before a factory or task may execute."""

        _validate_owner(owner)
        _validate_owner_phase(phase)
        if self._close_state is not RuntimeCloseState.OPEN:
            raise RuntimeError("runtime resource admission is closed")
        if owner in self._owners:
            raise RuntimeError(f"runtime owner {owner!r} is already reserved")
        reservation = _OwnerReservation(owner=owner, phase=phase)
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

        async with self._close_lock:
            if self._close_state is RuntimeCloseState.CLOSED:
                return
            self._close_state = RuntimeCloseState.CLOSING_RETRYABLE

            await self._close_admission()
            self._seal()
            await self._close_supervised()
            await self._close_owner_phase("workflow-handles")
            await self._close_owner_phase("world-handles")
            await self._close_dependency("audit")
            await self._close_dependency("storage")
            self._close_state = RuntimeCloseState.CLOSED

    async def _close_admission(self) -> None:
        failures: list[RuntimeShutdownFailure] = []
        if not self._admission_stopped:
            try:
                await self._dispatcher.stop_admission()
            except BaseException as exc:
                failures.append(RuntimeShutdownFailure("admission", "dispatcher", exc))
            else:
                self._admission_stopped = True
        if self._admission_stopped and not self._admission_drained:
            try:
                await self._dispatcher.wait_drained()
            except BaseException as exc:
                failures.append(RuntimeShutdownFailure("admission", "dispatcher", exc))
            else:
                self._admission_drained = True
        if failures:
            _raise_shutdown_error("admission", failures)

    def _seal(self) -> None:
        if self._sealed:
            return
        self._sealed = True
        for reservation in self._owners.values():
            reservation._seal()

    async def _close_supervised(self) -> None:
        failures: list[RuntimeShutdownFailure] = []
        for reservation in tuple(self._owners.values()):
            try:
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
        for owner, reservation in tuple(self._owners.items()):
            if reservation.phase != phase:
                continue
            try:
                await reservation._close_resource()
            except BaseException as exc:
                failures.append(RuntimeShutdownFailure(phase, owner, exc))
            else:
                del self._owners[owner]
        if failures:
            _raise_shutdown_error(phase, failures)

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
        else:
            self._storage = None


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
    "OwnerReservation",
    "RUNTIME_SHUTDOWN_PHASES",
    "RuntimeCloseState",
    "RuntimeResources",
    "RuntimeShutdownPhase",
    "SupervisedCoroutineFactory",
]
