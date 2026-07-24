# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Process-lifetime resource ownership contracts.

This module is reserved infrastructure.  It defines the stable PR-4 lifetime
surface without implementing reservation, task, or shutdown behavior in the
interface seed.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from enum import StrEnum
from typing import Any, Literal, Protocol, runtime_checkable

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


class RuntimeResources:
    """Explicit process-lifetime state owner.

    The interface seed intentionally cannot reserve or close resources.  Those
    methods fail rather than reporting success before the lifecycle invariants
    have an implementation.
    """

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
        self._close_state = RuntimeCloseState.OPEN

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

        del owner, phase
        raise NotImplementedError("runtime owner reservation is not implemented")

    async def aclose(self) -> None:
        """Serialize dependency-phased, retryable process shutdown."""

        raise NotImplementedError("runtime resource shutdown is not implemented")


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
