# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Typed lifecycle hooks for Archetype worlds.

Events are dataclasses, not strings. ``world.add_hook(OnSpawn, fn)`` is
type-checked; ``world.add_hook(NotARealHook, fn)`` is a NameError.

Every hook is registered against a concrete event type and handed back an
opaque :class:`HookHandle` for removal. This module ships two registries
that share the same event catalogue and handle type:

- :class:`HookRegistry` — async handlers, awaited inline or run detached via
  ``asyncio.create_task`` with ``mode="spawn"``.
- :class:`SyncHookRegistry` — plain callables, called inline. No event loop
  required, so no ``"spawn"`` mode.

Payloads intentionally omit the owning world. The handler was registered
against that world; a back-reference would be redundant and prevents the
events from being trivially serializable. Handlers that need the world
close over it at registration time.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol, TypeVar

if TYPE_CHECKING:
    from daft import DataFrame

    from archetype.core.component import Component
    from archetype.core.interfaces import ArchetypeSignature

logger = logging.getLogger(__name__)


# ─────────────────────────── Event payloads ───────────────────────────


@dataclass(frozen=True, slots=True)
class HookEvent:
    """Base type for all world lifecycle hook events."""

    # world_id is the world's identity as stamped by the factory: always a str
    # at runtime (spec §231; World.world_id is str). Not a UUID object.
    world_id: str


@dataclass(frozen=True, slots=True)
class PreTick(HookEvent):
    """Fire before processors run for a tick."""

    tick: int


@dataclass(frozen=True, slots=True)
class PostTick(HookEvent):
    """Fire after processors finish and the tick is persisted.

    `tick` is the next tick; the completed tick is `tick - 1`.
    """

    tick: int
    results: dict[ArchetypeSignature, DataFrame]


@dataclass(frozen=True, slots=True)
class OnSpawn(HookEvent):
    """Fire after an entity is registered but before its first row is persisted."""

    entity_id: int
    components: list[Component]


@dataclass(frozen=True, slots=True)
class OnDespawn(HookEvent):
    """Fire after an entity is removed from active state."""

    entity_id: int


@dataclass(frozen=True, slots=True)
class OnComponentAdded(HookEvent):
    """Fire after component types are added to an entity.

    `components` contains the supplied additions, not the entity's complete
    component set.
    """

    entity_id: int
    components: list[Component]


@dataclass(frozen=True, slots=True)
class OnComponentRemoved(HookEvent):
    """Fire after component types are removed from an entity."""

    entity_id: int
    component_types: list[type[Component]]


@dataclass(frozen=True, slots=True)
class OnDestroy(HookEvent):
    """Fire immediately before a world is destroyed.

    Handlers can perform cleanup or final reads while the world is still live.
    """


E = TypeVar("E", bound=HookEvent)


class AsyncHookHandler(Protocol[E]):
    """Async hook handler. Takes a single event argument of the matching
    event type and returns an awaitable. Declared as returning Awaitable
    (rather than an async method) so both ``async def`` handlers and sync
    callables returning an awaitable satisfy the protocol — the bus only
    ever awaits the result."""

    def __call__(self, event: E, /) -> Awaitable[None]: ...


class SyncHookHandler(Protocol[E]):
    """Synchronous hook handler. Takes a single event argument of the
    matching event type and returns None."""

    def __call__(self, event: E, /) -> None: ...


FireMode = Literal["blocking", "spawn"]


# ─────────────────────────── Handle + registries ───────────────────────────


@dataclass(frozen=True, slots=True)
class HookHandle:
    """Opaque token used to remove a registered hook.

    Handles are scoped to the world and hook registry that created them.
    """

    _id: int
    _event_type: type[HookEvent]
    _registry_token: object = field(repr=False)

    @property
    def id(self) -> int:
        """Registry-local identifier (stable for the handle's lifetime)."""
        return self._id

    @property
    def event_type(self) -> type[HookEvent]:
        """The event type this handle's handler is registered for."""
        return self._event_type


class HookRegistry:
    """Per-world async hook storage. Not thread-safe; ``AsyncWorld``
    serializes mutations via its own event loop."""

    __slots__ = ("_by_type", "_ids", "_token")

    def __init__(self) -> None:
        self._by_type: dict[
            type[HookEvent], list[tuple[HookHandle, Callable[..., Awaitable[None]], FireMode]]
        ] = {}
        self._ids = itertools.count(1)
        self._token = object()

    def add(
        self,
        event_type: type[E],
        fn: AsyncHookHandler[E],
        *,
        mode: FireMode = "blocking",
    ) -> HookHandle:
        handle = HookHandle(
            _id=next(self._ids),
            _event_type=event_type,
            _registry_token=self._token,
        )
        self._by_type.setdefault(event_type, []).append((handle, fn, mode))
        return handle

    def remove(self, handle: HookHandle) -> None:
        bucket = self._by_type.get(handle._event_type)
        if not bucket:
            return
        self._by_type[handle._event_type] = [row for row in bucket if row[0] != handle]

    def clear(self) -> None:
        self._by_type.clear()

    def items(
        self,
    ) -> Iterator[tuple[type[HookEvent], HookHandle, Callable[..., Awaitable[None]], FireMode]]:
        """Iterate registered hooks as (event_type, handle, fn, mode) rows."""
        for event_type, bucket in self._by_type.items():
            for handle, fn, mode in bucket:
                yield event_type, handle, fn, mode

    async def fire(self, event: HookEvent) -> None:
        for _handle, fn, mode in self._by_type.get(type(event), ()):
            if mode == "blocking":
                try:
                    await fn(event)
                except asyncio.CancelledError:
                    task = asyncio.current_task()
                    if task is not None and task.cancelling():
                        raise
                    logger.warning(
                        "Hook %s cancelled itself on %s",
                        getattr(fn, "__qualname__", fn),
                        type(event).__name__,
                    )
                except Exception as exc:
                    logger.warning(
                        "Hook %s failed on %s: %s",
                        getattr(fn, "__qualname__", fn),
                        type(event).__name__,
                        exc,
                    )
            else:
                asyncio.create_task(_fire_detached(fn, event))


class SyncHookRegistry:
    """Per-world synchronous hook storage. Handlers are called inline in
    the firing thread — there is no event loop to defer to, so there is no
    ``"spawn"`` fire mode.
    """

    __slots__ = ("_by_type", "_ids", "_token")

    def __init__(self) -> None:
        self._by_type: dict[type[HookEvent], list[tuple[HookHandle, Callable[..., None]]]] = {}
        self._ids = itertools.count(1)
        self._token = object()

    def add(
        self,
        event_type: type[E],
        fn: SyncHookHandler[E],
    ) -> HookHandle:
        handle = HookHandle(
            _id=next(self._ids),
            _event_type=event_type,
            _registry_token=self._token,
        )
        self._by_type.setdefault(event_type, []).append((handle, fn))
        return handle

    def remove(self, handle: HookHandle) -> None:
        bucket = self._by_type.get(handle._event_type)
        if not bucket:
            return
        self._by_type[handle._event_type] = [row for row in bucket if row[0] != handle]

    def clear(self) -> None:
        self._by_type.clear()

    def items(
        self,
    ) -> Iterator[tuple[type[HookEvent], HookHandle, Callable[..., None], FireMode]]:
        """Iterate registered hooks as (event_type, handle, fn, mode) rows.

        Sync hooks always fire inline, so mode is always "blocking"; the
        4-tuple shape is kept uniform with the async registry.
        """
        for event_type, bucket in self._by_type.items():
            for handle, fn in bucket:
                yield event_type, handle, fn, "blocking"

    def fire(self, event: HookEvent) -> None:
        for _handle, fn in self._by_type.get(type(event), ()):
            try:
                fn(event)
            except Exception as exc:
                logger.warning(
                    "Hook %s failed on %s: %s",
                    getattr(fn, "__qualname__", fn),
                    type(event).__name__,
                    exc,
                )


async def _fire_detached(fn: Callable[..., Awaitable[None]], event: HookEvent) -> None:
    try:
        await fn(event)
    except Exception as exc:
        logger.warning(
            "Detached hook %s failed on %s: %s",
            getattr(fn, "__qualname__", fn),
            type(event).__name__,
            exc,
        )


__all__ = [
    "FireMode",
    "HookEvent",
    "HookHandle",
    "AsyncHookHandler",
    "HookRegistry",
    "OnComponentAdded",
    "OnComponentRemoved",
    "OnDespawn",
    "OnDestroy",
    "OnSpawn",
    "PostTick",
    "PreTick",
    "SyncHookHandler",
    "SyncHookRegistry",
]
