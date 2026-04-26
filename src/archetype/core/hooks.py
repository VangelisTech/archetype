# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol, TypeVar

from uuid_utils import UUID

if TYPE_CHECKING:
    from daft import DataFrame

    from archetype.core.component import Component
    from archetype.core.interfaces import ArchetypeSignature

logger = logging.getLogger(__name__)


# ─────────────────────────── Event payloads ───────────────────────────


@dataclass(frozen=True, slots=True)
class HookEvent:
    """Base type for all world lifecycle hook events."""

    world_id: UUID


@dataclass(frozen=True, slots=True)
class PreTick(HookEvent):
    """Fires at the start of ``World.step``, before any archetype runs."""

    tick: int


@dataclass(frozen=True, slots=True)
class PostTick(HookEvent):
    """Fires after all archetypes have processed and ``_live`` has been
    refreshed. ``tick`` is the *next* tick (the one just completed was
    ``tick - 1``)."""

    tick: int
    results: dict[ArchetypeSignature, DataFrame]


@dataclass(frozen=True, slots=True)
class OnSpawn(HookEvent):
    """Fires after the entity has been registered in ``_entity2sig`` and the
    row appended to ``_spawn_cache``, but before the tick materializes it.
    A handler calling ``world.get_entity(event.entity_id)`` will not find
    the entity until ``PostTick`` of the current tick."""

    entity_id: int
    components: list[Component]


@dataclass(frozen=True, slots=True)
class OnDespawn(HookEvent):
    """Fires after the entity has been removed from ``_entity2sig`` and either
    the same-tick spawn has been cancelled or a despawn row has been queued
    in ``_despawn_cache``."""

    entity_id: int


@dataclass(frozen=True, slots=True)
class OnComponentAdded(HookEvent):
    """Fires after ``add_components`` has moved the entity to its new
    signature. ``components`` is the list of instances the caller supplied,
    not the full post-move component set."""

    entity_id: int
    components: list[Component]


@dataclass(frozen=True, slots=True)
class OnComponentRemoved(HookEvent):
    """Fires after ``remove_components`` has moved the entity to its new
    signature."""

    entity_id: int
    component_types: list[type[Component]]


@dataclass(frozen=True, slots=True)
class OnDestroy(HookEvent):
    """Fires when a world is destroyed via ``WorldOrchestrator.destroy_world``.

    Fires before the world is removed from the registry. Handlers can
    perform cleanup or final reads against the still-live world.
    """


E = TypeVar("E", bound=HookEvent)


class AsyncHookHandler(Protocol[E]):
    """Async hook handler. Takes a single event argument of the matching
    event type and returns an awaitable."""

    async def __call__(self, event: E, /) -> None: ...


class SyncHookHandler(Protocol[E]):
    """Synchronous hook handler. Takes a single event argument of the
    matching event type and returns None."""

    def __call__(self, event: E, /) -> None: ...


FireMode = Literal["blocking", "spawn"]


# ─────────────────────────── Handle + registries ───────────────────────────


@dataclass(frozen=True, slots=True)
class HookHandle:
    """Opaque token returned by ``world.add_hook``. Pass to
    ``world.remove_hook`` to unregister. Equality and hashing are registry-
    scoped so a handle from one world cannot accidentally match a same-shaped
    handle minted by another world.

    Shared by both ``HookRegistry`` and ``SyncHookRegistry`` — a handle
    minted by one registry is not meaningful to the other, but the type is
    uniform so interfaces can accept either.
    """

    _id: int
    _event_type: type[HookEvent]
    _registry_token: object = field(repr=False)


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

    async def fire(self, event: HookEvent) -> None:
        for _handle, fn, mode in self._by_type.get(type(event), ()):
            if mode == "blocking":
                try:
                    await fn(event)
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
