# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
App-Layer Service Interfaces

Protocols for the application services that surround the core engine. Every
concrete service in ``src/archetype/app/`` should satisfy one of these
Protocols structurally; callers should depend on the Protocol, not the
concrete class.

These interfaces capture the *current* public surface of each service
faithfully — they are not aspirational. A planned redesign that splits
cross-cutting concerns (broker injection, registry persistence, fork
ownership) out of ``WorldService`` is documented in
``docs/reports/2026-04-25-service-layer-redesign.md``; the migration
will refine these Protocols over several PRs.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Protocol

from uuid_utils import UUID

if TYPE_CHECKING:
    from archetype.app.models import (
        ActorCtx,
        Command,
        RunResult,
        WorldInfo,
        WorldSnapshot,
    )
    from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
    from archetype.core.interfaces import (
        iAsyncQueryManager,
        iAsyncStore,
        iAsyncSystem,
        iAsyncUpdateManager,
        iWorld,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Storage
# ─────────────────────────────────────────────────────────────────────────────


class iStorageService(Protocol):
    """Pool of storage backends keyed by ``(uri, namespace, backend, cache)``."""

    async def get_backend(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]: ...

    async def shutdown(self) -> None: ...


# ─────────────────────────────────────────────────────────────────────────────
# World construction & lifecycle
# ─────────────────────────────────────────────────────────────────────────────


class iWorldFactory(Protocol):
    """Assembles a fully-wired world from configs and a system."""

    async def create_world(
        self,
        world_config: WorldConfig,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> iWorld: ...


# Hook signature for cross-cutting concerns at world creation time. Covers
# broker injection, registry persistence, post-tick observers, etc.
WorldCreationHook = Callable[["iWorld", "StorageConfig"], Awaitable[None]]


class iWorldService(Protocol):
    """World lifecycle: create, register, look up, fork, remove.

    Concrete implementations today also handle broker injection and registry
    persistence inline; the redesign extracts those to ``WorldCreationHook``s.
    """

    async def create_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> iWorld: ...

    def get_world(self, world_id: UUID) -> iWorld: ...

    def get_world_by_name(self, name: str) -> iWorld: ...

    def list_worlds(self) -> list[WorldInfo]: ...

    async def remove_world(self, world_id: UUID) -> None: ...

    async def fork_world(
        self,
        source_world_id: UUID,
        name: str | None,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iWorld: ...

    async def discover_worlds(self) -> list[UUID]: ...

    async def shutdown(self) -> None: ...


# ─────────────────────────────────────────────────────────────────────────────
# Registry (durable metadata catalog)
# ─────────────────────────────────────────────────────────────────────────────


class iWorldRegistry(Protocol):
    """File-backed catalog of world metadata. Pure repository."""

    def get(self, world_id: UUID | str) -> dict[str, Any] | None: ...
    def upsert(self, world_id: UUID | str, entry: dict[str, Any]) -> None: ...
    def delete(self, world_id: UUID | str) -> None: ...
    def list_entries(self) -> list[dict[str, Any]]: ...


# ─────────────────────────────────────────────────────────────────────────────
# Command path
# ─────────────────────────────────────────────────────────────────────────────


class iCommandBroker(Protocol):
    """Per-world command queue plus history. Authorization is the caller's job."""

    async def enqueue(self, world_id: str | UUID, cmd: Command, ctx: ActorCtx | None) -> None: ...
    async def enqueue_bulk(
        self, world_id: str | UUID, cmds: list[Command], ctx: ActorCtx | None
    ) -> None: ...
    async def dequeue_due(
        self, world_id: str | UUID, tick: int, max_items: int | None = None
    ) -> list[Command]: ...
    async def ack(self, cmd_ids: list[UUID]) -> None: ...
    async def remove(self, world_id: str | UUID, cmd_id: UUID) -> None: ...
    async def peek(self, world_id: str | UUID, max_items: int | None = None) -> list[Command]: ...
    async def get_pending_count(self, world_id: str | UUID | None = None) -> int: ...
    async def get_history(self, world_id: str | UUID, limit: int = 100) -> list[Command]: ...
    async def clear(self, world_id: str | UUID | None = None) -> None: ...


class iCommandService(Protocol):
    """User-facing command submission and per-tick application."""

    async def submit(self, world_id: str | UUID, cmd: Command, ctx: ActorCtx) -> UUID: ...
    async def submit_batch(
        self, world_id: str | UUID, cmds: list[Command], ctx: ActorCtx
    ) -> list[UUID]: ...
    async def drain_and_apply(self, world_id: str | UUID, tick: int) -> list[Command]: ...


# ─────────────────────────────────────────────────────────────────────────────
# Simulation
# ─────────────────────────────────────────────────────────────────────────────


class iSimulationService(Protocol):
    """Drives the tick loop. Drains queued commands, advances the world."""

    async def step(self, world_id: UUID, run_config: RunConfig, **input_kwargs: Any) -> int: ...
    async def run(
        self, world_id: UUID, run_config: RunConfig, **input_kwargs: Any
    ) -> RunResult: ...


# ─────────────────────────────────────────────────────────────────────────────
# Query (read facade — currently a stub; full design pending)
# ─────────────────────────────────────────────────────────────────────────────


class iQueryService(Protocol):
    """External read facade. The full committed-snapshot contract will land
    in a follow-up PR (see redesign doc in ``docs/reports/``)."""

    async def get_world_state(self, world_id: UUID, tick: int | None = None) -> WorldSnapshot: ...
    async def get_entity(self, world_id: UUID, entity_id: int, tick: int | None = None) -> dict: ...
    async def get_components(
        self,
        world_id: UUID,
        component_types: list[str],
        entity_ids: list[int] | None = None,
    ) -> dict: ...
    async def get_command_history(self, world_id: UUID, limit: int = 100) -> list[Command]: ...
