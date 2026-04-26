# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Application ports and service facades."""

from __future__ import annotations

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
        AsyncWorldDescriptor,
        SyncWorldDescriptor,
        iAsyncHookBus,
        iAsyncQueryManager,
        iAsyncStore,
        iAsyncSystem,
        iAsyncUpdateManager,
        iResourceContainer,
        iStore,
        iSyncHookBus,
        iSystem,
        iWorld,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Storage factories
# ─────────────────────────────────────────────────────────────────────────────


class iAsyncStorageFactory(Protocol):
    async def create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iAsyncStore: ...

    async def shutdown(self) -> None: ...


class iSyncStorageFactory(Protocol):
    def create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iStore: ...

    def shutdown(self) -> None: ...


class iStorageService(Protocol):
    async def get_backend(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]: ...

    async def shutdown(self) -> None: ...


# ─────────────────────────────────────────────────────────────────────────────
# World factories
# ─────────────────────────────────────────────────────────────────────────────


class iAsyncWorldFactory(Protocol):
    async def build_world_descriptor(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
        *,
        system: iAsyncSystem | None = None,
        resources: iResourceContainer | None = None,
        hooks: iAsyncHookBus | None = None,
    ) -> AsyncWorldDescriptor: ...

    async def create_world(
        self,
        world_config: WorldConfig,
        descriptor: AsyncWorldDescriptor,
    ) -> iWorld: ...


class iSyncWorldFactory(Protocol):
    def build_world_descriptor(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
        *,
        system: iSystem | None = None,
        resources: iResourceContainer | None = None,
        hooks: iSyncHookBus | None = None,
    ) -> SyncWorldDescriptor: ...

    def create_sync_world(
        self,
        world_config: WorldConfig,
        descriptor: SyncWorldDescriptor,
    ) -> iWorld: ...


class iWorldFactory(iAsyncWorldFactory, iSyncWorldFactory, Protocol):
    pass


# ─────────────────────────────────────────────────────────────────────────────
# World lifecycle
# ─────────────────────────────────────────────────────────────────────────────


class iWorldOrchestrator(Protocol):
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


class iWorldService(iWorldOrchestrator, Protocol):
    pass


# ─────────────────────────────────────────────────────────────────────────────
# Registry
# ─────────────────────────────────────────────────────────────────────────────


class iWorldRegistry(Protocol):
    def get(self, world_id: UUID | str) -> dict[str, Any] | None: ...
    def upsert(self, world_id: UUID | str, entry: dict[str, Any]) -> None: ...
    def delete(self, world_id: UUID | str) -> None: ...
    def list_entries(self) -> list[dict[str, Any]]: ...


# ─────────────────────────────────────────────────────────────────────────────
# Command ports
# ─────────────────────────────────────────────────────────────────────────────


class iCommandBroker(Protocol):
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


class iCommandSubmission(Protocol):
    async def submit(self, world_id: str | UUID, cmd: Command, ctx: ActorCtx) -> UUID: ...

    async def submit_batch(
        self, world_id: str | UUID, cmds: list[Command], ctx: ActorCtx
    ) -> list[UUID]: ...

    async def submit_spawn(
        self,
        world_id: str | UUID,
        components: list[Any],
        ctx: ActorCtx,
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> int: ...


class iCommandDrain(Protocol):
    async def drain_and_apply(self, world_id: str | UUID, tick: int) -> list[Command]: ...


class iCommandHistory(Protocol):
    async def get_command_history(
        self, world_id: str | UUID, limit: int = 100
    ) -> list[Command]: ...


class iCommandService(iCommandSubmission, iCommandDrain, Protocol):
    pass


class iWorldRuntimeLookup(Protocol):
    def get_world(self, world_id: UUID) -> iWorld: ...
    def list_worlds(self) -> list[WorldInfo]: ...


class iWorldMutationPort(Protocol):
    def get_world(self, world_id: UUID) -> iWorld: ...
    async def create_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> iWorld: ...
    async def remove_world(self, world_id: UUID) -> None: ...
    async def fork_world(
        self,
        source_world_id: UUID,
        name: str | None,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iWorld: ...


# ─────────────────────────────────────────────────────────────────────────────
# Simulation facade
# ─────────────────────────────────────────────────────────────────────────────


class iSimulationService(Protocol):
    async def step(self, world_id: UUID, run_config: RunConfig, **input_kwargs: Any) -> int: ...
    async def run(
        self, world_id: UUID, run_config: RunConfig, **input_kwargs: Any
    ) -> RunResult: ...


# ─────────────────────────────────────────────────────────────────────────────
# Query facade
# ─────────────────────────────────────────────────────────────────────────────


class iQueryService(Protocol):
    async def get_world_state(self, world_id: UUID, tick: int | None = None) -> WorldSnapshot: ...
    async def get_entity(self, world_id: UUID, entity_id: int, tick: int | None = None) -> dict: ...
    async def get_components(
        self,
        world_id: UUID,
        component_types: list[str],
        entity_ids: list[int] | None = None,
    ) -> dict: ...
    async def get_command_history(self, world_id: UUID, limit: int = 100) -> list[Command]: ...
