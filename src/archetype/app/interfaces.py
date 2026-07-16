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

"""Application service contracts.

Every protocol here represents a service boundary visible to the
``ServiceContainer``.  Internal composition (factories, registries,
orchestrators) lives in the implementation modules, not here.

Service dependency graph (providers point to consumers)::

    iStorageService -> iWorldService -> iMutationService
                                  +---> iSimulationService
                    -> iQueryService -> iEvalService
                    -> iFactService
                    -> iAuditLog

    iWorldService + iMutationService + iSimulationService + iQueryService
        + iAuditLog + iCommandBroker -> iCommandService (the gate)
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Protocol

from daft import DataFrame

if TYPE_CHECKING:
    from pathlib import Path

    from uuid_utils import UUID

    from archetype.app.auth.models import ActorCtx
    from archetype.app.facts import FactProcessor, FactWriteReceipt
    from archetype.app.iceberg import IcebergCatalogContext
    from archetype.app.models import (
        AuditRow,
        Command,
        EpisodeConfig,
        EpisodeResult,
        HookInfo,
        ProcessorInfo,
        ResourceInfo,
        RolloutConfig,
        RolloutResult,
        RunResult,
        WorldInfo,
    )
    from archetype.core.component import Component
    from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
    from archetype.core.interfaces import (
        ArchetypeSignature,
        iAsyncProcessor,
        iAsyncStore,
        iAsyncSystem,
        iWorld,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Services (no ActorCtx — these do the work)
# ═══════════════════════════════════════════════════════════════════════════════


class iStorageService(Protocol):
    """Creates and pools async stores. Manages storage lifecycle."""

    async def get_or_create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iAsyncStore: ...

    async def get_iceberg_context(
        self,
        storage_config: StorageConfig,
    ) -> IcebergCatalogContext: ...

    async def shutdown(self) -> None: ...


class iWorldService(Protocol):
    """World lifecycle management.

    Depends on: ``iStorageService``
    """

    def __init__(self, storage_service: iStorageService) -> None: ...

    async def create_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> iWorld: ...

    def get_world(self, world_id: UUID) -> iWorld: ...
    def get_world_by_name(self, name: str) -> iWorld: ...
    def has_world(self, world_id: str | UUID) -> bool: ...
    def list_worlds(self) -> list[iWorld]: ...

    async def fork_world(
        self,
        source_world_id: UUID | str,
        name: str | None = None,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> iWorld: ...

    async def destroy_world(self, world_id: str | UUID) -> None: ...

    async def add_resource(self, world_id: str | UUID, resource: object) -> None: ...
    def add_hook(self, world_id: str | UUID, event_type, fn, *, mode: str = "blocking"): ...
    def remove_hook(self, world_id: str | UUID, handle) -> None: ...
    def list_processors(self, world_id: str | UUID) -> list: ...
    def list_hooks(self, world_id: str | UUID) -> list: ...
    def list_resources(self, world_id: str | UUID) -> list: ...

    async def shutdown(self) -> None: ...


class iMutationService(Protocol):
    """Mutates world contents: entities, components, processors.

    Depends on: ``iWorldService``
    """

    def __init__(self, world_service: iWorldService) -> None: ...

    async def create_entity(self, world_id: str | UUID, components: list[Component]) -> int: ...

    async def remove_entity(self, world_id: str | UUID, entity_id: int) -> None: ...

    async def update_entity(
        self, world_id: str | UUID, entity_id: int, components: list[Component]
    ) -> None: ...

    async def add_components(
        self, world_id: str | UUID, entity_id: int, components: list[Component]
    ) -> None: ...

    async def remove_components(
        self, world_id: str | UUID, entity_id: int, component_types: list[type[Component]]
    ) -> None: ...

    async def add_processor(self, world_id: str | UUID, processor: iAsyncProcessor) -> None: ...
    async def remove_processor(
        self, world_id: str | UUID, proc_type: type[iAsyncProcessor]
    ) -> None: ...


class iSimulationService(Protocol):
    """Execution engine. Drives stepping, episodes, and rollouts.

    Depends on: ``iWorldService``
    """

    def __init__(self, world_service: iWorldService) -> None: ...

    async def step(self, world_id: str | UUID, run_config: RunConfig, **input_kwargs) -> None: ...
    async def run(
        self, world_id: str | UUID, run_config: RunConfig, **input_kwargs
    ) -> RunResult: ...
    async def run_episode(
        self, world_id: str | UUID, config: EpisodeConfig, **input_kwargs
    ) -> EpisodeResult: ...
    async def run_rollout(
        self, world_id: str | UUID, config: RolloutConfig, **input_kwargs
    ) -> RolloutResult: ...


class iQueryService(Protocol):
    """Direct read path to storage. Any world, any run, any tick.

    Depends on: ``iStorageService``
    """

    def __init__(self, storage_service: iStorageService) -> None: ...

    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type[Component]] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> DataFrame: ...

    async def query_components(
        self,
        components: list[type[Component]],
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
        visibility_tokens: list[str] | None = None,
    ) -> DataFrame: ...

    async def list_signatures(
        self, storage_config: StorageConfig | None = None
    ) -> list[ArchetypeSignature]: ...


class iEvalService(Protocol):
    """Dataframe-first evaluation over persisted component rows.

    Depends on: ``iQueryService``
    """

    def __init__(self, query_service: iQueryService) -> None: ...

    async def query_components(
        self,
        components: Sequence[type[Component]],
        *,
        world_id: str | UUID,
        run_id: str | UUID,
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> DataFrame: ...

    async def query_episode(
        self,
        episode: EpisodeResult,
        *,
        components: Sequence[type[Component]],
        run_id: str | UUID | None = None,
        storage_config: StorageConfig | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> DataFrame: ...

    async def query_trajectory_component(
        self,
        component: type[Component],
        *,
        world_id: str | UUID,
        run_id: str | UUID,
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
        trajectory_ids: Sequence[str] | None = None,
        episode_ids: Sequence[str] | None = None,
        rollout_ids: Sequence[str] | None = None,
        task_ids: Sequence[str] | None = None,
        trial_idxs: Sequence[int] | None = None,
    ) -> DataFrame: ...

    async def run_graders(
        self,
        df: DataFrame,
        graders: Sequence[Callable[[DataFrame], object]],
    ) -> list[object]: ...

    async def grade_trajectory_component(
        self,
        component: type[Component],
        *,
        world_id: str | UUID,
        run_id: str | UUID,
        graders: Sequence[Callable[[DataFrame], object]],
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
        trajectory_ids: Sequence[str] | None = None,
        episode_ids: Sequence[str] | None = None,
        rollout_ids: Sequence[str] | None = None,
        task_ids: Sequence[str] | None = None,
        trial_idxs: Sequence[int] | None = None,
    ) -> list[object]: ...


class iFactService(Protocol):
    """Typed external facts stored alongside a world's Iceberg tables."""

    def __init__(
        self,
        storage_service: iStorageService,
        world_service: iWorldService,
    ) -> None: ...

    async def ingest_files(
        self,
        world_id: str,
        paths: str | Path | list[str | Path],
        processor: FactProcessor,
        *,
        storage_config: StorageConfig | None = None,
    ) -> FactWriteReceipt: ...

    async def write_facts(
        self,
        world_id: str,
        table_name: str,
        facts: DataFrame,
        *,
        storage_config: StorageConfig | None = None,
    ) -> FactWriteReceipt: ...

    async def read_facts(
        self,
        world_id: str,
        table_name: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame: ...


# ═══════════════════════════════════════════════════════════════════════════════
# Command broker — pure queue. No RBAC. No ActorCtx.
# ═══════════════════════════════════════════════════════════════════════════════


class iCommandBroker(Protocol):
    """Priority queue + history. Pure plumbing."""

    async def enqueue(self, world_id: str | UUID, cmd: Command) -> None: ...
    async def enqueue_bulk(self, world_id: str | UUID, cmds: list[Command]) -> None: ...
    async def dequeue(
        self, world_id: str | UUID, max_items: int | None = None
    ) -> list[Command]: ...
    async def dequeue_due(
        self, world_id: str | UUID, tick: int, limit: int | None = None
    ) -> list[Command]: ...
    async def ack(self, cmd_ids: list[UUID]) -> None: ...
    async def remove(self, world_id: str | UUID, cmd_id: UUID) -> None: ...
    async def peek(self, world_id: str | UUID, max_items: int | None = None) -> list[Command]: ...
    async def get_pending_count(self, world_id: str | UUID | None = None) -> int: ...
    async def get_history(self, world_id: str | UUID, limit: int = 100) -> list[Command]: ...
    async def clear(self, world_id: str | UUID | None = None) -> None: ...


# ═══════════════════════════════════════════════════════════════════════════════
# Audit log — append-only record
# ═══════════════════════════════════════════════════════════════════════════════


class iAuditLog(Protocol):
    """Append-only record of accepted-and-applied commands.

    Depends on: ``iStorageService``
    """

    def __init__(self, storage_service: iStorageService) -> None: ...

    async def record(self, row: AuditRow) -> None: ...
    async def flush(self) -> None: ...

    async def query(
        self,
        world_id: str | UUID | None = None,
        *,
        tick_range: tuple[int, int] | None = None,
        actor_id: str | UUID | None = None,
        idempotency_key: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> DataFrame: ...

    async def shutdown(self) -> None: ...


# ═══════════════════════════════════════════════════════════════════════════════
# Command service — the gate. The only ActorCtx-aware service.
# ═══════════════════════════════════════════════════════════════════════════════


class iCommandService(Protocol):
    """Policy enforcement point. Every external operation flows through here.

    Depends on: iMutationService, iWorldService, iSimulationService,
                iQueryService, iFactService, iCommandBroker, iAuditLog
    """

    def __init__(
        self,
        mutations: iMutationService,
        worlds: iWorldService,
        simulation: iSimulationService,
        queries: iQueryService,
        broker: iCommandBroker,
        audit: iAuditLog,
        facts: iFactService | None = None,
    ) -> None: ...

    # ── Mutations (gated, direct) ─────────────────────────────────────────

    async def create_entity(
        self, ctx: ActorCtx, world_id: str | UUID, components: list[Component]
    ) -> int: ...
    async def remove_entity(self, ctx: ActorCtx, world_id: str | UUID, entity_id: int) -> None: ...
    async def update_entity(
        self, ctx: ActorCtx, world_id: str | UUID, entity_id: int, components: list[Component]
    ) -> None: ...
    async def add_components(
        self, ctx: ActorCtx, world_id: str | UUID, entity_id: int, components: list[Component]
    ) -> None: ...
    async def remove_components(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        component_types: list[type[Component]],
    ) -> None: ...
    async def add_processor(
        self, ctx: ActorCtx, world_id: str | UUID, processor: iAsyncProcessor
    ) -> None: ...
    async def remove_processor(
        self, ctx: ActorCtx, world_id: str | UUID, proc_type: type[iAsyncProcessor]
    ) -> None: ...
    async def ingest_files(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        paths: str | Path | list[str | Path],
        processor: FactProcessor,
        *,
        storage_config: StorageConfig | None = None,
    ) -> FactWriteReceipt: ...
    async def write_facts(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        table_name: str,
        facts: DataFrame,
        *,
        storage_config: StorageConfig | None = None,
    ) -> FactWriteReceipt: ...
    async def query_facts(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        table_name: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame: ...

    # ── Lifecycle (gated, direct) — returns WorldInfo, never iWorld ────────

    async def create_world(
        self,
        ctx: ActorCtx,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> WorldInfo: ...
    async def fork_world(
        self,
        ctx: ActorCtx,
        source_world_id: str | UUID,
        name: str | None = None,
        *,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> WorldInfo: ...
    async def destroy_world(self, ctx: ActorCtx, world_id: str | UUID) -> None: ...
    async def get_world_info(self, ctx: ActorCtx, world_id: str | UUID) -> WorldInfo: ...
    async def list_worlds(self, ctx: ActorCtx) -> list[WorldInfo]: ...

    # ── Simulation (gated, direct) ────────────────────────────────────────

    async def step(
        self, ctx: ActorCtx, world_id: str | UUID, run_config: RunConfig, **input_kwargs
    ) -> int: ...
    async def run(
        self, ctx: ActorCtx, world_id: str | UUID, run_config: RunConfig, **input_kwargs
    ) -> RunResult: ...
    async def run_episode(
        self, ctx: ActorCtx, world_id: str | UUID, config: EpisodeConfig, **input_kwargs
    ) -> EpisodeResult: ...
    async def run_rollout(
        self, ctx: ActorCtx, world_id: str | UUID, config: RolloutConfig, **input_kwargs
    ) -> RolloutResult: ...

    # ── Resource / hook attachment (gated) ─────────────────────────────────

    async def add_resource(self, ctx: ActorCtx, world_id: str | UUID, resource: object) -> None: ...
    async def add_hook(
        self, ctx: ActorCtx, world_id: str | UUID, event_type, fn, *, mode: str = "blocking"
    ): ...
    async def remove_hook(self, ctx: ActorCtx, world_id: str | UUID, handle) -> None: ...

    # ── Read introspection (gated) ────────────────────────────────────────

    async def list_processors(self, ctx: ActorCtx, world_id: str | UUID) -> list[ProcessorInfo]: ...
    async def list_hooks(self, ctx: ActorCtx, world_id: str | UUID) -> list[HookInfo]: ...
    async def list_resources(self, ctx: ActorCtx, world_id: str | UUID) -> list[ResourceInfo]: ...
    async def get_audit_history(
        self,
        ctx: ActorCtx,
        world_id: str | UUID | None = None,
        *,
        tick_range: tuple[int, int] | None = None,
        actor_id: str | UUID | None = None,
        idempotency_key: str | None = None,
        limit: int | None = None,
    ) -> DataFrame: ...

    # ── Queries (gated reads) ─────────────────────────────────────────────

    async def query_archetype(
        self,
        ctx: ActorCtx,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type[Component]] | None = None,
    ) -> DataFrame: ...
    async def query_components(
        self,
        ctx: ActorCtx,
        components: list[type[Component]],
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ) -> DataFrame: ...
    async def list_signatures(
        self, ctx: ActorCtx, storage_config: StorageConfig | None = None
    ) -> list[ArchetypeSignature]: ...

    # ── Tick-deferred path (queued) ───────────────────────────────────────

    async def submit(self, ctx: ActorCtx, world_id: str | UUID, cmd: Command) -> UUID: ...
    async def submit_batch(
        self, ctx: ActorCtx, world_id: str | UUID, cmds: list[Command]
    ) -> list[UUID]: ...
    async def submit_spawn(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        components: list[Component],
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> int: ...
    async def drain_and_apply(self, world_id: str | UUID, tick: int) -> list[Command]: ...
