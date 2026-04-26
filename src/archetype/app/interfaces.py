# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Application service contracts.

Every protocol here represents a service boundary visible to the
``ServiceContainer``.  Internal composition (factories, registries,
orchestrators) lives in the implementation modules, not here.

Service dependency graph::

    iStorageService                       (leaf)
        ↑             ↑
    iWorldService     iQueryService
    (storage)         (storage)
        ↑
    iMutationService  iSimulationService
    (worlds)          (worlds)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from daft import DataFrame

if TYPE_CHECKING:
    from uuid_utils import UUID

    from archetype.core.component import Component
    from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
    from archetype.core.interfaces import ArchetypeSignature, iAsyncStore, iAsyncSystem, iWorld

    from archetype.app.models import (
        EpisodeConfig,
        EpisodeResult,
        RolloutConfig,
        RolloutResult,
        RunResult,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Services
# ═══════════════════════════════════════════════════════════════════════════════


class iStorageService(Protocol):
    """Creates and pools async stores. Manages storage lifecycle.

    Leaf service — no required service dependencies.
    """

    async def get_or_create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iAsyncStore: ...

    async def shutdown(self) -> None: ...


class iWorldService(Protocol):
    """World lifecycle management.

    Internally owns WorldFactory, WorldRegistry, WorldOrchestrator.

    Depends on:
      - ``iStorageService`` for resolving StorageConfig → concrete store
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

    def list_worlds(self) -> list[iWorld]: ...

    async def fork_world(
        self,
        source_world_id: UUID | str,
        name: str | None = None,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> iWorld: ...

    async def destroy_world(self, world_id: UUID | str) -> None: ...

    async def shutdown(self) -> None: ...


class iMutationService(Protocol):
    """Mutates world contents: entities, components, processors.

    Depends on:
      - ``iWorldService`` for world lookup
    """

    def __init__(self, world_service: iWorldService) -> None: ...

    async def create_entity(
        self, world_id: str | UUID, components: list[Component]
    ) -> int: ...

    async def remove_entity(self, world_id: str | UUID, entity_id: int) -> None: ...

    async def add_components(
        self, world_id: str | UUID, entity_id: int, components: list[Component]
    ) -> None: ...

    async def remove_components(
        self, world_id: str | UUID, entity_id: int, component_types: list[type[Component]]
    ) -> None: ...

    async def add_processor(self, world_id: str | UUID, processor: ...) -> None: ...

    async def remove_processor(self, world_id: str | UUID, proc_type: ...) -> None: ...


class iSimulationService(Protocol):
    """Execution engine. Drives world stepping, episodes, and rollouts.

    Owns execution semantics including run_episode and run_rollout.
    Internal forks inside run_rollout go through iWorldService directly.

    Depends on:
      - ``iWorldService`` for world lookup and forking
    """

    def __init__(self, world_service: iWorldService) -> None: ...

    async def step(
        self, world_id: str | UUID, run_config: RunConfig, **input_kwargs
    ) -> None: ...

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

    Depends on:
      - ``iStorageService`` for store resolution
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
    ) -> DataFrame: ...

    async def list_signatures(
        self,
        storage_config: StorageConfig | None = None,
    ) -> list[ArchetypeSignature]: ...



# ═══════════════════════════════════════════════════════════════════════════════
# Command broker — pure queue. No RBAC. No ActorCtx.
# ═══════════════════════════════════════════════════════════════════════════════


class iCommandBroker(Protocol):
    """Priority queue + history of submitted commands.

    Pure queue. RBAC, quotas, and audit emission happen in ``iCommandService``
    *before* commands reach the broker. The broker only stores, orders, and
    yields commands.

    Concurrency: implementations MUST be safe for concurrent enqueue/dequeue
    from multiple coroutines.
    """

    async def enqueue(self, world_id: str | UUID, cmd: Command) -> None:
        """Enqueue one command for a world."""
        ...

    async def enqueue_bulk(
        self, world_id: str | UUID, cmds: list[Command]
    ) -> None:
        """Enqueue multiple commands atomically."""
        ...

    async def dequeue(
        self, world_id: str | UUID, max_items: int | None = None
    ) -> list[Command]:
        """Pop all pending commands for a world (regardless of tick)."""
        ...

    async def dequeue_due(
        self, world_id: str | UUID, tick: int, limit: int | None = None
    ) -> list[Command]:
        """Pop commands where ``cmd.tick <= tick``, ordered by (tick, priority, seq)."""
        ...

    async def ack(self, cmd_ids: list[UUID]) -> None:
        """Mark commands as successfully applied."""
        ...

    async def remove(self, world_id: str | UUID, cmd_id: UUID) -> None:
        """Remove a command from queue + pending. Idempotent. Preserves history."""
        ...

    async def peek(
        self, world_id: str | UUID, max_items: int | None = None
    ) -> list[Command]:
        """Peek without removing."""
        ...

    async def get_pending_count(
        self, world_id: str | UUID | None = None
    ) -> int:
        """Pending command count, optionally scoped to one world."""
        ...

    async def get_history(
        self, world_id: str | UUID, limit: int = 100
    ) -> list[Command]:
        """Recent enqueued commands (most recent last)."""
        ...

    async def clear(self, world_id: str | UUID | None = None) -> None:
        """Drop pending + history. Optionally scoped to one world."""
        ...


# ═══════════════════════════════════════════════════════════════════════════════
# Audit log — append-only record of accepted-and-applied commands.
# ═══════════════════════════════════════════════════════════════════════════════


class iAuditLog(Protocol):
    """Append-only record of accepted-and-applied commands.

    Schema is fixed by the platform spec § 3.3 Appendix A.5. Backed by
    storage via ``iStorageService``; rows are buffered and flushed in
    record-batch chunks.

    Payment-bearing columns (``signer_address``, ``tx_hash``,
    ``price_paid_atomic``, ``asset``, ``chain_id``, ``idempotency_key``)
    are nullable so non-paid commands persist with the same schema.

    Depends on:
      - ``iStorageService`` for backing store
    """

    def __init__(self, storage_service: iStorageService) -> None: ...

    async def record(self, row: AuditRow) -> None:
        """Buffer one audit row. May or may not flush immediately."""
        ...

    async def flush(self) -> None:
        """Force any buffered rows to the backing store."""
        ...

    async def query(
        self,
        world_id: str | UUID | None = None,
        *,
        tick_range: tuple[int, int] | None = None,
        actor_id: str | UUID | None = None,
        signer_address: str | None = None,
        idempotency_key: str | None = None,
        limit: int | None = None,
    ) -> DataFrame:
        """Query the audit log with optional filters. Read-only.

        ``signer_address`` and ``idempotency_key`` filters exist for
        Tier 2 (payment) reconciliation flows.
        """
        ...

    async def shutdown(self) -> None:
        """Flush + close. Idempotent."""
        ...


# ═══════════════════════════════════════════════════════════════════════════════
# Command service — the gate. The only ActorCtx-aware service.
# ═══════════════════════════════════════════════════════════════════════════════


class iCommandService(Protocol):
    """Policy enforcement point.

    Every external mutation, lifecycle operation, and read flows through
    here. The gate runs the same three steps for every method:

      1. ``guardrail_allow(cmd, ctx)`` — RBAC + quotas
         (Tier 2 will add ``validate_payment(cmd, ctx)`` at this point.)
      2. delegate to the appropriate underlying service
      3. emit one ``AuditRow`` via ``iAuditLog.record``

    Two paths through the gate:

      * **Direct (sync semantics)** — ``create_entity``, ``create_world``,
        ``query_archetype``, etc. The caller wants the operation applied
        now and the result returned. Most callers (RuntimeWorld, REST
        handlers under typical loads) use this path.

      * **Tick-deferred (queued)** — ``submit``, ``submit_batch``,
        ``submit_spawn``. The caller wants the command applied at a
        specified tick. ``SimulationService`` calls ``drain_and_apply``
        at the start of each ``step()`` to drain due commands.

    The gate runs identically on both paths.

    Depends on:
      - ``iMutationService``     for entity/component/processor mutations
      - ``iWorldService``        for world lifecycle
      - ``iSimulationService``   for step/run
      - ``iQueryService``        for reads
      - ``iCommandBroker``       for tick-deferred command submission
      - ``iAuditLog``            for accepted-and-applied audit emission
    """

    def __init__(
        self,
        mutations: iMutationService,
        worlds: iWorldService,
        simulation: iSimulationService,
        queries: iQueryService,
        broker: iCommandBroker,
        audit: iAuditLog,
    ) -> None: ...

    # ── Mutations (gated, sync-direct) ─────────────────────────────────────

    async def create_entity(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        components: list[Component],
    ) -> int: ...

    async def remove_entity(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
    ) -> None: ...

    async def add_components(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        components: list[Component],
    ) -> None: ...

    async def remove_components(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        component_types: list[type[Component]],
    ) -> None: ...

    async def add_processor(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        processor: iAsyncProcessor,
    ) -> None: ...

    async def remove_processor(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        proc_type: type[iAsyncProcessor],
    ) -> None: ...

    # ── Lifecycle (gated, sync-direct) ─────────────────────────────────────

    async def create_world(
        self,
        ctx: ActorCtx,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> iWorld: ...

    async def destroy_world(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> None: ...

    async def fork_world(
        self,
        ctx: ActorCtx,
        source_world_id: str | UUID,
        name: str | None = None,
        *,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> iWorld: ...

    # ── Simulation (gated, sync-direct) ────────────────────────────────────

    async def step(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> None: ...

    async def run(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> RunResult: ...

    # ── Queries (gated reads) ──────────────────────────────────────────────

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

    async def list_signatures(
        self,
        ctx: ActorCtx,
        storage_config: StorageConfig | None = None,
    ) -> list[ArchetypeSignature]: ...

    # ── Tick-deferred path (queued submit) ─────────────────────────────────

    async def submit(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        cmd: Command,
    ) -> UUID:
        """Gate, then enqueue for application at ``cmd.tick``."""
        ...

    async def submit_batch(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        cmds: list[Command],
    ) -> list[UUID]:
        """Gate (all-or-nothing on RBAC), then enqueue atomically."""
        ...

    async def submit_spawn(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        components: list[Component],
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> int:
        """Reserve an entity_id, enqueue the spawn, return the id immediately.

        Used when the caller needs the entity_id before the tick boundary
        but wants the materialization deferred.
        """
        ...

    async def drain_and_apply(
        self,
        world_id: str | UUID,
        tick: int,
    ) -> list[Command]:
        """Drain commands due at ``tick``, apply them, emit audit rows.

        Called by ``SimulationService`` at the start of each ``step()``.
        No ``ActorCtx`` parameter — commands carry their own context that
        was validated at submit time.
        """
        ...
