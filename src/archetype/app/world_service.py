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

"""
World Service

WorldFactory  — store → world (pure construction)
WorldRegistry — holds live worlds (lookup, insert, remove)
WorldOrchestrator — lifecycle (create, fork, remove, discover)
WorldService  — facade (bridges StorageService into the orchestrator)
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING

from uuid_utils import UUID, uuid7

from archetype.app._catalog import SignatureRecord, WorldRecord, schema_fingerprint
from archetype.app._commit import CatalogCommitCoordinator
from archetype.app.models import WorldInfo
from archetype.app.storage_service import StorageService
from archetype.core.aio import (
    AsyncQueryManager,
    AsyncSystem,
    AsyncUpdateManager,
    AsyncWorld,
)
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import CacheConfig, StorageConfig, WorldConfig
from archetype.core.hooks import HookRegistry
from archetype.core.interfaces import iAsyncStore, iAsyncSystem
from archetype.core.lineage import load_lineage, persist_lineage
from archetype.core.resources import Resources

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _component_classes_by_name() -> dict[str, list[type[Component]]]:
    """All importable Component subclasses, keyed by class name.

    Distinct classes may share a name across modules (test doubles, forks of
    an old component). Resume disambiguates by schema fingerprint — identity
    is the schema, never the name alone.
    """
    classes: dict[str, list[type[Component]]] = {}
    stack: list[type[Component]] = list(Component.__subclasses__())
    seen: set[type[Component]] = set()
    while stack:
        cls = stack.pop()
        if cls in seen:
            continue
        seen.add(cls)
        stack.extend(cls.__subclasses__())
        classes.setdefault(cls.__name__, []).append(cls)
    return classes


@dataclass(frozen=True)
class _ResumeSnapshot:
    """Durable state a resume reconstructs a world from."""

    directory: dict[int, SignatureRecord]
    next_entity_id: int
    resume_tick: int


def _writer_holder() -> str:
    """Diagnostic fence-holder label; the epoch is the actual authority."""
    return f"{socket.gethostname()}:{os.getpid()}"


def _world_info_from_record(record: WorldRecord) -> WorldInfo:
    """Catalog record → the existing gate boundary descriptor."""
    return WorldInfo(
        world_id=record.world_id,
        name=record.name,
        tick=record.tick_head,
        run_id=record.run_id,
    )


# ─────────────────────────────────────────────────────────────────────────────
# WorldFactory
# ─────────────────────────────────────────────────────────────────────────────


class WorldFactory:
    """Takes a concrete store, returns a concrete world.

    Resolves all world dependencies (querier, updater, system, resources,
    hooks) and passes them as keyword args to the world constructor.
    """

    def create_async_world(
        self,
        store: iAsyncStore,
        config: WorldConfig,
        system: iAsyncSystem | None = None,
    ) -> AsyncWorld:
        return AsyncWorld(
            world_id=str(config.world_id),
            name=config.name,
            querier=AsyncQueryManager(store=store),
            updater=AsyncUpdateManager(store=store),
            system=system or AsyncSystem(),
            resources=Resources(),
            hooks=HookRegistry(),
            run_id=config.run_id,
            tick=config.tick,
            next_entity_id=config.next_entity_id,
            entity2sig=dict(config.entity2sig) if config.entity2sig else None,
            spawn_cache=dict(config.spawn_cache) if config.spawn_cache else None,
            despawn_cache=dict(config.despawn_cache) if config.despawn_cache else None,
            lineage=list(config.lineage) if config.lineage else None,
        )


# ─────────────────────────────────────────────────────────────────────────────
# WorldRegistry
# ─────────────────────────────────────────────────────────────────────────────


class WorldRegistry:
    """Holds live worlds. Lookup by ID or name, insert, remove, list."""

    def __init__(self) -> None:
        self._worlds: dict[str, AsyncWorld] = {}
        self._names: dict[str, str] = {}

    def insert(self, world: AsyncWorld) -> None:
        wid = str(world.world_id)
        self._worlds[wid] = world
        if world.name:
            self._names[world.name] = wid

    def get(self, world_id: UUID | str) -> AsyncWorld:
        wid = str(world_id)
        if wid not in self._worlds:
            raise KeyError(f"World with ID '{world_id}' not found.")
        return self._worlds[wid]

    def get_by_name(self, name: str) -> AsyncWorld:
        if name not in self._names:
            raise KeyError(f"World with name '{name}' not found.")
        return self.get(self._names[name])

    def remove(self, world_id: UUID | str) -> None:
        wid = str(world_id)
        world = self._worlds.pop(wid, None)
        if world and world.name:
            self._names.pop(world.name, None)

    def all(self) -> list[AsyncWorld]:
        return list(self._worlds.values())

    def has(self, world_id: UUID | str) -> bool:
        return str(world_id) in self._worlds

    def has_name(self, name: str) -> bool:
        return name in self._names


# ─────────────────────────────────────────────────────────────────────────────
# WorldOrchestrator
# ─────────────────────────────────────────────────────────────────────────────


class WorldOrchestrator:
    """Manages the full lifecycle of all worlds.

    Depends on WorldFactory (construction) and WorldRegistry (storage of
    live worlds). Does NOT depend on StorageService or CommandBroker.
    """

    def __init__(
        self,
        factory: WorldFactory,
        registry: WorldRegistry,
    ) -> None:
        self._factory = factory
        self._registry = registry

    def create_world(
        self,
        store: iAsyncStore,
        config: WorldConfig,
        system: iAsyncSystem | None = None,
    ) -> AsyncWorld:
        """Create a world from a concrete store and config.

        Assigns a world_id if not set. Validates name uniqueness.
        Registers the world in the registry.
        """
        world_id = config.world_id or uuid7()
        if config.world_id is None:
            config = config.model_copy(update={"world_id": world_id})

        if self._registry.has(world_id):
            return self._registry.get(world_id)

        if config.name and self._registry.has_name(config.name):
            raise ValueError(f"World with name '{config.name}' already exists.")

        world = self._factory.create_async_world(store, config, system=system)
        self._registry.insert(world)
        return world

    def get_world(self, world_id: str | UUID) -> AsyncWorld:
        return self._registry.get(world_id)

    def get_world_by_name(self, name: str) -> AsyncWorld:
        return self._registry.get_by_name(name)

    def has_world(self, world_id: str | UUID) -> bool:
        return self._registry.has(world_id)

    def list_worlds(self) -> list[AsyncWorld]:
        return self._registry.all()

    def fork_world(
        self,
        store: iAsyncStore,
        source_world_id: UUID | str,
        name: str | None = None,
    ) -> AsyncWorld:
        """Fork a world: create a new world with a snapshot of the source's state.

        Per-field semantics:
          generated:    world_id (uuid7), run_id (uuid7)
          deep-copied:  tick, next_entity_id, entity2sig, spawn_cache, despawn_cache
          shared:       resources (same instance), processors (same instances)
          lineage:      source's lineage plus a segment covering the source's
                        materialized rows, so the fork reads pre-fork ticks
                        from its ancestry (append-only store: those rows are
                        immutable history)
        """
        source = self._registry.get(source_world_id)
        if not isinstance(source, AsyncWorld):
            raise TypeError("Can only fork AsyncWorld instances")

        lineage = list(source.lineage)
        if source.tick > 0:
            # The source has written rows for ticks 0..tick-1 under its own
            # run; the fork's writes start at `tick`.
            lineage.append((str(source.world_id), str(source.run_id), source.tick - 1))

        fork_config = WorldConfig(
            name=name,
            # Minted here, not at first step: persist_lineage keys the fork's
            # provenance rows by run_id, so the id must exist at fork time.
            run_id=str(uuid7()),
            tick=source.tick,
            next_entity_id=source.next_entity_id,
            entity2sig=dict(source.entity2sig),
            spawn_cache={sig: list(rows) for sig, rows in source.spawn_cache.items()},
            despawn_cache={sig: list(ids) for sig, ids in source.despawn_cache.items()},
            lineage=lineage,
        )

        fork = self._factory.create_async_world(store, fork_config)

        # Share resources and processors from source
        fork.resources = source.resources
        fork.system.processors = list(source.system.processors)

        # Deep-copy hooks registry (independent post-fork)
        for event_type, _handle, fn, mode in source.hooks.items():
            fork.hooks.add(event_type, fn, mode=mode)

        self._registry.insert(fork)
        return fork

    async def destroy_world(self, world_id: UUID | str) -> None:
        """Destroy a world: fire OnDestroy, then remove from registry.

        Idempotent — returns silently if world_id is not in the registry.
        In-memory cleanup only. Storage and audit rows are preserved
        (append-only invariant).
        """
        from archetype.core.hooks import OnDestroy

        if not self._registry.has(world_id):
            return

        world = self._registry.get(world_id)
        if isinstance(world, AsyncWorld):
            await world.hooks.fire(OnDestroy(world_id=world.world_id))

        self._registry.remove(world_id)


# ─────────────────────────────────────────────────────────────────────────────
# WorldService
# ─────────────────────────────────────────────────────────────────────────────


class WorldService:
    """Service-layer facade. Bridges StorageService into the WorldOrchestrator.

    Only external dependency: StorageService.
    Internally owns: WorldFactory, WorldRegistry, WorldOrchestrator.
    """

    def __init__(self, storage_service: StorageService) -> None:
        self._storage_service = storage_service
        self._factory = WorldFactory()
        self._registry = WorldRegistry()
        self._orchestrator = WorldOrchestrator(self._factory, self._registry)
        # Records the storage/cache config that backs each world so fork_world
        # can default to "same store as source" per world-lifecycle.md § 4.5.
        self._storage_configs: dict[str, tuple[StorageConfig, CacheConfig | None]] = {}
        self._create_locks: dict[str, asyncio.Lock] = {}

    async def create_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> AsyncWorld:
        """Resolve storage, then delegate world creation to the orchestrator."""
        if config.world_id is None:
            config = config.model_copy(update={"world_id": uuid7()})
        world_id = str(config.world_id)
        lock = self._create_locks.setdefault(world_id, asyncio.Lock())

        async with lock:
            if self._orchestrator.has_world(world_id):
                return self._orchestrator.get_world(world_id)

            if storage_config is None:
                storage_config = StorageConfig()

            store = await self._storage_service.get_or_create_store(storage_config, cache_config)
            world = self._orchestrator.create_world(store, config, system=system)
            # Durable identity is authoritative (issue #272): registration failure
            # fails the create, loudly — a world the catalog cannot describe would
            # be undiscoverable after this process exits. Unwind the registry so
            # the failed create leaves no live, mutable world behind.
            catalog = self._storage_service.get_control_catalog(storage_config)
            try:
                await catalog.register_world(
                    WorldRecord(
                        world_id=str(world.world_id),
                        name=world.name,
                        run_id=str(world.run_id) if world.run_id else None,
                        parent_world_id=None,
                        status="active",
                        tick_head=world.tick,
                    )
                )
                # Fenced writer (issue #273): this process is the world's one live
                # writer; every tick it commits publishes under this epoch.
                epoch = await catalog.acquire_fence(str(world.world_id), _writer_holder())
            except Exception:
                self._registry.remove(world.world_id)
                raise
            world.commit_coordinator = CatalogCommitCoordinator(catalog, epoch=epoch)
            self._storage_configs[str(world.world_id)] = (storage_config, cache_config)
            return world

    def get_world(self, world_id: UUID) -> AsyncWorld:
        return self._orchestrator.get_world(world_id)

    def storage_record(
        self, world_id: str | UUID
    ) -> tuple[StorageConfig, CacheConfig | None] | None:
        """The storage/cache config backing a world, or None if unknown.

        This is how readers locate a world's rows without being told the
        storage config out of band. Records outlive destroy_world: storage is
        append-only and destroyed worlds remain queryable.
        """
        return self._storage_configs.get(str(world_id))

    def get_world_by_name(self, name: str) -> AsyncWorld:
        return self._orchestrator.get_world_by_name(name)

    def has_world(self, world_id: str | UUID) -> bool:
        return self._orchestrator.has_world(world_id)

    def list_worlds(self) -> list[AsyncWorld]:
        return self._orchestrator.list_worlds()

    async def fork_world(
        self,
        source_world_id: UUID | str,
        name: str | None = None,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> AsyncWorld:
        """Fork a world. Inherits source's storage when no override is given.

        Per ``docs/guide/world-lifecycle.md`` § 4.5, the fork writes to the
        same physical store as the source by default; an explicit
        ``storage_config`` argument routes the fork to a different store.
        """
        source_record = self._storage_configs.get(str(source_world_id))
        if storage_config is None:
            if source_record is not None:
                storage_config, source_cache = source_record
                if cache_config is None:
                    cache_config = source_cache
            else:
                storage_config = StorageConfig()
        elif source_record is not None and storage_config != source_record[0]:
            source = self._orchestrator.get_world(UUID(str(source_world_id)))
            if getattr(source, "tick", 0) > 0:
                # Lineage segments name rows in the source's store; a fork on
                # a different store cannot read them (world-lifecycle.md § 4.5).
                logger.warning(
                    "fork_world: explicit storage_config differs from source's; "
                    "the fork will not see the source's persisted history "
                    "(world %s, tick %d)",
                    source_world_id,
                    source.tick,
                )
        store = await self._storage_service.get_or_create_store(storage_config, cache_config)
        fork = self._orchestrator.fork_world(store, source_world_id, name=name)
        # Same authoritative-identity contract as create_world: a fork the
        # catalog cannot describe must not survive as a live world.
        catalog = self._storage_service.get_control_catalog(storage_config)
        try:
            await catalog.register_world(
                WorldRecord(
                    world_id=str(fork.world_id),
                    name=fork.name,
                    run_id=str(fork.run_id) if fork.run_id else None,
                    parent_world_id=str(source_world_id),
                    status="active",
                    tick_head=fork.tick,
                )
            )
            epoch = await catalog.acquire_fence(str(fork.world_id), _writer_holder())
        except Exception:
            self._registry.remove(fork.world_id)
            raise
        fork.commit_coordinator = CatalogCommitCoordinator(catalog, epoch=epoch)
        self._storage_configs[str(fork.world_id)] = (storage_config, cache_config)
        # Persist the fork's ancestor chain (append-only): provenance must
        # survive the fork being destroyed or the process restarting.
        await persist_lineage(
            store,
            world_id=str(fork.world_id),
            run_id=str(fork.run_id),
            tick=fork.tick,
            lineage=fork.lineage,
        )
        return fork

    async def destroy_world(self, world_id: UUID | str) -> None:
        """Destroy a world. In-memory cleanup only. Storage is preserved.

        The storage record is retained: destroyed worlds' rows are still in
        the store (append-only), and readers resolve them through
        storage_record(). The catalog marks status — append-only holds in
        the control plane too; nothing is deleted.
        """
        await self._orchestrator.destroy_world(world_id)
        record = self._storage_configs.get(str(world_id))
        if record is not None:
            catalog = self._storage_service.get_control_catalog(record[0])
            await catalog.set_world_status(str(world_id), "destroyed")

    # ── Durable discovery (issue #272) ───────────────────────────────────────

    async def discover_worlds(self, storage_config: StorageConfig) -> list[WorldInfo]:
        """Worlds recorded in a store's control catalog — works cold.

        Unlike ``list_worlds`` (live process registry), this answers from the
        durable catalog: a fresh process pointed at the same storage identity
        sees every world ever registered there, including destroyed ones
        (their rows are still queryable; append-only).
        """
        catalog = self._storage_service.get_control_catalog(storage_config)
        return [_world_info_from_record(record) for record in await catalog.list_worlds()]

    async def open_world_readonly(
        self, storage_config: StorageConfig, world_id: str | UUID
    ) -> WorldInfo:
        """Cold, read-only open: the world's durable descriptor.

        Returns the existing ``WorldInfo`` boundary type — identity, name,
        run, and (advisory until A2 manifests) tick head. Queryability comes
        through the ordinary gated query path with this info plus the same
        ``storage_config``. This never constructs a live mutable world:
        fenced mutable resume is a separate, A2-gated capability.
        """
        catalog = self._storage_service.get_control_catalog(storage_config)
        record = await catalog.get_world(str(world_id))
        if record is None:
            raise KeyError(f"world {world_id} is not recorded in catalog for {storage_config.uri}")
        return _world_info_from_record(record)

    async def open_world_mutable(
        self, storage_config: StorageConfig, world_id: str | UUID
    ) -> AsyncWorld:
        """Reconstruct a durable world and acquire its writer lease.

        The previous writer becomes stale and its next publish fails. Resume
        fails instead of returning a world whose state cannot be reconstructed
        faithfully.

        Processors, resources, and hooks are code rather than stored state and
        must be reinstalled by the caller.
        """
        wid = str(world_id)
        if self._orchestrator.has_world(wid):
            raise RuntimeError(
                f"world {wid} is already live in this process; resume is for cold worlds "
                "(re-fencing a world against itself would stale its own writer)"
            )

        catalog = self._storage_service.get_control_catalog(storage_config)
        record = await catalog.get_world(wid)
        if record is None:
            raise KeyError(f"world {wid} is not recorded in catalog for {storage_config.uri}")
        if record.status == "destroyed":
            raise RuntimeError(
                f"world {wid} is destroyed: queryable through the read paths, not resumable"
            )
        if record.name and self._registry.has_name(record.name):
            raise RuntimeError(
                f"a live world already holds the name {record.name!r}; cannot resume {wid}"
            )
        run_id = record.run_id
        if not run_id:
            raise RuntimeError(f"world {wid} has no recorded run; nothing to resume")

        store = await self._storage_service.get_or_create_store(storage_config, None)

        # Fork integrity: a fork record whose lineage rows are missing is
        # detectable corruption (atomic-visibility spec §6) — refuse.
        lineage = await load_lineage(store, world_id=wid, run_id=str(run_id))
        if record.parent_world_id and not lineage:
            raise RuntimeError(
                f"world {wid} records parent {record.parent_world_id} but has no "
                "persisted lineage rows; refusing to resume (corruption)"
            )

        # Preflight stable reconstruction failures before fencing. The world
        # may still advance before the authoritative post-fence snapshot.
        snapshot = await self._resume_snapshot(catalog, store, wid, str(run_id), lineage)
        self._resolve_live_signatures(snapshot.directory)

        # Fence, THEN take the authoritative snapshot. Post-fence the old
        # writer cannot publish (its epoch is stale), so the second read is
        # stable — no window where a late-published tick slips between the
        # snapshot and the fence.
        epoch = await catalog.acquire_fence(wid, _writer_holder())
        try:
            snapshot = await self._resume_snapshot(catalog, store, wid, str(run_id), lineage)
            entity2sig, _live_tables = self._resolve_live_signatures(snapshot.directory)

            world = self._factory.create_async_world(
                store,
                WorldConfig(
                    world_id=wid,
                    name=record.name,
                    run_id=str(run_id),
                    tick=snapshot.resume_tick,
                    next_entity_id=snapshot.next_entity_id,
                    entity2sig=entity2sig,
                    lineage=lineage or [],
                ),
            )
            world.commit_coordinator = CatalogCommitCoordinator(catalog, epoch=epoch)
            self._registry.insert(world)
        except Exception:
            logger.exception(
                "resume for world %s acquired fence epoch %d but failed before "
                "installing its replacement writer; the previous writer is now stale",
                wid,
                epoch,
            )
            raise
        self._storage_configs[wid] = (storage_config, None)
        logger.info(
            "resumed world %s at tick %d (epoch %d, %d entities)",
            wid,
            snapshot.resume_tick,
            epoch,
            len(entity2sig),
        )
        return world

    async def _resume_snapshot(
        self,
        catalog,
        store: iAsyncStore,
        world_id: str,
        run_id: str,
        lineage: list[tuple[str, str, int]] | None,
    ) -> _ResumeSnapshot:
        """The world's durable state: entity directory + counters + tick.

        Resume tick comes from manifests, never from rows (unpublished
        crashed attempts may have written higher ticks that do not exist).
        An unstepped coordinated fork has no own manifests: it resumes at
        the fork point (last ancestor boundary + 1).

        The directory seeds from ancestor lineage segments first (ascending)
        so a fork resumed before its first own step inherits its snapshot
        state; the fork's own rows override by tick.
        """
        visible = await catalog.visible_tokens(world_id, run_id)
        manifest_tick = await catalog.max_manifest_tick(world_id, run_id)
        if visible is not None:
            tokens: list[str] | None = sorted(
                {token for token_list in visible.values() for token in token_list}
            )
            resume_tick: int | None = (
                manifest_tick + 1
                if manifest_tick is not None
                else ((lineage[-1][2] + 1) if lineage else 0)
            )
        else:
            # Never-fenced legacy world: rows are implicitly visible and the
            # own-row head is the true head (resolved after the scan).
            resume_tick, tokens = None, None

        directory: dict[int, SignatureRecord] = {}
        latest_seen: dict[int, int] = {}
        max_entity_id = 0

        for ancestor_world, ancestor_run, up_to_tick in lineage or []:
            ancestor_visible = await catalog.visible_tokens(str(ancestor_world), str(ancestor_run))
            ancestor_tokens = (
                sorted({t for ts in ancestor_visible.values() for t in ts})
                if ancestor_visible
                else ([] if ancestor_visible is not None else None)
            )
            seg_max_eid, _ = await self._scan_world_rows(
                catalog,
                store,
                str(ancestor_world),
                str(ancestor_run),
                ancestor_tokens,
                directory,
                latest_seen,
                max_tick=int(up_to_tick),
            )
            max_entity_id = max(max_entity_id, seg_max_eid)

        own_max_eid, own_head = await self._scan_world_rows(
            catalog, store, world_id, run_id, tokens, directory, latest_seen
        )
        max_entity_id = max(max_entity_id, own_max_eid)
        if resume_tick is None:
            resume_tick = (
                own_head + 1 if own_head is not None else ((lineage[-1][2] + 1) if lineage else 0)
            )
        return _ResumeSnapshot(
            directory=directory,
            next_entity_id=max_entity_id + 1,
            resume_tick=resume_tick,
        )

    async def _scan_world_rows(
        self,
        catalog,
        store: iAsyncStore,
        world_id: str,
        run_id: str,
        tokens: list[str] | None,
        directory: dict[int, SignatureRecord],
        latest_seen: dict[int, int],
        *,
        max_tick: int | None = None,
    ) -> tuple[int, int | None]:
        """Merge one (world, run) segment's latest visible rows into the
        directory accumulator. Returns (max entity id seen, max row tick).

        Reads through the open-never-create seam on BASE columns only, so no
        Python component classes are needed to take inventory — classes are
        demanded later, and only for archetypes that still have LIVE
        entities (dead history should not hold a resume hostage).
        """
        from daft import col, lit

        max_entity_id = 0
        row_head: int | None = None
        for rec in await catalog.list_signatures():
            try:
                df = await store.get_existing_table_df(rec.table_id, world_id, run_id)
            except KeyError:
                continue  # recorded elsewhere, never materialized here
            if tokens is not None and "commit_token" in df.column_names:
                visible = df["commit_token"].is_in(tokens) if tokens else lit(False)
                df = df.where(visible)
            if max_tick is not None:
                # (Daft stubs Expression.__le__ as bool; this is an Expression.)
                df = df.where(df["tick"] <= max_tick)  # ty: ignore[unsupported-operator]
            latest = df.groupby("entity_id").agg(col("tick").max().alias("latest_tick"))
            current = df.join(
                latest,
                left_on=["entity_id", "tick"],
                right_on=["entity_id", "latest_tick"],
            ).select("entity_id", "tick", "is_active")
            rows = current.to_pylist()
            for row in rows:
                eid, tick = int(row["entity_id"]), int(row["tick"])
                if row_head is None or tick > row_head:
                    row_head = tick
                if eid < 0:
                    continue  # lineage/metadata rows, never live entities
                max_entity_id = max(max_entity_id, eid)
                # An entity's current sig is wherever its LATEST row lives —
                # migrations leave inactive rows behind in the old table.
                # Same-tick ties happen by design: update_entity and
                # migration write an inactive marker AND the new active row
                # at one tick, so at a tied tick any active row wins.
                prior = latest_seen.get(eid)
                if prior is not None and prior > tick:
                    continue
                if prior == tick:
                    if row["is_active"]:
                        directory[eid] = rec
                    continue
                latest_seen[eid] = tick
                if row["is_active"]:
                    directory[eid] = rec
                else:
                    directory.pop(eid, None)
        return max_entity_id, row_head

    def _resolve_live_signatures(
        self, directory: dict[int, SignatureRecord]
    ) -> tuple[dict[int, tuple], set[str]]:
        """Signature records of LIVE entities → Python class signatures.

        Candidate classes are matched by the record's stored schema
        fingerprint, so same-named classes from different modules cannot be
        confused with each other — and a class whose fields drifted since
        the rows were written is refused rather than silently misread.
        Fails loudly, naming every table it cannot faithfully resolve.
        """
        from itertools import product

        available = _component_classes_by_name()
        resolved: dict[str, tuple] = {}
        problems: dict[str, str] = {}
        for rec in directory.values():
            if rec.table_id in resolved or rec.table_id in problems:
                continue
            missing = sorted(n for n in rec.component_names if not available.get(n))
            if missing:
                problems[rec.table_id] = (
                    f"component class(es) {', '.join(missing)} are not imported"
                )
                continue
            matches: list[tuple] = []
            candidates = (
                sorted(available[n], key=lambda t: (t.__module__, t.__qualname__))
                for n in rec.component_names
            )
            for combo in product(*candidates):
                sig = tuple(sorted(set(combo), key=lambda t: t.__name__))
                try:
                    fingerprint = schema_fingerprint(Archetype.get_archetype_schema(sig))
                except Exception:
                    continue
                if fingerprint == rec.fingerprint and sig not in matches:
                    matches.append(sig)
            if matches:
                # Multiple matches are interchangeable BY CONSTRUCTION: the
                # fingerprint covers every column name and type, so any
                # matching combination reads and writes this table
                # faithfully. Candidate order makes the pick deterministic.
                resolved[rec.table_id] = matches[0]
            else:
                problems[rec.table_id] = (
                    "no imported class combination matches the stored schema "
                    "(the definitions may have drifted since the rows were written)"
                )
        if problems:
            detail = "; ".join(f"table {tid}: {msg}" for tid, msg in sorted(problems.items()))
            raise RuntimeError(
                f"cannot resume: {detail} (code is not rows — import the exact component "
                "definitions before resuming)"
            )
        entity2sig = {eid: resolved[rec.table_id] for eid, rec in directory.items()}
        return entity2sig, set(resolved)

    async def record_step(self, world_id: str | UUID) -> None:
        """Refresh the world's current run in the catalog after a step.

        Called by SimulationService post-step. Since #273, the durable tick
        head is maintained transactionally by manifest publication inside
        world.step, and signature registration rides publication in the
        commit coordinator — so this hook only keeps the world's run_id
        current. Failures log loudly rather than failing a tick whose
        data-plane writes and manifest already succeeded.
        """
        record = self._storage_configs.get(str(world_id))
        if record is None:
            return
        try:
            world = self._orchestrator.get_world(UUID(str(world_id)))
            if not world.run_id:
                return
            catalog = self._storage_service.get_control_catalog(record[0])
            await catalog.set_world_run(str(world_id), str(world.run_id))
        except Exception:
            logger.exception("catalog run update failed for world %s", world_id)

    async def add_resource(self, world_id: str | UUID, resource: object) -> None:
        """Attach a resource to a world's Resources container."""
        world = self._orchestrator.get_world(UUID(str(world_id)))
        world.resources.insert(resource)

    def list_processors(self, world_id: str | UUID) -> list:
        """Return the world's registered processor instances."""
        world = self._orchestrator.get_world(UUID(str(world_id)))
        return list(world.system.processors)

    def list_hooks(self, world_id: str | UUID) -> list:
        """Return registered hooks as (event_type, handle, fn, mode) rows."""
        world = self._orchestrator.get_world(UUID(str(world_id)))
        return list(world.hooks.items())

    def list_resources(self, world_id: str | UUID) -> list:
        """Return (type, instance) pairs from the world's Resources container."""
        world = self._orchestrator.get_world(UUID(str(world_id)))
        if hasattr(world, "resources"):
            return list(world.resources.items())
        return []

    def add_hook(self, world_id: str | UUID, event_type, fn, *, mode="blocking"):
        """Add a hook to a world's hook bus. Returns the HookHandle."""
        world = self._orchestrator.get_world(UUID(str(world_id)))
        return world.add_hook(event_type, fn, mode=mode)

    def remove_hook(self, world_id: str | UUID, handle) -> None:
        """Remove a hook by handle."""
        world = self._orchestrator.get_world(UUID(str(world_id)))
        world.remove_hook(handle)

    async def shutdown(self) -> None:
        await self._storage_service.shutdown()
