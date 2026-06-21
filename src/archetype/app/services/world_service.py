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

import logging
from typing import TYPE_CHECKING

from uuid_utils import UUID, uuid7

from archetype.app.services.storage_service import StorageService
from archetype.core.aio import (
    AsyncQueryManager,
    AsyncSystem,
    AsyncUpdateManager,
    AsyncWorld,
)
from archetype.core.config import CacheConfig, StorageConfig, WorldConfig
from archetype.core.hooks import HookRegistry
from archetype.core.interfaces import iAsyncStore, iAsyncSystem
from archetype.core.lineage import persist_lineage
from archetype.core.resources import Resources

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


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

    def get_world(self, world_id: UUID) -> AsyncWorld:
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

    async def create_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> AsyncWorld:
        """Resolve storage, then delegate world creation to the orchestrator."""
        if storage_config is None:
            storage_config = StorageConfig()

        store = await self._storage_service.get_or_create_store(storage_config, cache_config)
        world = self._orchestrator.create_world(store, config, system=system)
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
        storage_record().
        """
        await self._orchestrator.destroy_world(world_id)

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
