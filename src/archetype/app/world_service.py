# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
World Service

WorldFactory  — store → world (pure construction)
WorldRegistry — holds live worlds (lookup, insert, remove)
WorldOrchestrator — lifecycle (create, fork, remove, discover)
WorldService  — facade (bridges StorageService into the orchestrator)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from uuid_utils import UUID, uuid7

from archetype.app.storage_service import StorageService
from archetype.core.aio import (
    AsyncQueryManager,
    AsyncSystem,
    AsyncUpdateManager,
    AsyncWorld,
)
from archetype.core.config import CacheConfig, StorageConfig, WorldConfig
from archetype.core.hooks import HookRegistry
from archetype.core.interfaces import iAsyncStore, iAsyncSystem, iWorld
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
        )


# ─────────────────────────────────────────────────────────────────────────────
# WorldRegistry
# ─────────────────────────────────────────────────────────────────────────────


class WorldRegistry:
    """Holds live worlds. Lookup by ID or name, insert, remove, list."""

    def __init__(self) -> None:
        self._worlds: dict[str, iWorld] = {}
        self._names: dict[str, str] = {}

    def insert(self, world: iWorld) -> None:
        wid = str(world.world_id)
        self._worlds[wid] = world
        if world.name:
            self._names[world.name] = wid

    def get(self, world_id: UUID | str) -> iWorld:
        wid = str(world_id)
        if wid not in self._worlds:
            raise KeyError(f"World with ID '{world_id}' not found.")
        return self._worlds[wid]

    def get_by_name(self, name: str) -> iWorld:
        if name not in self._names:
            raise KeyError(f"World with name '{name}' not found.")
        return self.get(self._names[name])

    def remove(self, world_id: UUID | str) -> None:
        wid = str(world_id)
        world = self._worlds.pop(wid, None)
        if world and world.name:
            self._names.pop(world.name, None)

    def list(self) -> list[iWorld]:
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

    def get_world(self, world_id: UUID) -> iWorld:
        return self._registry.get(world_id)

    def get_world_by_name(self, name: str) -> iWorld:
        return self._registry.get_by_name(name)

    def list_worlds(self) -> list[iWorld]:
        return self._registry.list()

    def remove_world(self, world_id: UUID) -> None:
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

    async def create_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> iWorld:
        """Resolve storage, then delegate world creation to the orchestrator."""
        if storage_config is None:
            storage_config = StorageConfig()

        store = await self._storage_service.get_or_create_store(storage_config, cache_config)
        return self._orchestrator.create_world(store, config, system=system)

    def get_world(self, world_id: UUID) -> iWorld:
        return self._orchestrator.get_world(world_id)

    def get_world_by_name(self, name: str) -> iWorld:
        return self._orchestrator.get_world_by_name(name)

    def list_worlds(self) -> list[iWorld]:
        return self._orchestrator.list_worlds()

    async def remove_world(self, world_id: UUID) -> None:
        self._orchestrator.remove_world(world_id)

    async def shutdown(self) -> None:
        await self._storage_service.shutdown()
