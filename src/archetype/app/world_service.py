# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
World Service

Manages the lifecycle of multiple worlds. Renamed from WorldOrchestrator for v0.1.
"""

from __future__ import annotations

from uuid_utils import UUID, uuid7

from archetype.app.broker import CommandBroker
from archetype.app.factory import WorldFactory
from archetype.app.models import WorldInfo
from archetype.app.storage_service import StorageService
from archetype.core.aio import AsyncSystem, AsyncWorld
from archetype.core.config import CacheConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import iAsyncSystem, iWorld


class WorldService:
    """
    Manages the lifecycle of multiple worlds.

    Provides:
    - World creation with automatic resource sharing
    - World lookup by ID or name
    - World forking
    - Clean shutdown of all managed resources
    """

    def __init__(
        self,
        storage_service: StorageService,
        broker: CommandBroker | None = None,
        default_storage_config: StorageConfig | None = None,
    ):
        self.storage_service = storage_service
        self.factory = WorldFactory(storage_service)
        self._broker = broker
        self._default_storage_config = default_storage_config or StorageConfig()
        self._worlds: dict[UUID, iWorld] = {}
        self._world_names: dict[str, UUID] = {}

    async def shutdown(self):
        """Gracefully shuts down all managed resources."""
        await self.storage_service.shutdown()
        self._worlds.clear()
        self._world_names.clear()

    async def create_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> iWorld:
        """
        Creates or retrieves a world based on the provided configuration.
        Idempotent: if a world_id already exists, returns the existing instance.
        Injects CommandBroker into world resources if available.
        """
        storage_config = storage_config or self._default_storage_config

        world_id = config.world_id or uuid7()

        if world_id in self._worlds:
            return self._worlds[world_id]

        world = await self.factory.create_world(
            world_config=config,
            storage_config=storage_config,
            cache_config=cache_config,
            system=system or AsyncSystem(),
        )

        # Inject broker into world resources for processor access
        if self._broker and isinstance(world, AsyncWorld) and hasattr(world, "resources"):
            world.resources.insert(self._broker)

        self._worlds[world.world_id] = world

        if config.name:
            if config.name in self._world_names:
                raise ValueError(f"World with name '{config.name}' already exists.")
            self._world_names[config.name] = world.world_id

        return world

    def get_world(self, world_id: UUID) -> iWorld:
        """Retrieves a managed world instance by its ID."""
        if world_id not in self._worlds:
            raise KeyError(f"World with ID '{world_id}' not found.")
        return self._worlds[world_id]

    def get_world_by_name(self, name: str) -> iWorld:
        """Retrieves a managed world instance by its human-readable name."""
        if name not in self._world_names:
            raise KeyError(f"World with name '{name}' not found.")
        return self.get_world(self._world_names[name])

    def list_worlds(self) -> list[WorldInfo]:
        """Returns info for all managed worlds."""
        result = []
        for wid, world in self._worlds.items():
            info = WorldInfo(
                world_id=wid,
                name=getattr(world, "name", None),
                tick=getattr(world, "tick", 0),
                entity_count=getattr(world, "entity_count", 0),
                archetype_signatures=[],
            )
            result.append(info)
        return result

    def remove_world(self, world_id: UUID) -> None:
        """Removes a world from management by its ID."""
        if world_id in self._worlds:
            for name, uid in list(self._world_names.items()):
                if uid == world_id:
                    del self._world_names[name]
            del self._worlds[world_id]

    async def fork_world(
        self,
        source_world_id: UUID,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> iWorld:
        """
        Create a new world from a snapshot of an existing one.
        New world_id, shared storage backend.
        """
        source = self.get_world(source_world_id)
        if not isinstance(source, AsyncWorld):
            raise TypeError("Can only fork AsyncWorld instances")

        # Use same storage config as source or provided override
        if storage_config is None:
            storage_config = self._default_storage_config

        new_world = await self.create_world(
            config=config,
            storage_config=storage_config,
            cache_config=cache_config,
        )

        return new_world
