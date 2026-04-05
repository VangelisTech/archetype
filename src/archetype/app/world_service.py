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
    ):
        self.storage_service = storage_service
        self.factory = WorldFactory(storage_service)
        self._broker = broker
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
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> iWorld:
        """
        Creates or retrieves a world based on the provided configuration.
        Idempotent: if a world_id already exists, returns the existing instance.
        Injects CommandBroker into world resources if available.
        """
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
        Fork a world: create a new world that clones the current state of the source.

        The fork gets a new ``world_id`` but starts from an identical entity/component
        snapshot at the source's current tick. Source and fork then diverge independently.

        Inheritance policy (see archetype#61):
          * Copied: ``tick``, ``run_id``, ``_entity2sig``, ``_entity_counter``,
            live archetype snapshots (re-stamped with the new ``world_id``),
            processors (shared instances), and non-broker resources.
          * Persisted: the live snapshots are written to the shared store under
            the new ``world_id`` at tick ``(source.tick - 1)`` so store-backed
            reads see the forked state from tick 0.
          * Not copied: pending spawn/despawn caches (already reflected in
            ``_live`` once materialized), lifecycle hooks (fork-specific
            observers), and the ``CommandBroker`` reference (re-injected by
            ``create_world`` via the service's own broker).
        """
        import copy as _copy

        from daft import lit

        from archetype.app.broker import CommandBroker
        from archetype.core.aio.async_system import AsyncSystem

        source = self.get_world(source_world_id)
        if not isinstance(source, AsyncWorld):
            raise TypeError("Can only fork AsyncWorld instances")

        # Use same storage config as source or provided override
        if storage_config is None:
            storage_config = StorageConfig()

        # Build a fresh system that shares processor instances with the source.
        # Processors are stateless DataFrame transforms, so sharing is safe.
        new_system = AsyncSystem()
        new_system.processors = list(source.system.processors)

        new_world = await self.create_world(
            config=config,
            storage_config=storage_config,
            cache_config=cache_config,
            system=new_system,
        )

        if not isinstance(new_world, AsyncWorld):
            return new_world

        # --- Clone in-memory state ---
        new_world.tick = source.tick
        new_world.run_id = source.run_id
        new_world._entity2sig = dict(source._entity2sig)
        new_world._entity_counter = _copy.copy(source._entity_counter)

        # Re-stamp live snapshots with the new world_id so they stay consistent
        # with the fork's identity (used by prefer_live_reads).
        new_world_id_str = str(new_world.world_id)
        new_live: dict = {}
        for sig, df in source._live.items():
            new_live[sig] = df.with_column("world_id", lit(new_world_id_str))
        new_world._live = new_live

        # --- Copy non-broker resources (selective policy) ---
        # The broker is world-scoped governance; create_world already injected
        # the service's broker into new_world.resources.
        for resource_type, resource in source.resources._store.items():
            if resource_type is CommandBroker or isinstance(resource, CommandBroker):
                continue
            if resource_type in new_world.resources:
                continue
            new_world.resources.insert(resource)

        # --- Persist snapshot under the new world_id ---
        # After step(), source.tick is the NEXT tick to process and _live holds
        # rows stamped with (source.tick - 1). Replaying that under the new
        # world_id lets the default store-backed reads find the forked state.
        if source.tick > 0 and new_live:
            persist_tick = source.tick - 1
            persist_run_id = new_world.run_id or ""
            for sig, df in new_live.items():
                await new_world.updater.update(
                    df, sig, persist_tick, new_world.world_id, persist_run_id
                )

        return new_world
