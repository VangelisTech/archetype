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
World Orchestrator

Manages the lifecycle and coordinated execution of multiple worlds.
Provides a high-level API for creating, running, and managing worlds.
"""

import asyncio

from uuid_utils import UUID, uuid7

from archetype.app.factory import WorldFactory
from archetype.app.storage_manager import StorageBackendManager
from archetype.core.aio import AsyncWorld
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import iAsyncSystem, iSystem, iWorld


class WorldOrchestrator:
    """
    Manages the lifecycle and coordinated execution of multiple worlds.

    Provides:
    - World creation with automatic resource sharing
    - World lookup by ID or name
    - Coordinated single or parallel world execution
    - Clean shutdown of all managed resources
    """

    def __init__(self):
        """Initializes the orchestrator and its owned resources."""
        self.backend_manager = StorageBackendManager()
        self.factory = WorldFactory(self.backend_manager)
        self._worlds: dict[UUID, iWorld] = {}
        self._world_names: dict[str, UUID] = {}

    async def shutdown(self):
        """Gracefully shuts down all managed resources and worlds."""
        await self.backend_manager.shutdown()
        self._worlds.clear()
        self._world_names.clear()

    # -------------------------------------------------------------------------
    # World Lifecycle Management
    # -------------------------------------------------------------------------

    async def create_world(
        self,
        config: WorldConfig,
        system: iAsyncSystem | iSystem,
        storage_config: StorageConfig,
        cache_config: CacheConfig = None,
    ) -> iWorld:
        """
        Creates or retrieves a world based on the provided configuration.

        This method is idempotent: if a world_id is provided and already exists,
        it returns the existing world instance.

        Args:
            config: World configuration (name, id, etc.)
            system: System instance for processing
            storage_config: Storage backend configuration
            cache_config: Optional caching configuration

        Returns:
            The created or existing world instance

        Raises:
            ValueError: If a world with the same name already exists
        """
        world_id = config.world_id or uuid7()

        if world_id in self._worlds:
            return self._worlds[world_id]

        world = await self.factory.create_world(
            world_config=config,
            storage_config=storage_config,
            cache_config=cache_config,
            system=system,
        )

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

    def remove_world(self, world_id: UUID):
        """Removes a world from management by its ID."""
        if world_id in self._worlds:
            for name, uid in list(self._world_names.items()):
                if uid == world_id:
                    del self._world_names[name]
            del self._worlds[world_id]

    def remove_world_by_name(self, name: str):
        """Removes a world from management by its name."""
        world_id = self.get_world_by_name(name).world_id
        self.remove_world(world_id)

    def list_worlds(self) -> list[UUID]:
        """Returns the IDs of all managed worlds."""
        return list(self._worlds.keys())

    # -------------------------------------------------------------------------
    # World Execution Coordination
    # -------------------------------------------------------------------------

    async def run_world(self, world_id: UUID, run_config: RunConfig, **input_kwargs):
        """Runs a specific world based on the provided run configuration."""
        world = self.get_world(world_id)
        if isinstance(world, AsyncWorld):
            await world.run(run_config, **input_kwargs)

    async def run_world_by_name(self, name: str, run_config: RunConfig, **input_kwargs):
        """Runs a specific world, looked up by its name."""
        world = self.get_world_by_name(name)
        if isinstance(world, AsyncWorld):
            await world.run(run_config, **input_kwargs)

    async def run_all_worlds(self, run_config: RunConfig, **input_kwargs):
        """Runs all managed async worlds concurrently."""
        tasks = [
            world.run(run_config, **input_kwargs)
            for world in self._worlds.values()
            if isinstance(world, AsyncWorld)
        ]
        if tasks:
            await asyncio.gather(*tasks)
