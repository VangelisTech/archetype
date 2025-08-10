import asyncio
from typing import Dict, List, Union
from uuid_utils import UUID, uuid7

from archetype.core.resources import StorageResourceManager
from .factory import WorldFactory
from .interfaces import iWorld, iSystem
from .aio import iAsyncSystem, AsyncWorld
from archetype.core.config import WorldConfig, StorageConfig, CacheConfig, RunConfig


class WorldOrchestrator:
    """
    Manages the lifecycle and coordinated execution of multiple worlds.
    """

    def __init__(self):
        """Initializes the orchestrator and its owned resources."""
        self.storage_manager = StorageResourceManager()
        self.factory = WorldFactory(self.storage_manager)
        self._worlds: Dict[UUID, iWorld] = {}
        self._world_names: Dict[str, UUID] = {}

    async def shutdown(self):
        """Gracefully shuts down all managed resources and worlds."""
        await self.storage_manager.shutdown()
        self._worlds.clear()
        self._world_names.clear()

    # --- World Lifecycle Management ---

    async def create_world(
        self,
        config: WorldConfig,
        system: Union[iAsyncSystem, iSystem],
        storage_config: StorageConfig,
        cache_config: CacheConfig = None,
        *,
        instrumented: bool | None = None,
    ) -> iWorld:
        """
        Creates or retrieves a world based on the provided configuration.
        This method is idempotent. If a world_id is provided and already exists,
        it will return the existing world instance.
        """
        world_id = config.world_id or uuid7()

        if world_id in self._worlds:
            return self._worlds[world_id]

        world = await self.factory.create_world(
            world_config=config,
            storage_config=storage_config,
            cache_config=cache_config,
            system=system,
            instrumented=instrumented,
        )
        self._worlds[world.world_id] = world
        
        if config.name:
            if config.name in self._world_names:
                # Note: We could raise an error here if names must be unique
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
        world_id = self._world_names[name]
        return self.get_world(world_id)

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

    def list_worlds(self) -> List[UUID]:
        """Returns the IDs of all managed worlds."""
        return list(self._worlds.keys())

    # --- World Execution Coordination ---

    async def run_world(self, world_id: UUID, run_config: RunConfig, **input_kwargs):
        """Tells a specific world to run based on the provided run configuration."""
        world = self.get_world(world_id)
        if isinstance(world, AsyncWorld):
            await world.run(run_config, **input_kwargs)

    async def run_world_by_name(self, name: str, run_config: RunConfig, **input_kwargs):
        """Tells a specific world to run, looked up by its name."""
        world = self.get_world_by_name(name)
        if isinstance(world, AsyncWorld):
            await world.run(run_config, **input_kwargs)

    async def run_all_worlds(self, run_config: RunConfig, **input_kwargs):
        """Tells all managed async worlds to run concurrently."""
        tasks = [
            world.run(run_config, **input_kwargs)
            for world in self._worlds.values() if isinstance(world, AsyncWorld)
        ]
        if tasks:
            await asyncio.gather(*tasks)
