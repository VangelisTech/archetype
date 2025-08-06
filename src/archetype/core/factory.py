from typing import Union
from uuid_utils import UUID

from archetype.app.config import StorageConfig, CacheConfig
from archetype.core.resources import StorageResourceManager
from archetype.core.interfaces import iWorld, iSystem
from archetype.core.aio import AsyncWorld, iAsyncSystem, iAsyncStore
from archetype.core.sync import SyncWorld


class WorldFactory:
    """
    Factory for creating world instances.

    This factory assembles a world, delegating the management of shared, 
    stateful resources (like storage backends) to an injected resource manager.
    It accepts a pre-configured system object, ensuring that the world's logic
    is defined as code by the user.
    """
    def __init__(self, resource_manager: StorageResourceManager):
        self._resource_manager = resource_manager

    async def create_world(
        self,
        world_id: UUID,
        system: Union[iAsyncSystem, iSystem],
        storage_config: StorageConfig,
        cache_config: CacheConfig = None,
    ) -> iWorld:
        """
        Assembles and returns a new world instance.
        """
        # 1. Get the shared backend resources from the resource manager
        store, querier, updater = await self._resource_manager.get_backend(
            storage_config, cache_config
        )

        # 2. Determine the world class based on the system type
        world_class = AsyncWorld if isinstance(store, iAsyncStore) else SyncWorld

        # 3. Create the world instance
        world = world_class(
            world_id=world_id,
            querier=querier,
            updater=updater,
            system=system,
        )

        return world
