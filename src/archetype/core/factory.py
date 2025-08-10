from typing import Union, Optional

from archetype.core.config import StorageConfig, CacheConfig, WorldConfig
from archetype.core.resources import StorageResourceManager
from archetype.core.interfaces import iWorld, iSystem
from archetype.core.aio import AsyncWorld, iAsyncSystem, AsyncSystem
from archetype.core.sync import SyncWorld, SyncSystem
from archetype.core.instrumentation.profiling_shim import TRACY_ENABLED
try:
    from archetype.core.instrumentation.instrumented_async_world import InstrumentedAsyncWorld
    from archetype.core.instrumentation.instrumented_async_system import InstrumentedAsyncSystem
except Exception:
    InstrumentedAsyncWorld = None  # type: ignore
    InstrumentedAsyncSystem = None  # type: ignore


class WorldFactory:
    """
    Factory for creating world instances.

    This factory assembles a world, delegating the management of shared, 
    stateful resources (like storage backends) to an injected resource manager.
    It accepts a pre-configured system object, ensuring that the world's logic
    is defined as code by the user.
    """
    def __init__(self, resource_manager: StorageResourceManager):
        self._resource_manager = resource_manager # holds all active storage backends

    async def create_world(
        self,
        world_config: WorldConfig,
        storage_config: StorageConfig,
        cache_config: Optional[CacheConfig] = None,
        system: Optional[Union[iAsyncSystem, iSystem]] = None,
        *,
        instrumented: Optional[bool] = None,
    ) -> iWorld:
        """
        Assembles and returns a new world instance.
        """
        # 1. Get the shared backend resources from the resource manager
        store, querier, updater = await self._resource_manager.get_backend(
            storage_config, cache_config, instrumented=instrumented
        )

        # 2. Determine the world class based on the storage config
        if storage_config.is_async:
            use_instrumented = instrumented if instrumented is not None else TRACY_ENABLED
            if use_instrumented and InstrumentedAsyncWorld and InstrumentedAsyncSystem:
                world_class = InstrumentedAsyncWorld
                system_class = InstrumentedAsyncSystem
            else:
                world_class = AsyncWorld
                system_class = AsyncSystem
        else:
            world_class = SyncWorld
            system_class = SyncSystem
        
        # 3. Create the world instance
        world = world_class(
            world_config=world_config,
            querier=querier,
            updater=updater,
            system=system_class(),
        )

        return world
