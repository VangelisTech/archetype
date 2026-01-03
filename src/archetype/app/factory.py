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
World Factory

Assembles world instances by wiring together storage, querier, updater, and system
components. Delegates backend management to the injected StorageBackendManager.
"""

from archetype.app.storage_manager import StorageBackendManager
from archetype.core.aio import AsyncSystem, AsyncWorld
from archetype.core.config import CacheConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import iAsyncSystem, iWorld
from archetype.core.sync import SyncSystem, SyncWorld

# OpenTelemetry instrumentation is now handled via decorators in archetype.core.metrics.otel
# See metrics/otel.py for trace/span utilities


class WorldFactory:
    """
    Factory for creating world instances.

    Assembles a world by:
    1. Delegating storage backend management to the backend manager
    2. Selecting the appropriate world class (sync/async)
    3. Wiring dependencies together
    """

    def __init__(self, backend_manager: StorageBackendManager):
        self._backend_manager = backend_manager

    async def create_world(
        self,
        world_config: WorldConfig,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | object | None = None,
    ) -> iWorld:
        """
        Assembles and returns a new world instance.

        Args:
            world_config: Configuration for the world (name, id, etc.)
            storage_config: Storage backend configuration
            cache_config: Optional caching configuration
            system: Optional system instance; creates default if not provided

        Returns:
            Configured world instance ready for use
        """
        # Get shared backend resources
        store, querier, updater = await self._backend_manager.get_backend(
            storage_config, cache_config
        )

        # Select world class based on storage config
        if storage_config.is_async:
            world_class = AsyncWorld
            default_system = AsyncSystem()
        else:
            world_class = SyncWorld
            default_system = SyncSystem()

        # Assemble and return
        return world_class(
            world_config=world_config,
            querier=querier,
            updater=updater,
            system=system or default_system,
        )
