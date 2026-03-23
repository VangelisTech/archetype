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
components. Delegates backend management to the injected StorageService.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from archetype.core.aio import AsyncSystem, AsyncWorld
from archetype.core.config import CacheConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import iAsyncSystem, iWorld

if TYPE_CHECKING:
    from archetype.app.storage_service import StorageService


class WorldFactory:
    """
    Factory for creating world instances.

    Assembles a world by:
    1. Delegating storage backend management to the StorageService
    2. Wiring dependencies together
    """

    def __init__(self, storage_service: StorageService):
        self._storage_service = storage_service

    async def create_world(
        self,
        world_config: WorldConfig,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> iWorld:
        """
        Assembles and returns a new async world instance.

        Args:
            world_config: Configuration for the world (name, id, etc.)
            storage_config: Storage backend configuration
            cache_config: Optional caching configuration
            system: Optional system instance; creates default if not provided

        Returns:
            Configured world instance ready for use
        """
        store, querier, updater = await self._storage_service.get_backend(
            storage_config, cache_config
        )

        return AsyncWorld(
            world_config=world_config,
            querier=querier,
            updater=updater,
            system=system or AsyncSystem(),
        )
