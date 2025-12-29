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
Storage Resource Management

Manages the lifecycle of shared storage backend resources using a multiton pattern.
This ensures that for any given storage URI, only one (Store, Querier, Updater) triplet
is created and shared among all worlds using that backend.
"""

from __future__ import annotations

import asyncio

from archetype.core.aio import AsyncCachedStore, AsyncQueryManager, AsyncStore, AsyncUpdateManager
from archetype.core.config import CacheConfig, StorageConfig
from archetype.core.interfaces import iAsyncQueryManager, iAsyncStore, iAsyncUpdateManager
from archetype.core.runtime.storage import StorageContext, StorageContextFactory
from archetype.core.storage import AsyncLancedbStore
from archetype.core.sync import SyncStore

# OpenTelemetry instrumentation is now handled via decorators in archetype.core.metrics.otel
# See metrics/otel.py for trace/span utilities


class StorageResourceManager:
    """
    Manages shared storage backend resources.

    Implements a multiton pattern: for any (uri, namespace) pair, only one
    (Store, Querier, Updater) triplet is created and reused.
    """

    def __init__(self):
        self._instances: dict[str, tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def get_backend(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]:
        """
        Retrieves or creates a shared backend triplet for the given storage config.

        Args:
            storage_config: Storage configuration (uri, namespace, etc.)
            cache_config: Optional cache configuration; True uses defaults

        Returns:
            Tuple of (Store, Querier, Updater) implementing async interfaces
        """
        key = f"{storage_config.uri}::{storage_config.namespace}"

        if key not in self._instances:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()

            async with self._locks[key]:
                # Double-check after acquiring lock
                if key not in self._instances:
                    self._instances[key] = self._create_backend(storage_config, cache_config)

        return self._instances[key]

    def _create_backend(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None,
    ) -> tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]:
        """
        Creates a new backend triplet.

        Args:
            storage_config: Storage configuration
            cache_config: Optional cache configuration

        Returns:
            Tuple of (Store, Querier, Updater)
        """
        context = StorageContextFactory.build(storage_config)
        store = self._create_store(storage_config, context)

        # Apply caching layer if configured
        if isinstance(cache_config, bool):
            cache_config = CacheConfig() if cache_config else None

        if cache_config:
            store = AsyncCachedStore(async_store=store, cache_config=cache_config)

        querier = AsyncQueryManager(store=store)
        updater = AsyncUpdateManager(store=store)

        return (store, querier, updater)

    def _create_store(self, storage_config: StorageConfig, context: StorageContext) -> iAsyncStore:
        """Factory method to create the appropriate store based on config."""
        if not storage_config.is_async:
            return SyncStore(storage_config)
        if storage_config.use_lancedb:
            return AsyncLancedbStore(context)
        return AsyncStore(context)

    async def shutdown(self):
        """Gracefully shuts down all managed storage backends."""
        for store, _, _ in self._instances.values():
            if asyncio.iscoroutinefunction(store.shutdown):
                await store.shutdown()
            else:
                store.shutdown()

        self._instances.clear()
        self._locks.clear()
