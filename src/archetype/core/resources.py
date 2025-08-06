from __future__ import annotations
from typing import Dict, Tuple
import asyncio

from archetype.app.config import StorageConfig, CacheConfig
from archetype.core.storage import AsyncStore, AsyncLancedbStore, AsyncCachedStore
from archetype.core.aio import (
    iAsyncStore,
    iAsyncQueryManager,
    iAsyncUpdateManager,
    AsyncQueryManager,
    AsyncUpdateManager,
)


class StorageResourceManager:
    """
    Manages the lifecycle of shared storage backend resources.

    This class implements a multiton pattern to ensure that for any given storage
    URI, only one instance of the (Store, Querier, Updater) triplet is created
    and shared among all worlds that use that backend.
    """
    def __init__(self):
        self._instances: Dict[str, Tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    async def get_backend(
        self, storage_config: StorageConfig, cache_config: CacheConfig = None
    ) -> Tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]:
        """
        Retrieves or creates a shared backend triplet for the given storage config.
        """
        uri = storage_config.uri
        if uri not in self._instances:
            # Create a lock for this specific URI if it doesn't exist
            if uri not in self._locks:
                self._locks[uri] = asyncio.Lock()

            async with self._locks[uri]:
                # Double-check if another coroutine created the instance while we waited for the lock
                if uri not in self._instances:
                    store = self._create_store(storage_config)
                    if cache_config:
                        store = AsyncCachedStore(store=store, config=cache_config)
                    
                    querier = AsyncQueryManager(store=store)
                    updater = AsyncUpdateManager(store=store)
                    self._instances[uri] = (store, querier, updater)
        
        return self._instances[uri]

    def _create_store(self, storage_config: StorageConfig) -> iAsyncStore:
        """Factory method to create the appropriate store based on config."""
        # This can be expanded to support more store types in the future
        if "lancedb" in storage_config.uri: # Simple check for now
            return AsyncLancedbStore(storage_config=storage_config)
        return AsyncStore(storage_config=storage_config)

    async def shutdown(self):
        """Gracefully shuts down all managed storage backends."""
        for store, _, _ in self._instances.values():
            # Assuming stores have a shutdown method, which they should
            if hasattr(store, 'shutdown'):
                await store.shutdown()
        self._instances.clear()
        self._locks.clear()


class RuntimeResourceManager:
    """
    Placeholder for managing runtime resources like Ray actor pools.
    This will be implemented as part of the Ray integration.
    """
    pass
