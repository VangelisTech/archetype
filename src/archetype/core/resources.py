from __future__ import annotations
from typing import Dict, Tuple, Optional
import asyncio
from daft.session import Session

from archetype.core.config import StorageConfig, CacheConfig
from archetype.core.storage import  AsyncLancedbStore
from archetype.core.sync import SyncStore
from archetype.core.aio import (
    iAsyncStore,
    iAsyncQueryManager,
    iAsyncUpdateManager,
    AsyncQueryManager,
    AsyncUpdateManager,
    AsyncStore,
    AsyncCachedStore
)
from archetype.core.instrumentation.instrumented_async_store import InstrumentedAsyncStore
from archetype.core.instrumentation.instrumented_async_querier import InstrumentedAsyncQueryManager



class StorageResourceManager:
    """
    Manages the lifecycle of shared storage backend resources.

    This class implements a multiton pattern to ensure that for any given storage
    URI, only one instance of the (Store, Querier, Updater) triplet is created
    and shared among all worlds that use that backend.
    """
    def __init__(self, session: Optional[Session] = None):
        self._instances: Dict[str, Tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._session = session or Session()


    async def get_backend(
        self, storage_config: StorageConfig, cache_config: CacheConfig = None, *, instrumented: bool | None = None
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
                        # Wrap with cached store using CacheConfig directly
                        store = AsyncCachedStore(async_store=store, cache_config=cache_config)
                    
                    # Optionally instrument store and querier
                    if instrumented:
                        store = InstrumentedAsyncStore(storage_config)  # type: ignore[assignment]
                        querier = InstrumentedAsyncQueryManager(store=store)
                    else:
                        querier = AsyncQueryManager(store=store)
                    updater = AsyncUpdateManager(store=store)
                    self._instances[uri] = (store, querier, updater)
        
        return self._instances[uri]

    def _create_store(self, storage_config: StorageConfig) -> iAsyncStore:
        """Factory method to create the appropriate store based on config."""
        if not storage_config.is_async:
            return SyncStore(storage_config)
        if storage_config.use_lancedb:
            return AsyncLancedbStore(storage_config)
        return AsyncStore(storage_config)

    async def shutdown(self):
        """Gracefully shuts down all managed storage backends."""
        for store, _, _ in self._instances.values():
            # Check if store shutdown is awaitable
            if asyncio.iscoroutinefunction(store.shutdown):
                await store.shutdown()
            else:
                store.shutdown()
        self._instances.clear()
        self._locks.clear()


class RuntimeResourceManager:
    """
    Placeholder for managing runtime resources like Ray actor pools.
    This will be implemented as part of the Ray integration.
    """
    pass
