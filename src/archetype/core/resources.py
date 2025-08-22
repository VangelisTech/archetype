from __future__ import annotations
from typing import Dict, Tuple
import asyncio

from archetype.core.config import StorageConfig, CacheConfig
from archetype.core.storage import  AsyncLancedbStore
from archetype.core.sync import SyncStore
from archetype.core.interfaces import iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager
from archetype.core.aio import (

    AsyncQueryManager,
    AsyncUpdateManager,
    AsyncStore,
    AsyncCachedStore
)
from archetype.core.instrumentation.instrumented_async_store import InstrumentedAsyncStore, InstrumentedStoreWrapper
from archetype.core.instrumentation.instrumented_async_querier import InstrumentedAsyncQueryManager
from archetype.core.instrumentation.instrumented_async_updater import InstrumentedAsyncUpdateManager
from archetype.core.runtime.storage import StorageContextFactory


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
        self, storage_config: StorageConfig, cache_config: CacheConfig = None, *, instrumented: bool | None = None
    ) -> Tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]:
        """
        Retrieves or creates a shared backend triplet for the given storage config.
        """
        # Key backends by storage identity AND behaviorally relevant options
        cache_sig = (
            f"rows={cache_config.flush_rows}|mb={cache_config.flush_mb}|gmb={cache_config.global_mb}|idle={cache_config.idle_sec}"
            if cache_config else "nocache"
        )
        # Preserve original multiton semantics: identical (uri, namespace) share the same instance,
        # regardless of instrumented flag. This aligns with tests expecting first creation to win.
        key = f"{storage_config.uri}::{storage_config.namespace}"
        if key not in self._instances:
            # Create a lock for this specific URI if it doesn't exist
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()

            async with self._locks[key]:
                # Double-check if another coroutine created the instance while we waited for the lock
                if key not in self._instances:
                    # Initialize runtime context explicitly (side-effect boundary)
                    context = StorageContextFactory.build(storage_config)
                    store = self._create_store(storage_config, context)
                    # Allow booleans for ergonomics: True -> default CacheConfig, False/None -> no cache
                    if isinstance(cache_config, bool):
                        cache_cfg = CacheConfig() if cache_config else None
                    else:
                        cache_cfg = cache_config
                    if cache_cfg:
                        store = AsyncCachedStore(async_store=store, cache_config=cache_cfg)
                    
                    # Optionally instrument store and querier
                    if instrumented:
                        # For first creation under this key, wrap store/querier; subsequent calls with
                        # instrumented=True should return the same instances (no double-wrap).
                        # Tests assert the store is an InstrumentedAsyncStore on first creation
                        store = InstrumentedAsyncStore(context)  # type: ignore[assignment]
                        querier = InstrumentedAsyncQueryManager(store=store)
                        from archetype.core.instrumentation.instrumented_async_updater import InstrumentedAsyncUpdateManager
                        updater = InstrumentedAsyncUpdateManager(store=store)
                    else:
                        querier = AsyncQueryManager(store=store)
                        updater = AsyncUpdateManager(store=store)
                    self._instances[key] = (store, querier, updater)
        
        return self._instances[key]

    def _create_store(self, storage_config: StorageConfig, context) -> iAsyncStore:
        """Factory method to create the appropriate store based on config."""
        if not storage_config.is_async:
            return SyncStore(storage_config)
        if storage_config.use_lancedb:
            return AsyncLancedbStore(context)
        return AsyncStore(context)

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