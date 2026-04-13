# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Storage Service

Manages the lifecycle of shared storage backend resources using a multiton pattern.
Renamed from StorageBackendManager for v0.1 service layer.
"""

from __future__ import annotations

import asyncio
import logging

from archetype.core.aio import AsyncCachedStore, AsyncQueryManager, AsyncStore, AsyncUpdateManager
from archetype.core.config import CacheConfig, StorageConfig
from archetype.core.interfaces import iAsyncQueryManager, iAsyncStore, iAsyncUpdateManager
from archetype.core.runtime.storage import StorageContextFactory
from archetype.core.storage import AsyncLancedbStore

logger = logging.getLogger(__name__)


class StorageService:
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
        """
        key = f"{storage_config.uri}::{storage_config.namespace}"

        if key not in self._instances:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()

            async with self._locks[key]:
                if key not in self._instances:
                    self._instances[key] = self._create_backend(storage_config, cache_config)

        return self._instances[key]

    def _create_backend(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None,
    ) -> tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]:
        context = StorageContextFactory.build(storage_config)
        store: iAsyncStore
        if storage_config.use_lancedb:
            store = AsyncLancedbStore(context)
        else:
            store = AsyncStore(context)

        if isinstance(cache_config, bool):
            cache_config = CacheConfig() if cache_config else None

        if cache_config:
            store = AsyncCachedStore(async_store=store, cache_config=cache_config)

        querier = AsyncQueryManager(store=store)
        updater = AsyncUpdateManager(store=store)

        return (store, querier, updater)

    async def shutdown(self):
        """Gracefully shuts down all managed storage backends.

        Best-effort: a failure in one store's shutdown does not abort the
        loop. Every store gets a chance to run its cleanup, and the pool
        dicts are always cleared. If any store's shutdown raised, an
        aggregate RuntimeError is raised after the cleanup completes.
        """
        errors: list[BaseException] = []
        for store, _, _ in self._instances.values():
            try:
                if asyncio.iscoroutinefunction(getattr(store, "shutdown", None)):
                    await store.shutdown()
                elif hasattr(store, "shutdown"):
                    store.shutdown()
            except Exception as e:
                logger.error("Failed to shut down store %r: %s", store, e)
                errors.append(e)

        self._instances.clear()
        self._locks.clear()

        if errors:
            raise RuntimeError(
                f"StorageService.shutdown failed for {len(errors)} store(s); "
                f"first error: {errors[0]!r}"
            )
