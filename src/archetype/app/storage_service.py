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

        The pool key includes ``uri``, ``namespace``, ``backend``, and the
        effective ``cache_config``. Keying on ``(uri, namespace)`` alone would
        silently hand a subsequent caller the first caller's wrapped triplet —
        ignoring the subsequent caller's explicit ``backend`` or
        ``cache_config`` choice (e.g. opting out of caching or switching
        between Iceberg/LanceDB).
        """
        key = self._pool_key(storage_config, cache_config)

        if key not in self._instances:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()

            async with self._locks[key]:
                if key not in self._instances:
                    self._instances[key] = self._create_backend(storage_config, cache_config)

        return self._instances[key]

    @staticmethod
    def _pool_key(
        storage_config: StorageConfig,
        cache_config: CacheConfig | None,
    ) -> str:
        """Build a pool key that distinguishes every dimension the backend
        triplet depends on.

        Callers that share ``(uri, namespace)`` but disagree on ``backend`` or
        ``cache_config`` must receive distinct (correctly-wrapped) triplets,
        so those dimensions are part of the key.
        """
        # Normalize cache_config's bool-style shorthand the same way
        # _create_backend does, so equivalent specs share a key.
        if isinstance(cache_config, bool):
            effective_cache = CacheConfig() if cache_config else None
        else:
            effective_cache = cache_config

        cache_part = "none"
        if effective_cache is not None:
            cache_part = (
                f"rows={effective_cache.flush_rows},"
                f"mb={effective_cache.flush_mb},"
                f"global={effective_cache.global_mb},"
                f"idle={effective_cache.idle_sec}"
            )

        return (
            f"{storage_config.uri}"
            f"::{storage_config.namespace}"
            f"::backend={storage_config.backend.value}"
            f"::cache({cache_part})"
        )

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
        errors: list[Exception] = []
        for store, _, _ in self._instances.values():
            try:
                if asyncio.iscoroutinefunction(getattr(store, "shutdown", None)):
                    await store.shutdown()
                elif hasattr(store, "shutdown"):
                    store.shutdown()
            except Exception as e:
                logger.exception("Failed to shut down store %r", store)
                errors.append(e)

        self._instances.clear()
        self._locks.clear()

        if errors:
            raise RuntimeError(
                f"StorageService.shutdown failed for {len(errors)} store(s); "
                f"first error: {errors[0]!r}"
            ) from errors[0]
