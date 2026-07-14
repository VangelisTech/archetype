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
Storage Service

Resolves StorageConfig → concrete store. Pools instances by config key.
Session configuration is handled at the runtime level (see runtime/session.py).
"""

from __future__ import annotations

import asyncio
import logging
import pathlib
from urllib.parse import urlparse

from daft.session import Session

from archetype.app._catalog import SqliteControlCatalog, catalog_path_for
from archetype.core.aio import AsyncCachedStore, AsyncLancedbStore, AsyncStore
from archetype.core.config import CacheConfig, StorageBackend, StorageConfig
from archetype.core.interfaces import iAsyncStore
from archetype.core.sync import SyncStore

logger = logging.getLogger(__name__)


def _resolve_uri(uri: str) -> str:
    """Resolve local storage paths to absolute. Remote URIs pass through."""
    scheme = urlparse(uri).scheme.lower()
    if scheme not in ("", "file"):
        return uri

    base_path = pathlib.Path(uri)
    if not base_path.is_absolute():
        base_path = pathlib.Path.cwd() / base_path
    base_path.mkdir(parents=True, exist_ok=True)
    return str(base_path)


def create_async_store(
    config: StorageConfig,
    session: Session | None = None,
    cache_config: CacheConfig | None = None,
) -> iAsyncStore:
    """Create an async store from a StorageConfig.

    For LanceDB: uses resolved uri + namespace directly.
    For Iceberg: uses the Daft session (global if not provided).
    """
    store: iAsyncStore
    if config.backend == StorageBackend.LANCEDB:
        uri = _resolve_uri(str(config.uri))
        store = AsyncLancedbStore(uri, config.namespace)
    else:
        from archetype.runtime.session import configure_session

        sess = configure_session(config, session or Session())
        store = AsyncStore(sess, io_config=config.io_config)

    if isinstance(cache_config, bool):
        cache_config = CacheConfig() if cache_config else None

    if cache_config:
        store = AsyncCachedStore(async_store=store, cache_config=cache_config)

    return store


def create_sync_store(
    config: StorageConfig,
    session: Session | None = None,
) -> SyncStore:
    """Create a sync store from a StorageConfig."""
    uri = _resolve_uri(str(config.uri))
    if config.backend == StorageBackend.ICEBERG:
        from archetype.runtime.session import configure_session

        sess = configure_session(config, session or Session())
    else:
        sess = session or Session()
    return SyncStore(uri, sess, io_config=config.io_config)


class StorageService:
    """Creates and pools async stores. Manages storage lifecycle.

    Multiton semantics: for any (uri, namespace, backend, cache) tuple,
    only one store instance is created and reused.
    """

    def __init__(self, session: Session | None = None) -> None:
        self._session = session
        self._instances: dict[str, iAsyncStore] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        # Control catalogs, pooled by resolved catalog path (issue #272).
        # The catalog is an implementation resource of this service — it is
        # authoritative for discovery, and its location is a pure function
        # of the storage identity (see app/_catalog.py).
        self._catalogs: dict[str, SqliteControlCatalog] = {}

    def get_control_catalog(self, storage_config: StorageConfig) -> SqliteControlCatalog:
        """The durable control catalog for a storage location (pooled)."""
        path = catalog_path_for(storage_config)
        key = str(path)
        catalog = self._catalogs.get(key)
        if catalog is None:
            catalog = SqliteControlCatalog(path)
            self._catalogs[key] = catalog
        return catalog

    @staticmethod
    def _pool_key(
        storage_config: StorageConfig,
        cache_config: CacheConfig | None,
    ) -> str:
        """Build a pool key covering uri, namespace, backend, and cache_config."""
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

    async def get_or_create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iAsyncStore:
        """Return the pooled async store for a storage/cache configuration.

        Creates the store on first call for a given config key.
        Subsequent calls with the same config return the cached instance.
        """
        key = self._pool_key(storage_config, cache_config)

        if key not in self._instances:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()

            async with self._locks[key]:
                if key not in self._instances:
                    self._instances[key] = create_async_store(
                        storage_config, self._session, cache_config
                    )

        return self._instances[key]

    async def shutdown(self):
        """Gracefully shuts down all managed storage backends."""
        errors: list[Exception] = []
        for store in self._instances.values():
            try:
                if asyncio.iscoroutinefunction(getattr(store, "shutdown", None)):
                    await store.shutdown()
                elif hasattr(store, "shutdown"):
                    # Runtime-sync shutdown on a duck-typed store; the
                    # iscoroutinefunction branch above handles async stores.
                    store.shutdown()  # ty: ignore[unused-awaitable]
            except Exception as e:
                logger.exception("Failed to shut down store %r", store)
                errors.append(e)

        for catalog in self._catalogs.values():
            try:
                await catalog.close()
            except Exception as e:
                logger.exception("Failed to close control catalog %r", catalog.path)
                errors.append(e)

        self._instances.clear()
        self._locks.clear()
        self._catalogs.clear()

        if errors:
            raise RuntimeError(
                f"StorageService.shutdown failed for {len(errors)} store(s); "
                f"first error: {errors[0]!r}"
            ) from errors[0]
