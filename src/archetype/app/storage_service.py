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
import pathlib
from urllib.parse import urlparse

from daft.catalog import Catalog
from daft.session import Session

from archetype.core.aio import AsyncCachedStore, AsyncQueryManager, AsyncStore, AsyncUpdateManager
from archetype.core.config import CacheConfig, StorageConfig
from archetype.core.interfaces import iAsyncQueryManager, iAsyncStore, iAsyncUpdateManager
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

    @staticmethod
    def _resolve_storage_uri(uri: str) -> tuple[str, bool]:
        """Resolve local storage paths while preserving remote object-store URIs."""
        scheme = urlparse(uri).scheme.lower()
        is_remote = scheme not in ("", "file")

        if is_remote:
            return uri, True

        base_path = pathlib.Path(uri)
        if not base_path.is_absolute():
            base_path = pathlib.Path.cwd() / base_path
        base_path.mkdir(parents=True, exist_ok=True)
        return str(base_path), False

    @classmethod
    def build_session(cls, config: StorageConfig) -> Session:
        """Build the Daft session for the default catalog-backed path."""
        _, _, session = cls.build_session_with_metadata(config)
        return session

    @classmethod
    def build_session_with_metadata(
        cls,
        config: StorageConfig,
    ) -> tuple[str, str, Session]:
        """
        Build Daft catalog/session resources from a storage config.

        The default implementation uses Iceberg with a SQLite catalog for local
        storage, or remote object stores (S3, GCS, etc.) with local SQLite
        metadata.
        """
        from pyiceberg.catalog.sql import SqlCatalog

        resolved_uri, is_remote = cls._resolve_storage_uri(str(config.uri))

        if is_remote:
            local_meta_dir = pathlib.Path(".archetype_meta")
            local_meta_dir.mkdir(parents=True, exist_ok=True)
            sqlite_db_path = local_meta_dir / "catalog.db"
            warehouse_uri = str(config.uri)
        else:
            base_path = pathlib.Path(resolved_uri)
            sqlite_db_path = base_path / "catalog.db"
            warehouse_uri = f"file://{base_path}"

        catalog = getattr(config, "catalog", None) or Catalog.from_iceberg(
            SqlCatalog(
                "archetype_iceberg_sql_catalog",
                **{
                    "uri": f"sqlite:///{sqlite_db_path}",
                    "warehouse": warehouse_uri,
                },
            )
        )

        session = Session()
        session.attach_catalog(catalog)
        session.create_namespace_if_not_exists(config.namespace)
        session.set_namespace(config.namespace)

        return resolved_uri, config.namespace, session

    @classmethod
    def resolve_location(cls, config: StorageConfig) -> tuple[str, str]:
        """Resolve storage URI and namespace without constructing a Daft session."""
        resolved_uri, _ = cls._resolve_storage_uri(str(config.uri))
        return resolved_uri, config.namespace

    async def get_backend(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]:
        """Retrieves or creates a shared backend triplet for the given storage config."""
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
        """Build a pool key covering uri, namespace, backend, and cache_config."""
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
        store: iAsyncStore
        if storage_config.use_lancedb:
            uri, namespace = self.resolve_location(storage_config)
            store = AsyncLancedbStore(uri, namespace)
        else:
            store = AsyncStore(self.build_session(storage_config))

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
