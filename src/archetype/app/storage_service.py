# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Storage Service

Layer 1: StorageContextFactory — session/catalog construction
Layer 2: AsyncStorageFactory, SyncStorageFactory — store creation
Layer 3: StorageService — multiton pooling facade
"""

from __future__ import annotations

import asyncio
import logging
import pathlib
from urllib.parse import urlparse

from daft.catalog import Catalog
from daft.session import Session

from archetype.core.aio import AsyncCachedStore, AsyncLancedbStore, AsyncStore
from archetype.core.config import CacheConfig, StorageBackend, StorageConfig
from archetype.core.interfaces import iAsyncStore
from archetype.core.sync import SyncStore

from dataclasses import dataclass
from daft.io import IOConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StorageContext:
    """Resolved storage resources for a single StorageConfig."""

    uri: str
    namespace: str
    session: Session
    io_config: IOConfig | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Storage Context Factory
# ─────────────────────────────────────────────────────────────────────────────


class StorageContextFactory:
    """Builds StorageContext (uri, namespace, session) from StorageConfig.

    Owns URI resolution, catalog creation, and Daft session setup.
    No pooling, no caching, no lifecycle.
    """

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

    def resolve_location(self, config: StorageConfig) -> tuple[str, str]:
        """Resolve storage URI and namespace without constructing a session.

        Used by backends (e.g. LanceDB) that don't need a Daft catalog session.
        """
        resolved_uri, _ = self._resolve_storage_uri(str(config.uri))
        return resolved_uri, config.namespace

    def build(self, config: StorageConfig) -> StorageContext:
        """Build resolved storage resources from a StorageConfig.

        Creates the Daft catalog, session, and namespace for the Iceberg backend.
        """
        from pyiceberg.catalog.sql import SqlCatalog

        resolved_uri, is_remote = self._resolve_storage_uri(str(config.uri))

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

        return StorageContext(
            uri=resolved_uri,
            namespace=config.namespace,
            session=session,
            io_config=config.io_config,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Layer 2 — Storage Factories
# ─────────────────────────────────────────────────────────────────────────────


class AsyncStorageFactory:
    """Creates async stores from configuration.

    Each call creates a fresh store instance — no pooling.
    """

    def __init__(self, context_factory: StorageContextFactory) -> None:
        self._context_factory = context_factory

    def create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iAsyncStore:
        store: iAsyncStore
        if storage_config.backend == StorageBackend.LANCEDB:
            uri, namespace = self._context_factory.resolve_location(storage_config)
            store = AsyncLancedbStore(uri, namespace)
        else:
            ctx = self._context_factory.build(storage_config)
            store = AsyncStore(ctx.session, io_config=ctx.io_config)

        if isinstance(cache_config, bool):
            cache_config = CacheConfig() if cache_config else None

        if cache_config:
            store = AsyncCachedStore(async_store=store, cache_config=cache_config)

        return store


class SyncStorageFactory:
    """Creates sync stores from configuration."""

    def __init__(self, context_factory: StorageContextFactory) -> None:
        self._context_factory = context_factory

    def create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> SyncStore:
        ctx = self._context_factory.build(storage_config)
        return SyncStore(ctx.uri, ctx.session, io_config=ctx.io_config)


# ─────────────────────────────────────────────────────────────────────────────
# Layer 3 — Storage Service (pooling facade)
# ─────────────────────────────────────────────────────────────────────────────


class StorageService:
    """Creates and pools async stores.  Manages storage lifecycle.

    Multiton semantics: for any (uri, namespace, backend, cache) tuple,
    only one store instance is created and reused.

    Internal composition: ``StorageContextFactory`` → ``AsyncStorageFactory``
    are built automatically.  Pass *factory* only when you need to override
    store construction in tests.
    """

    def __init__(self, factory: AsyncStorageFactory | None = None) -> None:
        self._factory = factory or AsyncStorageFactory(StorageContextFactory())
        self._instances: dict[str, iAsyncStore] = {}
        self._locks: dict[str, asyncio.Lock] = {}

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

    async def create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iAsyncStore:
        """Return the pooled async store for a storage/cache configuration."""
        key = self._pool_key(storage_config, cache_config)

        if key not in self._instances:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()

            async with self._locks[key]:
                if key not in self._instances:
                    self._instances[key] = self._factory.create_store(
                        storage_config, cache_config
                    )

        return self._instances[key]

    async def shutdown(self):
        """Gracefully shuts down all managed storage backends.

        Best-effort: a failure in one store's shutdown does not abort the
        loop. Every store gets a chance to run its cleanup, and the pool
        dicts are always cleared. If any store's shutdown raised, an
        aggregate RuntimeError is raised after the cleanup completes.
        """
        errors: list[Exception] = []
        for store in self._instances.values():
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
