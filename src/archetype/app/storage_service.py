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
import os
import pathlib
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import daft
import lancedb
from daft import DataFrame
from daft.catalog import Catalog
from daft.io import IOConfig
from daft.session import Session
from lancedb.index import Bitmap, BTree

from archetype.core.aio import AsyncCachedStore, AsyncQueryManager, AsyncStore, AsyncUpdateManager
from archetype.core.archetype import Archetype
from archetype.core.config import CacheConfig, StorageConfig
from archetype.core.interfaces import iAsyncQueryManager, iAsyncStore, iAsyncUpdateManager
from archetype.core.sync import SyncStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StorageContext:
    uri: str
    namespace: str
    session: Session
    io_config: IOConfig | None = None


class StorageContextFactory:
    @staticmethod
    def build(config: StorageConfig) -> StorageContext:
        uri, namespace, session = StorageService.build_session_with_metadata(config)
        return StorageContext(
            uri=uri,
            namespace=namespace,
            session=session,
            io_config=config.io_config,
        )


class AsyncLancedbStore(iAsyncStore):
    def __init__(
        self,
        uri: str | object,
        namespace: str | None = None,
    ):
        if namespace is None and not isinstance(uri, str):
            legacy_storage = uri
            uri = legacy_storage.uri
            namespace = legacy_storage.namespace

        if namespace is None:
            raise TypeError("AsyncLancedbStore requires a namespace")

        self.uri = uri
        self.namespace = namespace
        self.lancedb = None
        self._known_sigs: dict[str, tuple[type, ...]] = {}

    async def _ensure_table(self, sig):
        table_name = Archetype.get_name(sig)
        pyarrow_schema = Archetype.get_archetype_schema(sig)

        if self.lancedb is None:
            subdir = os.environ.get("ARCT_LANCEDB_SUBDIR", "lance")
            self.lancedb = await lancedb.connect_async(
                os.path.join(self.uri, self.namespace, subdir)
            )

        if table_name in await self._list_table_names():
            try:
                async_table = await self.lancedb.open_table(table_name)
            except Exception as e:
                raise RuntimeError(f"Error opening LanceDB table {table_name}: {e}") from e

            self._known_sigs[table_name] = sig
            return async_table

        try:
            async_table = await self.lancedb.create_table(
                name=table_name,
                schema=pyarrow_schema,
                exist_ok=True,
            )
            if os.environ.get("ARCT_LANCEDB_INDEX_ENTITY", "1") == "1":
                await async_table.create_index(column="entity_id", config=BTree(), replace=True)
            if os.environ.get("ARCT_LANCEDB_INDEX_WORLD", "1") == "1":
                await async_table.create_index(column="world_id", config=Bitmap(), replace=True)
            if os.environ.get("ARCT_LANCEDB_INDEX_RUN", "1") == "1":
                await async_table.create_index(column="run_id", config=Bitmap(), replace=True)
            if os.environ.get("ARCT_LANCEDB_INDEX_TICK", "1") == "1":
                await async_table.create_index(column="tick", config=BTree(), replace=True)
        except Exception as e:
            logger.error(f"Error creating LanceDB table {table_name}: {e}")
            raise RuntimeError(f"Error creating LanceDB table {table_name}: {e}") from e

        self._known_sigs[table_name] = sig
        return async_table

    async def _list_table_names(self) -> list[str]:
        if self.lancedb is None:
            return []

        list_tables = getattr(self.lancedb, "list_tables", None)
        if list_tables is not None:
            response = await list_tables()
            if hasattr(response, "tables"):
                return list(response.tables)
            return list(response)

        return list(await self.lancedb.table_names())

    async def get_archetype_df(
        self,
        sig,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        active_only: bool = False,
    ) -> DataFrame:
        table_name = Archetype.get_name(sig)
        async_table = await self._ensure_table(sig)

        try:
            safe_world = str(world_id).replace("'", "''")
            safe_run = str(run_id).replace("'", "''")
            clauses = [
                f"world_id = '{safe_world}'",
                f"run_id = '{safe_run}'",
            ]

            if active_only:
                clauses.append("is_active = true")

            if ticks is not None:
                tick_list = ", ".join(str(int(t)) for t in ticks)
                clauses.append(f"tick IN ({tick_list})")

            if entity_ids is not None:
                id_list = ", ".join(str(int(eid)) for eid in entity_ids)
                clauses.append(f"entity_id IN ({id_list})")

            where_str = " AND ".join(clauses)
            filtered_arrow = await async_table.query().where(where_str).to_arrow()
            df = daft.from_arrow(filtered_arrow)

        except Exception as e:
            logger.error(f"Error reading archetype table {table_name}: {e}")
            raise e

        return df

    async def list_signatures(self) -> list[tuple[type, ...]]:
        return list(self._known_sigs.values())

    async def append(self, sig, df: DataFrame) -> None:
        try:
            df.collect()
            if df.count_rows() == 0 or not df.column_names:
                logger.info(
                    f"Append skipped (lancedb): archetype={Archetype.get_name(sig)} rows=0 or empty schema"
                )
                return
        except Exception as e:
            logger.error(f"Append collect failed for {Archetype.get_name(sig)}: {e}")
            return

        async_table = await self._ensure_table(sig)
        table_name = async_table.name
        try:
            start_time = time.time()
            arrow_table = df.to_arrow()
            await async_table.add(arrow_table, mode="append")
            end_time = time.time()
            logger.info(
                f"Appended dataframe to table {table_name} in {end_time - start_time} seconds"
            )
        except Exception as e:
            logger.error(f"Error appending dataframe to table {table_name}: {e}")
            raise

    async def shutdown(self) -> None:
        if self.lancedb is not None:
            try:
                close = getattr(self.lancedb, "close", None)
                if close:
                    result = close()
                    if hasattr(result, "__await__"):
                        await result
            finally:
                self.lancedb = None

    async def optimize_tables(self) -> None:
        if self.lancedb is None:
            return

        for table_name in await self._list_table_names():
            try:
                async_table = await self.lancedb.open_table(table_name)
                await async_table.optimize(retrain=False)
            except Exception as e:
                raise RuntimeError(f"Error optimizing table {table_name}: {e}") from e


class AsyncLancedbQueryManager(AsyncQueryManager):
    def __init__(self, store: AsyncLancedbStore):
        super().__init__(store)


class AsyncStorageFactory:
    def __init__(self, storage_service: StorageService | None = None):
        self._storage_service = storage_service or StorageService()

    async def create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iAsyncStore:
        return await self._storage_service.create_store(storage_config, cache_config)

    async def shutdown(self) -> None:
        await self._storage_service.shutdown()


class SyncStorageFactory:
    def __init__(self, storage_service: StorageService | None = None):
        self._storage_service = storage_service or StorageService()

    def create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> SyncStore:
        uri, _, session = self._storage_service.build_session_with_metadata(storage_config)
        return SyncStore(uri, session, io_config=storage_config.io_config)

    def shutdown(self) -> None:
        return None


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

    async def create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iAsyncStore:
        """Return the pooled async store for a storage/cache configuration."""
        store, _, _ = await self.get_backend(storage_config, cache_config)
        return store

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
        store = self._create_store(storage_config, cache_config)
        querier = AsyncQueryManager(store=store)
        updater = AsyncUpdateManager(store=store)

        return (store, querier, updater)

    def _create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None,
    ) -> iAsyncStore:
        store: iAsyncStore
        if storage_config.use_lancedb:
            uri, namespace = self.resolve_location(storage_config)
            store = AsyncLancedbStore(uri, namespace)
        else:
            store = AsyncStore(
                self.build_session(storage_config), io_config=storage_config.io_config
            )

        if isinstance(cache_config, bool):
            cache_config = CacheConfig() if cache_config else None

        if cache_config:
            store = AsyncCachedStore(async_store=store, cache_config=cache_config)

        return store

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
