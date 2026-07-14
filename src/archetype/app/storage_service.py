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
import os
import pathlib
from urllib.parse import unquote, urlsplit

from daft.session import Session

from archetype.core.aio import AsyncCachedStore, AsyncLancedbStore, AsyncStore
from archetype.core.config import CacheConfig, StorageBackend, StorageConfig
from archetype.core.interfaces import iAsyncStore
from archetype.core.sync import SyncStore
from archetype.ledger.errors import (
    StorageRefMismatchError,
    UnsupportedAtomicInsertError,
)
from archetype.ledger.models import StorageRef
from archetype.ledger.sqlite_control import SQLiteAtomicRecordStore

logger = logging.getLogger(__name__)


def _local_path(uri: str) -> pathlib.Path | None:
    """Return the canonical local path for a plain path or ``file://`` URI.

    ``Path("file:///tmp/db")`` is a relative path whose first segment happens
    to contain a colon.  Parse file URIs explicitly so equivalent relative,
    absolute, and URI spellings resolve to one durable storage identity.
    """
    if not uri:
        raise ValueError("local storage URI must not be empty")
    parsed = urlsplit(uri)
    scheme = parsed.scheme.lower()
    if scheme not in ("", "file"):
        return None
    if scheme == "file":
        if parsed.netloc not in ("", "localhost"):
            raise ValueError("local LanceDB file URIs must not name a remote host")
        if parsed.query or parsed.fragment:
            raise ValueError("local LanceDB file URIs must not contain query or fragment data")
        if not parsed.path or not parsed.path.startswith("/"):
            raise ValueError("local LanceDB file URIs must contain an absolute path")
        path = pathlib.Path(unquote(parsed.path))
    else:
        path = pathlib.Path(uri)
    return path.expanduser().resolve(strict=False)


def _resolve_uri(uri: str) -> str:
    """Resolve local storage paths to absolute. Remote URIs pass through."""
    base_path = _local_path(uri)
    if base_path is None:
        return uri
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
        self._atomic_instances: dict[str, SQLiteAtomicRecordStore] = {}
        self._atomic_locks: dict[str, asyncio.Lock] = {}

    @staticmethod
    def storage_ref(storage_config: StorageConfig) -> StorageRef:
        """Build the credential-free identity for a supported durable store.

        The v1 atomic control catalog is intentionally scoped to a local
        LanceDB root.  Iceberg and remote LanceDB locators need a separately
        supplied shared CAS implementation before they can claim this
        restart/concurrency contract.
        """
        if storage_config.backend != StorageBackend.LANCEDB:
            raise UnsupportedAtomicInsertError(
                "durable ledger catalogs currently require local LanceDB storage"
            )
        root = _local_path(str(storage_config.uri))
        if root is None:
            raise UnsupportedAtomicInsertError(
                "durable ledger catalogs currently require a local LanceDB path"
            )
        subdir = os.environ.get("ARCT_LANCEDB_SUBDIR", "lance")
        if subdir != "lance":
            raise UnsupportedAtomicInsertError(
                "durable ledger storage requires the canonical LanceDB subdirectory 'lance'"
            )
        namespace_path = pathlib.Path(storage_config.namespace)
        if (
            len(namespace_path.parts) != 1
            or storage_config.namespace in {"", ".", ".."}
            or namespace_path.is_absolute()
            or namespace_path.name != storage_config.namespace
            or "/" in storage_config.namespace
            or "\\" in storage_config.namespace
        ):
            raise ValueError("durable ledger namespace must use one canonical local path segment")
        catalog_path = root / storage_config.namespace / ".archetype" / "catalog-v1.sqlite3"
        return StorageRef.create(
            backend=StorageBackend.LANCEDB,
            data_uri=root.as_uri(),
            namespace=storage_config.namespace,
            catalog_uri=catalog_path.as_uri(),
        )

    # Naming retained for protocol-oriented callers and documentation drafts.
    resolve_storage_ref = storage_ref

    @classmethod
    def verify_storage_ref(
        cls,
        reference: StorageRef,
        storage_config: StorageConfig,
    ) -> None:
        """Fail closed unless caller credentials locate the referenced store."""
        actual = cls.storage_ref(storage_config)
        if actual != reference:
            raise StorageRefMismatchError(
                f"storage reference {reference.storage_id} does not match "
                f"caller storage {actual.storage_id}"
            )

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

    async def get_or_create_atomic_record_store(
        self,
        storage_config: StorageConfig,
    ) -> SQLiteAtomicRecordStore:
        """Return the local SQLite catalog for one durable storage identity."""
        reference = self.storage_ref(storage_config)
        key = reference.storage_id
        if key not in self._atomic_instances:
            if key not in self._atomic_locks:
                self._atomic_locks[key] = asyncio.Lock()
            async with self._atomic_locks[key]:
                if key not in self._atomic_instances:
                    if reference.catalog_uri is None:
                        raise UnsupportedAtomicInsertError(
                            "storage reference has no atomic control catalog"
                        )
                    catalog_path = _local_path(reference.catalog_uri)
                    if catalog_path is None:
                        raise UnsupportedAtomicInsertError(
                            "SQLite control catalog must use a local file URI"
                        )
                    catalog = SQLiteAtomicRecordStore(catalog_path)
                    await catalog.initialize()
                    self._atomic_instances[key] = catalog
        return self._atomic_instances[key]

    async def get_read_existing_store(
        self,
        storage_config: StorageConfig,
    ) -> AsyncLancedbStore:
        """Return a physical-table reader whose calls cannot create tables."""
        # Validate the capability before touching the data store.  In
        # particular, never let Iceberg or a remote URI fall through to the
        # ordinary compatibility path here.
        reference = self.storage_ref(storage_config)
        key = f"ledger-read::{reference.storage_id}"
        if key not in self._instances:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            async with self._locks[key]:
                if key not in self._instances:
                    root = _local_path(reference.data_uri)
                    if root is None:
                        raise UnsupportedAtomicInsertError(
                            "read-existing ledger queries require a local LanceDB path"
                        )
                    self._instances[key] = AsyncLancedbStore(
                        str(root),
                        reference.namespace,
                        subdir="lance",
                    )
        store = self._instances[key]
        if not isinstance(store, AsyncLancedbStore):
            raise UnsupportedAtomicInsertError(
                "read-existing ledger queries currently require local LanceDB storage"
            )
        return store

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

        for store in self._atomic_instances.values():
            try:
                shutdown = getattr(store, "shutdown", None)
                if shutdown is not None:
                    result = shutdown()
                    if hasattr(result, "__await__"):
                        await result
            except Exception as e:
                logger.exception("Failed to shut down atomic record store %r", store)
                errors.append(e)

        self._instances.clear()
        self._locks.clear()
        self._atomic_instances.clear()
        self._atomic_locks.clear()

        if errors:
            raise RuntimeError(
                f"StorageService.shutdown failed for {len(errors)} store(s); "
                f"first error: {errors[0]!r}"
            ) from errors[0]
