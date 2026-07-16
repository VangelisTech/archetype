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
Caller-configured Daft sessions are the extension point for managed Iceberg.
"""

from __future__ import annotations

import asyncio
import logging
import os
import pathlib
from urllib.parse import urlparse

from daft.session import Session

from archetype.app._catalog import ControlCatalog, SqliteControlCatalog, catalog_path_for
from archetype.core.aio import AsyncCachedStore, AsyncLancedbStore, AsyncStore
from archetype.core.config import CacheConfig, StorageBackend, StorageConfig
from archetype.core.interfaces import iAsyncStore

logger = logging.getLogger(__name__)


def _validate_session_namespace(session: Session, config: StorageConfig) -> None:
    """Fail before a shared session can route a store into the wrong namespace."""
    current = session.current_namespace()
    if current is None:
        raise ValueError("injected Daft Session must have a current namespace")
    if str(current) != config.namespace:
        raise ValueError(
            "injected Daft Session namespace mismatch: "
            f"current={current}, requested={config.namespace}"
        )


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
    For Iceberg without a session: builds Archetype's concrete local
    SQLite-catalog lakehouse. A supplied session is already authoritative for
    catalog, namespace, and credentials and passes through unchanged.
    """
    store: iAsyncStore
    if config.backend == StorageBackend.LANCEDB:
        uri = _resolve_uri(str(config.uri))
        store = AsyncLancedbStore(uri, config.namespace)
    else:
        from archetype.runtime.session import configure_session

        if session is not None:
            _validate_session_namespace(session, config)
            sess = session
        else:
            sess = configure_session(config)
        store = AsyncStore(sess, io_config=config.io_config)

    if isinstance(cache_config, bool):
        cache_config = CacheConfig() if cache_config else None

    if cache_config:
        store = AsyncCachedStore(async_store=store, cache_config=cache_config)

    return store


class StorageService:
    """Creates and pools async stores. Manages storage lifecycle.

    Multiton semantics: stores with the same location, backend, cache, and
    effective Daft ``IOConfig`` share one instance.
    """

    def __init__(self, session: Session | None = None) -> None:
        """Create a pool around an optional caller-configured Daft session."""
        self._session = session
        self._session_identity: tuple[str, str] | None = None
        self._session_lock = asyncio.Lock()
        self._instances: dict[str, iAsyncStore] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        # Control catalogs, pooled by resolved catalog path (issue #272).
        # The catalog is an implementation resource of this service — it is
        # authoritative for discovery, and its location is a pure function
        # of the storage identity (see app/_catalog.py).
        self._catalogs: dict[str, ControlCatalog] = {}

    def get_control_catalog(self, storage_config: StorageConfig):
        """The durable control catalog for a storage location (pooled).

        Default: the local SQLite catalog (the reference implementation,
        single-host authority). Setting ``ARCHETYPE_CONTROL_CATALOG_URL``
        (+ optional ``ARCHETYPE_CONTROL_CATALOG_TOKEN``) selects the remote
        Durable Objects catalog (issue #281), namespaced by the storage
        fingerprint — the same identity key both implementations pool by,
        so a config resolves to ONE catalog whichever backend serves it.
        """
        remote_url = os.environ.get("ARCHETYPE_CONTROL_CATALOG_URL", "").strip()
        if remote_url:
            from archetype.app._catalog import storage_fingerprint
            from archetype.app._remote_catalog import RemoteControlCatalog

            namespace = storage_fingerprint(storage_config)[:24]
            key = f"{remote_url}::{namespace}"
            catalog = self._catalogs.get(key)
            if catalog is None:
                catalog = RemoteControlCatalog(
                    remote_url,
                    namespace,
                    token=os.environ.get("ARCHETYPE_CONTROL_CATALOG_TOKEN") or None,
                )
                self._catalogs[key] = catalog
            return catalog

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
        """Build a pool key without serializing credential-bearing I/O config."""
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

        io_config_part = (
            f"fingerprint={hash(storage_config.io_config)}"
            if storage_config.backend == StorageBackend.ICEBERG
            and storage_config.io_config is not None
            else "none"
        )

        return (
            f"{storage_config.uri}"
            f"::{storage_config.namespace}"
            f"::backend={storage_config.backend.value}"
            f"::io_config({io_config_part})"
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

        if self._session is not None and storage_config.backend == StorageBackend.ICEBERG:
            async with self._session_lock:
                return self._get_or_create_injected_store(key, storage_config, cache_config)

        if key not in self._instances:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()

            async with self._locks[key]:
                if key not in self._instances:
                    self._instances[key] = create_async_store(
                        storage_config, self._session, cache_config
                    )

        return self._instances[key]

    def _get_or_create_injected_store(
        self,
        key: str,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None,
    ) -> iAsyncStore:
        """Create successfully before committing an injected session binding."""
        assert self._session is not None
        _validate_session_namespace(self._session, storage_config)
        requested = (str(storage_config.uri), storage_config.namespace)
        if self._session_identity is not None and requested != self._session_identity:
            raise ValueError(
                "one injected Daft Session cannot serve multiple storage identities; "
                f"bound={self._session_identity}, requested={requested}. "
                "Use a separate StorageService and Session."
            )

        store = self._instances.get(key)
        if store is None:
            store = create_async_store(storage_config, self._session, cache_config)
            self._instances[key] = store
            self._session_identity = requested
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

        for key, catalog in self._catalogs.items():
            try:
                await catalog.close()
            except Exception as e:
                logger.exception("Failed to close control catalog %r", key)
                errors.append(e)

        self._instances.clear()
        self._locks.clear()
        self._catalogs.clear()

        if errors:
            raise RuntimeError(
                f"StorageService.shutdown failed for {len(errors)} store(s); "
                f"first error: {errors[0]!r}"
            ) from errors[0]
