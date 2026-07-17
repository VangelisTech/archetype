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
from pathlib import Path

from daft.session import Session

from archetype._storage_uri import local_storage_path, normalized_storage_uri
from archetype.app._catalog import ControlCatalog, SqliteControlCatalog, catalog_path_for
from archetype.app.iceberg import IcebergCatalogContext
from archetype.core.aio import AsyncCachedStore, AsyncLancedbStore, AsyncStore
from archetype.core.config import CacheConfig, StorageBackend, StorageConfig
from archetype.core.interfaces import iAsyncStore
from archetype.core.paths import require_safe_namespace, resolve_local_root

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
    """Resolve local storage paths to absolute. Remote URIs pass through.

    Local paths route through ``resolve_local_root`` (issue #327): NUL bytes
    are rejected and, when ``ARCHETYPE_DATA_ROOT`` is set, escapes fail closed.
    """
    if local_storage_path(uri) is None:
        return uri
    base_path = resolve_local_root(uri)
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
    # Unconditional: the injected-session Iceberg branch must reject an unsafe
    # namespace here too, before any world can bind to the store — the catalog
    # path derived from the same config validates it either way (issue #327).
    namespace = require_safe_namespace(config.namespace)
    if config.backend == StorageBackend.LANCEDB:
        uri = _resolve_uri(str(config.uri))
        if local_storage_path(str(config.uri)) is not None:
            # A pre-planted symlink at <uri>/<namespace> could redirect writes
            # outside ARCHETYPE_DATA_ROOT even with a safe segment name;
            # resolve the namespace directory under the same containment rule.
            resolve_local_root(str(Path(uri) / namespace))
        store = AsyncLancedbStore(uri, namespace)
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
        self._required_session_identity: tuple[str, str] | None = None
        self._session_lock = asyncio.Lock()
        self._instances: dict[str, iAsyncStore] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        # Control catalogs, pooled by resolved catalog path (issue #272).
        # The catalog is an implementation resource of this service — it is
        # authoritative for discovery, and its location is a pure function
        # of the storage identity (see app/_catalog.py).
        self._catalogs: dict[str, ControlCatalog] = {}

    @property
    def has_injected_session(self) -> bool:
        """Whether this service is restricted to one caller-owned catalog."""
        return self._session is not None

    def require_iceberg_identity(self, storage_config: StorageConfig) -> None:
        """Constrain an injected session before any store binds it."""
        if self._session is None:
            return
        if storage_config.backend != StorageBackend.ICEBERG:
            raise ValueError("an injected Daft Session requires backend=iceberg")
        _validate_session_namespace(self._session, storage_config)
        requested = (
            normalized_storage_uri(str(storage_config.uri)),
            storage_config.namespace,
        )
        bound = self._session_identity
        required = self._required_session_identity
        if bound is not None and requested != bound:
            raise ValueError(
                "injected Daft Session is already bound to a different storage identity; "
                f"bound={bound}, requested={requested}"
            )
        if required is not None and requested != required:
            raise ValueError(
                "injected Daft Session already requires a different storage identity; "
                f"required={required}, requested={requested}"
            )
        self._required_session_identity = requested

    def get_control_catalog(self, storage_config: StorageConfig):
        """The durable control catalog for a storage location (pooled).

        Default: the local SQLite catalog (the reference implementation,
        single-host authority). Setting ``ARCHETYPE_CONTROL_CATALOG_URL``
        and ``ARCHETYPE_CONTROL_CATALOG_TOKEN`` selects the remote
        Durable Objects catalog (issue #281), namespaced by the storage
        fingerprint — the same identity key both implementations pool by,
        so a config resolves to ONE catalog whichever backend serves it.
        """
        remote_url = os.environ.get("ARCHETYPE_CONTROL_CATALOG_URL", "").strip()
        if remote_url:
            from archetype.app._catalog import storage_fingerprint
            from archetype.app._remote_catalog import RemoteControlCatalog

            token = os.environ.get("ARCHETYPE_CONTROL_CATALOG_TOKEN", "").strip()
            if not token:
                raise RuntimeError(
                    "ARCHETYPE_CONTROL_CATALOG_TOKEN is required when "
                    "ARCHETYPE_CONTROL_CATALOG_URL is configured"
                )
            namespace = storage_fingerprint(storage_config)[:24]
            key = f"{remote_url}::{namespace}"
            catalog = self._catalogs.get(key)
            if catalog is None:
                catalog = RemoteControlCatalog(
                    remote_url,
                    namespace,
                    token=token,
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
            f"{normalized_storage_uri(str(storage_config.uri))}"
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
        requested = (
            normalized_storage_uri(str(storage_config.uri)),
            storage_config.namespace,
        )
        if (
            self._required_session_identity is not None
            and requested != self._required_session_identity
        ):
            raise ValueError(
                "injected Daft Session is configured for a different storage identity; "
                f"required={self._required_session_identity}, requested={requested}"
            )
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

    async def get_iceberg_context(
        self,
        storage_config: StorageConfig,
    ) -> IcebergCatalogContext:
        """Return the authoritative Daft catalog context for an Iceberg config."""
        if storage_config.backend != StorageBackend.ICEBERG:
            raise ValueError("Iceberg catalog context requires backend=iceberg")
        store = await self.get_or_create_store(storage_config, cache_config=None)
        if not isinstance(store, AsyncStore):
            raise TypeError(f"expected AsyncStore for Iceberg, got {type(store).__name__}")
        return IcebergCatalogContext(session=store.session, io_config=store.io_config)

    async def shutdown(self):
        """Gracefully shuts down all managed storage backends."""
        errors: list[Exception] = []
        cancelled: asyncio.CancelledError | None = None
        try:
            for store in self._instances.values():
                try:
                    if asyncio.iscoroutinefunction(getattr(store, "shutdown", None)):
                        await store.shutdown()
                    elif hasattr(store, "shutdown"):
                        store.shutdown()  # ty: ignore[unused-awaitable]
                except asyncio.CancelledError as exc:
                    cancelled = cancelled or exc
                except Exception as exc:
                    logger.exception("Failed to shut down store %r", store)
                    errors.append(exc)

            for key, catalog in self._catalogs.items():
                try:
                    await catalog.close()
                except asyncio.CancelledError as exc:
                    cancelled = cancelled or exc
                except Exception as exc:
                    logger.exception("Failed to close control catalog %r", key)
                    errors.append(exc)
        finally:
            self._instances.clear()
            self._locks.clear()
            self._catalogs.clear()

        shutdown_error = (
            RuntimeError(
                f"StorageService.shutdown failed for {len(errors)} store(s); "
                f"first error: {errors[0]!r}"
            )
            if errors
            else None
        )
        if cancelled is not None:
            if shutdown_error is not None:
                raise cancelled from shutdown_error
            raise cancelled
        if shutdown_error is not None:
            raise shutdown_error from errors[0]
