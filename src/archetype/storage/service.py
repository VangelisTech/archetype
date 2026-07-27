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

"""Application storage execution and durability authority."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

import pyarrow as pa
from daft import DataFrame, DataType, col, lit, read_iceberg
from daft.catalog import Catalog, Table
from daft.io import IOConfig
from daft.session import Session
from pyiceberg.exceptions import (
    CommitFailedException,
    CommitStateUnknownException,
    TableAlreadyExistsError,
)

from archetype.core.aio import AsyncCachedStore, AsyncLancedbStore, AsyncStore
from archetype.core.config import CacheConfig, StorageBackend, StorageConfig
from archetype.core.interfaces import AppendReceipt, ArchetypeSignature, iAsyncStore
from archetype.core.paths import (
    local_storage_path,
    normalized_storage_uri,
    require_safe_namespace,
    resolve_local_root,
)
from archetype.storage.catalog import (
    ControlCatalog,
    RemoteControlCatalog,
    SignatureRecord,
    SqliteControlCatalog,
    WorldRecord,
    catalog_path_for,
    storage_fingerprint,
)
from archetype.storage.commit import CatalogCommitCoordinator
from archetype.storage.config import ControlCatalogConfig

logger = logging.getLogger(__name__)

_MAX_COMMIT_ATTEMPTS = 16
_T = TypeVar("_T")
_WORLD_ENVELOPE_COLUMNS = ("world_id", "run_id")


class AmbiguousCommitError(RuntimeError):
    """An Iceberg commit whose outcome cannot be proven either way.

    Raised when the catalog reports an unknown commit state
    (``CommitStateUnknownException``): the append may or may not have become
    part of the table. Retrying could double-append and swallowing could drop
    a durable write, so the operation freezes as this typed outcome — the
    deliberate v0.5 posture. The caller (or operator) reconciles by inspecting
    the table before resubmitting.
    """

    def __init__(self, table_name: str, attempt: int) -> None:
        super().__init__(
            f"Iceberg commit outcome unknown for table {table_name!r} "
            f"(attempt {attempt + 1}): the append may or may not have "
            "committed; not retrying (a retry could double-append). "
            "Inspect the table before resubmitting."
        )
        self.table_name = table_name
        self.attempt = attempt


@dataclass(frozen=True, slots=True)
class PinnedVisibility:
    """Immutable physical visibility selected for one world/run segment."""

    world_id: str
    run_id: str
    head_tick: int | None
    head_tokens: tuple[str, ...]
    visibility_tokens: tuple[str, ...] | None
    max_tick: int | None


@dataclass(frozen=True, slots=True)
class VisibleTableRows:
    """One signature table after physical world/run/manifest filtering."""

    signature: SignatureRecord
    frame: DataFrame
    latest_physical_tick: int | None


@dataclass(frozen=True, slots=True)
class VisibleWorldRows:
    """Raw visible signature-table frames without world-state interpretation."""

    visibility: PinnedVisibility
    tables: tuple[VisibleTableRows, ...]
    latest_physical_tick: int | None


class _DaftExecutionGate:
    """One reentrant admission lane for terminal Daft work in this process.

    Reentrancy is required because a cached-store append can trigger a flush
    into its inner durable store in the same task. Background flushes run in a
    different task and therefore wait for the active operation.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._owner: asyncio.Task[Any] | None = None
        self._depth = 0

    @asynccontextmanager
    async def admit(self) -> AsyncIterator[None]:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("Daft execution admission requires an asyncio task")
        if task is self._owner:
            self._depth += 1
            try:
                yield
            finally:
                self._depth -= 1
            return

        await self._lock.acquire()
        self._owner = task
        self._depth = 1
        try:
            yield
        finally:
            self._depth = 0
            self._owner = None
            self._lock.release()


class _AdmittedAsyncStore(AsyncStore):
    """Iceberg ECS store whose terminal append shares StorageService admission."""

    def __init__(
        self,
        session: Session | object,
        io_config: IOConfig | None,
        execution_gate: _DaftExecutionGate,
    ) -> None:
        super().__init__(session, io_config=io_config)
        self._execution_gate = execution_gate

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> AppendReceipt:
        async with self._execution_gate.admit():
            return await super().append(sig, df)


class _AdmittedAsyncLancedbStore(AsyncLancedbStore):
    """LanceDB ECS store whose terminal append shares StorageService admission."""

    def __init__(
        self,
        uri: str,
        namespace: str,
        execution_gate: _DaftExecutionGate,
    ) -> None:
        super().__init__(uri, namespace)
        self._execution_gate = execution_gate

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> AppendReceipt:
        async with self._execution_gate.admit():
            return await super().append(sig, df)


class _AdmittedAsyncCachedStore(AsyncCachedStore):
    """Cache whose materialization and explicit drains use the shared lane."""

    def __init__(
        self,
        async_store: iAsyncStore,
        cache_config: CacheConfig,
        execution_gate: _DaftExecutionGate,
    ) -> None:
        super().__init__(async_store, cache_config)
        self._execution_gate = execution_gate

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> AppendReceipt:
        async with self._execution_gate.admit():
            return await super().append(sig, df)

    async def flush(self) -> None:
        async with self._execution_gate.admit():
            await super().flush()


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
    *,
    execution_gate: _DaftExecutionGate | None = None,
) -> iAsyncStore:
    """Create an async store from a StorageConfig.

    For LanceDB: uses resolved uri + namespace directly.
    For Iceberg without a session: builds Archetype's concrete local lakehouse,
    whose PyIceberg catalog metadata uses SQLite. That data-catalog metadata is
    distinct from ``ControlCatalog`` transaction state. A supplied session is
    already authoritative for catalog, namespace, and credentials and passes
    through unchanged.
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
        store = (
            _AdmittedAsyncLancedbStore(uri, namespace, execution_gate)
            if execution_gate is not None
            else AsyncLancedbStore(uri, namespace)
        )
    else:
        from archetype.storage.session import configure_session

        if session is not None:
            _validate_session_namespace(session, config)
            sess = session
        else:
            sess = configure_session(config)
        store = (
            _AdmittedAsyncStore(sess, config.io_config, execution_gate)
            if execution_gate is not None
            else AsyncStore(sess, io_config=config.io_config)
        )

    if isinstance(cache_config, bool):
        cache_config = CacheConfig() if cache_config else None

    if cache_config:
        store = (
            _AdmittedAsyncCachedStore(store, cache_config, execution_gate)
            if execution_gate is not None
            else AsyncCachedStore(async_store=store, cache_config=cache_config)
        )

    return store


class StorageService:
    """Coordinate Daft execution and manage both durable storage planes.

    Multiton semantics make stores with the same location, backend, cache, and
    effective Daft ``IOConfig`` share one instance. The local execution gate
    orders terminal plans within this process; Iceberg's atomic optimistic
    commits remain the cross-process data-table authority.
    """

    def __init__(
        self,
        session: Session | None = None,
        *,
        control_catalog_config: ControlCatalogConfig | None = None,
    ) -> None:
        """Create a pool around an optional caller-configured Daft session."""
        self._session = session
        self._control_catalog_config = control_catalog_config or ControlCatalogConfig()
        self._session_identity: tuple[str, str] | None = None
        self._required_session_identity: tuple[str, str] | None = None
        self._session_lock = asyncio.Lock()
        self._execution_gate = _DaftExecutionGate()
        self._instances: dict[str, iAsyncStore] = {}
        self._store_locks: dict[str, asyncio.Lock] = {}
        # Control catalogs, pooled by resolved catalog path (issue #272).
        # The catalog is an implementation resource of this service — it is
        # authoritative for discovery, and its location is a pure function
        # of the storage identity (see storage/catalog/sqlite.py).
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

    def get_control_catalog(self, storage_config: StorageConfig) -> ControlCatalog:
        """The durable control catalog for a storage location (pooled).

        The immutable bootstrap configuration selects the local SQLite
        reference implementation or remote Durable Objects authority. The
        storage fingerprint namespaces both implementations so one storage
        identity resolves to exactly one pooled catalog.
        """
        remote_url = self._control_catalog_config.remote_url
        if remote_url:
            token = self._control_catalog_config.remote_token
            assert token is not None
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

        path = catalog_path_for(storage_config, self._control_catalog_config)
        key = str(path)
        catalog = self._catalogs.get(key)
        if catalog is None:
            catalog = SqliteControlCatalog(path)
            self._catalogs[key] = catalog
        return catalog

    def bind_commit_coordinator(
        self,
        storage_config: StorageConfig,
        *,
        world_id: str,
        run_id: str,
        writer_epoch: int,
    ) -> CatalogCommitCoordinator:
        """Construct a coordinator bound to one durable writer identity."""
        return CatalogCommitCoordinator.bound(
            self.get_control_catalog(storage_config),
            str(world_id),
            str(run_id),
            int(writer_epoch),
        )

    async def pin_visibility(
        self,
        storage_config: StorageConfig,
        world_id: str,
        *,
        run_id: str | None = None,
        max_tick: int | None = None,
    ) -> PinnedVisibility:
        """Capture one stable manifest allowlist for a physical read.

        ``None`` visibility means legacy, never-fenced history and remains
        distinct from an empty tuple, which means a coordinated world with no
        published manifest. Interpretation of the resulting rows belongs to
        the consuming family.
        """
        wid = str(world_id)
        catalog = self.get_control_catalog(storage_config)
        record = await catalog.get_world(wid)
        if record is None:
            raise KeyError(f"world {wid} is not recorded in catalog for {storage_config.uri}")
        selected_run = str(run_id or record.run_id or "")
        if not selected_run:
            raise RuntimeError(f"world {wid} has no recorded run; visibility cannot be pinned")

        manifests = await catalog.list_manifests(wid, selected_run)
        eligible = [
            manifest for manifest in manifests if max_tick is None or int(manifest.tick) <= max_tick
        ]
        eligible_ticks = {int(manifest.tick) for manifest in eligible}
        eligible_tokens = {str(manifest.commit_token) for manifest in eligible}
        head_tick = max((int(manifest.tick) for manifest in eligible), default=None)
        head_tokens = (
            tuple(
                sorted(
                    manifest.commit_token
                    for manifest in eligible
                    if int(manifest.tick) == head_tick
                )
            )
            if head_tick is not None
            else ()
        )

        visible = await catalog.visible_tokens(wid, selected_run)
        visibility_tokens = (
            None
            if visible is None
            else tuple(
                sorted(
                    token
                    for tick, tokens in visible.items()
                    if int(tick) in eligible_ticks
                    for token in tokens
                    if token in eligible_tokens
                )
            )
        )
        return PinnedVisibility(
            world_id=wid,
            run_id=selected_run,
            head_tick=head_tick,
            head_tokens=head_tokens,
            visibility_tokens=visibility_tokens,
            max_tick=max_tick,
        )

    async def scan_visible_world_rows(
        self,
        storage_config: StorageConfig,
        world_record: WorldRecord,
        visibility: PinnedVisibility,
    ) -> VisibleWorldRows:
        """Return raw physical rows admitted by one pinned visibility.

        This method does not group by entity, resolve active-state ties, load
        component classes, derive lineage, choose a resume tick, or allocate an
        entity id. Those are world semantics layered over these frames.
        """
        if str(world_record.world_id) != visibility.world_id:
            raise ValueError(
                "world record and visibility identify different worlds: "
                f"{world_record.world_id!r} != {visibility.world_id!r}"
            )

        store = await self.get_or_create_store(storage_config)
        catalog = self.get_control_catalog(storage_config)
        tables: list[VisibleTableRows] = []
        latest_physical_tick: int | None = None
        for signature in await catalog.list_signatures():
            try:
                frame = await store.get_existing_table_df(
                    signature.table_id,
                    visibility.world_id,
                    visibility.run_id,
                )
            except KeyError:
                continue

            if visibility.visibility_tokens is not None and "commit_token" in frame.column_names:
                admitted = (
                    frame["commit_token"].is_in(list(visibility.visibility_tokens))
                    if visibility.visibility_tokens
                    else lit(False)
                )
                frame = frame.where(admitted)
            if visibility.max_tick is not None:
                frame = frame.where(
                    frame["tick"] <= visibility.max_tick  # ty: ignore[unsupported-operator]
                )

            tick_frame = await self.materialize(frame.agg(col("tick").max().alias("latest_tick")))
            latest_values = tick_frame.to_pydict().get("latest_tick", [])
            latest_value = latest_values[0] if latest_values else None
            table_head = int(latest_value) if latest_value is not None else None
            if table_head is not None and (
                latest_physical_tick is None or table_head > latest_physical_tick
            ):
                latest_physical_tick = table_head
            tables.append(
                VisibleTableRows(
                    signature=signature,
                    frame=frame,
                    latest_physical_tick=table_head,
                )
            )

        return VisibleWorldRows(
            visibility=visibility,
            tables=tuple(tables),
            latest_physical_tick=latest_physical_tick,
        )

    async def append_world_rows(
        self,
        storage_config: StorageConfig,
        world_id: str,
        table_name: str,
        rows: DataFrame,
        *,
        key_columns: tuple[str, ...] = (),
    ) -> int:
        """Stamp the durable world/run envelope and append typed rows."""
        self._require_frame(rows)
        wid, rid = await self._durable_world_context(storage_config, world_id)
        names = tuple(rows.schema().column_names())
        owned = [name for name in _WORLD_ENVELOPE_COLUMNS if name in names]
        if owned:
            raise ValueError("world_id and run_id are assigned by storage, not callers")
        missing = [name for name in key_columns if name not in names]
        if missing:
            raise ValueError(
                f"world table {table_name!r} is missing key column(s): " + ", ".join(missing)
            )
        payload = rows.with_columns({"world_id": lit(wid), "run_id": lit(rid)}).select(
            *_WORLD_ENVELOPE_COLUMNS,
            *names,
        )
        if key_columns:
            return await self.append_missing(
                storage_config,
                table_name,
                payload,
                key_columns=(*_WORLD_ENVELOPE_COLUMNS, *key_columns),
            )
        return await self.append_table(storage_config, table_name, payload)

    async def read_world_rows(
        self,
        storage_config: StorageConfig,
        world_id: str,
        table_name: str,
    ) -> DataFrame:
        """Return a lazy app-table read scoped to the durable world/run."""
        wid, rid = await self._durable_world_context(storage_config, world_id)
        rows = await self.read_table(storage_config, table_name)
        rows = rows.where(rows["world_id"] == wid)  # ty: ignore[invalid-argument-type]
        return rows.where(rows["run_id"] == rid)  # ty: ignore[invalid-argument-type]

    async def _durable_world_context(
        self,
        storage_config: StorageConfig,
        world_id: str,
    ) -> tuple[str, str]:
        if storage_config.backend != StorageBackend.ICEBERG:
            raise ValueError("catalog-backed world rows require backend=iceberg")
        wid = str(world_id)
        record = await self.get_control_catalog(storage_config).get_world(wid)
        if record is None:
            raise KeyError(f"world {wid} is not recorded in catalog for {storage_config.uri}")
        if not record.run_id:
            raise RuntimeError(f"world {wid} has no recorded run; world rows need a run key")
        return wid, str(record.run_id)

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
            if key not in self._store_locks:
                self._store_locks[key] = asyncio.Lock()

            async with self._store_locks[key]:
                if key not in self._instances:
                    self._instances[key] = create_async_store(
                        storage_config,
                        self._session,
                        cache_config,
                        execution_gate=self._execution_gate,
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
            store = create_async_store(
                storage_config,
                self._session,
                cache_config,
                execution_gate=self._execution_gate,
            )
            self._instances[key] = store
            self._session_identity = requested
        return store

    async def materialize(self, frame: DataFrame) -> DataFrame:
        """Execute one Archetype-owned lazy Daft plan through the shared lane."""
        self._require_frame(frame)
        async with self._execution_gate.admit():
            return await self._blocking(frame.collect, num_preview_rows=0)

    async def read_table(
        self,
        storage_config: StorageConfig,
        table_name: str,
    ) -> DataFrame:
        """Resolve one existing app-owned table and return its lazy Iceberg read."""
        store = await self._iceberg_store(storage_config)
        async with self._execution_gate.admit():
            catalog, identifier = self._catalog_identity(store, table_name)
            if not catalog.has_table(identifier):
                raise KeyError(f"Iceberg table {table_name!r} does not exist")
            return self._read_table(catalog.get_table(identifier), store.io_config)

    async def append_table(
        self,
        storage_config: StorageConfig,
        table_name: str,
        rows: DataFrame,
    ) -> int:
        """Register, align, materialize, and append with optimistic retry."""
        self._require_frame(rows)
        store = await self._iceberg_store(storage_config)
        frozen: DataFrame | None = None
        rows_written = 0

        for attempt in range(_MAX_COMMIT_ATTEMPTS):
            try:
                async with self._execution_gate.admit():
                    table = self._ensure_table(store, table_name, rows.schema())
                    if attempt:
                        self._native_table(table).refresh()
                    if frozen is None:
                        aligned = self._align_table_schema(table, rows, table_name)
                        frozen = await self._blocking(
                            aligned.collect,
                            num_preview_rows=0,
                        )
                        rows_written = frozen.count_rows()
                    if rows_written:
                        await self._blocking(
                            frozen.write_iceberg,
                            self._native_table(table),
                            mode="append",
                            io_config=store.io_config,
                        )
                    return rows_written
            except CommitStateUnknownException as exc:
                # Ambiguous: the commit may have landed. A definite-conflict
                # retry would risk a double-append; freeze as a typed outcome.
                raise AmbiguousCommitError(table_name, attempt) from exc
            except CommitFailedException:
                if attempt + 1 == _MAX_COMMIT_ATTEMPTS:
                    raise
                await asyncio.sleep(min(0.005 * (2**attempt), 0.1))

        raise AssertionError("unreachable Iceberg append retry state")

    async def append_missing(
        self,
        storage_config: StorageConfig,
        table_name: str,
        rows: DataFrame,
        *,
        key_columns: tuple[str, ...],
    ) -> int:
        """Append rows absent by key within this service's execution authority.

        The producer graph is evaluated once, then null key values and duplicate
        candidate keys are rejected before persistence. After an optimistic
        conflict, the frozen candidate rows are anti-joined against the refreshed
        table before another write. This prevents both same-batch ambiguity and
        the stale-pending retry bug where two writers can publish the same logical
        key.
        """
        self._require_frame(rows)
        if not key_columns:
            raise ValueError("append_missing requires at least one key column")
        store = await self._iceberg_store(storage_config)
        candidates: DataFrame | None = None

        for attempt in range(_MAX_COMMIT_ATTEMPTS):
            try:
                async with self._execution_gate.admit():
                    table = self._ensure_table(store, table_name, rows.schema())
                    if attempt:
                        self._native_table(table).refresh()

                    if candidates is None:
                        aligned = self._align_table_schema(table, rows, table_name)
                        candidates = await self._blocking(
                            aligned.collect,
                            num_preview_rows=0,
                        )
                        candidate_count = candidates.count_rows()
                        null_key_filter = col(key_columns[0]).is_null()
                        for key_column in key_columns[1:]:
                            null_key_filter = null_key_filter | col(key_column).is_null()
                        contains_null_key = await self._blocking(
                            candidates.where(null_key_filter).limit(1).count_rows
                        )
                        if contains_null_key:
                            raise ValueError(
                                f"conditional append for table {table_name!r} contains "
                                f"null key values for {key_columns!r}; key columns must "
                                "be non-null"
                            )
                        distinct_key_count = await self._blocking(
                            candidates.distinct(*key_columns).count_rows
                        )
                        if distinct_key_count != candidate_count:
                            raise ValueError(
                                f"conditional append for table {table_name!r} contains "
                                f"duplicate key values for {key_columns!r}"
                            )
                    existing = self._read_table(table, store.io_config).select(*key_columns)
                    pending = candidates.join(existing, on=list(key_columns), how="anti")
                    pending = await self._blocking(
                        pending.collect,
                        num_preview_rows=0,
                    )
                    rows_written = pending.count_rows()
                    if not rows_written:
                        return 0
                    if candidates is not pending and attempt == 0:
                        # Freeze only rows that were candidates for the first
                        # write. Rows already present cannot disappear from an
                        # append-only table and need not be re-evaluated.
                        candidates = pending
                    await self._blocking(
                        pending.write_iceberg,
                        self._native_table(table),
                        mode="append",
                        io_config=store.io_config,
                    )
                    return rows_written
            except CommitStateUnknownException as exc:
                # Same freeze as append_table: an anti-join retry against a
                # table whose commit state is unknown could double-append.
                raise AmbiguousCommitError(table_name, attempt) from exc
            except CommitFailedException:
                if attempt + 1 == _MAX_COMMIT_ATTEMPTS:
                    raise
                await asyncio.sleep(min(0.005 * (2**attempt), 0.1))

        raise AssertionError("unreachable conditional append retry state")

    async def _iceberg_store(self, storage_config: StorageConfig) -> AsyncStore:
        if storage_config.backend != StorageBackend.ICEBERG:
            raise ValueError("app table storage requires backend=iceberg")
        store = await self.get_or_create_store(storage_config, cache_config=None)
        if not isinstance(store, AsyncStore):
            raise TypeError(f"expected AsyncStore for Iceberg, got {type(store).__name__}")
        return store

    @staticmethod
    def _catalog_identity(store: AsyncStore, table_name: str) -> tuple[Catalog, str]:
        catalog = store.session.current_catalog()
        if catalog is None:
            raise RuntimeError("Daft session has no current catalog")
        namespace = store.session.current_namespace()
        if namespace is None:
            raise RuntimeError("Daft session has no current namespace")
        return catalog, f"{namespace}.{table_name}"

    @classmethod
    def _ensure_table(
        cls,
        store: AsyncStore,
        table_name: str,
        schema,
    ) -> Table:
        catalog, identifier = cls._catalog_identity(store, table_name)
        try:
            return catalog.create_table_if_not_exists(identifier, schema)
        except TableAlreadyExistsError:
            # Daft's helper checks for existence before creating, so a
            # concurrent first-use creator can win between those operations.
            # The winning catalog row is authoritative; resolve it and let the
            # caller's normal schema-alignment check reject any incompatible
            # concurrent definition before writing.
            return catalog.get_table(identifier)

    @staticmethod
    def _read_table(table: Table, io_config: IOConfig | None) -> DataFrame:
        return read_iceberg(StorageService._native_table(table), io_config=io_config)

    @staticmethod
    def _native_table(table: Table):
        native = getattr(table, "_inner", None)
        if native is None:
            raise RuntimeError("Daft table does not expose an Iceberg handle")
        return native

    @staticmethod
    def _align_table_schema(table: Table, rows: DataFrame, table_name: str) -> DataFrame:
        existing = table.schema().to_pyarrow_schema()
        incoming = rows.schema().to_pyarrow_schema()
        existing_shape = {field.name: field.type for field in existing}
        incoming_shape = {field.name: field.type for field in incoming}
        compatible = existing_shape.keys() == incoming_shape.keys() and all(
            StorageService._iceberg_compatible(incoming_shape[name], existing_type)
            for name, existing_type in existing_shape.items()
        )
        if not compatible:
            raise ValueError(
                f"Iceberg table {table_name!r} already has a different typed schema: "
                f"existing={existing_shape!r}, incoming={incoming_shape!r}"
            )
        return rows.select(
            *(
                col(field.name).cast(DataType.from_arrow_type(field.type)).alias(field.name)
                for field in existing
            )
        )

    @staticmethod
    def _iceberg_compatible(incoming: pa.DataType, existing: pa.DataType) -> bool:
        if incoming == existing:
            return True
        if pa.types.is_timestamp(incoming) and pa.types.is_timestamp(existing):
            return incoming.tz == existing.tz
        if pa.types.is_unsigned_integer(incoming) and pa.types.is_signed_integer(existing):
            return incoming.bit_width <= existing.bit_width
        return False

    @staticmethod
    def _require_frame(frame: DataFrame) -> None:
        if not isinstance(frame, DataFrame):
            raise TypeError("rows must be a daft.DataFrame")

    @staticmethod
    async def _blocking(
        function: Callable[..., _T],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> _T:
        return await asyncio.to_thread(function, *args, **kwargs)

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
            self._store_locks.clear()
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
