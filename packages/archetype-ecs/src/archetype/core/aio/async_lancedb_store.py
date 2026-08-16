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

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Protocol

import daft
import lancedb
from daft import DataFrame, lit
from lancedb.index import Bitmap, BTree

from archetype.core.archetype import Archetype
from archetype.core.interfaces import AppendReceipt, ArchetypeSignature, iAsyncStore

logger = logging.getLogger(__name__)

_INDEX_CREATE_ATTEMPTS = 6


class _StorageContextLike(Protocol):
    """Legacy runtime storage context: anything carrying uri + namespace."""

    uri: str
    namespace: str


class AsyncLancedbStore(iAsyncStore):
    def __init__(
        self,
        uri: str | _StorageContextLike,
        namespace: str | None = None,
    ):
        if isinstance(uri, str):
            resolved_uri = uri
        else:
            # Legacy path: unpack a storage-context object.
            resolved_uri = uri.uri
            if namespace is None:
                namespace = uri.namespace

        if namespace is None:
            raise TypeError("AsyncLancedbStore requires a namespace")

        self.uri: str = resolved_uri
        self.namespace: str = namespace
        self.lancedb = None
        self._known_sigs: dict[str, ArchetypeSignature] = {}
        # Tracks only signatures that have been durably committed via append();
        # excludes tables opened/created by get_archetype_df (create-on-read).
        self._committed_sigs: set[str] = set()
        # Cache of opened AsyncTable handles by name. _ensure_table used to
        # re-run list_tables() + open_table() on EVERY append/query; cheap when
        # serial but it dominated concurrent workloads (50 concurrent spawns:
        # ~100s of it). A LanceDB handle always reads the current version, so
        # reusing it is safe — appends and queries see the latest data.
        self._tables: dict[str, Any] = {}
        # Memoized presence of v0.2 legacy tables per legacy table id — frozen
        # history, checked once (see _legacy_df).
        self._legacy_present: dict[str, bool] = {}

    async def _ensure_table(self, sig):
        table_name = Archetype.get_name(sig)

        # Reuse an already-opened handle: this turns a per-op list_tables() +
        # open_table() round-trip into a dict lookup (the concurrent-spawn killer).
        cached = self._tables.get(table_name)
        if cached is not None:
            return cached

        pyarrow_schema = Archetype.get_archetype_schema(sig)

        connection = await self._connect()
        try:
            async_table = await connection.open_table(table_name)
        except (KeyError, ValueError):
            async_table = None
        except Exception as e:
            raise RuntimeError(f"Error opening LanceDB table {table_name}: {e}") from e
        if async_table is not None:
            self._known_sigs[table_name] = sig
            self._tables[table_name] = async_table
            return async_table

        try:
            async_table = await connection.create_table(
                name=table_name,
                schema=pyarrow_schema,
                exist_ok=True,
            )
            indexes = (
                ("ARCT_LANCEDB_INDEX_ENTITY", "entity_id", BTree()),
                ("ARCT_LANCEDB_INDEX_WORLD", "world_id", Bitmap()),
                ("ARCT_LANCEDB_INDEX_RUN", "run_id", Bitmap()),
                ("ARCT_LANCEDB_INDEX_TICK", "tick", BTree()),
            )
            for setting, column, config in indexes:
                if os.environ.get(setting, "1") == "1":
                    async_table = await self._create_index(
                        connection, async_table, table_name, column, config
                    )
        except Exception as e:
            logger.error(f"Error creating LanceDB table {table_name}: {e}")
            raise RuntimeError(f"Error creating LanceDB table {table_name}: {e}") from e

        self._known_sigs[table_name] = sig
        self._tables[table_name] = async_table
        return async_table

    @staticmethod
    async def _create_index(connection, table, table_name: str, column: str, config):
        """Retry index transaction conflicts against a freshly opened handle."""
        for attempt in range(_INDEX_CREATE_ATTEMPTS):
            try:
                await table.create_index(column=column, config=config, replace=True)
                return table
            except RuntimeError:
                if attempt + 1 == _INDEX_CREATE_ATTEMPTS:
                    raise
                delay = min(0.01 * (2**attempt), 0.2) + (os.getpid() % 17) * 0.001
                await asyncio.sleep(delay)
                table = await connection.open_table(table_name)
        raise AssertionError("unreachable")

    async def _connect(self):
        """Open the database connection without creating any user table.

        Connecting may create the database *directory*; it never creates or
        registers an archetype table, so read paths stay create-free.
        """
        if self.lancedb is None:
            subdir = os.environ.get("ARCT_LANCEDB_SUBDIR", "lance")
            self.lancedb = await lancedb.connect_async(
                os.path.join(self.uri, self.namespace, subdir)
            )
        return self.lancedb

    # ── Durable-discovery seam (issue #272) ─────────────────────────────────
    # Salvaged from the A1 draft branch: open-never-create reads by persisted
    # physical table identity. Deliberately does not touch _known_sigs /
    # _committed_sigs — those stay process-local caches, never discovery
    # authority — and does not populate the mutable _tables handle cache.

    async def _open_existing_table(self, table_id: str):
        db = await self._connect()
        if table_id not in await self._list_table_names():
            raise KeyError(f"LanceDB table {table_id!r} does not exist")
        try:
            return await db.open_table(table_id)
        except Exception as exc:
            raise RuntimeError(f"Error opening LanceDB table {table_id}: {exc}") from exc

    async def get_existing_table_schema(self, table_id: str):
        """Return an existing table's Arrow schema without creating it."""
        table = await self._open_existing_table(table_id)
        return await table.schema()

    async def get_existing_table_df(
        self,
        table_id: str,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        active_only: bool = False,
    ) -> DataFrame:
        """Read an existing table by durable physical identity.

        Unlike ``get_archetype_df``, this never calls ``_ensure_table`` and
        therefore cannot turn a read into a committed-looking table.
        """
        table = await self._open_existing_table(table_id)
        safe_world = str(world_id).replace("'", "''")
        safe_run = str(run_id).replace("'", "''")
        clauses = [f"world_id = '{safe_world}'", f"run_id = '{safe_run}'"]
        if active_only:
            clauses.append("is_active = true")
        if ticks is not None:
            tick_list = ", ".join(str(int(t)) for t in ticks)
            clauses.append(f"tick IN ({tick_list})" if ticks else "false")
        if entity_ids is not None:
            id_list = ", ".join(str(int(eid)) for eid in entity_ids)
            clauses.append(f"entity_id IN ({id_list})" if entity_ids else "false")
        where_str = " AND ".join(clauses)
        try:
            arrow = await table.query().where(where_str).to_arrow()
        except Exception as exc:
            logger.error("Error reading existing table %s: %s", table_id, exc)
            raise
        return daft.from_arrow(arrow)

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

    @staticmethod
    def _where(
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None,
        entity_ids: list[int] | None,
        active_only: bool,
        commit_tokens: list[str] | None = None,
    ) -> str:
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

        if commit_tokens is not None:
            if commit_tokens:
                token_list = ", ".join("'{}'".format(t.replace("'", "''")) for t in commit_tokens)
                clauses.append(f"commit_token IN ({token_list})")
            else:
                # An empty allowlist means no current-generation row is visible.
                clauses.append("false")

        return " AND ".join(clauses)

    async def get_archetype_df(
        self,
        sig,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        active_only: bool = False,
        commit_tokens: list[str] | None = None,
    ) -> DataFrame:
        """Read one archetype's rows.

        `commit_tokens` is the reader-side visibility allowlist:
        current-generation rows must match a published manifest token. Legacy
        (v0.2) rows carry no commit identity and are implicitly epoch-0
        visible — the allowlist never applies to them.
        """
        table_name = Archetype.get_name(sig)
        async_table = await self._ensure_table(sig)

        try:
            where_str = self._where(
                world_id,
                run_id,
                ticks=ticks,
                entity_ids=entity_ids,
                active_only=active_only,
                commit_tokens=commit_tokens,
            )
            filtered_arrow = await async_table.query().where(where_str).to_arrow()
            df = daft.from_arrow(filtered_arrow)

        except Exception as e:
            logger.error(f"Error reading archetype table {table_name}: {e}")
            raise e

        legacy_df = await self._legacy_df(
            sig, world_id, run_id, ticks=ticks, entity_ids=entity_ids, active_only=active_only
        )
        if legacy_df is not None:
            df = df.concat(legacy_df)

        return df

    async def _legacy_df(
        self,
        sig,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None,
        entity_ids: list[int] | None,
        active_only: bool,
    ) -> DataFrame | None:
        """Rows persisted by v0.2 under the legacy table id, if any.

        Presence is memoized per legacy table id: legacy tables are frozen
        history (nothing writes them after #273), so one existence check
        suffices. Rows are stamped with the epoch-0 identity and aligned to
        the current schema so callers see one uniform shape.
        """
        legacy_name = Archetype.get_legacy_name(sig)
        known = self._legacy_present.get(legacy_name)
        if known is None:
            await self._connect()
            known = legacy_name in await self._list_table_names()
            self._legacy_present[legacy_name] = known
        if not known:
            return None

        table = await self._open_existing_table(legacy_name)
        where_str = self._where(
            world_id, run_id, ticks=ticks, entity_ids=entity_ids, active_only=active_only
        )
        arrow = await table.query().where(where_str).to_arrow()
        df = daft.from_arrow(arrow).with_columns(
            {
                "commit_token": lit(""),
                "writer_epoch": lit(0).cast(daft.DataType.int64()),
            }
        )
        return df.select(*Archetype.get_archetype_schema(sig).names)

    async def list_signatures(self) -> list[ArchetypeSignature]:
        return list(self._known_sigs.values())

    async def list_committed_signatures(self) -> list[ArchetypeSignature]:
        """List signatures backed by a successful durable append."""
        return [sig for key, sig in self._known_sigs.items() if key in self._committed_sigs]

    async def append(self, sig, df: DataFrame) -> AppendReceipt:
        # ONE materialization: to_arrow() is the conversion the backend add()
        # requires, and the receipt's row count is metadata that conversion
        # already produced. Core never asks whether the frame is empty before
        # the write (issue #538) — a zero-row add() is LanceDB's own
        # successful no-op. A separate df.collect() + df.count_rows() re-ran
        # the Daft plan twice for nothing; this redundancy is a recurring
        # rewrite regression (perf-fixed before in the predecessor stores).
        table_id = Archetype.get_name(sig)
        if not df.column_names:
            # Schema availability, not row cardinality: a schemaless frame
            # cannot even name a backing table.
            logger.info(f"Append skipped (lancedb): archetype={table_id} empty schema")
            return AppendReceipt(table_id=table_id, rows=0, durable=True)
        arrow_table = df.to_arrow()

        async_table = await self._ensure_table(sig)
        result = await async_table.add(arrow_table, mode="append")
        self._committed_sigs.add(table_id)
        # Backend version is an auxiliary diagnostic, never the visibility mechanism.
        version = getattr(result, "version", None)
        return AppendReceipt(
            table_id=table_id,
            rows=arrow_table.num_rows,
            durable=True,
            backend_ref=str(version) if version is not None else None,
        )

    async def flush(self) -> None:
        """No-op: appends write through to LanceDB."""

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
