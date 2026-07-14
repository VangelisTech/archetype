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

import logging
import os
from typing import Any, Protocol

import daft
import lancedb
from daft import DataFrame
from lancedb.index import Bitmap, BTree

from archetype.core.archetype import Archetype
from archetype.core.interfaces import ArchetypeSignature, iAsyncStore

logger = logging.getLogger(__name__)


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

    async def _ensure_table(self, sig):
        table_name = Archetype.get_name(sig)

        # Reuse an already-opened handle: this turns a per-op list_tables() +
        # open_table() round-trip into a dict lookup (the concurrent-spawn killer).
        cached = self._tables.get(table_name)
        if cached is not None:
            return cached

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
            self._tables[table_name] = async_table
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
        self._tables[table_name] = async_table
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

    async def list_signatures(self) -> list[ArchetypeSignature]:
        return list(self._known_sigs.values())

    async def list_committed_signatures(self) -> list[ArchetypeSignature]:
        """List only signatures that have been durably committed via append().

        Excludes signatures that were only auto-created by get_archetype_df
        (create-on-read).
        """
        return [sig for key, sig in self._known_sigs.items() if key in self._committed_sigs]

    async def append(self, sig, df: DataFrame) -> None:
        # ONE materialization. Lance rejects zero-row / schemaless frames, so the
        # empty guard stays — but the row count comes free from the Arrow table we
        # must build anyway. A separate df.collect() + df.count_rows() re-ran the
        # Daft plan twice for nothing; this redundancy is a recurring rewrite
        # regression (perf-fixed before in the predecessor stores) — keep it lean.
        if not df.column_names:
            logger.info(
                f"Append skipped (lancedb): archetype={Archetype.get_name(sig)} empty schema"
            )
            return
        arrow_table = df.to_arrow()
        if arrow_table.num_rows == 0:
            logger.info(f"Append skipped (lancedb): archetype={Archetype.get_name(sig)} rows=0")
            return

        async_table = await self._ensure_table(sig)
        # Record this sig as durably committed for list_committed_signatures().
        self._committed_sigs.add(Archetype.get_name(sig))
        # No local try/except: AsyncUpdateManager.update already logs + re-raises
        # (the "loud failures" contract), so wrapping here would only double-log.
        await async_table.add(arrow_table, mode="append")

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
