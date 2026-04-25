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

import os
import time
from logging import getLogger

import daft
import lancedb
from daft import DataFrame
from daft.io.object_store_options import io_config_to_storage_options
from lancedb.index import Bitmap, BTree

from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.archetype import Archetype
from archetype.core.interfaces import ArchetypeSignature, iAsyncStore
from archetype.core.storage.handles import LanceDbStorage

logger = getLogger(__name__)


class AsyncLancedbStore(iAsyncStore):
    """
    The ArchetypeStore is a component that manages the storage and retrieval of archetype tables.

    Since our Schema supports multiple simulations and runs, our namespace is simply "archetypes".
    This allows us to have multiple simulations and runs in the same catalog since archetypes, by definition,
    the exact set of components attached to an entity. So we don't really which simulation or run we're using,
    so long as we differentiate between the simulation and run.

    Using Daft Sessions/Catalogs enables us to reference arbitrary
    numbers of archetype tables without having to hold them in memory, provided we

    """

    def __init__(self, storage: LanceDbStorage):
        """Initialize LanceDB storage from backend-specific connection inputs."""
        self.uri = storage.uri
        self.namespace = storage.namespace
        self.io_config = storage.io_config
        self.storage_options = (
            io_config_to_storage_options(self.io_config) if self.io_config else None
        )
        self.lancedb = None  # Initialize lancedb connection
        self._known_sigs: dict[str, ArchetypeSignature] = {}

    # --------------------------------------------------------------------------
    # Helper methods
    # --------------------------------------------------------------------------
    async def _ensure_table(self, sig: ArchetypeSignature) -> lancedb.AsyncTable:
        """
        Ensure that the table for the given archetype signature exists in the Daft session.
        Returns the table name (hash_val).
        """
        table_name = Archetype.get_name(sig)
        pyarrow_schema = Archetype.get_archetype_schema(sig)

        if self.lancedb is None:
            # Allow override via ARCT_LANCEDB_SUBDIR; default to "lance"
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

        else:
            try:
                async_table = await self.lancedb.create_table(
                    name=table_name,
                    schema=pyarrow_schema,
                    storage_options=io_config_to_storage_options(self.io_config)
                    if self.io_config
                    else None,
                    exist_ok=True,
                )
                # Optional eager index creation controlled by env flags (default on)
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
        """List table names using the modern LanceDB API when available."""
        if self.lancedb is None:
            return []

        list_tables = getattr(self.lancedb, "list_tables", None)
        if list_tables is not None:
            response = await list_tables()
            if hasattr(response, "tables"):
                return list(response.tables)
            return list(response)

        return list(await self.lancedb.table_names())

    # ---------------------------------------------------------------------
    # Querying
    # ---------------------------------------------------------------------
    async def get_archetype_df(
        self,
        sig: ArchetypeSignature,
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
            # Build filter predicate with pushdown for LanceDB indexes
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

            # Convert to Daft DataFrame
            df = daft.from_arrow(filtered_arrow)

        except Exception as e:
            logger.error(f"Error reading archetype table {table_name}: {e}")
            raise e

        return df

    async def list_signatures(self) -> list[ArchetypeSignature]:
        """
        List all archetype signatures that have been registered via _ensure_table.
        """
        return list(self._known_sigs.values())

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> None:
        """
        Append a table with a new dataframe.
        """
        # Defensive no-op for empty frames to avoid Lance casting errors
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

            # Convert to arrow and add to the table
            await async_table.add(arrow_table, mode="append")
            end_time = time.time()
            logger.info(
                f"Appended dataframe to table {table_name} in {end_time - start_time} seconds"
            )
        except Exception as e:
            logger.error(f"Error appending dataframe to table {table_name}: {e}")
            raise

    async def shutdown(self) -> None:
        """
        Shutdown the store.
        """
        if self.lancedb is not None:
            try:
                close = getattr(self.lancedb, "close", None)
                if close:
                    result = close()
                    if hasattr(result, "__await__"):
                        await result  # ensure async close is awaited if applicable
            finally:
                self.lancedb = None

    async def optimize_tables(self) -> None:
        """
        Optimize all tables in the LanceDB catalog.
        """
        if self.lancedb is None:
            return

        for table_name in await self._list_table_names():
            try:
                async_table = await self.lancedb.open_table(table_name)
                await async_table.optimize(retrain=False)
            except Exception as e:
                raise RuntimeError(f"Error optimizing table {table_name}: {e}") from e


class AsyncLancedbQueryManager(AsyncQueryManager):
    def __init__(self, store: "AsyncLancedbStore"):
        super().__init__(store)
