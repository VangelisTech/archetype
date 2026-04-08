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
from archetype.core.runtime.storage import StorageContext

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

    def __init__(self, context_or_config):
        """Initialize with a StorageContext (preferred) or a legacy config with get_session()."""
        if hasattr(context_or_config, "get_session"):
            cfg = context_or_config
            self.uri = cfg.uri
            self.namespace = cfg.namespace
            self.session = cfg.get_session()
            self.io_config = getattr(cfg, "io_config", None)
        else:
            context: StorageContext = context_or_config
            self.uri = context.uri
            self.namespace = context.namespace
            self.session = context.session
            self.io_config = context.io_config
        self.storage_options = (
            io_config_to_storage_options(self.io_config) if self.io_config else None
        )
        self.lancedb = None  # Initialize lancedb connection

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

        if table_name in await self.lancedb.table_names():
            try:
                async_table = await self.lancedb.open_table(table_name)
            except Exception as e:
                raise RuntimeError(f"Error opening LanceDB table {table_name}: {e}") from e

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

        return async_table

    # ---------------------------------------------------------------------
    # Querying
    # ---------------------------------------------------------------------
    async def get_archetype_df(
        self, sig: ArchetypeSignature, world_id: str, run_id: str
    ) -> DataFrame:
        table_name = Archetype.get_name(sig)
        async_table = await self._ensure_table(sig)

        try:
            # Query LanceDB directly; new/empty tables will simply yield empty results
            filtered_arrow = await (
                async_table.query()
                .where(
                    f"world_id = '{str(world_id)}' AND run_id = '{str(run_id)}' AND is_active = true"
                )
                .to_arrow()
            )

            # Convert to Daft DataFrame
            df = daft.from_arrow(filtered_arrow)

        except Exception as e:
            logger.error(f"Error reading archetype table {table_name}: {e}")
            raise e

        return df

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> None:
        """
        Append a table with a new dataframe.
        """
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
        for table_name in await self.lancedb.table_names():
            try:
                async_table = await self.lancedb.open_table(table_name)
                await async_table.optimize(retrain=False)
            except Exception as e:
                raise RuntimeError(f"Error optimizing table {table_name}: {e}") from e


class AsyncLancedbQueryManager(AsyncQueryManager):
    def __init__(self, store: "AsyncLancedbStore"):
        super().__init__(store)
