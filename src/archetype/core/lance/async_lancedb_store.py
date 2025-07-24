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

from typing import Optional, List, Dict, Tuple, Any

import time
import os

import daft
from daft import DataFrame, col, lit
from daft.session import Session
from daft.io import IOConfig
from daft.io.object_store_options import io_config_to_storage_options

import lancedb
from lancedb.index import Bitmap, BTree

from archetype.core.aio.async_interfaces import iAsyncStore
from archetype.core.interfaces import Archetype, ArchetypeSignature

from logging import getLogger

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
    def __init__(self,
        uri: str,
        namespace: str,
        session: Session,
        io_config: Optional[IOConfig] = None,
        debug: bool = False,
    ):
        self.uri = uri
        self.namespace = namespace 
        self.debug = debug

        self.io_config = io_config
        self.storage_options = io_config_to_storage_options(self.io_config) if self.io_config else None
        self.lancedb = None  # Initialize lancedb connection

    #--------------------------------------------------------------------------
    # Helper methods
    #--------------------------------------------------------------------------
    async def _ensure_table(self, sig: ArchetypeSignature) -> lancedb.AsyncTable:
        """
        Ensure that the table for the given archetype signature exists in the Daft session.
        Returns the table name (hash_val).
        """
        table_name = Archetype.get_name(sig)
        pyarrow_schema = Archetype.get_archetype_schema(sig)

        if self.lancedb is None:
            self.lancedb = await lancedb.connect_async(os.path.join(self.uri, self.namespace + "lance"))

        if table_name in await self.lancedb.table_names():
            try:
                async_table = await self.lancedb.open_table(table_name)
            except Exception as e:
                raise Exception(f"Error opening LanceDB table {table_name}: {e}")

            return async_table

        else:
            try:
                async_table = await self.lancedb.create_table(
                    name=table_name,
                    schema=pyarrow_schema,
                    storage_options= io_config_to_storage_options(self.io_config) if self.io_config else None,
                    exist_ok=True
                )
                await async_table.create_index(column="entity_id", config= BTree(), replace=True)
                await async_table.create_index(column="world_id", config= Bitmap(), replace=True)
                await async_table.create_index(column="run_id", config= Bitmap(), replace=True)
                await async_table.create_index(column="tick", config= BTree(), replace=True)
            except Exception as e:
                logger.error(f"Error creating LanceDB table {table_name}: {e}")
                raise Exception(f"Error creating LanceDB table {table_name}: {e}")

        return async_table



    # ---------------------------------------------------------------------
    # Querying
    # ---------------------------------------------------------------------
    async def get_archetype_df(self, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame:

        table_name = Archetype.get_name(sig)
        async_table = await self._ensure_table(sig)

        try:
    
            lance_uri = os.path.join(self.uri, self.namespace + "lance", table_name + ".lance")
            if os.path.exists(lance_uri):
                # Query with LanceDB directly
                filtered_arrow = await async_table.query().where(
                    f"world_id = '{world_id}' AND run_id = '{run_id}' AND tick = {tick} AND is_active = true"
                ).to_arrow()

                # Convert to Daft DataFrame
                df = daft.from_arrow(filtered_arrow)
            else:
                # Return empty dataframe with correct schema if table doesn't exist yet
                schema = Archetype.get_archetype_schema(sig)
                df = daft.from_arrow(schema.empty_table())

        except Exception as e:
            logger.error(f"Error reading archetype table {table_name}: {e}")
            raise e

        return df

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> None:
        """
        Append a table with a new dataframe.
        """

        async_table = await self.ensure_table(sig)
        table_name = async_table.name
        try:
            start_time = time.time()
            arrow_table = df.to_arrow()

            # Convert to arrow and add to the table
            await async_table.add(, mode="append")
            end_time = time.time()
            logger.info(f"Appended dataframe to table {table_name} for tick {tick} in {end_time - start_time} seconds")
        except Exception as e:
            logger.error(f"Error appending dataframe to table {table_name} for tick {tick}: {e}")
            raise

        if self.debug:
            logger.info(f"Appending dataframe to table {table_name}")
            df_with_tick.show()
            print(f"Appending {df_with_tick.count_rows()} rows to table {table_name} for tick {tick}")

    async def optimize_tables(self) -> None:
        """
        Optimize all tables in the LanceDB catalog.
        """
        for table_name in await self.lancedb.table_names():
            try:
                async_table = await self.lancedb.open_table(table_name)
                async_table.optimize(retrain=False)
            except Exception as e:
                raise Exception(f"Error optimizing table {table_name}: {e}")
