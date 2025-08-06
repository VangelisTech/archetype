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

# Standard Python Libraries
from logging import getLogger


# Technologies
from daft import  DataFrame, Schema
from daft.session import Session
from daft.catalog import Table
import pyarrow as pa


# Internals
from archetype.core import ArchetypeSignature, Archetype
from archetype.core.aio.async_interfaces import iAsyncStore

# Logger
logger = getLogger(__name__)




class AsyncStore(iAsyncStore):
    """
    The ArchetypeStore is a component that manages the storage and retrieval of archetype tables.

    Since our Schema supports multiple simulations and runs, our namespace is simply "archetypes".
    This allows us to run multiple simulations across many worlds using the same catalog.archetypes, by definition,
    the exact set of components attached to an entity. So we don't really which simulation or run we're using,
    so long as we differentiate between the simulation and run.

    Using Daft Sessions/Catalogs enables us to reference archetype tables without having to hold them in memory. 

    """
    def __init__(self,
        uri: str,
        session: Session,
        debug: bool = False
    ):
        self.uri = uri
        self.debug = debug
        self.sess = session

    def _ensure_table(self, sig: ArchetypeSignature) -> Table:
        """
        Ensure that the table for the given archetype signature exists in the Daft session.
        Returns the table name (hash_val).
        """
        hash_val = Archetype.get_name(sig)
        pyarrow_schema = Archetype.get_archetype_schema(sig)
        daft_schema = Schema.from_pyarrow_schema(pyarrow_schema)
        try:
            table = self.sess.create_table_if_not_exists(hash_val, source=daft_schema)
        except Exception as e:
            raise Exception(f"Error creating table {hash_val}: {e}")

        return table

    async def get_archetype_df(self, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types.
        """
        table: Table = self._ensure_table(sig)
        df: DataFrame = await table.read() # Cheap, Lazy

        if self.debug: 
            print(f'Reading {table.name}')
            df_debug = df.limit(5).show()

        return df.where(df["world_id"] == world_id) \
               .where(df["run_id"] == run_id) 


    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> None:
        """
        Append a table with a new dataframe.
        """
        table_name = Archetype.get_name(sig)
        table = self.sess.get_table(table_name)

        if self.debug:
            df.collect()
            print(f"Appending {df.count_rows()} rows to table {table_name}")
            df.show()
            
        await table.append(df)

    async def shutdown(self) -> None:
        """
        Shutdown the store.
        """
        pass # Daft handles this automatically



