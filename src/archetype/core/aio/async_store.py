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
from typing import Dict, Tuple, List, Optional, Any
from logging import getLogger

# Technologies
import daft
from daft import col, DataFrame, Schema
from daft.expressions import lit
from daft.session import Session
from daft.catalog import Table
import time

# Internals
from archetype.core.interfaces import Archetype, ArchetypeSignature, 
from archetype.core.aio.interfaces import iAsyncStore

logger = getLogger(__name__)

# Store classes
class AsyncStore(iAsyncStore):
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
            table = self.sess.create_table_if_not_exists(hash_val, source=daft_schema, )
        except Exception as e:
            raise Exception(f"Error creating table {hash_val}: {e}")

        return table

    async def get_archetype_df(self, sig: ArchetypeSignature) -> DataFrame:
        """
        Get all archetypes.
        """
        table: Table = self._ensure_table(sig)
        df: DataFrame = await table.read() # Cheap, Lazy
        
        if self.debug:
            debug_df = df.limit(10)
            debug_df.collect()
            debug_df.show(10)

        return df


    async def append(self, sig: ArchetypeSignature, df: DataFrame, tick: int, world_id: str, run_id: str) -> None:
        """
        Append a table with a new dataframe.
        """
        table_name = Archetype.get_name(sig)
        table = self.sess.get_table(table_name)
        try:
            if self.debug:
                df.collect()
                df.show()
                print(f"Appending {df.count_rows()} rows to table {table_name} for tick {tick}")
            
            await table.append(df)
        except Exception as e:
            raise 

