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

from typing import List

import daft
from daft import DataFrame
from lancedb import AsyncTable
from archetype.core.interfaces import ArchetypeSignature
from archetype.core.aio.async_interfaces import iAsyncQueryManager
from archetype.core.lance.async_lancedb_store import AsyncLancedbStore

class AsyncLancedbQueryManager(iAsyncQueryManager):
    def __init__(self, store: "AsyncLancedbStore", debug: bool = False):
        self._store = store 
        self._debug = debug
    
    async def get_archetype(self, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types.
        """
        df = await self._store.get_archetype_df(sig,tick,world_id, run_id)
        
        df = df.where(df["world_id"] == world_id) \
               .where(df["run_id"] == run_id) \
               .where(df["tick"] == max(0, tick)) \
               .where(df["is_active"])

        return df
    
    async def get_archetype_for_entity(self, entity_id: int, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame:
        """Get a component for an entity."""
        df = await self.get_archetype(sig, tick, world_id=world_id, run_id=run_id)

        return df.where(df["entity_id"] == entity_id)
    
    async def get_component(self, component_type: type, sig: ArchetypeSignature,  tick: int, world_id: str, run_id: str) -> DataFrame:
        """
        Get component state across all archetypes. PERFORMS JOIN. 
        """
        df = await self.get_archetype(sig, tick, world_id, run_id)

        df = df.select()

        return df
    
    async def fts_search(self, query: str, fts_columns: List[str], sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame:
        """
        Search for entities in the LanceDB store.
        """
        raise NotImplementedError("FTS search is not implemented yet")
        #table: AsyncTable = await self._store._ensure_table(sig)

        #df = daft.from_arrow(table.search(query, query_type="fts", fts_columns=fts_columns).to_arrow())

        #return df
    

    

